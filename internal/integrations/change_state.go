package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ChangeSection carries a section and its observation time for coalescing.
type ChangeSection struct {
	At    time.Time
	Value any
}
type ChangeBatch struct {
	Repo     string `json:"repo"`
	PR       int    `json:"pr"`
	State    any    `json:"state,omitempty"`
	Checks   any    `json:"checks,omitempty"`
	Threads  any    `json:"threads,omitempty"`
	Findings any    `json:"findings,omitempty"`
	Gate     any    `json:"gate,omitempty"`
}
type changeKey struct {
	repo string
	pr   int
}
type changePending struct {
	sections map[string]ChangeSection
	findings []ChangeSection
}
type ChangeCounts struct{ Writes, Stale, Rejected, Unavailable uint64 }

// ChangeWriter sends observations without holding the caller on hk. It folds
// bursts by PR and section, retaining the newest observation in each section.
type ChangeWriter struct {
	mu                                   sync.Mutex
	sendMu                               sync.Mutex
	pending                              map[changeKey]*changePending
	timer                                *time.Timer
	active                               sync.WaitGroup
	endpoint                             func() (*hkJobs, error)
	log                                  *slog.Logger
	unavailableUntil                     time.Time
	writes, stale, rejected, unavailable atomic.Uint64
}

func NewChangeWriter(log *slog.Logger) *ChangeWriter {
	if log == nil {
		log = slog.Default()
	}
	return &ChangeWriter{endpoint: hkJobEndpoint, log: log}
}
func (w *ChangeWriter) Counts() ChangeCounts {
	if w == nil {
		return ChangeCounts{}
	}
	return ChangeCounts{w.writes.Load(), w.stale.Load(), w.rejected.Load(), w.unavailable.Load()}
}
func (w *ChangeWriter) Queue(repo string, pr int, name string, s ChangeSection) {
	if w == nil || repo == "" || pr < 1 || s.Value == nil || s.At.IsZero() {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if time.Now().Before(w.unavailableUntil) {
		return
	}
	if w.pending == nil {
		w.pending = map[changeKey]*changePending{}
	}
	k := changeKey{repo, pr}
	p := w.pending[k]
	if p == nil {
		p = &changePending{sections: map[string]ChangeSection{}}
		w.pending[k] = p
	}
	if name == "findings" {
		p.findings = append(p.findings, s)
	} else if old, ok := p.sections[name]; !ok || !s.At.Before(old.At) {
		p.sections[name] = s
	}
	if w.timer == nil {
		w.active.Add(1)
		w.timer = time.AfterFunc(150*time.Millisecond, w.flush)
	}
}
func (w *ChangeWriter) flush() {
	defer w.active.Done()
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	w.mu.Lock()
	pending := w.pending
	w.pending = nil
	w.timer = nil
	w.mu.Unlock()
	for k, p := range pending {
		b := ChangeBatch{Repo: k.repo, PR: k.pr}
		for name, s := range p.sections {
			switch name {
			case "state":
				b.State = s.Value
			case "checks":
				b.Checks = s.Value
			case "threads":
				b.Threads = s.Value
			case "gate":
				b.Gate = s.Value
			}
		}
		if len(p.findings) > 0 {
			b.Findings = p.findings[0].Value
		}
		w.send(b)
		for i := 1; i < len(p.findings); i++ {
			w.send(ChangeBatch{Repo: k.repo, PR: k.pr, Findings: p.findings[i].Value})
		}
	}
}

// Wait drains queued writes, bounded by the caller's deadline.
func (w *ChangeWriter) Wait(ctx context.Context) {
	if w == nil {
		return
	}
	done := make(chan struct{})
	go func() { w.active.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

type changeResult struct {
	Reason   string `json:"reason"`
	Sections map[string]struct {
		Applied bool   `json:"applied"`
		Reason  string `json:"reason"`
	} `json:"sections"`
}

func (w *ChangeWriter) request(c *hkJobs, method, path string, body any) (int, changeResult) {
	var result changeResult
	data, e := json.Marshal(body)
	if e != nil {
		return 0, result
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(data))
	if e != nil {
		return 0, result
	}
	c.authorize(req)
	req.Header.Set("Content-Type", "application/json")
	resp, e := c.client(2 * time.Second).Do(req)
	if e != nil {
		return 0, result
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
	return resp.StatusCode, result
}
func (w *ChangeWriter) count(r changeResult) {
	for _, s := range r.Sections {
		if s.Applied {
			w.writes.Add(1)
		} else if s.Reason == "stale" {
			w.stale.Add(1)
		}
	}
}
func (w *ChangeWriter) send(b ChangeBatch) {
	// hk accepts at most 2 MiB per request. Split a large batch without
	// splitting a section, since complete sets and finding ordinals are atomic.
	if raw, err := json.Marshal(b); err != nil {
		return
	} else if len(raw) > 2<<20 {
		parts := []ChangeBatch{{Repo: b.Repo, PR: b.PR, State: b.State}, {Repo: b.Repo, PR: b.PR, Checks: b.Checks}, {Repo: b.Repo, PR: b.PR, Threads: b.Threads}, {Repo: b.Repo, PR: b.PR, Findings: b.Findings}, {Repo: b.Repo, PR: b.PR, Gate: b.Gate}}
		for _, part := range parts {
			if part.State == nil && part.Checks == nil && part.Threads == nil && part.Findings == nil && part.Gate == nil {
				continue
			}
			encoded, e := json.Marshal(part)
			if e == nil && len(encoded) <= 2<<20 {
				w.send(part)
			} else {
				w.rejected.Add(1)
				w.log.Warn("hk change section exceeds request limit")
			}
		}
		return
	}
	w.mu.Lock()
	blocked := time.Now().Before(w.unavailableUntil)
	w.mu.Unlock()
	if blocked {
		return
	}
	c, e := w.endpoint()
	if e != nil {
		return
	}
	status, result := w.request(c, http.MethodPost, "/v1/changes/batch", b)
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
		if !w.sections(c, b, true) {
			w.mu.Lock()
			if !time.Now().Before(w.unavailableUntil) {
				w.unavailableUntil = time.Now().Add(10 * time.Minute)
				w.unavailable.Add(1)
				w.log.Warn("hk change state unavailable")
			}
			w.mu.Unlock()
		}
		return
	}
	if status == http.StatusBadRequest {
		w.log.Warn("hk change batch rejected", "reason", errorMessage(result.Reason))
		w.sections(c, b, false)
		return
	}
	if status >= 200 && status < 300 {
		w.count(result)
	}
}
func (w *ChangeWriter) sections(c *hkJobs, b ChangeBatch, probe bool) bool {
	sections := []struct {
		name  string
		value any
	}{{"state", b.State}, {"checks", b.Checks}, {"threads", b.Threads}, {"findings", b.Findings}, {"gate", b.Gate}}
	supported := false
	for _, s := range sections {
		if s.value == nil {
			continue
		}
		raw, e := json.Marshal(s.value)
		if e != nil {
			continue
		}
		var body map[string]any
		if json.Unmarshal(raw, &body) != nil {
			continue
		}
		body["repo"], body["pr"] = b.Repo, b.PR
		method, path := http.MethodPut, "/v1/changes/"+s.name
		var payload any = body
		if !probe {
			method, path = http.MethodPost, "/v1/changes/batch"
			payload = map[string]any{"repo": b.Repo, "pr": b.PR, s.name: s.value}
		}
		status, result := w.request(c, method, path, payload)
		if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
			if probe && !supported {
				return false
			}
			continue
		}
		supported = true
		if status == http.StatusBadRequest {
			w.rejected.Add(1)
			w.log.Warn("hk change section rejected", "section", s.name, "reason", errorMessage(result.Reason))
		} else if status >= 200 && status < 300 {
			w.count(result)
		}
	}
	return supported
}

// Async registers observation work so one-shot commands can drain it on exit.
func (w *ChangeWriter) Async(fn func()) {
	if w == nil {
		return
	}
	w.active.Add(1)
	go func() { defer w.active.Done(); fn() }()
}
