package integrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/mergewake/internal/changestate"
)

// ChangeSection carries a section and its observation time for coalescing.
type ChangeSection = changestate.Section
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
	files    []string
}
type ChangeCounts = changestate.Counts

// ChangeWriter sends observations without holding the caller on hk. It folds
// bursts by PR and section, retaining the newest observation in each section.
type ChangeWriter struct {
	mu      sync.Mutex
	sendMu  sync.Mutex
	pending map[changeKey]*changePending
	// sent holds, per change and section, the sectionKey of the value last
	// written for it in this process; sendMu guards it.
	sent                                 map[changeKey]map[string]string
	outbox                               string
	seen                                 map[string]bool
	retryTimer                           *time.Timer
	timer                                *time.Timer
	active                               sync.WaitGroup
	endpoint                             func() (*hkJobs, error)
	log                                  *slog.Logger
	unavailableUntil                     time.Time
	claims                               map[string]*os.File
	writes, stale, rejected, unavailable atomic.Uint64
	unchanged                            atomic.Uint64
	// stale holds the sections hk rejected as stale in the flush under way;
	// sendMu guards it.
	staleNow map[string]bool
}

func NewChangeWriter(log *slog.Logger, stateDir ...string) *ChangeWriter {
	if log == nil {
		log = slog.Default()
	}
	w := &ChangeWriter{endpoint: hkJobEndpoint, log: log}
	if len(stateDir) > 0 && stateDir[0] != "" {
		w.outbox = filepath.Join(stateDir[0], "hk-change-outbox")
		w.reload()
	}
	return w
}
func (w *ChangeWriter) Counts() ChangeCounts {
	if w == nil {
		return ChangeCounts{}
	}
	return ChangeCounts{Writes: w.writes.Load(), Stale: w.stale.Load(), Rejected: w.rejected.Load(), Unavailable: w.unavailable.Load(), Unchanged: w.unchanged.Load()}
}
func (w *ChangeWriter) Queue(repo string, pr int, name string, s ChangeSection) {
	if w == nil || repo == "" || pr < 1 || s.Value == nil || s.At.IsZero() {
		return
	}
	raw, err := json.Marshal(s.Value)
	if err != nil {
		w.log.Warn("hk change observation invalid")
		return
	}
	s.Value = json.RawMessage(raw)
	file, claim, err := w.persist(repo, pr, name, s)
	if err != nil {
		w.log.Warn("hk change outbox write failed")
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if claim != nil {
		w.holdLocked(file, claim)
	}
	w.addLocked(repo, pr, name, s, file)
}

// addLocked queues an observation for the next flush. The caller holds w.mu.
func (w *ChangeWriter) addLocked(repo string, pr int, name string, s ChangeSection, file string) {
	if file != "" {
		if w.seen == nil {
			w.seen = map[string]bool{}
		}
		if w.seen[file] {
			return
		}
		w.seen[file] = true
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
	if file != "" {
		p.files = append(p.files, file)
	}
	if name == "findings" {
		p.findings = append(p.findings, s)
	} else if old, ok := p.sections[name]; !ok || !s.At.Before(old.At) {
		p.sections[name] = s
	}
	if time.Now().Before(w.unavailableUntil) {
		w.retryLocked(time.Until(w.unavailableUntil))
		return
	}
	if w.timer == nil {
		w.active.Add(1)
		w.timer = time.AfterFunc(150*time.Millisecond, w.flush)
	}
}
func (w *ChangeWriter) retryLocked(after time.Duration) {
	if w.retryTimer != nil {
		return
	}
	if after < 10*time.Second {
		after = 10 * time.Second
	}
	w.retryTimer = time.AfterFunc(after, func() {
		w.mu.Lock()
		w.retryTimer = nil
		w.mu.Unlock()
		w.reload()
		w.mu.Lock()
		if len(w.pending) > 0 && w.timer == nil {
			w.active.Add(1)
			w.timer = time.AfterFunc(150*time.Millisecond, w.flush)
		}
		w.mu.Unlock()
	})
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
		w.staleNow = nil
		last := w.sent[k]
		keys := map[string]string{}
		b := ChangeBatch{Repo: k.repo, PR: k.pr}
		for name, s := range p.sections {
			key := sectionKey(s.Value)
			if last[name] == key {
				// The change's value is what this process last wrote for it.
				w.unchanged.Add(1)
				continue
			}
			keys[name] = key
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
		// Findings are a log of review passes, each written in order and never
		// re-observed by a pending change, so they are all sent (see replay).
		findings := p.findings
		slices.SortStableFunc(findings, func(a, b ChangeSection) int { return a.At.Compare(b.At) })
		if len(findings) > 0 {
			b.Findings = findings[0].Value
		}
		done := true
		if b.State != nil || b.Checks != nil || b.Threads != nil || b.Gate != nil || b.Findings != nil {
			done = w.send(b)
		}
		for i := 1; done && i < len(findings); i++ {
			if !w.send(ChangeBatch{Repo: k.repo, PR: k.pr, Findings: findings[i].Value}) {
				done = false
			}
		}
		w.mu.Lock()
		for _, file := range p.files {
			delete(w.seen, file)
			if done {
				if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
					w.log.Warn("hk change outbox cleanup failed")
				} else {
					w.releaseLocked(file)
				}
			}
		}
		if !done && w.outbox != "" {
			if !time.Now().Before(w.unavailableUntil) {
				w.unavailableUntil = time.Now().Add(10 * time.Second)
			}
			w.retryLocked(time.Until(w.unavailableUntil))
		}
		w.mu.Unlock()
		stale := w.staleNow
		w.staleNow = nil
		w.noteSent(k, keys, stale, done)
	}
}

// noteSent records the sections a flush wrote for k. A flush that did not
// finish may have reached hk in part, so the sections it tried lose their
// recorded value and the next flush writes them again.
func (w *ChangeWriter) noteSent(k changeKey, keys map[string]string, stale map[string]bool, done bool) {
	if !done {
		for name := range keys {
			delete(w.sent[k], name)
		}
		return
	}
	// hk holds a newer value for a section it rejected as stale, so nothing is
	// recorded for that section: the next observation of it is written, even
	// when it matches the value that was rejected.
	for name := range stale {
		delete(w.sent[k], name)
		delete(keys, name)
	}
	if len(keys) == 0 {
		return
	}
	if w.sent == nil {
		w.sent = map[changeKey]map[string]string{}
	}
	if w.sent[k] == nil {
		w.sent[k] = map[string]string{}
	}
	maps.Copy(w.sent[k], keys)
}

// sectionKey identifies a section's value for the unchanged check: a hash of
// its canonical JSON without the observation times (observed_at, decided_at)
// and without the provenance of the observation (source: fresh, poll, webhook
// or review; delivery_id: the webhook delivery that carried it). The same
// state observed again keys alike, whichever read or delivery saw it. Queue
// stores the value as JSON.
func sectionKey(v any) string {
	raw, _ := v.(json.RawMessage)
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&m) == nil && m != nil {
		for _, k := range []string{"observed_at", "decided_at", "source", "delivery_id"} {
			delete(m, k)
		}
		if canon, e := json.Marshal(m); e == nil {
			raw = canon
		}
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
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
	err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && err != nil {
		return 0, result
	}
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
func (w *ChangeWriter) send(b ChangeBatch) bool {
	// hk accepts at most 2 MiB per request. Split a large batch without
	// splitting a section, since complete sets and finding ordinals are atomic.
	if raw, err := json.Marshal(b); err != nil {
		return true
	} else if len(raw) > 2<<20 {
		parts := []ChangeBatch{{Repo: b.Repo, PR: b.PR, State: b.State}, {Repo: b.Repo, PR: b.PR, Checks: b.Checks}, {Repo: b.Repo, PR: b.PR, Threads: b.Threads}, {Repo: b.Repo, PR: b.PR, Findings: b.Findings}, {Repo: b.Repo, PR: b.PR, Gate: b.Gate}}
		done := true
		for _, part := range parts {
			if part.State == nil && part.Checks == nil && part.Threads == nil && part.Findings == nil && part.Gate == nil {
				continue
			}
			encoded, e := json.Marshal(part)
			if e == nil && len(encoded) <= 2<<20 {
				if !w.send(part) {
					done = false
				}
			} else {
				w.rejected.Add(1)
				w.log.Warn("hk change section exceeds request limit")
			}
		}
		return done
	}
	w.mu.Lock()
	blocked := time.Now().Before(w.unavailableUntil)
	w.mu.Unlock()
	if blocked {
		return false
	}
	c, e := w.endpoint()
	if e != nil {
		w.log.Warn("hk change endpoint unavailable")
		return false
	}
	status, result := w.request(c, http.MethodPost, "/v1/changes/batch", b)
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
		if supported, done := w.sections(c, b, true); !supported {
			w.mu.Lock()
			if !time.Now().Before(w.unavailableUntil) {
				w.unavailableUntil = time.Now().Add(10 * time.Minute)
				w.unavailable.Add(1)
				w.log.Warn("hk change state unavailable")
			}
			w.mu.Unlock()
			return false
		} else {
			return done
		}
	}
	if status == http.StatusBadRequest {
		w.log.Warn("hk change batch rejected", "reason", errorMessage(result.Reason))
		_, done := w.sections(c, b, false)
		return done
	}
	if status >= 200 && status < 300 {
		for name, needed := range map[string]bool{"state": b.State != nil, "checks": b.Checks != nil, "threads": b.Threads != nil, "findings": b.Findings != nil, "gate": b.Gate != nil} {
			if needed {
				section, ok := result.Sections[name]
				if !ok || (!section.Applied && section.Reason != "stale") {
					w.log.Warn("hk change response incomplete")
					return false
				}
				if !section.Applied {
					w.markStale(name)
				}
			}
		}
		w.count(result)
		return true
	}
	w.log.Warn("hk change request failed", "status", status)
	return false
}

// markStale records that hk rejected name as stale in the flush under way, so
// the flush does not record the value it sent as written. The caller holds sendMu.
func (w *ChangeWriter) markStale(name string) {
	if w.staleNow == nil {
		w.staleNow = map[string]bool{}
	}
	w.staleNow[name] = true
}

func (w *ChangeWriter) sections(c *hkJobs, b ChangeBatch, probe bool) (bool, bool) {
	sections := []struct {
		name  string
		value any
	}{{"state", b.State}, {"checks", b.Checks}, {"threads", b.Threads}, {"findings", b.Findings}, {"gate", b.Gate}}
	supported := false
	done := true
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
				return false, false
			}
			done = false
			continue
		}
		supported = true
		if status == http.StatusBadRequest {
			w.rejected.Add(1)
			w.log.Warn("hk change section rejected", "section", s.name, "reason", errorMessage(result.Reason))
		} else if status >= 200 && status < 300 {
			section, ok := result.Sections[s.name]
			if !ok || (!section.Applied && section.Reason != "stale") {
				w.log.Warn("hk change response incomplete", "section", s.name)
				done = false
				continue
			}
			if !section.Applied {
				w.markStale(s.name)
			}
			w.count(result)
		} else {
			w.log.Warn("hk change section request failed", "section", s.name, "status", status)
			done = false
		}
	}
	return supported, done
}

// Async registers observation work so one-shot commands can drain it on exit.
func (w *ChangeWriter) Async(fn func()) {
	if w == nil {
		return
	}
	w.active.Add(1)
	go func() { defer w.active.Done(); fn() }()
}
