package daemon

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

// MaxWebhookBody caps a delivery at 5 MiB.
const MaxWebhookBody = 5 << 20

// Webhook receives signed GitHub deliveries, invalidates cached evidence, and
// wakes roles early. Polling bounds missed deliveries and landing reads fresh
// evidence. Listen, Path and Secret arrive pre-validated from configuration.
type Webhook struct {
	Listen string
	Path   string
	Secret []byte
	// Repos is the configured owner/name set; deliveries for any other
	// repository are accepted and trigger nothing.
	Repos map[string]bool
	// Trigger wakes a role; nil means Daemon.Trigger.
	Trigger    func(role string) bool
	Invalidate func(repo string)
	// Dirty, when set, records the PRs a delivery names (see deliveryPayload.mark).
	Dirty *Dirty
	// State, when set, keeps what each delivery carries (see webhookstate). A
	// payload it cannot parse is logged once per event type and never changes
	// the response or the wake.
	State   *webhookstate.Store
	Applied func(repo, event, delivery string, body []byte)
	// Status, when set, answers StatusPath on the same listener. It is local
	// and read-only, so it takes no signature; the delivery Path always wins.
	Status http.Handler

	parseWarned sync.Map
}

// StatusPath is where Status is served.
const StatusPath = "/v1/pr-status"

// webhookEventRoles maps X-GitHub-Event to the roles an event wakes. ping
// and unlisted events wake none.
var webhookEventRoles = map[string][]string{
	"pull_request":                {"pr", "watch"},
	"pull_request_review":         {"pr", "watch"},
	"pull_request_review_thread":  {"pr", "watch"},
	"pull_request_review_comment": {"pr", "watch"},
	"check_suite":                 {"pr", "watch"},
	"check_run":                   {"pr", "watch"},
	"status":                      {"pr", "watch"},
	"release":                     {"release", "rollout"},
	"workflow_run":                {"release", "rollout"},
	"push":                        {"release", "rollout"},
}

// serve runs the receiver until ctx ends. The listener opens first so a bad
// bind fails before the loops start; Shutdown drains on daemon cancel.
func (d *Daemon) serveWebhook(ctx context.Context, wg *sync.WaitGroup) error {
	wh := d.Webhook
	ln, err := net.Listen("tcp", wh.Listen)
	if err != nil {
		return fmt.Errorf("webhook listen: %w", err)
	}
	// ReadTimeout bounds the whole request, body included, so a dripped body
	// cannot hold a connection open.
	if wh.State != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.flushState(ctx)
		}()
	}
	srv := &http.Server{Handler: http.HandlerFunc(d.handleDelivery), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			d.Log.Warn("webhook receiver stopped", "error", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	return nil
}

func (d *Daemon) handleDelivery(w http.ResponseWriter, r *http.Request) {
	wh := d.Webhook
	if r.URL.Path != wh.Path {
		if wh.Status != nil && r.URL.Path == StatusPath {
			wh.Status.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxWebhookBody))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	mac := hmac.New(sha256.New, wh.Secret)
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(r.Header.Get("X-Hub-Signature-256")), []byte(expected)) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	var delivery deliveryPayload
	// GitHub sends JSON either raw or, for the form content type, in the
	// payload field; both are read after the signature over the raw body.
	payload := body
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt == "application/x-www-form-urlencoded" {
		form, err := url.ParseQuery(string(body))
		if err != nil {
			http.Error(w, "bad form body", http.StatusBadRequest)
			return
		}
		payload = []byte(form.Get("payload"))
	}
	_ = json.Unmarshal(payload, &delivery)
	trigger := wh.Trigger
	if trigger == nil {
		trigger = d.Trigger
	}
	triggered := []string{}
	if wh.Repos[delivery.Repository.FullName] {
		event := r.Header.Get("X-GitHub-Event")
		if event == "pull_request" || strings.HasPrefix(event, "pull_request_review") || event == "check_suite" || event == "check_run" || event == "status" {
			if wh.State != nil {
				if applied, e := wh.State.ApplyObserved(delivery.Repository.FullName, event, r.Header.Get("X-GitHub-Delivery"), payload); e != nil {
					if _, seen := wh.parseWarned.LoadOrStore(event, true); !seen {
						d.Log.Warn("webhook state ignored a delivery", "event", event, "error", e.Error())
					}
				} else if applied && wh.Applied != nil {
					wh.Applied(delivery.Repository.FullName, event, r.Header.Get("X-GitHub-Delivery"), payload)
				}
			}
			if wh.Invalidate != nil {
				wh.Invalidate(delivery.Repository.FullName)
			}
			if wh.Dirty != nil {
				delivery.mark(wh.Dirty, event)
			}
		}
		for _, role := range webhookEventRoles[event] {
			if trigger(role) {
				triggered = append(triggered, role)
			}
		}
	}
	// The body, the signature and every other header stay out of the log.
	d.Log.Info("webhook delivery",
		"event", r.Header.Get("X-GitHub-Event"),
		"delivery", r.Header.Get("X-GitHub-Delivery"),
		"repo", delivery.Repository.FullName,
		"triggered", triggered)
	w.WriteHeader(http.StatusAccepted)
}

// deliveryPayload is the part of a delivery that names a repository, a PR or a
// commit.
type deliveryPayload struct {
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	// Number is set by pull_request; SHA by status.
	Number      int    `json:"number"`
	SHA         string `json:"sha"`
	PullRequest struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_request"`
	CheckRun   checkPayload `json:"check_run"`
	CheckSuite checkPayload `json:"check_suite"`
}

type checkPayload struct {
	HeadSHA      string `json:"head_sha"`
	PullRequests []struct {
		Number int `json:"number"`
	} `json:"pull_requests"`
}

// mark records, on d, every PR the delivery names: by number for the
// pull_request* events and the PRs a check payload lists, and by head commit
// for check_run, check_suite and status, whose payload leaves the PR out for a
// fork or a re-run. Only those PRs go dirty; their siblings stay clean.
func (p deliveryPayload) mark(d *Dirty, event string) {
	repo := p.Repository.FullName
	switch event {
	case "check_run", "check_suite":
		c := p.CheckRun
		if event == "check_suite" {
			c = p.CheckSuite
		}
		d.Mark(repo, 0, c.HeadSHA)
		for _, pr := range c.PullRequests {
			d.Mark(repo, pr.Number, "")
		}
	case "status":
		d.Mark(repo, 0, p.SHA)
	default:
		d.Mark(repo, max(p.PullRequest.Number, p.Number), p.PullRequest.Head.SHA)
	}
}

// stateFlushEvery is how often the kept state reaches disk; the daemon also
// flushes when it stops.
const stateFlushEvery = 2 * time.Second

// flushState writes the webhook state to disk until ctx ends, then once more,
// and logs the shadow-comparison counters when they changed.
func (d *Daemon) flushState(ctx context.Context) {
	st := d.Webhook.State
	tick := time.NewTicker(stateFlushEvery)
	defer tick.Stop()
	var logged webhookstate.Counters
	var lastLog time.Time
	flush := func() {
		if e := st.Flush(); e != nil {
			d.Log.Warn("webhook state not written", "error", e.Error())
		}
		if c := st.Counters(); c != logged && time.Since(lastLog) >= time.Minute {
			d.Log.Info("webhook_state counters", "webhook_state_hits", c.Hits, "webhook_state_misses", c.Misses, "webhook_state_mismatches", c.Mismatches)
			logged, lastLog = c, time.Now()
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-tick.C:
			flush()
		}
	}
}
