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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

// MaxWebhookBody caps a delivery at 5 MiB.
const MaxWebhookBody = 5 << 20

const maxRememberedWebhookDeliveries = 1024

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
	// Queue, when set, evaluates the PRs a delivery names (see deliveryPayload.refs)
	// at once, and every open PR of a repo when its base branch (Branches) is
	// pushed to.
	Queue *Queue
	// Branches is the branch each repository's watch role reads. A check or
	// status delivery wakes watch for that branch's head (see wakesWatch), and
	// a push to it queues every open PR (SourceBase).
	Branches map[string]string
	// State, when set, keeps what each delivery carries (see webhookstate). A
	// payload it cannot parse is logged once per event type and never changes
	// the response or the wake.
	State   *webhookstate.Store
	Applied func(repo, event, delivery string, body []byte)

	parseWarned sync.Map
	deliveryMu  sync.Mutex
	deliveryIDs [maxRememberedWebhookDeliveries]string
	deliveryAt  int
	deliverySet map[string]struct{}
}

// rememberDelivery atomically records a verified delivery id and reports
// whether it was already in the bounded in-memory window.
func (wh *Webhook) rememberDelivery(id string) bool {
	if id == "" {
		return false
	}
	wh.deliveryMu.Lock()
	defer wh.deliveryMu.Unlock()
	if _, ok := wh.deliverySet[id]; ok {
		return true
	}
	if wh.deliverySet == nil {
		wh.deliverySet = make(map[string]struct{}, maxRememberedWebhookDeliveries)
	}
	old := wh.deliveryIDs[wh.deliveryAt]
	if old != "" {
		delete(wh.deliverySet, old)
	}
	wh.deliveryIDs[wh.deliveryAt] = id
	wh.deliverySet[id] = struct{}{}
	wh.deliveryAt = (wh.deliveryAt + 1) % len(wh.deliveryIDs)
	return false
}

// webhookEventRoles maps X-GitHub-Event to the roles an event wakes. ping
// and unlisted events wake none. A check or status event wakes watch only for
// the head of the branch the watch role reads (see wakesWatch), and the PR it
// names is evaluated from the queue, so it wakes no full pr sweep.
var webhookEventRoles = map[string][]string{
	"pull_request":                {"pr", "watch"},
	"pull_request_review":         {"pr", "watch"},
	"pull_request_review_thread":  {"pr", "watch"},
	"pull_request_review_comment": {"pr", "watch"},
	"check_suite":                 {"watch"},
	"check_run":                   {"watch"},
	"status":                      {"watch"},
}

// checkEvent reports whether event carries a check or status result, whose
// wake and evaluation rules differ from a pull_request's.
func checkEvent(event string) bool {
	return event == "check_run" || event == "check_suite" || event == "status"
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
	repeat := wh.rememberDelivery(r.Header.Get("X-GitHub-Delivery"))
	if repeat {
		payload := body
		if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt == "application/x-www-form-urlencoded" {
			if form, err := url.ParseQuery(string(body)); err == nil {
				payload = []byte(form.Get("payload"))
			}
		}
		var delivery deliveryPayload
		_ = json.Unmarshal(payload, &delivery)
		args := webhookLogArgs(r, delivery.Repository.FullName, []string{})
		args = append(args, "repeat", true)
		d.Log.Info("webhook delivery", args...)
		w.WriteHeader(http.StatusAccepted)
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
		if event == "pull_request" || strings.HasPrefix(event, "pull_request_review") || checkEvent(event) {
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
			// After the marks and the invalidation: the evaluation reads what
			// the delivery changed.
			if wh.Queue != nil && delivery.evaluates(event) {
				src := SourceWebhook
				if checkEvent(event) {
					src = SourceCheck
				}
				for _, ref := range delivery.refs(event) {
					wh.Queue.EnqueueNamed(src, delivery.Repository.FullName, ref.number, ref.sha)
				}
			}
		}
		if wh.Queue != nil && event == "push" {
			if branch := wh.Branches[delivery.Repository.FullName]; branch != "" && delivery.Ref == "refs/heads/"+branch {
				wh.Queue.EnqueueRepo(SourceBase, delivery.Repository.FullName)
			}
		}
		for _, role := range webhookEventRoles[event] {
			if role == "watch" && checkEvent(event) && !delivery.wakesWatch(event, wh.watchedBranch(delivery.Repository.FullName, delivery.Repository.DefaultBranch)) {
				continue
			}
			if trigger(role) {
				triggered = append(triggered, role)
			}
		}
	}
	// The body, the signature and every other header stay out of the log.
	d.Log.Info("webhook delivery", webhookLogArgs(r, delivery.Repository.FullName, triggered)...)
	w.WriteHeader(http.StatusAccepted)
}

func webhookLogArgs(r *http.Request, repo string, triggered []string) []any {
	args := []any{
		"event", r.Header.Get("X-GitHub-Event"),
		"delivery", r.Header.Get("X-GitHub-Delivery"),
		"repo", repo,
		"triggered", triggered,
	}
	if hookID := r.Header.Get("X-GitHub-Hook-ID"); hookID != "" {
		args = append(args, "hook_id", hookID)
	}
	if target := r.Header.Get("X-GitHub-Hook-Installation-Target-Type"); target != "" {
		args = append(args, "hook_target", target)
	}
	return args
}

// deliveryPayload is the part of a delivery that names a repository, a PR or a
// commit.
type deliveryPayload struct {
	Repository struct {
		FullName      string `json:"full_name"`
		DefaultBranch string `json:"default_branch"`
	} `json:"repository"`
	// Action is set by check_run and check_suite; a status has none.
	Action string `json:"action"`
	// Number is set by pull_request; SHA by status.
	Number int    `json:"number"`
	SHA    string `json:"sha"`
	// Ref is the pushed ref of a push.
	Ref         string `json:"ref"`
	PullRequest struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_request"`
	CheckRun   checkPayload `json:"check_run"`
	CheckSuite checkPayload `json:"check_suite"`
	// Branches are the branches a status commit is on.
	Branches []struct {
		Name string `json:"name"`
	} `json:"branches"`
}

type checkPayload struct {
	HeadSHA    string `json:"head_sha"`
	HeadBranch string `json:"head_branch"`
	// CheckSuite is the suite of a check_run; a check_suite carries the branch
	// itself.
	CheckSuite struct {
		HeadBranch string `json:"head_branch"`
	} `json:"check_suite"`
	PullRequests []struct {
		Number int `json:"number"`
	} `json:"pull_requests"`
}

// prRef is what a delivery says about one PR: its number, its head commit, or both.
type prRef struct {
	number int
	sha    string
}

// refs lists every PR the delivery names: by number for the pull_request*
// events and the PRs a check payload lists, and by head commit for check_run,
// check_suite and status, whose payload leaves the PR out for a fork or a
// re-run. Only those PRs are named; their siblings are not. A check event
// names the commit its result is about with every PR it lists, so the
// evaluation can tell whether the check ran on the PR's own head.
func (p deliveryPayload) refs(event string) []prRef {
	switch event {
	case "check_run", "check_suite":
		c := p.CheckRun
		if event == "check_suite" {
			c = p.CheckSuite
		}
		out := []prRef{{0, c.HeadSHA}}
		for _, pr := range c.PullRequests {
			out = append(out, prRef{pr.Number, c.HeadSHA})
		}
		return out
	case "status":
		return []prRef{{0, p.SHA}}
	default:
		return []prRef{{max(p.PullRequest.Number, p.Number), p.PullRequest.Head.SHA}}
	}
}

// evaluates reports whether the delivery queues an evaluation of the PRs it
// names. A check that has not completed has no result to evaluate, and the
// completed delivery follows; a status carries no action and always counts.
func (p deliveryPayload) evaluates(event string) bool {
	switch event {
	case "check_run", "check_suite":
		return p.Action == "completed"
	}
	return true
}

// watchedBranch is the branch the repository's watch role reads: the
// configured base branch, or the repository's default branch when none is
// configured.
func (wh *Webhook) watchedBranch(repo, defaultBranch string) string {
	if branch := wh.Branches[repo]; branch != "" {
		return branch
	}
	return defaultBranch
}

// checkBranches lists the branches a check or status delivery names: the
// branch the check suite ran on, or the branches a status commit is on.
func (p deliveryPayload) checkBranches(event string) []string {
	var out []string
	add := func(branch string) {
		if branch != "" {
			out = append(out, branch)
		}
	}
	switch event {
	case "check_run":
		add(p.CheckRun.HeadBranch)
		add(p.CheckRun.CheckSuite.HeadBranch)
	case "check_suite":
		add(p.CheckSuite.HeadBranch)
	case "status":
		for _, branch := range p.Branches {
			add(branch.Name)
		}
	}
	return out
}

// wakesWatch reports whether a check or status delivery is about the head of
// the watched branch. A delivery that names no branch, or a repository whose
// watched branch is unknown, cannot be ruled out, so it wakes watch too: the
// reaction to main CI must not wait for the sweep.
func (p deliveryPayload) wakesWatch(event, watched string) bool {
	branches := p.checkBranches(event)
	return len(branches) == 0 || watched == "" || slices.Contains(branches, watched)
}

// mark records, on d, every PR the delivery names (see refs). Only those PRs
// go dirty; their siblings stay clean.
func (p deliveryPayload) mark(d *Dirty, event string) {
	for _, ref := range p.refs(event) {
		d.Mark(p.Repository.FullName, ref.number, ref.sha)
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
