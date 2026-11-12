package daemon

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
)

// checkDeliveries is a webhook receiver in front of a real queue, recording
// both the evaluations the deliveries start and the roles they wake.
type checkDeliveries struct {
	t   *testing.T
	ev  *evals
	q   *Queue
	srv *httptest.Server
	mu  sync.Mutex
	// woke is every role a delivery woke, in arrival order.
	woke []string
}

func newCheckDeliveries(t *testing.T) *checkDeliveries {
	t.Helper()
	ev := newEvals()
	close(ev.release) // every evaluation returns at once
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	f := &checkDeliveries{t: t, ev: ev, q: q}
	d := Daemon{Log: q.logger(), Webhook: &Webhook{
		Path: "/github", Secret: testSecret, Repos: map[string]bool{"o/r": true},
		Queue: q, Branches: map[string]string{"o/r": "main"},
		Trigger: func(role string) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.woke = append(f.woke, role)
			return true
		}}}
	f.srv = httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *checkDeliveries) deliver(event, body string) {
	f.t.Helper()
	resp := post(f.t, f.srv.Client(), f.srv.URL+"/github", event, "d-"+event, []byte(body), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		f.t.Fatalf("%s delivery: status %d", event, resp.StatusCode)
	}
}

func (f *checkDeliveries) wakes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.woke)
}

// (a) A completed check_run for a PR head enqueues that PR and wakes no full
// pr sweep: the per-PR evaluation does not need the sweep.
func TestCheckRunCompletedEnqueuesItsPRWithoutASweep(t *testing.T) {
	f := newCheckDeliveries(t)
	f.deliver("check_run", `{"action":"completed","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"completed","conclusion":"success","check_suite":{"head_branch":"feature"},"pull_requests":[{"number":7}]}}`)
	f.q.Wait()
	if got := f.ev.started(); !slices.Equal(got, []PRKey{pr(7)}) {
		t.Fatalf("started %v, want PR 7", got)
	}
	if got := f.wakes(); len(got) != 0 {
		t.Fatalf("a completed check woke %v, want no sweep", got)
	}
}

// (b) A check_run or check_suite that has not completed enqueues nothing and
// wakes nothing: it has no result to evaluate, and the completed delivery
// follows. The events name a feature branch, so nothing is about the watched
// branch either.
func TestUnfinishedCheckEnqueuesAndWakesNothing(t *testing.T) {
	for _, tt := range []struct{ name, event, body string }{
		{"check_run queued", "check_run", `{"action":"created","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"queued","check_suite":{"head_branch":"feature"},"pull_requests":[{"number":7}]}}`},
		{"check_run in_progress", "check_run", `{"action":"created","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"in_progress","check_suite":{"head_branch":"feature"},"pull_requests":[{"number":7}]}}`},
		{"check_run rerequested", "check_run", `{"action":"rerequested","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"queued","check_suite":{"head_branch":"feature"},"pull_requests":[{"number":7}]}}`},
		{"check_suite requested", "check_suite", `{"action":"requested","repository":{"full_name":"o/r"},"check_suite":{"head_sha":"bbb","head_branch":"feature","pull_requests":[{"number":7}]}}`},
		{"check_suite rerequested", "check_suite", `{"action":"rerequested","repository":{"full_name":"o/r"},"check_suite":{"head_sha":"bbb","head_branch":"feature","pull_requests":[{"number":7}]}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCheckDeliveries(t)
			f.deliver(tt.event, tt.body)
			f.q.Wait()
			if got := f.ev.started(); len(got) != 0 {
				t.Fatalf("an unfinished check started %v, want nothing", got)
			}
			if got := f.wakes(); len(got) != 0 {
				t.Fatalf("an unfinished check woke %v, want nothing", got)
			}
		})
	}
}

// (c) A completed check_suite and a status for a PR head each enqueue the PR
// without waking a sweep. A status has no action and is enqueued as before.
func TestCheckSuiteAndStatusEnqueueTheirPRWithoutASweep(t *testing.T) {
	t.Run("check_suite", func(t *testing.T) {
		f := newCheckDeliveries(t)
		f.deliver("check_suite", `{"action":"completed","repository":{"full_name":"o/r"},"check_suite":{"head_sha":"ccc","head_branch":"feature","pull_requests":[{"number":3}]}}`)
		f.q.Wait()
		if got := f.ev.started(); !slices.Equal(got, []PRKey{pr(3)}) {
			t.Fatalf("started %v, want PR 3", got)
		}
		if got := f.wakes(); len(got) != 0 {
			t.Fatalf("a completed check suite woke %v, want no sweep", got)
		}
	})
	t.Run("status", func(t *testing.T) {
		f := newCheckDeliveries(t)
		f.q.SetOpen("o/r", map[int]string{4: "ddd"})
		f.deliver("status", `{"repository":{"full_name":"o/r"},"sha":"ddd","branches":[{"name":"feature"}]}`)
		f.q.Wait()
		if got := f.ev.started(); !slices.Equal(got, []PRKey{pr(4)}) {
			t.Fatalf("started %v, want PR 4", got)
		}
		if got := f.wakes(); len(got) != 0 {
			t.Fatalf("a status woke %v, want no sweep", got)
		}
	})
}

// (d) A check or status event for the watched branch's head wakes watch at
// once, so the reaction to main CI is not delayed; one for another ref does
// not wake it.
func TestCheckEventsWakeWatchOnlyForTheWatchedBranch(t *testing.T) {
	for _, tt := range []struct {
		name, event, body string
		woke              []string
	}{
		{"check_run on main", "check_run",
			`{"action":"completed","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"completed","conclusion":"success","check_suite":{"head_branch":"main"}}}`,
			[]string{"watch"}},
		{"check_suite on main", "check_suite",
			`{"action":"completed","repository":{"full_name":"o/r"},"check_suite":{"head_sha":"bbb","head_branch":"main"}}`,
			[]string{"watch"}},
		{"status on main", "status",
			`{"repository":{"full_name":"o/r"},"sha":"bbb","branches":[{"name":"main"},{"name":"release"}]}`,
			[]string{"watch"}},
		{"check_run on a feature branch", "check_run",
			`{"action":"completed","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"completed","conclusion":"success","check_suite":{"head_branch":"feature"}}}`,
			nil},
		{"check_suite on a feature branch", "check_suite",
			`{"action":"completed","repository":{"full_name":"o/r"},"check_suite":{"head_sha":"bbb","head_branch":"feature"}}`,
			nil},
		{"status on a feature branch", "status",
			`{"repository":{"full_name":"o/r"},"sha":"bbb","branches":[{"name":"feature"}]}`,
			nil},
		// The watch reaction does not wait for the result: a check that is
		// still running on the watched branch wakes watch, though nothing is
		// evaluated for it.
		{"unfinished check_run on main", "check_run",
			`{"action":"created","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb","status":"in_progress","check_suite":{"head_branch":"main"},"pull_requests":[{"number":7}]}}`,
			[]string{"watch"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCheckDeliveries(t)
			f.deliver(tt.event, tt.body)
			f.q.Wait()
			if got := f.wakes(); !slices.Equal(got, tt.woke) {
				t.Fatalf("%s woke %v, want %v", tt.event, got, tt.woke)
			}
			// None of these names a PR, and an unfinished check has nothing to
			// evaluate: the wake is all a check event does.
			if got := f.ev.started(); len(got) != 0 {
				t.Fatalf("%s started %v, want nothing", tt.event, got)
			}
		})
	}
}

// (e) Guard: a pull_request still wakes the pr sweep and watch, and a check on
// the watched branch's head wakes watch without enqueueing a sweep.
func TestPullRequestStillWakesPRAndWatch(t *testing.T) {
	f := newCheckDeliveries(t)
	f.deliver("pull_request", `{"action":"synchronize","number":7,"repository":{"full_name":"o/r"},"pull_request":{"number":7,"head":{"sha":"bbb"}}}`)
	f.q.Wait()
	if got := f.wakes(); !slices.Equal(got, []string{"pr", "watch"}) {
		t.Fatalf("pull_request woke %v, want pr and watch", got)
	}
	if got := f.ev.started(); !slices.Equal(got, []PRKey{pr(7)}) {
		t.Fatalf("started %v, want PR 7", got)
	}
}

// A check delivery queues its PR under its own source and carries the head it
// ran on, but it does not say the PR is at that head: only a pull_request
// delivery names the PR's head.
func TestCheckDeliverySourceAndHead(t *testing.T) {
	ev := newEvals()
	close(ev.release)
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	q.SetOpen("o/r", map[int]string{1: "aaa"})
	q.EnqueueNamed(SourceCheck, "o/r", 1, "bbb")
	q.Wait()
	if w := ev.works[pr(1)][0]; !slices.Equal(w.Sources, []Source{SourceCheck}) || w.CheckSHA != "bbb" {
		t.Fatalf("work %+v, want a check source naming bbb", w)
	}
	if got := q.Head("o/r", 1); got != "aaa" {
		t.Fatalf("head = %q, want aaa: a check delivery names the head it ran on, not the PR's", got)
	}
	q.EnqueueNamed(SourceWebhook, "o/r", 1, "ccc")
	q.Wait()
	if got := q.Head("o/r", 1); got != "ccc" {
		t.Fatalf("head = %q, want ccc: a pull_request delivery names the PR's head", got)
	}
}
