package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

// evals is the fake evaluator of a queue: every Run records its start and then
// blocks until the test sends a token, so a test decides when each one ends.
type evals struct {
	mu      sync.Mutex
	order   []PRKey
	works   map[PRKey][]Work
	running map[PRKey]int
	// sameKeyMax is the most evaluations of one PR that ran at once.
	sameKeyMax int
	release    chan struct{}
}

func newEvals() *evals {
	return &evals{works: map[PRKey][]Work{}, running: map[PRKey]int{}, release: make(chan struct{})}
}

func (e *evals) Run(ctx context.Context, w Work) {
	e.mu.Lock()
	e.order = append(e.order, w.Key)
	e.works[w.Key] = append(e.works[w.Key], w)
	e.running[w.Key]++
	e.sameKeyMax = max(e.sameKeyMax, e.running[w.Key])
	e.mu.Unlock()
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	e.mu.Lock()
	e.running[w.Key]--
	e.mu.Unlock()
}

func (e *evals) started() []PRKey {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.order)
}

func (e *evals) runs(k PRKey) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.works[k])
}

func (e *evals) waitStarted(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(e.started()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %d evaluations started, want %d: %v", len(e.started()), n, e.started())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// finishOne ends one running evaluation.
func (e *evals) finishOne(t *testing.T) {
	t.Helper()
	select {
	case e.release <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("no evaluation was running")
	}
}

func pr(n int) PRKey { return PRKey{"o/r", n} }

func startQueue(t *testing.T, q *Queue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx)
	t.Cleanup(func() {
		cancel()
		q.Wait()
	})
}

func deliverEvent(t *testing.T, url, event string, body string) {
	deliverEventID(t, url, event, "d-"+event, body)
}

func deliverEventID(t *testing.T, url, event, id, body string) {
	t.Helper()
	resp := post(t, http.DefaultClient, url, event, id, []byte(body), testSecret)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("%s delivery: status %d", event, resp.StatusCode)
	}
}

func webhookServer(t *testing.T, q *Queue) string {
	t.Helper()
	d := Daemon{Log: nil, Webhook: &Webhook{Path: "/github", Secret: testSecret, Repos: map[string]bool{"o/r": true, "o/other": true},
		Queue: q, Branches: map[string]string{"o/r": "main", "o/other": "main"}, Trigger: func(string) bool { return true }}}
	d.Log = q.logger()
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	t.Cleanup(srv.Close)
	return srv.URL + "/github"
}

// (a) A pull_request synchronize delivery for PR 2 starts PR 2's evaluation at
// once while PR 1's evaluation is blocked: nothing waits for a pass, or for PR 1.
func TestEventQueueWebhookStartsPRWhileAnotherReviewIsBlocked(t *testing.T) {
	ev := newEvals()
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	q.Enqueue(SourceSweep, pr(1))
	ev.waitStarted(t, 1)
	url := webhookServer(t, q)
	deliverEvent(t, url, "pull_request", `{"action":"synchronize","number":2,"repository":{"full_name":"o/r"},"pull_request":{"number":2,"head":{"sha":"s2"}}}`)
	ev.waitStarted(t, 2)
	if got := ev.started(); !slices.Equal(got, []PRKey{pr(1), pr(2)}) {
		t.Fatalf("started %v, want PR 1 then PR 2", got)
	}
	w := ev.works[pr(2)][0]
	if !slices.Equal(w.Sources, []Source{SourceWebhook}) || w.Waited > time.Second {
		t.Fatalf("PR 2 work %+v", w)
	}
	if ev.runs(pr(1)) != 1 || ev.running[pr(1)] != 1 {
		t.Fatalf("PR 1 should still be in its first evaluation")
	}
	ev.finishOne(t)
	ev.finishOne(t)
}

// (b) The same PR queued five times while it waits runs once.
func TestEventQueueCoalescesRepeatsWhileQueued(t *testing.T) {
	ev := newEvals()
	q := &Queue{Cap: 1, Run: ev.Run}
	startQueue(t, q)
	q.Enqueue(SourceSweep, pr(9))
	ev.waitStarted(t, 1)
	for _, src := range []Source{SourceWebhook, SourceWebhook, SourceReviewer, SourceSweep, SourceWebhook} {
		q.Enqueue(src, pr(2))
	}
	if st := q.Take("o/r"); st.Depth != 1 || st.Running != 1 {
		t.Fatalf("stats %+v, want one item waiting behind one running", st)
	}
	ev.finishOne(t)
	ev.waitStarted(t, 2)
	ev.finishOne(t)
	q.Wait()
	if n := ev.runs(pr(2)); n != 1 {
		t.Fatalf("PR 2 ran %d times, want once", n)
	}
	if got := ev.works[pr(2)][0].Sources; !slices.Equal(got, []Source{SourceWebhook, SourceReviewer, SourceSweep}) {
		t.Fatalf("sources %v, want each kept once in arrival order", got)
	}
}

// (c) A PR that is being evaluated is not evaluated a second time at once; the
// events that arrive meanwhile run it once more afterwards.
func TestEventQueueEventDuringEvaluationRerunsOnceAfterwards(t *testing.T) {
	ev := newEvals()
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	q.Enqueue(SourceWebhook, pr(1))
	ev.waitStarted(t, 1)
	q.Enqueue(SourceWebhook, pr(1))
	q.Enqueue(SourceReviewer, pr(1))
	q.Enqueue(SourceSweep, pr(1))
	time.Sleep(50 * time.Millisecond)
	if ev.runs(pr(1)) != 1 {
		t.Fatalf("PR 1 started again while it was being evaluated")
	}
	ev.finishOne(t)
	ev.waitStarted(t, 2)
	time.Sleep(50 * time.Millisecond)
	if n := ev.runs(pr(1)); n != 2 {
		t.Fatalf("PR 1 ran %d times, want the first and one more", n)
	}
	if got := ev.works[pr(1)][1].Sources; !slices.Equal(got, []Source{SourceWebhook, SourceReviewer, SourceSweep}) {
		t.Fatalf("rerun sources %v", got)
	}
	ev.finishOne(t)
	q.Wait()
	if ev.sameKeyMax != 1 || ev.runs(pr(1)) != 2 {
		t.Fatalf("same-PR peak %d, runs %d", ev.sameKeyMax, ev.runs(pr(1)))
	}
}

// (e) A reviewer job that finished enqueues exactly its PR.
func TestEventQueueReviewerFinishedEnqueuesExactlyThatPR(t *testing.T) {
	ev := newEvals()
	close(ev.release)
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	hk := &fakeHK{
		jobs:  []WatchedJob{{Repo: "o/r", PR: 3, Label: "r-3"}, {Repo: "o/r", PR: 4, Label: "r-4"}, {Repo: "o/other", PR: 5, Label: "r-5"}},
		views: map[string]JobView{"r-3": {State: "completed"}, "r-4": {State: "running"}},
	}
	w := &ReviewerWatch{Jobs: hk.list, Read: hk.read, NotFound: func(err error) bool { return err == errNotFound },
		Enqueue: func(repo string, pr int) { q.EnqueueNamed(SourceReviewer, repo, pr, "") }}
	w.Tick(context.Background())
	q.Wait()
	if got := ev.started(); !slices.Equal(got, []PRKey{pr(3)}) {
		t.Fatalf("started %v, want only PR 3", got)
	}
	if got := ev.works[pr(3)][0].Sources; !slices.Equal(got, []Source{SourceReviewer}) {
		t.Fatalf("sources %v", got)
	}
	// A job already reported does not enqueue its PR again.
	w.Tick(context.Background())
	q.Wait()
	if ev.runs(pr(3)) != 1 {
		t.Fatalf("PR 3 ran %d times", ev.runs(pr(3)))
	}
}

// (f) A push to a repo's base branch enqueues that repo's open PRs, and only
// them; a push to another branch enqueues nothing.
func TestEventQueueBasePushEnqueuesTheReposOpenPRs(t *testing.T) {
	ev := newEvals()
	close(ev.release)
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	q.SetOpen("o/r", map[int]string{1: "a", 2: "b", 3: "c"})
	q.SetOpen("o/other", map[int]string{7: "z"})
	url := webhookServer(t, q)
	deliverEventID(t, url, "push", "d-push-feature", `{"ref":"refs/heads/feature","repository":{"full_name":"o/r"}}`)
	q.Wait()
	if got := ev.started(); len(got) != 0 {
		t.Fatalf("a push to a feature branch started %v", got)
	}
	deliverEventID(t, url, "push", "d-push-main", `{"ref":"refs/heads/main","repository":{"full_name":"o/r"}}`)
	q.Wait()
	got := ev.started()
	slices.SortFunc(got, func(a, b PRKey) int { return a.PR - b.PR })
	if !slices.Equal(got, []PRKey{pr(1), pr(2), pr(3)}) {
		t.Fatalf("started %v, want PRs 1-3 of o/r", got)
	}
	for _, k := range got {
		if s := ev.works[k][0].Sources; !slices.Equal(s, []Source{SourceBase}) {
			t.Fatalf("PR %d sources %v", k.PR, s)
		}
	}
}

// A delivery that names only a commit finds the PR through its head.
func TestEventQueueDeliveryNamingOnlyACommitFindsItsPR(t *testing.T) {
	ev := newEvals()
	close(ev.release)
	q := &Queue{Run: ev.Run}
	startQueue(t, q)
	q.SetOpen("o/r", map[int]string{1: "aaa", 2: "bbb"})
	url := webhookServer(t, q)
	deliverEvent(t, url, "check_run", `{"action":"completed","repository":{"full_name":"o/r"},"check_run":{"head_sha":"bbb"}}`)
	deliverEvent(t, url, "status", `{"repository":{"full_name":"o/r"},"sha":"unknown"}`)
	q.Wait()
	if got := ev.started(); !slices.Equal(got, []PRKey{pr(2)}) {
		t.Fatalf("started %v, want only PR 2", got)
	}
}

func TestSetOpenMergesChangesSinceBeginList(t *testing.T) {
	t.Run("forgotten PR stays absent", func(t *testing.T) {
		q := &Queue{}
		q.SetOpen("o/r", map[int]string{2: "head-2"})
		q.BeginList("o/r")
		q.Forget("o/r", 2)
		q.SetOpen("o/r", map[int]string{2: "head-2"})
		q.EnqueueNamed(SourceCheck, "o/r", 0, "head-2")
		if got := q.Take("o/r").Depth; got != 0 {
			t.Fatalf("forgotten PR was queued by its head: depth %d", got)
		}
	})
	t.Run("newer learned head wins", func(t *testing.T) {
		q := &Queue{}
		q.BeginList("o/r")
		q.Learn("o/r", 1, "new")
		q.SetOpen("o/r", map[int]string{1: "old"})
		if got := q.Head("o/r", 1); got != "new" {
			t.Fatalf("head %q, want new", got)
		}
	})
	t.Run("forget after learn stays absent", func(t *testing.T) {
		q := &Queue{}
		q.BeginList("o/r")
		q.Learn("o/r", 2, "learned")
		q.Forget("o/r", 2)
		q.SetOpen("o/r", map[int]string{2: "listed"})
		if got := q.Head("o/r", 2); got != "" {
			t.Fatalf("head after forget %q, want absent", got)
		}
		q.EnqueueNamed(SourceCheck, "o/r", 0, "learned")
		if got := q.Take("o/r").Depth; got != 0 {
			t.Fatalf("forgotten PR was queued by its learned head: depth %d", got)
		}
	})
	t.Run("learn after forget restores head", func(t *testing.T) {
		q := &Queue{}
		q.BeginList("o/r")
		q.Forget("o/r", 3)
		q.Learn("o/r", 3, "new")
		q.SetOpen("o/r", map[int]string{3: "old"})
		if got := q.Head("o/r", 3); got != "new" {
			t.Fatalf("head %q, want new", got)
		}
	})
	t.Run("stale learn cannot undo a current forget", func(t *testing.T) {
		q := &Queue{}
		q.SetOpen("o/r", map[int]string{5: "old"})
		readMark := q.ReadMark("o/r")
		epoch := q.BeginList("o/r")
		q.Forget("o/r", 5, epoch)
		q.Learn("o/r", 5, "stale", readMark)
		if got := q.Head("o/r", 5); got != "" {
			t.Fatalf("stale learn restored head %q before commit", got)
		}
		q.SetOpen("o/r", map[int]string{5: "listed"})
		if got := q.Head("o/r", 5); got != "" {
			t.Fatalf("stale learn restored head %q after commit", got)
		}
	})
	t.Run("aborted list applies held stale forget", func(t *testing.T) {
		clock := &fakeTimers{}
		q := &Queue{After: clock.after}
		q.SetOpen("o/r", map[int]string{5: "head"})
		q.EnqueueAt(SourceTimer, "o/r", 5, "review", time.Now().Add(time.Hour))
		readMark := q.ReadMark("o/r")
		epoch := q.BeginList("o/r")
		q.Forget("o/r", 5, readMark)
		if got := q.Head("o/r", 5); got != "head" {
			t.Fatalf("stale forget applied before outcome: head %q", got)
		}
		q.AbortList("o/r", epoch)
		if got := q.Head("o/r", 5); got != "" {
			t.Fatalf("head after abort %q, want absent", got)
		}
		if got := q.TimerIDs("o/r", 5); len(got) != 0 {
			t.Fatalf("timers after abort %v, want none", got)
		}
		q.EnqueueNamed(SourceCheck, "o/r", 0, "head")
		if got := q.Take("o/r").Depth; got != 0 {
			t.Fatalf("aborted closed PR was queued by its head: depth %d", got)
		}
	})
	t.Run("no mark replaces", func(t *testing.T) {
		q := &Queue{}
		q.SetOpen("o/r", map[int]string{1: "a", 2: "b"})
		q.SetOpen("o/r", map[int]string{3: "c"})
		if got := q.Head("o/r", 1); got != "" {
			t.Fatalf("PR 1 head %q", got)
		}
		if got := q.Head("o/r", 3); got != "c" {
			t.Fatalf("PR 3 head %q", got)
		}
	})
}

func TestSetOpenIgnoresEvaluationReadsStartedBeforeList(t *testing.T) {
	q := &Queue{}
	readEpoch := uint64(0)
	epoch := q.BeginList("o/r")
	q.Learn("o/r", 5, "old", readEpoch)
	q.SetOpen("o/r", map[int]string{5: "new"})
	if got := q.Head("o/r", 5); got != "new" {
		t.Fatalf("head %q, want listed head new", got)
	}
	q.Learn("o/r", 5, "older", readEpoch)
	q.Forget("o/r", 5, readEpoch)
	if got := q.Head("o/r", 5); got != "new" {
		t.Fatalf("stale evaluation changed listed head to %q", got)
	}
	if epoch != 1 {
		t.Fatalf("first list epoch %d, want 1", epoch)
	}
}

// (h) More items wait than run: the lowest priority number starts first, ties in
// arrival order, and an item with no priority last; a priority-1 item starts at
// once past the cap, up to Extra of them.
func TestEventQueuePriorityOrderWhenMoreWaitThanRun(t *testing.T) {
	ranks := map[int]Priority{2: {Rank: 3}, 3: {Rank: 2}, 4: {Rank: int(^uint(0) >> 1)}, 5: {Rank: 2}}
	lookup := func(_ context.Context, k PRKey) (Priority, any) { return ranks[k.PR], nil }
	t.Run("order", func(t *testing.T) {
		ev := newEvals()
		q := &Queue{Cap: 1, Run: ev.Run, Lookup: lookup}
		startQueue(t, q)
		q.Enqueue(SourceSweep, pr(9))
		ev.waitStarted(t, 1)
		q.Enqueue(SourceSweep, pr(2), pr(3), pr(4), pr(5))
		for i := 2; i <= 5; i++ {
			ev.finishOne(t)
			ev.waitStarted(t, i)
		}
		ev.finishOne(t)
		q.Wait()
		if got, want := ev.started(), []PRKey{pr(9), pr(3), pr(5), pr(2), pr(4)}; !slices.Equal(got, want) {
			t.Fatalf("started %v, want %v", got, want)
		}
	})
	t.Run("urgent", func(t *testing.T) {
		ev := newEvals()
		urgent := func(_ context.Context, k PRKey) (Priority, any) {
			if k.PR >= 10 {
				return Priority{Rank: 1, Urgent: true}, nil
			}
			return Priority{Rank: 2}, nil
		}
		q := &Queue{Cap: 1, Extra: 2, Run: ev.Run, Lookup: urgent}
		startQueue(t, q)
		q.Enqueue(SourceSweep, pr(9))
		ev.waitStarted(t, 1)
		q.Enqueue(SourceSweep, pr(2), pr(10), pr(11), pr(12))
		ev.waitStarted(t, 3)
		time.Sleep(50 * time.Millisecond)
		got := ev.started()
		urgentStarted := 0
		for _, k := range got {
			if k.PR >= 10 {
				urgentStarted++
			}
		}
		if len(got) != 3 || got[0] != pr(9) || urgentStarted != 2 {
			t.Fatalf("started %v, want PR 9 and two of the three urgent ones past the cap", got)
		}
		for range 5 {
			ev.finishOne(t)
		}
		q.Wait()
	})
	t.Run("slow lookup", func(t *testing.T) {
		ev := newEvals()
		hang := make(chan struct{})
		slow := func(ctx context.Context, k PRKey) (Priority, any) {
			if k.PR == 2 {
				select {
				case <-hang:
				case <-ctx.Done():
				}
			}
			return Priority{Rank: 5}, nil
		}
		q := &Queue{Cap: 1, Run: ev.Run, Lookup: slow, Grace: 50 * time.Millisecond}
		startQueue(t, q)
		defer close(hang)
		q.Enqueue(SourceSweep, pr(9))
		ev.waitStarted(t, 1)
		q.Enqueue(SourceSweep, pr(2), pr(3))
		ev.finishOne(t)
		ev.waitStarted(t, 2)
		if got := ev.started(); got[1] != pr(3) {
			t.Fatalf("started %v: the hung lookup of PR 2 held PR 3", got)
		}
		ev.finishOne(t)
		ev.waitStarted(t, 3)
		if w := ev.works[pr(2)][0]; !w.Expired || w.Known {
			t.Fatalf("PR 2 work %+v, want an expired lookup", w)
		}
		ev.finishOne(t)
	})
}

// Different repos never wait for one another's cap.
func TestEventQueueCapIsPerRepo(t *testing.T) {
	ev := newEvals()
	q := &Queue{Cap: 1, Run: ev.Run}
	startQueue(t, q)
	q.Enqueue(SourceSweep, PRKey{"o/r", 1}, PRKey{"o/other", 1})
	ev.waitStarted(t, 2)
	if got := ev.started(); len(got) != 2 {
		t.Fatalf("started %v", got)
	}
	ev.finishOne(t)
	ev.finishOne(t)
}

// Items queued before Start wait for it, and a cancelled queue starts nothing.
func TestEventQueueStartsNothingAfterCancel(t *testing.T) {
	ev := newEvals()
	q := &Queue{Run: ev.Run}
	q.Enqueue(SourceWebhook, pr(1))
	ctx, cancel := context.WithCancel(context.Background())
	if len(ev.started()) != 0 {
		t.Fatal("started before Start")
	}
	q.Start(ctx)
	ev.waitStarted(t, 1)
	cancel()
	q.Enqueue(SourceWebhook, pr(2))
	q.Wait()
	if got := ev.started(); !slices.Equal(got, []PRKey{pr(1)}) {
		t.Fatalf("started %v", got)
	}
}

// A waiting PR whose lookup answered "not urgent" is asked again after Refresh:
// something outside the queue made it urgent, and it must start past the cap
// instead of waiting for a slot.
func TestEventQueueRefreshReordersAWaitingPR(t *testing.T) {
	var mu sync.Mutex
	urgentNow := map[int]bool{}
	lookup := func(_ context.Context, k PRKey) (Priority, any) {
		mu.Lock()
		defer mu.Unlock()
		if urgentNow[k.PR] {
			return Priority{Rank: 1, Urgent: true}, nil
		}
		return Priority{Rank: 5}, nil
	}
	ev := newEvals()
	q := &Queue{Cap: 1, Extra: 1, Run: ev.Run, Lookup: lookup}
	startQueue(t, q)
	q.Enqueue(SourceSweep, pr(9))
	ev.waitStarted(t, 1)
	q.Enqueue(SourceSweep, pr(2), pr(3))
	time.Sleep(50 * time.Millisecond)
	if got := ev.started(); len(got) != 1 {
		t.Fatalf("started %v before the refresh", got)
	}

	mu.Lock()
	urgentNow[3] = true
	mu.Unlock()
	q.Refresh("o/r", 3)
	ev.waitStarted(t, 2)
	if got := ev.started(); got[1] != pr(3) {
		t.Fatalf("started %v, want PR 3 past the cap", got)
	}
	if w := ev.works[pr(3)][0]; !w.Over || !w.Priority.Urgent {
		t.Fatalf("PR 3 work %+v, want an urgent start past the cap", w)
	}
	for range 2 {
		ev.finishOne(t)
	}
	ev.finishOne(t)
	q.Wait()
}
