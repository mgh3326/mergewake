package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeHK serves job records and counts reads. The watch has no GitHub
// dependency, so a counter of GitHub requests would have nothing to observe;
// the pipeline test runs the real readers with a nil GitHub client.
type fakeHK struct {
	mu    sync.Mutex
	jobs  []WatchedJob
	views map[string]JobView
	err   error
	reads int
}

var errNotFound = errors.New("not found")

func (h *fakeHK) list() []WatchedJob {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]WatchedJob(nil), h.jobs...)
}

func (h *fakeHK) read(_ context.Context, label string) (JobView, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reads++
	if h.err != nil {
		return JobView{}, h.err
	}
	v, ok := h.views[label]
	if !ok {
		return JobView{}, errNotFound
	}
	return v, nil
}

func (h *fakeHK) set(label string, v JobView) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.views[label] = v
}

func (h *fakeHK) readCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reads
}

func newHK(jobs ...WatchedJob) *fakeHK {
	return &fakeHK{jobs: jobs, views: map[string]JobView{}}
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) lines() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(s.b.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

func (s *syncBuf) msgs(msg string) []map[string]any {
	var out []map[string]any
	for _, m := range s.lines() {
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func watchFor(h *fakeHK, buf *syncBuf, dirty *Dirty, wake func()) *ReviewerWatch {
	return &ReviewerWatch{
		Jobs: h.list, Read: h.read, NotFound: func(e error) bool { return errors.Is(e, errNotFound) },
		Dirty: dirty, Wake: wake, Log: slog.New(slog.NewJSONHandler(buf, nil)),
	}
}

const repo = "example/project"

// (a) A live job turning completed marks its PR dirty and wakes the role on the next tick.
func TestReviewerWatchMarksDirtyAndWakesWhenJobCompletes(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Pass: 1, Label: "rev-7"}, WatchedJob{Repo: repo, PR: 8, Pass: 1, Label: "rev-8"})
	h.set("rev-7", JobView{State: "running"})
	h.set("rev-8", JobView{State: "running"})
	dirty := &Dirty{}
	var buf syncBuf
	wakes := 0
	w := watchFor(h, &buf, dirty, func() { wakes++ })
	seq := dirty.Seq()
	w.Tick(context.Background())
	if wakes != 0 || dirty.Since(seq, repo, 7, "") {
		t.Fatalf("running job woke: wakes=%d", wakes)
	}
	h.set("rev-7", JobView{State: "completed", HasResult: true})
	w.Tick(context.Background())
	if wakes != 1 {
		t.Fatalf("wakes=%d, want 1", wakes)
	}
	if !dirty.Since(seq, repo, 7, "") || dirty.Since(seq, repo, 8, "") {
		t.Fatal("only the finished job's PR is dirty")
	}
	logged := buf.msgs("reviewer watch: job finished")
	if len(logged) != 1 || logged[0]["repo"] != repo || logged[0]["pr"] != float64(7) || logged[0]["label"] != "rev-7" || logged[0]["state"] != "completed" {
		t.Fatalf("transition log=%v", logged)
	}
}

// A result that arrives before the job is terminal is also a finished review.
func TestReviewerWatchWakesWhenResultIsAvailable(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	h.set("rev-7", JobView{State: "running", HasResult: true})
	wakes := 0
	watchFor(h, &syncBuf{}, &Dirty{}, func() { wakes++ }).Tick(context.Background())
	if wakes != 1 {
		t.Fatalf("wakes=%d", wakes)
	}
}

// (c) The same finished job wakes once, however many ticks it stays recorded.
func TestReviewerWatchWakesOncePerJob(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	h.set("rev-7", JobView{State: "escalated"})
	wakes := 0
	w := watchFor(h, &syncBuf{}, &Dirty{}, func() { wakes++ })
	for range 5 {
		w.Tick(context.Background())
	}
	if wakes != 1 {
		t.Fatalf("wakes=%d, want 1", wakes)
	}
	reads := h.readCount()
	if reads != 1 {
		t.Fatalf("a reported job is read again: reads=%d", reads)
	}
	// A replacement reviewer under a new label is a new job.
	h.mu.Lock()
	h.jobs = []WatchedJob{{Repo: repo, PR: 7, Pass: 1, Label: "rev-7-b"}}
	h.mu.Unlock()
	h.set("rev-7-b", JobView{State: "lost"})
	w.Tick(context.Background())
	if wakes != 2 {
		t.Fatalf("replacement job: wakes=%d, want 2", wakes)
	}
}

// (d) hk errors wake nothing, log one line per outage and never stop the role.
// Ticks are driven explicitly: the number of reads a wall-clock sleep produces
// depends on the scheduler, so the test counts reads per tick instead.
func TestReviewerWatchHKOutageLogsOnceAndRoleKeepsRunning(t *testing.T) {
	t.Run("one read per tick, one log line per outage, no wake", func(t *testing.T) {
		h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"}, WatchedJob{Repo: repo, PR: 8, Label: "rev-8"})
		h.err = errors.New("hk unreachable")
		var buf syncBuf
		wakes := 0
		w := watchFor(h, &buf, &Dirty{}, func() { wakes++ })
		// The tick stops at the first failed read, so a tick reads exactly one
		// record while hk is down, however much work the two jobs would be.
		base := h.readCount()
		for i := range 20 {
			w.Tick(context.Background())
			if reads := h.readCount() - base; reads != i+1 {
				t.Fatalf("after %d ticks reads=%d, want one per tick: the tick must stop at the first failed read", i+1, reads)
			}
		}
		if wakes != 0 {
			t.Fatalf("an hk outage woke the role: wakes=%d", wakes)
		}
		if n := len(buf.msgs("reviewer watch: hk unreadable; interval and webhook wakes continue")); n != 1 {
			t.Fatalf("outage log lines over 20 ticks=%d, want 1", n)
		}
	})

	t.Run("role keeps running, the webhook wakes it and the watch recovers", func(t *testing.T) {
		h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
		h.err = errors.New("hk unreachable")
		var buf syncBuf
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		d := Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "host-a", Log: slog.New(slog.NewJSONHandler(&buf, nil)),
			ReviewerWakeGap: 10 * time.Millisecond, Reviewers: watchFor(h, &buf, &Dirty{}, nil)}
		d.Reviewers.Every = 10 * time.Millisecond
		runs := make(chan struct{}, 16)
		done := make(chan error, 1)
		go func() {
			done <- d.Run(ctx, []Loop{{Role: "pr", Interval: time.Hour, Run: func(context.Context) error { runs <- struct{}{}; return nil }}})
		}()
		<-runs
		// Wait for the running watch to hit the outage, then show it woke nothing.
		deadline := time.After(3 * time.Second)
		for len(buf.msgs("reviewer watch: hk unreadable; interval and webhook wakes continue")) == 0 {
			select {
			case <-deadline:
				t.Fatal("the watch did not observe the outage")
			case <-time.After(time.Millisecond):
			}
		}
		select {
		case <-runs:
			t.Fatal("an hk outage woke the role")
		default:
		}
		if n := len(buf.msgs("reviewer watch: hk unreadable; interval and webhook wakes continue")); n != 1 {
			t.Fatalf("outage log lines=%d, want 1", n)
		}
		// The webhook seam still works. Wait for its pass before hk returns, so
		// the recovery wake is not coalesced into the webhook wake.
		d.Trigger("pr")
		select {
		case <-runs:
		case <-time.After(3 * time.Second):
			t.Fatal("webhook wake did not start a pass")
		}
		// hk returns; the watch sees the finished job and wakes the role.
		h.mu.Lock()
		h.err = nil
		h.mu.Unlock()
		h.set("rev-7", JobView{State: "completed"})
		select {
		case <-runs:
		case <-time.After(3 * time.Second):
			t.Fatal("role stopped waking after the outage")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

// (e) With no live reviewer jobs the watch reads nothing; and it reads at most 50 per tick.
func TestReviewerWatchReadsOnlyLiveJobsBounded(t *testing.T) {
	h := newHK()
	w := watchFor(h, &syncBuf{}, &Dirty{}, func() {})
	w.Tick(context.Background())
	if h.readCount() != 0 {
		t.Fatalf("reads=%d with no live jobs", h.readCount())
	}
	for i := range 60 {
		h.jobs = append(h.jobs, WatchedJob{Repo: repo, PR: i + 1, Label: fmt.Sprintf("rev-%d", i)})
		h.views[fmt.Sprintf("rev-%d", i)] = JobView{State: "running"}
	}
	w.Tick(context.Background())
	if h.readCount() != reviewerWatchMax {
		t.Fatalf("reads=%d, want %d", h.readCount(), reviewerWatchMax)
	}
}

// A job whose hk record does not exist yet is not an outage.
func TestReviewerWatchMissingRecordIsNotAnOutage(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	var buf syncBuf
	wakes := 0
	watchFor(h, &buf, &Dirty{}, func() { wakes++ }).Tick(context.Background())
	if wakes != 0 || len(buf.lines()) != 0 {
		t.Fatalf("wakes=%d log=%v", wakes, buf.lines())
	}
}

// passes runs a pr loop that reports the instant each pass starts.
func startPR(t *testing.T, d *Daemon, interval time.Duration) (<-chan time.Time, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runs := make(chan time.Time, 16)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "pr", Interval: interval, Run: func(context.Context) error { runs <- time.Now(); return nil }}})
	}()
	return runs, func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

// (b) A reviewer wake starts the next pass after its own short gap; a webhook wake
// still waits for WakeGap.
func TestReviewerWakeUsesShortGapWebhookKeepsMinGap(t *testing.T) {
	const minGap, reviewerGap = 900 * time.Millisecond, 100 * time.Millisecond
	t.Run("reviewer", func(t *testing.T) {
		d := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", WakeGap: minGap, ReviewerWakeGap: reviewerGap, Jitter: func() float64 { return 0 }}
		runs, stop := startPR(t, d, time.Hour)
		defer stop()
		first := <-runs
		d.TriggerReviewer("pr")
		select {
		case second := <-runs:
			if gap := second.Sub(first); gap < reviewerGap-20*time.Millisecond || gap >= minGap-200*time.Millisecond {
				t.Fatalf("reviewer-woken pass ran %v after the last, want about %v and well before min_gap %v", gap, reviewerGap, minGap)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("reviewer wake did not start a pass")
		}
	})
	t.Run("webhook", func(t *testing.T) {
		d := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", WakeGap: minGap, ReviewerWakeGap: reviewerGap, Jitter: func() float64 { return 0 }}
		runs, stop := startPR(t, d, time.Hour)
		defer stop()
		first := <-runs
		d.Trigger("pr")
		select {
		case <-runs:
			t.Fatal("webhook wake started a pass inside min_gap")
		case <-time.After(minGap - 300*time.Millisecond):
		}
		second := <-runs
		if gap := second.Sub(first); gap < minGap-20*time.Millisecond {
			t.Fatalf("webhook-woken pass ran %v after the last, want min_gap %v", gap, minGap)
		}
	})
	t.Run("reviewer wake behind a pending webhook wake pulls the pass forward", func(t *testing.T) {
		d := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", WakeGap: minGap, ReviewerWakeGap: reviewerGap, Jitter: func() float64 { return 0 }}
		runs, stop := startPR(t, d, time.Hour)
		defer stop()
		first := <-runs
		d.Trigger("pr")
		time.Sleep(50 * time.Millisecond)
		d.TriggerReviewer("pr")
		second := <-runs
		if gap := second.Sub(first); gap >= minGap-200*time.Millisecond {
			t.Fatalf("pass ran %v after the last: the reviewer wake waited behind the webhook gap", gap)
		}
	})
}

// A wake while a pass runs coalesces into one pass right after it.
func TestReviewerWakesDuringPassCoalesce(t *testing.T) {
	d := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", WakeGap: time.Hour, ReviewerWakeGap: 50 * time.Millisecond, Jitter: func() float64 { return 0 }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	count := 0
	inPass := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, []Loop{{Role: "pr", Interval: time.Hour, Run: func(context.Context) error {
			mu.Lock()
			count++
			n := count
			mu.Unlock()
			if n == 2 {
				close(inPass)
				<-release
			}
			return nil
		}}})
	}()
	for {
		mu.Lock()
		n := count
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	d.TriggerReviewer("pr")
	<-inPass
	for range 5 {
		d.TriggerReviewer("pr")
	}
	close(release)
	time.Sleep(400 * time.Millisecond)
	mu.Lock()
	n := count
	mu.Unlock()
	if n != 3 {
		t.Fatalf("passes=%d, want 3: one initial, one woken, one coalesced", n)
	}
	cancel()
	<-done
}

// End to end: a job completing in hk starts the next pass within a tick and the
// gap, with no webhook and an hour-long interval.
func TestReviewerWatchStartsNextPass(t *testing.T) {
	h := newHK(WatchedJob{Repo: repo, PR: 7, Label: "rev-7"})
	h.set("rev-7", JobView{State: "running"})
	dirty := &Dirty{}
	d := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", WakeGap: time.Hour, ReviewerWakeGap: 50 * time.Millisecond, Jitter: func() float64 { return 0 },
		Reviewers: watchFor(h, &syncBuf{}, dirty, nil)}
	d.Reviewers.Every = 20 * time.Millisecond
	runs, stop := startPR(t, d, time.Hour)
	defer stop()
	<-runs
	seq := dirty.Seq()
	h.set("rev-7", JobView{State: "completed", HasResult: true})
	select {
	case <-runs:
	case <-time.After(3 * time.Second):
		t.Fatal("a finished reviewer job did not start a pass")
	}
	if !dirty.Since(seq, repo, 7, "") {
		t.Fatal("the PR was not marked dirty before the pass")
	}
}

// (f) The pass log line names what started the pass.
func TestPassLogNamesWakeSource(t *testing.T) {
	var buf syncBuf
	d := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", Log: slog.New(slog.NewJSONHandler(&buf, nil)),
		WakeGap: 10 * time.Millisecond, ReviewerWakeGap: 10 * time.Millisecond, Jitter: func() float64 { return 0 }}
	runs, stop := startPR(t, d, time.Hour)
	<-runs
	d.Trigger("pr")
	<-runs
	d.TriggerReviewer("pr")
	<-runs
	stop()
	var got []string
	for _, m := range buf.msgs("loop completed") {
		got = append(got, fmt.Sprint(m["wake"]))
	}
	if len(got) < 3 || got[0] != "interval" || got[1] != "webhook" || got[2] != "reviewer" {
		t.Fatalf("wake sources=%v, want interval, webhook, reviewer", got)
	}
	// An interval that fires with no wake is named too.
	var buf2 syncBuf
	d2 := &Daemon{StateDir: t.TempDir(), LockDir: t.TempDir(), Host: "h", Log: slog.New(slog.NewJSONHandler(&buf2, nil)), Jitter: func() float64 { return 0 }}
	runs2, stop2 := startPR(t, d2, 20*time.Millisecond)
	<-runs2
	<-runs2
	stop2()
	for _, m := range buf2.msgs("loop completed") {
		if m["wake"] != "interval" {
			t.Fatalf("interval pass named %v", m["wake"])
		}
	}
}
