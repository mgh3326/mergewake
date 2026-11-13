package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// interruptedReviewer runs a first review whose spawn wait ends early (panewire
// exit 85) while the reviewer job stays claimed, and expires the pass lease so
// the next run may take the pass again.
func interruptedReviewer(t *testing.T) (*Service, *fakeGH, config.Repo, *panewireRunner, *clock) {
	t.Helper()
	runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
	s, g, r := panewireSetup(t, runner)
	s.Config.Notifier.Lane = "desk"
	c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	err := s.Review(context.Background(), r, 7, "", Options{})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 0 {
		t.Fatalf("first run spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	rj := readReviewerJob(t, s, r, 1)
	if rj.Label != arg(runner.spawns[0], "-l") || !slices.Equal(panewireSpawnArgs(rj.Args), runner.spawns[0]) || rj.Head != shaA || rj.Checkout != arg(runner.spawns[0], "-c") {
		t.Fatalf("record %+v for spawn %v", rj, runner.spawns[0])
	}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	return s, g, r, runner, c
}

func readReviewerJob(t *testing.T, s *Service, r config.Repo, pass int) reviewerJob {
	t.Helper()
	var rj reviewerJob
	ok, err := fsutil.ReadJSON(s.reviewerJobPath(r.Name, 7, pass), &rj)
	if err != nil || !ok {
		t.Fatalf("reviewer job record pass %d: ok=%v err=%v", pass, ok, err)
	}
	return rj
}

func reviewerJobGone(t *testing.T, s *Service, r config.Repo, pass int) {
	t.Helper()
	if _, err := os.Stat(s.reviewerJobPath(r.Name, 7, pass)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer job record pass %d still present: %v", pass, err)
	}
}

func auditRows(t *testing.T, s *Service, r config.Repo, command string) []map[string]any {
	t.Helper()
	path := filepath.Join(s.state(r.Name), "decisions.jsonl")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	_, rows := readLines(t, path)
	var out []map[string]any
	for _, row := range rows {
		if row["command"] == command {
			out = append(out, row["value"].(map[string]any))
		}
	}
	return out
}

// withoutWait drops the --wait-timeout value, which a replay bounds by the
// time left; every other argument must be the recorded one.
func withoutWait(args []string) []string {
	out := slices.Clone(args)
	if i := slices.Index(out, "--wait-timeout"); i >= 0 {
		out = slices.Delete(out, i, i+2)
	}
	return out
}

// (a) A wait cut short leaves the job claimed: the next run replays the
// recorded spawn once, with the same label and arguments, and posts the
// result once the job completes.
func TestReviewerJobResumeReplaysSpawn(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	// A later run: a fresh label would carry a later second. Still inside
	// the stale bound (twice the 5s review.timeout).
	c.t = c.t.Add(2 * time.Second)
	runner.afterRun = func() {
		runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || runner.headless != 0 || g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	first, replay := runner.spawns[0], runner.spawns[1]
	if arg(replay, "--author-family") != "anthropic" || slices.Contains(replay, "--builder-family") {
		t.Fatalf("replayed spawn did not use the current family flag: %v", replay)
	}
	if !slices.Equal(withoutWait(first), withoutWait(replay)) || arg(replay, "-l") != arg(first, "-l") {
		t.Fatalf("replay changed the spawn:\n%v\n%v", first, replay)
	}
	was, _ := strconv.Atoi(arg(first, "--wait-timeout"))
	now, err := strconv.Atoi(arg(replay, "--wait-timeout"))
	if err != nil || now > was || now < 1 {
		t.Fatalf("--wait-timeout %d then %q", was, arg(replay, "--wait-timeout"))
	}
	if got := g.passes[shaA]; len(got) != 1 || got[0].Executor != "panewire" || got[0].Family != "openai" {
		t.Fatalf("%+v", got)
	}
	reviewerJobGone(t, s, r, 1)
}

func TestReviewerJobResumeBoundsBuilderFamily(t *testing.T) {
	for _, tc := range []struct {
		name, saved, want string
	}{
		{name: "unaccepted", saved: "cognition", want: "other"},
		{name: "accepted", saved: "google", want: "google"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, r, runner, c := interruptedReviewer(t)
			job := readReviewerJob(t, s, r, 1)
			job.Args[slices.Index(job.Args, "--builder-family")+1] = tc.saved
			if err := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, 1), job); err != nil {
				t.Fatal(err)
			}
			c.t = c.t.Add(2 * time.Second)
			runner.afterRun = func() {
				runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			}
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(runner.spawns) != 2 {
				t.Fatalf("spawns=%d, want resumed launch", len(runner.spawns))
			}
			first, replay := runner.spawns[0], runner.spawns[1]
			if got := arg(replay, "--author-family"); got != tc.want {
				t.Fatalf("replayed --author-family=%q, want %q; args=%v", got, tc.want, replay)
			}
			want := slices.Clone(first)
			want[slices.Index(want, "--author-family")+1] = tc.want
			if !slices.Equal(withoutWait(want), withoutWait(replay)) || arg(first, "-l") != arg(replay, "-l") {
				t.Fatalf("replay changed saved arguments beyond family and wait timeout:\nwant %v\ngot  %v", want, replay)
			}
		})
	}
}

// (b) A recorded job that completed with a result is read and posted without
// any spawn.
func TestReviewerJobCompletedPostsWithoutSpawn(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	c.t = c.t.Add(2 * time.Second)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 1 || !g.refs[doneRef(7, shaA)] {
		t.Fatalf("spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	if got := g.passes[shaA]; len(got) != 1 || got[0].Executor != "panewire" || got[0].Family != "openai" {
		t.Fatalf("%+v", got)
	}
	reviewerJobGone(t, s, r, 1)
}

// (c) A recorded job that escalated is held as today, with no spawn, and its
// record is removed.
func TestReviewerJobEscalatedHoldsAndDrops(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	runner.sink.missing = &integrations.JobRecord{State: "escalated"}
	label := arg(runner.spawns[0], "-l")
	// Today's hold for an escalated record: the pass skips with the record's
	// state, the same skip reason a fresh spawn's escalated record gives, and
	// under the panewire executor a skip fails the pass with nothing headless.
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), "reviewer job record escalated") {
		t.Fatalf("err=%v", err)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 0 || len(g.passes[shaA]) != 0 {
		t.Fatalf("spawns=%d headless=%d posts=%d passes=%+v", len(runner.spawns), runner.headless, g.postCount, g.passes[shaA])
	}
	cleanups := auditRows(t, s, r, "reviewer-cleanup")
	if len(cleanups) == 0 || cleanups[len(cleanups)-1]["label"] != label || cleanups[len(cleanups)-1]["result"] == "closed" {
		t.Fatalf("pane not kept: %v", cleanups)
	}
	reviewerJobGone(t, s, r, 1)
}

// (d) A recorded job live past the bound is held: one reviewer-stale audit
// row and one desk notice across runs, and no new label or replay.
func TestReviewerJobStaleNotifiesOnce(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	s.Config.Review.Panewire.StaleAfter = "1s"
	c.t = c.t.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		err := s.Review(context.Background(), r, 7, "", Options{})
		if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), "still claimed") {
			t.Fatalf("run %d: %v", i, err)
		}
		g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 0 {
		t.Fatalf("spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	stale := auditRows(t, s, r, "reviewer-stale")
	if len(stale) != 1 || stale[0]["label"] != arg(runner.spawns[0], "-l") || stale[0]["state"] != "claimed" {
		t.Fatalf("stale audit=%v", stale)
	}
	n := s.Notifier.(*fakeNotifier)
	if n.count != 1 || n.events[0].Lane != "desk" || !strings.Contains(n.events[0].Text, arg(runner.spawns[0], "-l")) {
		t.Fatalf("events=%v", n.events)
	}
	readReviewerJob(t, s, r, 1)
	// The bound is configurable.
	s.Config.Review.Panewire.StaleAfter = "2h"
	runner.afterRun = func() {
		runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") != arg(runner.spawns[0], "-l") || g.postCount != 1 {
		t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
	}
}

// (e) A fresh spawn refused because another reviewer job of the task holds
// the reservation is a held pass: a reviewer-held audit row, a pass failure
// and no headless reviewer.
func TestReviewerHeldIsNotASkip(t *testing.T) {
	runner := &panewireRunner{family: "openai", refuse: map[string]int{"reviewer": panewireHeld}}
	s, g, r := panewireSetup(t, runner)
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), "holds the reviewer reservation") {
		t.Fatalf("err=%v", err)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 0 || len(g.passes[shaA]) != 0 {
		t.Fatalf("spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	held := auditRows(t, s, r, "reviewer-held")
	if len(held) != 1 || held[0]["label"] != arg(runner.spawns[0], "-l") || held[0]["task"] != "42" || held[0]["pass"] != float64(1) {
		t.Fatalf("held audit=%v", held)
	}
	reviewerJobGone(t, s, r, 1)
	// RetryReview counts no failure and no backoff: the next evaluation retries.
	c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewHeld) {
		t.Fatalf("retry err=%v, want ErrReviewHeld", err)
	}
	var state reviewRetryState
	if _, err := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), &state); err != nil || state.Failures != 0 || state.Backoffs != 0 || !state.Held || runner.headless != 0 {
		t.Fatalf("retry state %+v err=%v headless=%d", state, err, runner.headless)
	}
}

// (f) Records left by an older head are ignored and dropped; the new head
// spawns a fresh reviewer.
func TestReviewerJobOldHeadDropped(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r := panewireSetup(t, runner)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}
	old := reviewerJob{PR: 7, Head: shaB, Label: "mwr-7-" + shaB[:7] + "-p1-1", Checkout: "/nowhere", Args: []string{"spawn", "-l", "mwr-7-old"}, Recorded: time.Now()}
	for _, pass := range []int{1, 2} {
		old.Pass = pass
		if err := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, pass), old); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 1 {
		t.Fatalf("spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	if l := arg(runner.spawns[0], "-l"); !strings.HasPrefix(l, "mwr-7-"+shaA[:7]+"-p1-") {
		t.Fatalf("label %s", l)
	}
	// Pass 2 never runs on this single-pass head, so only the old-head drop
	// removes its record.
	reviewerJobGone(t, s, r, 1)
	reviewerJobGone(t, s, r, 2)
}

func TestReviewerJobActiveLeaseCompleted(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	claims := g.claimCount
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.postCount != 1 || len(runner.spawns) != 1 || g.claimCount != claims+1 {
		t.Fatalf("posts=%d spawns=%d", g.postCount, len(runner.spawns))
	}
	reviewerJobGone(t, s, r, 1)
}

func TestReviewerJobActiveLeaseLive(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	claims := g.claimCount
	runner.afterRun = func() {
		runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || !slices.Equal(withoutWait(runner.spawns[0]), withoutWait(runner.spawns[1])) || g.claimCount != claims+1 || g.postCount != 1 {
		t.Fatalf("spawns=%v claims=%d posts=%d", runner.spawns, g.claimCount, g.postCount)
	}
}

func TestReviewerJobActiveLeaseNoRecord(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	if err := s.dropReviewerJob(r.Name, 7, 1); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	s.Out = &output
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"action":"review-pending"`) {
		t.Fatalf("output: %s", output.String())
	}
	if len(runner.spawns) != 1 || g.postCount != 0 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
}

func TestReviewerJobActiveLeaseOldHead(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	runner.sink.missing = &integrations.JobRecord{State: "claimed", OwnerLane: "owner"}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	job := readReviewerJob(t, s, r, 1)
	job.Head = shaB
	if err := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, 1), job); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	s.Out = &output
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"action":"review-pending"`) {
		t.Fatalf("output: %s", output.String())
	}
	if len(runner.spawns) != 1 || g.postCount != 0 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	// The preflight only reads: the older head's record and job are left for
	// the pass that runs.
	if len(runner.closes) != 0 || len(auditRows(t, s, r, "reviewer-head-moved")) != 0 {
		t.Fatalf("closes=%v", runner.closes)
	}
	if rj := readReviewerJob(t, s, r, 1); rj.Head != shaB {
		t.Fatalf("record %+v", rj)
	}
}

func TestReviewerJobExpiredLeaseClaimsAndResumes(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	claims := g.claimCount
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if g.claimCount != claims+2 || g.postCount != 1 || len(runner.spawns) != 1 {
		t.Fatalf("claims=%d posts=%d spawns=%d", g.claimCount, g.postCount, len(runner.spawns))
	}
	reviewerJobGone(t, s, r, 1)
}

func TestReviewerJobConcurrentResumePostsOnce(t *testing.T) {
	s, g, r, runner, _ := interruptedReviewer(t)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; errs <- s.Review(context.Background(), r, 7, "", Options{}) }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if g.postCount != 1 || len(runner.spawns) != 1 {
		t.Fatalf("posts=%d spawns=%d", g.postCount, len(runner.spawns))
	}
	reviewerJobGone(t, s, r, 1)
}

func TestReviewerJobActiveLeaseDifferentCommit(t *testing.T) {
	for _, lease := range []string{"other-lease", ""} {
		t.Run(lease, func(t *testing.T) {
			s, g, r, runner, _ := interruptedReviewer(t)
			g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
			job := readReviewerJob(t, s, r, 1)
			job.LeaseSHA = lease
			if err := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, 1), job); err != nil {
				t.Fatal(err)
			}
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			var output bytes.Buffer
			s.Out = &output
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), `"action":"review-pending"`) || g.postCount != 0 || len(runner.spawns) != 1 {
				t.Fatalf("output=%s posts=%d spawns=%d", output.String(), g.postCount, len(runner.spawns))
			}
		})
	}
}

func TestReviewerPassWaitIsBounded(t *testing.T) {
	runner := &panewireRunner{exits: []int{panewireWaitTimeout}}
	s, _, r := panewireSetup(t, runner)
	s.Config.Review.Timeout = "25m"
	_ = s.Review(context.Background(), r, 7, "", Options{})
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%d", len(runner.spawns))
	}
	wait, err := strconv.Atoi(arg(runner.spawns[0], "--wait-timeout"))
	if err != nil || wait > 90 {
		t.Fatalf("wait=%d, want at most 90 seconds (err=%v)", wait, err)
	}
	readReviewerJob(t, s, r, 1)
}

func TestReviewerFinishedWithoutResultIsReplaced(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := newFakeReapHub()
	hub.agents["panes"] = []any{map[string]any{"pane_id": "p1", "cwd": "/repo/" + old.Label}}
	hub.agents["agents"] = []any{map[string]any{"pane_id": "p1", "name": old.Label, "agent_status": "done", "cwd": "/repo/" + old.Label}}
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	c.t = c.t.Add(time.Second)
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 1 || runner.closes[0][2] != old.Label || len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
		t.Fatalf("closes=%v spawns=%v posts=%d", runner.closes, runner.spawns, g.postCount)
	}
	if len(auditRows(t, s, r, "review-result-lost")) != 1 {
		t.Fatal("missing lost audit")
	}
}

func TestReviewerLiveLeaseRenewsAndExcludesOtherHost(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	ref := claimRef(7, shaA, 1)
	s.Config.Review.Timeout = "1h"
	runner.exits = []int{panewireWaitTimeout, panewireWaitTimeout}
	for i := 0; i < 2; i++ {
		c.t = c.t.Add(time.Minute)
		g.leaseTimes[ref] = time.Now().Add(-30 * time.Minute)
		before := g.leaseTimes[ref]
		if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
			t.Fatal(err)
		}
		if !g.leaseTimes[ref].After(before) {
			t.Fatal("lease not renewed")
		}
	}
	if err := s.dropReviewerJob(r.Name, 7, 1); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	s.Out = &output
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"action":"review-pending"`) || len(runner.spawns) != 3 {
		t.Fatalf("output=%s spawns=%d", output.String(), len(runner.spawns))
	}
}

func TestReviewerSecondLostResultDoesNotSpawnThird(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	old := readReviewerJob(t, s, r, 1)
	hub := newFakeReapHub()
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	done := func(job reviewerJob) {
		pane := map[string]any{"pane_id": "p1", "name": job.Label, "agent_status": "done", "cwd": "/repo/" + job.Label}
		hub.agents["panes"] = []any{pane}
		hub.agents["agents"] = []any{pane}
		runner.sink.jobs[job.Label] = integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
	}
	done(old)
	runner.exits = []int{panewireWaitTimeout}
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
	c.t = c.t.Add(time.Second)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacement := readReviewerJob(t, s, r, 1)
	done(replacement)
	c.t = c.t.Add(time.Second)
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || len(runner.spawns) != 2 || len(runner.closes) != 2 {
		t.Fatalf("err=%v spawns=%d closes=%d", err, len(runner.spawns), len(runner.closes))
	}
}

func TestReviewerLeaseAcrossReplacementOutlivesOriginalTTL(t *testing.T) {
	s, g, r, runner, c := interruptedReviewer(t)
	start := c.t
	old := readReviewerJob(t, s, r, 1)
	hub := newFakeReapHub()
	pane := map[string]any{"pane_id": "p1", "name": old.Label, "agent_status": "done", "cwd": "/repo/" + old.Label}
	hub.agents["panes"] = []any{pane}
	hub.agents["agents"] = []any{pane}
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	runner.sink.jobs[old.Label] = integrations.JobRecord{State: "claimed", Host: "node-a", PaneID: "p1", OwnerLane: "owner"}
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	runner.exits = []int{panewireWaitTimeout, panewireWaitTimeout}
	ref := claimRef(7, shaA, 1)
	c.t = c.t.Add(4 * time.Second)
	g.leaseTimes[ref] = time.Now().Add(-4 * time.Second)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	replacement := readReviewerJob(t, s, r, 1)
	if replacement.Label == old.Label || replacement.LeaseSHA != g.leaseIDs[ref] {
		t.Fatalf("replacement=%+v lease=%s", replacement, g.leaseIDs[ref])
	}
	c.t = c.t.Add(3 * time.Second)
	g.leaseTimes[ref] = time.Now().Add(-3 * time.Second)
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	current := readReviewerJob(t, s, r, 1)
	if c.t.Sub(start) <= 6*time.Second || current.LeaseSHA != g.leaseIDs[ref] || current.LeaseSHA == replacement.LeaseSHA {
		t.Fatalf("elapsed=%s record=%+v lease=%s", c.t.Sub(start), current, g.leaseIDs[ref])
	}
	other := *s
	other.Config.StateDir = t.TempDir()
	var output bytes.Buffer
	other.Out = &output
	if err := other.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"action":"review-pending"`) || len(runner.spawns) != 3 {
		t.Fatalf("output=%s spawns=%d", output.String(), len(runner.spawns))
	}
}

// movedHeadSetup records a reviewer job of an older head for both passes and
// answers the new head's own job with a finished review.
func movedHeadSetup(t *testing.T, runner *panewireRunner, old integrations.JobRecord) (*Service, *fakeGH, config.Repo, []string) {
	t.Helper()
	s, g, r := panewireSetup(t, runner)
	runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}
	var labels []string
	for _, pass := range []int{1, 2} {
		label := fmt.Sprintf("mwr-7-%s-p%d-1", shaB[:7], pass)
		labels = append(labels, label)
		runner.sink.jobs[label] = old
		rj := reviewerJob{PR: 7, Head: shaB, Pass: pass, Label: label, Checkout: "/nowhere", Args: []string{"spawn", "-l", label}, Recorded: time.Now()}
		if err := fsutil.WriteJSON(s.reviewerJobPath(r.Name, 7, pass), rj); err != nil {
			t.Fatal(err)
		}
	}
	return s, g, r, labels
}

// movedHeadClosed checks the closes, in order: each is the abandoned close of
// the named label by the job's owner lane.
func movedHeadClosed(t *testing.T, runner *panewireRunner, labels ...string) {
	t.Helper()
	if len(runner.closes) != len(labels) {
		t.Fatalf("closes=%v, want %v", runner.closes, labels)
	}
	for i, label := range labels {
		want := []string{"job", "close", label, "--lane", "owner", "--outcome", "abandoned", "--reason", "PR head moved"}
		if !slices.Equal(runner.closes[i], want) {
			t.Fatalf("close %v, want %v", runner.closes[i], want)
		}
	}
}

// (a) A live job of an older head is closed before its record is dropped, so
// the new head's reviewer is not refused for the held reservation.
func TestReviewerHeadMovedClosesLiveJob(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	movedHeadClosed(t, runner, labels...)
	if len(runner.spawns) != 1 || g.postCount != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	if l := arg(runner.spawns[0], "-l"); !strings.HasPrefix(l, "mwr-7-"+shaA[:7]+"-p1-") {
		t.Fatalf("label %s", l)
	}
	rows := auditRows(t, s, r, "reviewer-head-moved")
	if len(rows) != 2 || rows[0]["old_head"] != shaB || rows[0]["new_head"] != shaA || rows[0]["label"] != labels[0] || rows[1]["label"] != labels[1] {
		t.Fatalf("audit=%v", rows)
	}
	reviewerJobGone(t, s, r, 1)
	reviewerJobGone(t, s, r, 2)
}

// (b) A completed job of an older head holds no reservation: no close.
func TestReviewerHeadMovedCompletedJobNotClosed(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r, _ := movedHeadSetup(t, runner, integrations.JobRecord{State: "completed", OwnerLane: "owner"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 1 || g.postCount != 1 || len(auditRows(t, s, r, "reviewer-head-moved")) != 0 {
		t.Fatalf("closes=%v spawns=%d posts=%d", runner.closes, len(runner.spawns), g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
	reviewerJobGone(t, s, r, 2)
}

// A job record hk does not have holds nothing either.
func TestReviewerHeadMovedMissingJobDropped(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, _, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{})
	for _, label := range labels {
		delete(runner.sink.jobs, label)
	}
	runner.sink.missing = nil
	// Any id but the new head's label is unknown to hk; the new head's job
	// is answered once it is spawned.
	runner.afterRun = func() {
		runner.sink.missing = &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"findings":[]}`)}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 1 {
		t.Fatalf("closes=%v spawns=%d", runner.closes, len(runner.spawns))
	}
	reviewerJobGone(t, s, r, 2)
}

// (c) A close that fails is audited, not fatal: the record is dropped and the
// new head's reviewer spawns in the same pass.
func TestReviewerHeadMovedCloseFailureIsSkipped(t *testing.T) {
	runner := &panewireRunner{family: "openai", closeErrs: []error{panewireExit(1)}}
	s, g, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 1 || g.postCount != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
	reviewerJobGone(t, s, r, 2)
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 1 || rows[0]["label"] != labels[0] || rows[0]["head"] != shaB || rows[0]["reason"] == "" {
		t.Fatalf("skipped audit=%v", rows)
	}
	if rows := auditRows(t, s, r, "reviewer-head-moved"); len(rows) != 1 || rows[0]["label"] != labels[1] {
		t.Fatalf("head-moved audit=%v", rows)
	}
}

// (d) A fresh spawn refused as held, after this run closed the older head's
// job, closes it once more and spawns again once.
func TestReviewerHeadMovedHeldSpawnRetriesOnce(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld}}
	s, g, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	// Both records are closed up front; the refusal closes each again.
	movedHeadClosed(t, runner, labels[0], labels[1], labels[0], labels[1])
	if len(runner.spawns) != 2 || g.postCount != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	if !slices.Equal(runner.spawns[0], runner.spawns[1]) {
		t.Fatalf("retry changed the spawn:\n%v\n%v", runner.spawns[0], runner.spawns[1])
	}
	if len(auditRows(t, s, r, "reviewer-held")) != 0 {
		t.Fatal("a held spawn that succeeded on retry is not a held pass")
	}
	reviewerJobGone(t, s, r, 1)
}

// A second refusal is a held pass: the spawn is retried once, not forever.
func TestReviewerHeadMovedHeldTwiceFailsPass(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld, panewireHeld}}
	s, g, r, _ := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	err := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(err, ErrReviewUnavailable) || len(runner.spawns) != 2 || g.postCount != 0 {
		t.Fatalf("err=%v spawns=%d posts=%d", err, len(runner.spawns), g.postCount)
	}
	reviewerJobGone(t, s, r, 1)
}

// (e) A record for the current head is untouched: its job is replayed, not
// closed.
func TestReviewerCurrentHeadRecordNotClosed(t *testing.T) {
	s, _, r, runner, c := interruptedReviewer(t)
	runner.sink.missing = &integrations.JobRecord{State: "claimed", OwnerLane: "owner"}
	c.t = c.t.Add(2 * time.Second)
	runner.afterRun = func() {
		runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 2 || len(auditRows(t, s, r, "reviewer-head-moved")) != 0 {
		t.Fatalf("closes=%v spawns=%d", runner.closes, len(runner.spawns))
	}
}

// A dry run never acts on the older head's job: the record stays, so a later
// acting run closes the job.
func TestReviewerHeadMovedDryRunLeavesJobAndRecord(t *testing.T) {
	runner := &panewireRunner{family: "openai"}
	s, g, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	g.refs = map[string]bool{claimRef(7, shaA, 1): true}
	g.leaseTimes = map[string]time.Time{claimRef(7, shaA, 1): time.Now()}
	if err := s.Review(context.Background(), r, 7, "", Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if len(runner.closes) != 0 || len(runner.spawns) != 0 || len(auditRows(t, s, r, "reviewer-head-moved")) != 0 {
		t.Fatalf("closes=%v spawns=%d", runner.closes, len(runner.spawns))
	}
	for i, pass := range []int{1, 2} {
		if rj := readReviewerJob(t, s, r, pass); rj.Label != labels[i] {
			t.Fatalf("record %+v", rj)
		}
	}
}

// A repeat close that fails is audited like the first: the spawn is retried
// once and the records are dropped.
func TestReviewerHeadMovedRepeatCloseFailureIsSkipped(t *testing.T) {
	runner := &panewireRunner{family: "openai", exits: []int{panewireHeld}, closeErrs: []error{nil, nil, panewireExit(1)}}
	s, g, r, labels := movedHeadSetup(t, runner, integrations.JobRecord{State: "claimed", OwnerLane: "owner"})
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || g.postCount != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	if rows := auditRows(t, s, r, "reviewer-close-skipped"); len(rows) != 1 || rows[0]["label"] != labels[0] {
		t.Fatalf("skipped audit=%v", rows)
	}
	reviewerJobGone(t, s, r, 1)
	reviewerJobGone(t, s, r, 2)
}
