package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	if !errors.Is(err, ErrReviewUnavailable) || !strings.Contains(err.Error(), "the next run resumes it") {
		t.Fatalf("first run: %v", err)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 0 {
		t.Fatalf("first run spawns=%d headless=%d posts=%d", len(runner.spawns), runner.headless, g.postCount)
	}
	rj := readReviewerJob(t, s, r, 1)
	if rj.Label != arg(runner.spawns[0], "-l") || !slices.Equal(rj.Args, runner.spawns[0]) || rj.Head != shaA || rj.Checkout != arg(runner.spawns[0], "-c") {
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
	c.t = c.t.Add(time.Hour) // review.timeout is 5s: well past twice it
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
	// RetryReview counts it as a failed attempt and backs off.
	c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("retry err=%v", err)
	}
	var state reviewRetryState
	if _, err := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-retry-7.json"), &state); err != nil || state.Failures != 1 || runner.headless != 0 {
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
	reviewerJobGone(t, s, r, 1)
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
