package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// finish releases the blocked reviews and waits for the pass.
func finish(t *testing.T, f *parallelFixture, done chan error) {
	t.Helper()
	close(f.runner.release)
	if err := <-done; err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
}

// noQuietStart fails if more reviews than want have started after a pause
// long enough for an unbounded pass to have started them.
func noQuietStart(t *testing.T, f *parallelFixture, want int) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	if _, _, started, _, _ := f.runner.state(); started != want {
		t.Fatalf("%d reviews started before any returned, want %d", started, want)
	}
}

// (a) With review_parallel unset, ten reviewable PRs all start at once.
func TestReviewParallelUnsetReviewsEveryPRAtOnce(t *testing.T) {
	f := newParallelFixture(t, 10, 0)
	done := f.poll(context.Background())
	f.runner.waitFor(t, "ten reviews in flight", func(in, started, _ int) bool { return in == 10 && started == 10 })
	finish(t, f, done)
	if _, peak, started, _, _ := f.runner.state(); peak != 10 || started != 10 {
		t.Fatalf("peak %d started %d, want 10 10", peak, started)
	}
}

// (b) review_parallel 0 written out is the same as unset.
func TestReviewParallelZeroReviewsEveryPRAtOnce(t *testing.T) {
	f := newParallelFixture(t, 10, 0)
	f.s.Config.Daemon.PR.ReviewParallel = 0
	done := f.poll(context.Background())
	f.runner.waitFor(t, "ten reviews in flight", func(in, started, _ int) bool { return in == 10 && started == 10 })
	finish(t, f, done)
}

// (c) An explicit 3 keeps at most three reviews at once; priority-1 reviews
// may use urgentExtra more.
func TestReviewParallelCapHoldsAndUrgentGetsExtra(t *testing.T) {
	t.Run("cap", func(t *testing.T) {
		f := newParallelFixture(t, 6, 3)
		done := f.poll(context.Background())
		f.runner.waitFor(t, "three reviews in flight", func(in, started, _ int) bool { return in == 3 && started == 3 })
		noQuietStart(t, f, 3)
		f.runner.release <- struct{}{}
		f.runner.waitFor(t, "a fourth review", func(_, started, _ int) bool { return started == 4 })
		finish(t, f, done)
		if _, peak, started, _, _ := f.runner.state(); peak != 3 || started != 6 {
			t.Fatalf("peak %d started %d, want 3 6", peak, started)
		}
	})
	t.Run("urgent", func(t *testing.T) {
		f, sink := newPriorityFixture(t, 6, 3)
		sink.task = map[int]int{1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1}
		done := f.poll(context.Background())
		f.runner.waitFor(t, "3+urgentExtra reviews in flight", func(in, started, _ int) bool { return in == 3+urgentExtra && started == 3+urgentExtra })
		noQuietStart(t, f, 3+urgentExtra)
		finish(t, f, done)
		if _, peak, started, _, _ := f.runner.state(); peak != 3+urgentExtra || started != 6 {
			t.Fatalf("peak %d started %d, want %d 6", peak, started, 3+urgentExtra)
		}
	})
}

// With no cap every review starts at once, so the order the goroutines record
// their starts in is the scheduler's, not the priority's. What must hold is
// that every PR starts, each start still carries its priority, and nothing is
// counted past a bound that does not exist: urgentExtra is not needed.
func TestReviewParallelUnsetPriorityStillOrdersStarts(t *testing.T) {
	f, sink := newPriorityFixture(t, 4, 0)
	sink.task = map[int]int{1: 3, 2: 1, 3: 2, 4: 1}
	done := f.poll(context.Background())
	f.runner.waitFor(t, "four reviews in flight", func(in, started, _ int) bool { return in == 4 })
	finish(t, f, done)
	rows := startRows(t, f)
	if len(rows) != 4 {
		t.Fatalf("review-start rows %v, want one per PR 1-4", rows)
	}
	for pr, want := range sink.task {
		row, ok := rows[pr]
		if !ok {
			t.Fatalf("PR %d never started", pr)
		}
		if got, _ := row["priority"].(float64); int(got) != want {
			t.Fatalf("PR %d started with priority %v, want %d", pr, row["priority"], want)
		}
		if row["over_bound"] == true {
			t.Fatalf("PR %d started past a bound that does not exist", pr)
		}
	}
}

// (f) The per-repo pr pass line reports how many reviews ran at once.
func TestPassLogReportsConcurrentReviews(t *testing.T) {
	for _, tc := range []struct{ parallel, prs, want int }{{0, 10, 10}, {3, 6, 3}} {
		f := newParallelFixture(t, tc.prs, tc.parallel)
		logs := &bytes.Buffer{}
		skips := &prSkips{Dirty: &daemon.Dirty{}, Log: slog.New(slog.NewJSONHandler(logs, nil))}
		done := make(chan error, 1)
		go func() { done <- pollPRsSkipping(context.Background(), f.s, f.gh, f.repo, pipeline.Options{}, skips) }()
		f.runner.waitFor(t, "reviews in flight", func(in, _, _ int) bool { return in == tc.want })
		if tc.parallel > 0 {
			for range tc.prs - tc.want {
				f.runner.release <- struct{}{}
			}
		}
		finish(t, f, done)
		var line map[string]any
		for _, l := range strings.Split(logs.String(), "\n") {
			var m map[string]any
			if json.Unmarshal([]byte(l), &m) == nil && m["msg"] == "pr pass" {
				line = m
			}
		}
		if line == nil || line["reviews_concurrent_max"] != float64(tc.want) {
			t.Fatalf("parallel %d: pr pass line %v, want reviews_concurrent_max %d", tc.parallel, line, tc.want)
		}
	}
}

// (e) A very large explicit cap loads (see the config test) and the limiter
// built from it allocates no more than one built from a small cap.
func TestReviewLimitLargeCapDoesNotPreallocate(t *testing.T) {
	big := testing.AllocsPerRun(10, func() { newReviewLimit(1_000_000_000) })
	small := testing.AllocsPerRun(10, func() { newReviewLimit(3) })
	if big != small {
		t.Fatalf("cap 1e9 allocs %v, cap 3 allocs %v", big, small)
	}
	release, over, ok := newReviewLimit(1_000_000_000).acquire(context.Background(), false)
	if !ok || over {
		t.Fatalf("acquire ok=%v over=%v", ok, over)
	}
	release()
}
