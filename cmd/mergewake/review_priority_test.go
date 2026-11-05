package main

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// prioritySink answers hk priority reads for the fixture's PRs. Task 1000+n
// belongs to PR n; a change is keyed by its PR URL.
type prioritySink struct {
	parallelSink
	mu          sync.Mutex
	task        map[int]int // PR number → task priority
	change      map[int]int // PR number → change priority
	failTask    map[int]bool
	failChange  map[int]bool
	taskReads   map[int]int
	changeReads map[int]int
}

func (p *prioritySink) TaskPriority(_ context.Context, task string) (int, error) {
	n, _ := strconv.Atoi(task)
	n -= 1000
	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskReads[n]++
	if p.failTask[n] {
		return 0, errors.New("hk unreachable")
	}
	return p.task[n], nil
}

func (p *prioritySink) ChangePriority(_ context.Context, pr string) (int, error) {
	n, _ := strconv.Atoi(pr[strings.LastIndex(pr, "/")+1:])
	p.mu.Lock()
	defer p.mu.Unlock()
	p.changeReads[n]++
	if p.failChange[n] {
		return 0, errors.New("hk unreachable")
	}
	return p.change[n], nil
}

func newPriorityFixture(t *testing.T, prs, parallel int) (*parallelFixture, *prioritySink) {
	t.Helper()
	f := newParallelFixture(t, prs, parallel)
	f.hub.taskPerPR = true
	sink := &prioritySink{task: map[int]int{}, change: map[int]int{}, failTask: map[int]bool{}, failChange: map[int]bool{}, taskReads: map[int]int{}, changeReads: map[int]int{}}
	f.s.Sink = sink
	return f, sink
}

// startOrder is the PR numbers in the order their reviews began.
func startOrder(events []string) []string {
	var out []string
	for _, e := range events {
		if pr, ok := strings.CutPrefix(e, "review "); ok {
			out = append(out, pr)
		}
	}
	return out
}

// startRows reads the review-start audit rows the pass printed, by PR.
func startRows(t *testing.T, f *parallelFixture) map[int]map[string]any {
	t.Helper()
	rows := map[int]map[string]any{}
	for _, line := range strings.Split(f.out.String(), "\n") {
		var rec struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Command != "review-start" {
			continue
		}
		rows[int(rec.Value["pr"].(float64))] = rec.Value
	}
	return rows
}

func runPass(t *testing.T, f *parallelFixture) {
	t.Helper()
	close(f.runner.release)
	if err := <-f.poll(context.Background()); err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
}

// (a) Three PRs with task priorities 3, 1, 2 and review_parallel 1 start in
// priority order, not list order.
func TestPollPRsStartsReviewsInTaskPriorityOrder(t *testing.T) {
	f, sink := newPriorityFixture(t, 3, 1)
	sink.task = map[int]int{1: 3, 2: 1, 3: 2}
	runPass(t, f)
	if got, want := startOrder(f.trace.snapshot()), []string{"2", "3", "1"}; !slices.Equal(got, want) {
		t.Fatalf("start order %v, want %v", got, want)
	}
	rows := startRows(t, f)
	for pr, want := range map[int]float64{1: 3, 2: 1, 3: 2} {
		if rows[pr]["priority"] != want || rows[pr]["priority_source"] != "task" {
			t.Fatalf("PR %d review-start row %v", pr, rows[pr])
		}
	}
	// Only the start order moved: every PR is still reviewed and settled.
	notified := 0
	for _, e := range f.trace.snapshot() {
		if strings.HasPrefix(e, "notify ") {
			notified++
		}
	}
	if notified != 3 {
		t.Fatalf("%d PRs settled, want 3: %q", notified, f.trace.snapshot())
	}
}

// Without priorities the order is today's list order.
func TestPollPRsWithoutPrioritiesKeepsListOrder(t *testing.T) {
	f, _ := newPriorityFixture(t, 4, 1)
	runPass(t, f)
	if got, want := startOrder(f.trace.snapshot()), []string{"1", "2", "3", "4"}; !slices.Equal(got, want) {
		t.Fatalf("start order %v, want %v", got, want)
	}
	if row := startRows(t, f)[1]; row["priority"] != float64(0) || row["priority_source"] != "none" {
		t.Fatalf("review-start row %v", row)
	}
}

// (b) With the bound full, priority-1 reviews start at once, up to two past
// it; a third waits for a review to return.
func TestPollPRsPriorityOneStartsPastTheBound(t *testing.T) {
	f, sink := newPriorityFixture(t, 4, 1)
	sink.task = map[int]int{1: 1, 2: 1, 3: 1, 4: 1}
	done := f.poll(context.Background())
	f.runner.waitFor(t, "three reviews in flight", func(in, started, _ int) bool { return in == 3 && started == 3 })
	time.Sleep(300 * time.Millisecond)
	if _, _, started, _, _ := f.runner.state(); started != 3 {
		t.Fatalf("%d reviews started before any returned, want 3", started)
	}
	f.runner.release <- struct{}{}
	f.runner.waitFor(t, "the waiting priority-1 review", func(_, started, _ int) bool { return started == 4 })
	close(f.runner.release)
	if err := <-done; err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
	if _, peak, _, _, _ := f.runner.state(); peak != 3 {
		t.Fatalf("peak %d, want 3", peak)
	}
	over := 0
	for _, row := range startRows(t, f) {
		if row["over_bound"] == true {
			over++
		}
	}
	if over < 2 {
		t.Fatalf("%d review-start rows past the bound, want 2: %v", over, startRows(t, f))
	}
}

// A priority-2 review never starts past the bound.
func TestPollPRsOtherPrioritiesStayWithinTheBound(t *testing.T) {
	f, sink := newPriorityFixture(t, 3, 1)
	sink.task = map[int]int{1: 2, 2: 2, 3: 2}
	done := f.poll(context.Background())
	f.runner.waitFor(t, "one review in flight", func(in, started, _ int) bool { return in == 1 && started == 1 })
	time.Sleep(300 * time.Millisecond)
	if _, _, started, _, _ := f.runner.state(); started != 1 {
		t.Fatalf("%d reviews started, want 1", started)
	}
	close(f.runner.release)
	if err := <-done; err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
	if _, peak, _, _, _ := f.runner.state(); peak != 1 {
		t.Fatalf("peak %d, want 1", peak)
	}
}

// (c) The change priority from hk overrides the task priority, and each is
// read at most once per PR.
func TestPollPRsChangePriorityOverridesTask(t *testing.T) {
	f, sink := newPriorityFixture(t, 3, 1)
	sink.task = map[int]int{1: 1, 2: 3, 3: 4}
	sink.change = map[int]int{1: 4, 3: 2}
	runPass(t, f)
	// By task priority alone the order would be 1, 2, 3.
	if got, want := startOrder(f.trace.snapshot()), []string{"3", "2", "1"}; !slices.Equal(got, want) {
		t.Fatalf("start order %v, want %v", got, want)
	}
	rows := startRows(t, f)
	for pr, want := range map[int][2]any{1: {float64(4), "change"}, 2: {float64(3), "task"}, 3: {float64(2), "change"}} {
		if rows[pr]["priority"] != want[0] || rows[pr]["priority_source"] != want[1] {
			t.Fatalf("PR %d review-start row %v", pr, rows[pr])
		}
	}
	for pr := 1; pr <= 3; pr++ {
		if sink.changeReads[pr] != 1 || sink.taskReads[pr] > 1 {
			t.Fatalf("PR %d read change %d times and task %d times", pr, sink.changeReads[pr], sink.taskReads[pr])
		}
	}
	if sink.taskReads[1] != 0 || sink.taskReads[3] != 0 {
		t.Fatalf("task read for a PR whose change had a priority: %v", sink.taskReads)
	}
}

// (d) A failed hk lookup keeps that PR's place in list order and never fails
// the pass.
func TestPollPRsLookupErrorKeepsListOrder(t *testing.T) {
	t.Run("every lookup fails", func(t *testing.T) {
		f, sink := newPriorityFixture(t, 3, 1)
		sink.task = map[int]int{1: 3, 2: 1, 3: 2}
		sink.failChange = map[int]bool{1: true, 2: true, 3: true}
		runPass(t, f)
		if got, want := startOrder(f.trace.snapshot()), []string{"1", "2", "3"}; !slices.Equal(got, want) {
			t.Fatalf("start order %v, want %v", got, want)
		}
		if row := startRows(t, f)[2]; row["priority_source"] != "error" || row["priority"] != float64(0) {
			t.Fatalf("review-start row %v", row)
		}
		if strings.Contains(f.out.String(), "hk unreachable") {
			t.Fatalf("a lookup error reached the output: %s", f.out.String())
		}
	})
	t.Run("one task read fails", func(t *testing.T) {
		f, sink := newPriorityFixture(t, 3, 1)
		sink.task = map[int]int{2: 2, 3: 3}
		sink.failTask = map[int]bool{1: true}
		runPass(t, f)
		// PR 1 has no usable priority: it sorts after the PRs that have one.
		if got, want := startOrder(f.trace.snapshot()), []string{"2", "3", "1"}; !slices.Equal(got, want) {
			t.Fatalf("start order %v, want %v", got, want)
		}
	})
}
