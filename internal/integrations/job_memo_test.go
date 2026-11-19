package integrations

import (
	"context"
	"errors"
	"testing"
)

// countingJobs is the job reader under test; it counts the reads that reach it.
type countingJobs struct {
	reads int
	recs  map[string]JobRecord
	fail  error
}

func (c *countingJobs) Job(_ context.Context, id string) (JobRecord, error) {
	c.reads++
	if c.fail != nil {
		return JobRecord{}, c.fail
	}
	rec, ok := c.recs[id]
	if !ok {
		return JobRecord{}, ErrJobNotFound
	}
	return rec, nil
}

// A route lookup that asks for one job several times in an evaluation reads it
// from hk once.
func TestEvaluationReadsJobOnce(t *testing.T) {
	jobs := &countingJobs{recs: map[string]JobRecord{"job-1": {State: "spawned", Label: "w-7"}}}
	ctx := WithTaskMemo(context.Background())
	for i := 0; i < 3; i++ {
		rec, e := JobReadOnce(ctx, jobs, "job-1")
		if e != nil || rec.Label != "w-7" || rec.State != "spawned" {
			t.Fatalf("read %d: JobReadOnce = %+v, %v", i, rec, e)
		}
	}
	if jobs.reads != 1 {
		t.Fatalf("job read %d times in one evaluation, want 1", jobs.reads)
	}
}

// A missing record is read again: a spawn can create it later in the same
// evaluation, so a not-found answer is not kept.
func TestEvaluationRereadsMissingJob(t *testing.T) {
	jobs := &countingJobs{recs: map[string]JobRecord{}}
	ctx := WithTaskMemo(context.Background())
	if _, e := JobReadOnce(ctx, jobs, "new-job"); !errors.Is(e, ErrJobNotFound) {
		t.Fatalf("first read: err = %v, want ErrJobNotFound", e)
	}
	jobs.recs["new-job"] = JobRecord{State: "spawned"}
	rec, e := JobReadOnce(ctx, jobs, "new-job")
	if e != nil || rec.State != "spawned" {
		t.Fatalf("read after the spawn: JobReadOnce = %+v, %v; the missing answer was kept", rec, e)
	}
	if jobs.reads != 2 {
		t.Fatalf("job read %d times, want 2 (a missing answer is not kept)", jobs.reads)
	}
}

// A failed job read is not kept: the next read asks hk again.
func TestJobReadFailureIsNotKept(t *testing.T) {
	jobs := &countingJobs{recs: map[string]JobRecord{"job-1": {State: "spawned"}}, fail: errors.New("hk unavailable")}
	ctx := WithTaskMemo(context.Background())
	if _, e := JobReadOnce(ctx, jobs, "job-1"); e == nil {
		t.Fatal("first read returned no error")
	}
	jobs.fail = nil
	rec, e := JobReadOnce(ctx, jobs, "job-1")
	if e != nil || rec.State != "spawned" {
		t.Fatalf("second read: JobReadOnce = %+v, %v", rec, e)
	}
	if jobs.reads != 2 {
		t.Fatalf("job read %d times, want 2 (a failure is not kept)", jobs.reads)
	}
}

// Outside an evaluation's memo every job read goes to hk, as before.
func TestJobReadsWithoutMemoGoEachTime(t *testing.T) {
	jobs := &countingJobs{recs: map[string]JobRecord{"job-1": {State: "spawned"}}}
	for i := 0; i < 2; i++ {
		if _, e := JobReadOnce(context.Background(), jobs, "job-1"); e != nil {
			t.Fatal(e)
		}
	}
	if jobs.reads != 2 {
		t.Fatalf("job read %d times without a memo, want 2", jobs.reads)
	}
}

// A polling read (Handoffkeep.Job, which waiters use to watch a job's state)
// is not memoized, even inside an evaluation: each poll sees the record as it
// is now.
func TestPollingJobReadsBypassTheMemo(t *testing.T) {
	jobs := &countingJobs{recs: map[string]JobRecord{"job-1": {State: "spawned"}}}
	h := Handoffkeep{Jobs: jobs}
	ctx := WithTaskMemo(context.Background())
	for i := 0; i < 2; i++ {
		if _, e := h.Job(ctx, "job-1"); e != nil {
			t.Fatal(e)
		}
	}
	if jobs.reads != 2 {
		t.Fatalf("polling reads reached the reader %d times, want 2", jobs.reads)
	}
}
