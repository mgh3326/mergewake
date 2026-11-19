package integrations

import (
	"context"
	"testing"

	"github.com/mgh3326/mergewake/internal/hktest"
)

// taskReads counts the GET /v1/tasks/42 requests the fake hk received.
func taskReads(hk *hktest.Server) int {
	n := 0
	for _, r := range hk.Requests() {
		if r.Route() == "GET /v1/tasks/42" {
			n++
		}
	}
	return n
}

// An evaluation that asks Lane, TaskJob and TaskPriority for one task reads
// it from hk once.
func TestEvaluationReadsTaskOnce(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(42, hkTask(42, "unstarted", "design-lane", "Fix the watch (grade B, T2)", 2, `{"job_id":"job-a"}`))
	h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
	ctx := WithTaskMemo(context.Background())
	if lane, e := h.Lane(ctx, "42"); e != nil || lane != "design-lane" {
		t.Fatalf("Lane = %q, %v", lane, e)
	}
	if job, title, e := h.TaskJob(ctx, "42"); e != nil || job != "job-a" || title != "Fix the watch (grade B, T2)" {
		t.Fatalf("TaskJob = %q %q %v", job, title, e)
	}
	if p, e := h.TaskPriority(ctx, "42"); e != nil || p != 2 {
		t.Fatalf("TaskPriority = %d %v", p, e)
	}
	if got := taskReads(hk); got != 1 {
		t.Fatalf("GET /v1/tasks/42 requests = %d, want 1", got)
	}
}

// Without an evaluation's memo every read goes to hk, as before.
func TestTaskReadsWithoutMemoGoEachTime(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(42, hkTask(42, "unstarted", "design-lane", "Fix the watch (grade B, T2)", 2, `{}`))
	h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
	ctx := context.Background()
	if _, e := h.Lane(ctx, "42"); e != nil {
		t.Fatal(e)
	}
	if _, e := h.TaskPriority(ctx, "42"); e != nil {
		t.Fatal(e)
	}
	if got := taskReads(hk); got != 2 {
		t.Fatalf("GET /v1/tasks/42 requests = %d, want 2", got)
	}
}

// A write to the task drops the memo's entry, so the next read sees hk again.
func TestTaskWriteDropsMemoEntry(t *testing.T) {
	hk := hktest.New(t)
	hk.SetTask(42, hkTask(42, "unstarted", "design-lane", "Fix the watch (grade B, T2)", 2, `{}`))
	h := Handoffkeep{Runner: noCLI(t), Command: "handoffkeep"}
	ctx := WithTaskMemo(context.Background())
	if _, e := h.Lane(ctx, "42"); e != nil {
		t.Fatal(e)
	}
	if e := h.Comment(ctx, "42", "pr 7 is ready"); e != nil {
		t.Fatalf("Comment: %v", e)
	}
	if _, e := h.Lane(ctx, "42"); e != nil {
		t.Fatal(e)
	}
	if got := taskReads(hk); got != 2 {
		t.Fatalf("GET /v1/tasks/42 requests = %d, want 2 (a comment ends the memo's answer)", got)
	}
}
