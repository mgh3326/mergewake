package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An unreadable author route is retried; when the same conflict outlasts the
// owner window, its owner gets one notice.
func TestConflictEscTaskLookupErrorDoesNotReroute(t *testing.T) {
	t.Run("three halts", func(t *testing.T) {
		rig := newEscRig(t)
		rig.g.pull.Body = "Mergewake-Task: 42\n"
		rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}, err: errors.New("hk: timeout")}
		for i := 1; i <= 3; i++ {
			if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e == nil {
				t.Fatalf("halt %d: a failed task-lane read was not returned", i)
			}
			rig.after(time.Minute)
		}
		wantTries(t, rig.n, 0)
		if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e == nil {
			t.Fatal("unreadable route was treated as absent")
		}
		wantEscs(t, rig.n, 0)
	})
	t.Run("thirty minutes", func(t *testing.T) {
		rig := newEscRig(t)
		rig.g.pull.Body = "Mergewake-Task: 42\n"
		rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}, err: errors.New("hk: timeout")}
		if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e == nil {
			t.Fatal("a failed task-lane read was not returned")
		}
		rig.after(31 * time.Minute)
		if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
		wantEscs(t, rig.n, 0)
		if len(rig.n.accepted) != 1 || rig.n.accepted[0].Lane != rig.s.ownerLane(rig.r) {
			t.Fatalf("owner notice after unreadable route = %+v", rig.n.accepted)
		}
		pulls := rig.g.pullCalls
		if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
		if rig.g.pullCalls != pulls {
			t.Fatalf("completed unreadable route retried GitHub pull: %d→%d", pulls, rig.g.pullCalls)
		}
		if rows := auditRows(t, rig.s, rig.r, "conflict-esc-suppressed"); len(rows) != 1 {
			t.Fatalf("unreadable route suppression audit = %v, want one", rows)
		}
	})
}
