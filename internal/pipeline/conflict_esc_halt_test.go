package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// (h) A task lookup that keeps failing does not hold a conflict ESC forever:
// after three halts the ESC goes to the desk. Each halt is still returned.
func TestConflictEscTaskLookupHaltsReachTheDesk(t *testing.T) {
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
		rig.escalate(t)
		wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk (task lookup failed)", shaA)
	})
	t.Run("thirty minutes", func(t *testing.T) {
		rig := newEscRig(t)
		rig.g.pull.Body = "Mergewake-Task: 42\n"
		rig.s.Sink = &fakeLaneSink{lanes: map[string]string{"42": "task-lane"}, err: errors.New("hk: timeout")}
		if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e == nil {
			t.Fatal("a failed task-lane read was not returned")
		}
		rig.after(31 * time.Minute)
		rig.escalate(t)
		wantEsc(t, wantEscs(t, rig.n, 1)[0], "desk", "desk (task lookup failed)", shaA)
	})
}
