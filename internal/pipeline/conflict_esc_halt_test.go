package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An unreadable author route is not treated as an absent route. The notice
// remains pending until findingsRoute can be resolved.
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
		if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e == nil {
			t.Fatal("unreadable route was treated as absent after waiting")
		}
		wantEscs(t, rig.n, 0)
	})
}
