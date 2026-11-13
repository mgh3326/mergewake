package pipeline

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// A mergeable PR with no conflict-esc state takes no lock and writes nothing:
// every evaluation used to create conflict-esc/<n>.json.lock for PRs that
// never conflicted.
func TestConflictEscMergeableWithoutStateTakesNoLock(t *testing.T) {
	s, g, _, r := notifySetup(t)
	file := s.conflictEscFile(r.Name, g.pull.Number)
	if e := s.EscalateConflict(context.Background(), r, g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	for _, p := range []string{file, file + ".lock"} {
		if _, e := os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Fatalf("%s exists after an idle resolve: %v", p, e)
		}
	}
}

// A mergeable head whose state still holds an unpinned pending ESC for it is
// cleared under the lock, as before.
func TestConflictEscMergeableClearsPendingForHead(t *testing.T) {
	s, g, _, r := notifySetup(t)
	file := s.conflictEscFile(r.Name, g.pull.Number)
	st := conflictEscState{Pending: map[string]conflictEscPending{g.pull.Head.SHA: {EventID: "ev-1"}}}
	if e := fsutil.WriteJSON(file, st); e != nil {
		t.Fatal(e)
	}
	if e := s.EscalateConflict(context.Background(), r, g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	got, e := readConflictEsc(file)
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := got.Pending[g.pull.Head.SHA]; ok {
		t.Fatalf("pending ESC for the mergeable head was not cleared: %+v", got.Pending)
	}
}
