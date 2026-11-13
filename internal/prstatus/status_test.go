package prstatus_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/prstatus"
)

// A gate decision after the marker clears it; one before the pass does not.
func TestDirtyMarkerClearsOnlyForALaterPass(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 11, 6, 2, 0, 0, 0, time.UTC)
	if err := prstatus.MarkDirty(dir, "o/r", 7, at); err != nil {
		t.Fatal(err)
	}
	prstatus.ClearDirty(dir, "o/r", 7, at.Add(-time.Second))
	if !prstatus.DirtyPRs(dir, "o/r")[7] {
		t.Fatal("a pass that began before the marker cleared it")
	}
	prstatus.ClearDirty(dir, "o/r", 7, at.Add(time.Second))
	if len(prstatus.DirtyPRs(dir, "o/r")) != 0 {
		t.Fatal("a later pass left the marker")
	}
}

// A marker written while a pass is clearing survives: ClearDirty reads and
// removes under the lock MarkDirty takes, so it cannot delete a newer marker it
// never read.
func TestClearDirtyDoesNotDeleteConcurrentMarker(t *testing.T) {
	dir := t.TempDir()
	started := time.Date(2026, 11, 6, 2, 0, 0, 0, time.UTC)
	if err := prstatus.MarkDirty(dir, "o/r", 7, started.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	// Hold MarkDirty's lock the way a marker write in flight does.
	unlock, err := fsutil.Lock(filepath.Join(dir, "o/r", "pr-status", "7.dirty.lock"), true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		prstatus.ClearDirty(dir, "o/r", 7, started)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("ClearDirty ran without taking the marker lock")
	case <-time.After(200 * time.Millisecond):
	}
	if err := fsutil.Atomic(filepath.Join(dir, "o/r", "pr-status", "7.dirty"), []byte(started.Add(time.Minute).Format(time.RFC3339Nano)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	unlock()
	<-done
	if !prstatus.DirtyPRs(dir, "o/r")[7] {
		t.Fatal("a marker written during the pass was cleared")
	}
}
