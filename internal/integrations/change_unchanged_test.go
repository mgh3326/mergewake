package integrations

import (
	"path/filepath"
	"testing"
	"time"
)

// A pending PR observed on three passes with nothing changed is written once:
// the repeats differ only in their observation times, which hk does not need
// again for an unchanged section.
func TestHKChangeUnchangedSectionIsNotRewritten(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	base := time.Now().UTC()
	observe := func(at time.Time, head string) {
		w.Queue("o/r", 7, "checks", ChangeSection{At: at, Value: map[string]any{"head_sha": head, "checks": []string{"lint ok"}, "observed_at": at, "source": "poll", "complete": true}})
		w.Queue("o/r", 7, "gate", ChangeSection{At: at, Value: map[string]any{"head_sha": head, "state": "pending", "decided_at": at, "source": "review"}})
		drainChange(t, w)
	}
	for i := range 3 {
		observe(base.Add(time.Duration(i)*time.Second), "abc")
	}
	f.mu.Lock()
	writes := f.requests
	f.mu.Unlock()
	if writes != 1 {
		t.Fatalf("batch writes for an unchanged PR = %d, want 1", writes)
	}
	if got := w.Counts().Unchanged; got != 4 {
		t.Fatalf("unchanged sections = %d, want 4 (two sections on each of two repeats)", got)
	}
	// A changed value is written again.
	observe(base.Add(10*time.Second), "def")
	f.mu.Lock()
	writes = f.requests
	f.mu.Unlock()
	if writes != 2 {
		t.Fatalf("batch writes after a change = %d, want 2", writes)
	}
}

// A section whose write did not finish loses its recorded value, so the next
// write of it goes out even when it matches the value written before: hk may
// hold the failed value.
func TestHKChangeFailedWriteForgetsRecordedValue(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	base := time.Now().UTC()
	checks := func(at time.Time, head string) ChangeSection {
		return ChangeSection{At: at, Value: map[string]any{"head_sha": head, "checks": []string{"lint ok"}, "observed_at": at, "source": "poll", "complete": true}}
	}
	key := changeKey{"o/r", 7}
	w.Queue("o/r", 7, "checks", checks(base, "abc"))
	drainChange(t, w)
	if w.sent[key]["checks"] == "" {
		t.Fatal("a finished write records its section's value")
	}
	// hk refuses the next write, so the flush does not finish.
	f.mu.Lock()
	f.missing = true
	f.mu.Unlock()
	w.Queue("o/r", 7, "checks", checks(base.Add(time.Second), "def"))
	drainChange(t, w)
	if got := w.sent[key]["checks"]; got != "" {
		t.Fatalf("a failed write still records %q", got)
	}
}
