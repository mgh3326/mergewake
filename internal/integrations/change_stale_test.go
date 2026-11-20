package integrations

import (
	"path/filepath"
	"testing"
	"time"
)

// hk rejects an observation older than the one it holds as stale. The value
// sent is not recorded as written, so the same content observed again later
// reaches hk and hk ends up holding the newest observation.
func TestHKChangeStaleSectionIsWrittenAgainWhenObservedLater(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	base := time.Now().UTC()
	observe := func(at time.Time, head string) {
		w.Queue("o/r", 7, "checks", ChangeSection{At: at, Value: map[string]any{"head_sha": head, "checks": []string{"lint ok"}, "observed_at": at, "source": "poll", "complete": true}})
		drainChange(t, w)
	}
	observe(base.Add(2*time.Second), "new") // applied: hk now holds this observation
	observe(base, "old")                    // older than hk's: stale
	observe(base.Add(3*time.Second), "old") // the old content again, now the newest
	f.mu.Lock()
	held := f.seen["checks"]
	f.mu.Unlock()
	if want := base.Add(3 * time.Second); !held.Equal(want) {
		t.Fatalf("hk holds the observation at %s, want the newest at %s", held, want)
	}
}
