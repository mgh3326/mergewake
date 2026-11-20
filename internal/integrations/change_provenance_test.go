package integrations

import (
	"path/filepath"
	"testing"
	"time"
)

// The same checks observed again by another read or delivery are not written
// again: source and delivery_id record how the observation was made, not what
// it saw, so an unchanged section keys alike whichever read carried it.
func TestHKChangeProvenanceAloneIsNotRewritten(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	base := time.Now().UTC()
	observe := func(i int, source, delivery string) {
		at := base.Add(time.Duration(i) * time.Second)
		v := map[string]any{"head_sha": "abc", "checks": []string{"lint ok"}, "observed_at": at, "source": source, "complete": true}
		if delivery != "" {
			v["delivery_id"] = delivery
		}
		w.Queue("o/r", 7, "checks", ChangeSection{At: at, Value: v})
		drainChange(t, w)
	}
	observe(0, "poll", "")
	observe(1, "fresh", "")
	observe(2, "webhook", "delivery-1")
	observe(3, "webhook", "delivery-2")
	f.mu.Lock()
	writes := f.requests
	f.mu.Unlock()
	if writes != 1 {
		t.Fatalf("batch writes for one unchanged check set = %d, want 1", writes)
	}
}
