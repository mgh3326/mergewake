package integrations

import (
	"path/filepath"
	"testing"
	"time"
)

// The same checks observed again by the same read are not written again. A
// different source or delivery is written even when the checks are identical:
// hk keeps the provenance and orders its writes by freshness, so a suppressed
// write would leave its watermark behind what was observed.
func TestHKChangeProvenanceIsWritten(t *testing.T) {
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
	observe(1, "poll", "")
	observe(2, "webhook", "delivery-1")
	observe(3, "webhook", "delivery-2")
	f.mu.Lock()
	writes := f.requests
	f.mu.Unlock()
	if writes != 3 {
		t.Fatalf("batch writes = %d, want 3: the first poll, then one per new source or delivery", writes)
	}
}

// A delayed webhook older than a poll that saw the same head is stale in hk.
// The poll is written because its source differs from the webhook's, so hk
// holds the poll's observation and rejects the delayed webhook.
func TestHKChangeDelayedWebhookDoesNotOverwritePoll(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	base := time.Now().UTC()
	observe := func(at time.Time, source, delivery string) {
		v := map[string]any{"head_sha": "abc", "checks": []string{"lint ok"}, "observed_at": at, "source": source, "complete": true}
		if delivery != "" {
			v["delivery_id"] = delivery
		}
		w.Queue("o/r", 7, "checks", ChangeSection{At: at, Value: v})
		drainChange(t, w)
	}
	observe(base.Add(1*time.Second), "webhook", "delivery-1")
	observe(base.Add(3*time.Second), "poll", "")
	observe(base.Add(2*time.Second), "webhook", "delivery-2")
	f.mu.Lock()
	held := f.seen["checks"]
	f.mu.Unlock()
	if want := base.Add(3 * time.Second); !held.Equal(want) {
		t.Fatalf("hk holds the observation at %s, want the poll at %s", held, want)
	}
}
