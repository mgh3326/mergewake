package integrations

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// A check set over the request limit reaches hk in partial chunks: every check
// is in some request, no request carries more than the limit, and an unchanged
// set is not sent again once every chunk has landed.
func TestHKChangeChecksOverLimitAreSentInPartialChunks(t *testing.T) {
	f := &changeFake{}
	w, _ := changeTestWriter(t, f)
	w.outbox = filepath.Join(t.TempDir(), "outbox")
	at := time.Now().UTC()
	checks := make([]any, 0, 501)
	for i := range 501 {
		checks = append(checks, map[string]any{"name": fmt.Sprintf("check-%03d", i), "state": "success"})
	}
	observe := func() {
		w.Queue("o/r", 7, "checks", ChangeSection{At: at, Value: map[string]any{"head_sha": "abc", "checks": checks, "observed_at": at, "source": "poll", "complete": true}})
		drainChange(t, w)
	}
	observe()

	f.mu.Lock()
	var sizes []int
	seen := map[string]bool{}
	for _, body := range f.batches {
		raw, ok := body["checks"]
		if !ok {
			continue
		}
		var section struct {
			Checks []struct {
				Name string `json:"name"`
			} `json:"checks"`
			Complete bool `json:"complete"`
		}
		if err := json.Unmarshal(raw, &section); err != nil {
			t.Fatal(err)
		}
		if section.Complete {
			t.Fatal("a partial chunk was sent complete")
		}
		sizes = append(sizes, len(section.Checks))
		for _, c := range section.Checks {
			seen[c.Name] = true
		}
	}
	requests := f.requests
	f.mu.Unlock()
	if len(sizes) != 2 || sizes[0] != 500 || sizes[1] != 1 {
		t.Fatalf("checks per request = %v, want [500 1]", sizes)
	}
	if len(seen) != 501 {
		t.Fatalf("distinct checks reaching hk = %d, want 501", len(seen))
	}

	observe()
	f.mu.Lock()
	again := f.requests
	f.mu.Unlock()
	if again != requests {
		t.Fatalf("an unchanged over-limit set was sent again: requests %d -> %d", requests, again)
	}
}
