package mirror

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// A first fetch into a mirror that has no objects gets the first-clone bound
// of ten minutes. Once the mirror holds a pack, a later fetch gets the 60 s
// bound.
func TestFirstCloneGetsLongerFetchBound(t *testing.T) {
	var bounds []time.Duration
	run := &fakeRunner{run: func(ctx context.Context, args []string) ([]byte, error) {
		if !slices.Contains(args, "fetch") {
			return nil, nil
		}
		d, ok := ctx.Deadline()
		if !ok {
			t.Fatal("fetch has no deadline")
		}
		bounds = append(bounds, time.Until(d))
		// The fake fetch lands a pack, as git does, so the mirror has objects.
		pack := filepath.Join(args[1], "objects", "pack")
		if e := os.MkdirAll(pack, 0o755); e != nil {
			t.Fatal(e)
		}
		return nil, os.WriteFile(filepath.Join(pack, "pack-fake.pack"), nil, 0o644)
	}}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "ghs_testtokenvalue1234567890", true },
		Run:     run,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: testBaseEnv(t),
	})
	for range 2 {
		if e := m.Fetch(context.Background(), "o/r"); e != nil {
			t.Fatalf("fetch: %v", e)
		}
	}
	if len(bounds) != 2 {
		t.Fatalf("fetches = %d, want 2", len(bounds))
	}
	if bounds[0] < 9*time.Minute {
		t.Errorf("first clone bound %v, want about 10m", bounds[0])
	}
	if bounds[1] > time.Minute || bounds[1] < 55*time.Second {
		t.Errorf("later fetch bound %v, want 60s", bounds[1])
	}
}
