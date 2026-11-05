package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Two PRs' goroutines load the same sent ledger, then each adds its own key:
// both keys survive, where a plain rewrite of the loaded copy dropped one.
func TestSentKeysKeepConcurrentAdds(t *testing.T) {
	file := filepath.Join(t.TempDir(), "pr-notify-sent")
	const n = 16
	ledgers := make([]*sentKeys, n)
	for i := range ledgers {
		l, e := loadSentKeys(file, 255)
		if e != nil {
			t.Fatal(e)
		}
		ledgers[i] = l
	}
	var wg sync.WaitGroup
	for i, l := range ledgers {
		wg.Go(func() {
			if e := l.add(fmt.Sprintf("ready %d", i)); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	final, e := loadSentKeys(file, 255)
	if e != nil {
		t.Fatal(e)
	}
	for i := range n {
		if !final.has(fmt.Sprintf("ready %d", i)) {
			t.Fatalf("key %d lost:\n%s", i, strings.Join(final.keys, "\n"))
		}
	}
	if e := ledgers[0].remove("ready 0"); e != nil {
		t.Fatal(e)
	}
	if final, _ = loadSentKeys(file, 255); final.has("ready 0") || !final.has(fmt.Sprintf("ready %d", n-1)) {
		t.Fatalf("remove dropped other keys or kept its own:\n%s", strings.Join(final.keys, "\n"))
	}
}

// overlapRunner counts git commands running at once.
type overlapRunner struct {
	mu        sync.Mutex
	in, peak  int
	readsPeak int
	reads     int
}

func (r *overlapRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	write := args[2] == "fetch" || args[2] == "worktree"
	r.mu.Lock()
	if write {
		r.in++
		r.peak = max(r.peak, r.in)
	} else {
		r.reads++
		r.readsPeak = max(r.readsPeak, r.reads)
	}
	r.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	r.mu.Lock()
	if write {
		r.in--
	} else {
		r.reads--
	}
	r.mu.Unlock()
	return nil, nil
}

// Reviews share the repo checkout: its fetches and worktree edits run one at
// a time, while reads still run side by side.
func TestGitInSerializesCheckoutWrites(t *testing.T) {
	runner := &overlapRunner{}
	s := &Service{Runner: runner}
	root := t.TempDir()
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			_, _ = s.gitIn(context.Background(), root, "fetch", "--no-tags", "origin", prHeadRefspec(i+1), "main")
		})
		wg.Go(func() {
			_, _ = s.gitIn(context.Background(), root, "worktree", "add", "--detach", filepath.Join(root, fmt.Sprint(i)), shaA)
		})
		wg.Go(func() { _, _ = s.gitIn(context.Background(), root, "cat-file", "-e", shaA) })
	}
	wg.Wait()
	if runner.peak != 1 {
		t.Fatalf("%d checkout writes overlapped", runner.peak)
	}
	if runner.readsPeak < 2 {
		t.Fatalf("reads were serialized too: peak %d", runner.readsPeak)
	}
}
