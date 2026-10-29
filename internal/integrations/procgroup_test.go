package integrations

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeReviewer forks a TERM-ignoring grandchild that inherits stdout, records
// both PIDs, prints "ready", then runs leader like a hung reviewer CLI.
func fakeReviewer(t *testing.T, leader string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	pids := filepath.Join(dir, "pids")
	script := filepath.Join(dir, "reviewer")
	body := "#!/bin/sh\n" +
		"(trap '' TERM; sleep 60) &\n" +
		"echo \"$$ $!\" > " + pids + ".tmp && mv " + pids + ".tmp " + pids + "\n" +
		"echo ready\n" +
		leader + "\n"
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	return script, pids
}

// cancelWhenReady cancels once the fake has recorded its PIDs, so a loaded
// host cannot cancel before the grandchild exists.
func cancelWhenReady(t *testing.T, pids string, cancel func()) {
	t.Helper()
	go func() {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(pids); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel()
	}()
}

func survivors(t *testing.T, pids string) []int {
	t.Helper()
	b, err := os.ReadFile(pids)
	if err != nil {
		t.Fatal(err)
	}
	var alive []int
	for _, f := range strings.Fields(string(b)) {
		pid, _ := strconv.Atoi(f)
		// Reparented children are reaped by init shortly after SIGKILL.
		deadline := time.Now().Add(5 * time.Second)
		for syscall.Kill(pid, 0) == nil && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if syscall.Kill(pid, 0) == nil {
			alive = append(alive, pid)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	return alive
}

// expired reports DeadlineExceeded once cancelled, standing in for a review
// timeout that fires only after the fake reviewer is ready.
type expired struct{ context.Context }

func (e expired) Err() error {
	if e.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

func TestReviewTimeoutLeavesNoSurvivors(t *testing.T) {
	for _, tt := range []struct{ name, leader string }{
		{"leader exits on TERM", "sleep 60"},
		{"leader ignores TERM", "trap '' TERM; while :; do sleep 1; done"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			script, pids := fakeReviewer(t, tt.leader)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancelWhenReady(t, pids, cancel)
			_, err := (Exec{Grace: 300 * time.Millisecond}).RunReview(expired{ctx}, script, nil, "", "")
			if err == nil || !strings.Contains(err.Error(), "timed out; process group terminated") || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err=%v", err)
			}
			if alive := survivors(t, pids); len(alive) > 0 {
				t.Fatalf("survivors %v", alive)
			}
		})
	}
}

func TestReviewCancelAndExitReapGroup(t *testing.T) {
	script, pids := fakeReviewer(t, "sleep 60")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelWhenReady(t, pids, cancel)
	if _, err := (Exec{Grace: 300 * time.Millisecond}).RunReview(ctx, script, nil, "", ""); err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("err=%v", err)
	}
	if alive := survivors(t, pids); len(alive) > 0 {
		t.Fatalf("survivors after cancel %v", alive)
	}
	// A reviewer that exits normally must not leave its background child, and
	// the child's inherited stdout must not hold the call open.
	script, pids = fakeReviewer(t, "echo '{}'")
	start := time.Now()
	out, err := (Exec{Grace: 30 * time.Second}).RunReview(context.Background(), script, nil, "", "")
	if err != nil || string(out) != "ready\n{}\n" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("waited for the grandchild's stdout: %v", elapsed)
	}
	if alive := survivors(t, pids); len(alive) > 0 {
		t.Fatalf("survivors after exit %v", alive)
	}
}

func TestDuplexReapsGroup(t *testing.T) {
	script, pids := fakeReviewer(t, "cat")
	out, err := (Exec{Grace: 300 * time.Millisecond}).RunDuplex(context.Background(), script, nil, "", func(r io.Reader, _ io.Writer) ([]byte, error) {
		line, err := bufio.NewReader(r).ReadString('\n')
		return []byte(line), err
	})
	if err != nil || string(out) != "ready\n" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if alive := survivors(t, pids); len(alive) > 0 {
		t.Fatalf("survivors %v", alive)
	}
}
