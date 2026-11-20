package integrations

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"
)

// A command that exits on its own keeps its exit status even when the
// deadline has passed by the time its failure is reported.
func TestStoppedKeepsOwnExitStatusAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	own := exec.Command("sh", "-c", "exit 3")
	err := own.Run()
	got := stopped(ctx, "jobs", err)
	var exit *exec.ExitError
	if !errors.As(got, &exit) || exit.ExitCode() != 3 {
		t.Fatalf("own exit status replaced: %v", got)
	}
	if errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("ordinary failure reported as a deadline stop: %v", got)
	}
}

// A process a cancel killed by signal still reports the deadline.
func TestStoppedReportsDeadlineForSignalledCommand(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	killed := exec.Command("sh", "-c", "kill -TERM $$")
	err := killed.Run()
	got := stopped(ctx, "jobs", err)
	if !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("signalled command not reported as deadline: %v", got)
	}
}
