package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

func TestReapRetryBacksOffIdenticalCloseFailure(t *testing.T) {
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	s := &Service{Now: func() time.Time { return now }}
	if !s.reapRetryAllowed("o/r", "mwr-1-job", 7) {
		t.Fatal("first attempt must be allowed")
	}
	s.reapRetryFailed("o/r", "mwr-1-job", "panewire exit status 5", 7)
	if s.reapRetryAllowed("o/r", "mwr-1-job", 8) {
		t.Fatal("identical failure retried in the next pass")
	}
	if !s.reapRetryAllowed("o/other", "mwr-1-job", 8) {
		t.Fatal("backoff leaked across repositories")
	}
	s.reapRetrySucceeded("o/r", "mwr-1-job")
	if !s.reapRetryAllowed("o/r", "mwr-1-job", 8) {
		t.Fatal("successful close did not clear backoff")
	}
	s.reapRetryFailed("o/r", "mwr-1-job", "panewire exit status 5", 10)
	now = now.Add(time.Hour) // a long pass must not make the immediate next pass eligible
	if s.reapRetryAllowed("o/r", "mwr-1-job", 11) {
		t.Fatal("elapsed wall time bypassed the next-pass hold")
	}
	if !s.reapRetryAllowed("o/r", "mwr-1-job", 12) {
		t.Fatal("bounded backoff did not release after the skipped pass")
	}
	s.reapRetryFailed("o/r", "mwr-1-job", "panewire exit status 5", 12)
	if s.reapRetryAllowed("o/r", "mwr-1-job", 13) {
		t.Fatal("second identical failure retried on its immediately next pass")
	}
}

func TestReapRetryPassGenerationIsRepositoryScoped(t *testing.T) {
	s := &Service{Config: config.Config{StateDir: t.TempDir(), Reap: config.Reap{Enabled: true, RepoBudget: "0s", WorktreeAfter: "0", ScanAfter: "1h"}}, Out: io.Discard}
	call := func(repo string) {
		t.Helper()
		if err := s.Reap(context.Background(), config.Repo{Name: repo, Branch: "main"}, 0, Options{DryRun: true}); err == nil {
			t.Fatal("expected missing-hosts error before discovery")
		}
	}
	call("o/one")
	s.reapRetryFailed("o/one", "job", "same exit", 1)
	call("o/two") // another repository's pass must not advance o/one's generation
	if err := s.Reap(context.Background(), config.Repo{Name: "o/one", Branch: "main"}, 9, Options{DryRun: true}); err == nil {
		t.Fatal("expected missing-hosts error before the reviewer-only sweep")
	}
	call("o/one")
	s.reapRetryMu.Lock()
	pass := s.reapPasses["o/one"]
	s.reapRetryMu.Unlock()
	if pass != 2 {
		t.Fatalf("o/one pass=%d, want 2", pass)
	}
	if s.reapRetryAllowed("o/one", "job", pass) {
		t.Fatal("retry was not held on the repository's next pass")
	}
}

func TestReapBudgetDoesNotHidePriorFailures(t *testing.T) {
	failure := errors.New("worktree removal failed")
	wrapped := fmt.Errorf("host-a /tree: %w", failure)
	joined := errors.Join(wrapped, fmt.Errorf("%w; left: reviewer sweep", ErrReapRepoBudget))
	if ReapBudgetOnly(joined) {
		t.Fatal("budget stop with another error classified as budget-only")
	}
	if got := ReapNonBudgetErrors(joined); !errors.Is(got, failure) || !strings.Contains(got.Error(), "host-a /tree: "+failure.Error()) {
		t.Fatalf("non-budget failure lost: %v", got)
	}
	if !ReapBudgetOnly(fmt.Errorf("%w; left: archive", ErrReapRepoBudget)) {
		t.Fatal("budget-only stop not recognized")
	}
}
