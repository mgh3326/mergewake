package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// rowDetail is the detail field of an audit row, or "" when it has none.
func rowDetail(row map[string]any) string {
	d, _ := row["detail"].(string)
	return d
}

// gitErrRunner fails each git call that fails matches with err and runs every
// other call on the wrapped runner.
type gitErrRunner struct {
	integrations.Runner
	fails func(args []string) bool
	err   error
}

func (r gitErrRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" && r.fails(args) {
		return nil, r.err
	}
	return r.Runner.Run(ctx, command, args, input, dir)
}

// gitRefused is the error integrations returns for a git command that exited
// 128: its message is the bare exit, and its stderr is kept beside it.
func gitRefused(stderr string) error {
	return &integrations.CommandError{Command: "git", Err: errors.New("exit status 128"), Stderr: stderr}
}

// firstSkip returns the reviewer skip the review-skipped row recorded first.
func firstSkip(t *testing.T, s *Service, r config.Repo) map[string]any {
	t.Helper()
	rows := auditRows(t, s, r, "review-skipped")
	if len(rows) != 1 {
		t.Fatalf("review-skipped rows=%v", rows)
	}
	skips, ok := rows[0]["skipped"].([]any)
	if !ok || len(skips) != 1 {
		t.Fatalf("skipped=%v", rows[0]["skipped"])
	}
	sk, ok := skips[0].(map[string]any)
	if !ok {
		t.Fatalf("skip=%v", skips[0])
	}
	return sk
}

// A checkout git refuses keeps the refusal's stderr in a detail field on the
// skip, the lease release and the failure rows. The reason and error text stay
// as they were: they reach PR comments, lane events and GitHub.
func TestCheckoutFailureRecordsGitStderr(t *testing.T) {
	const stderr = "fatal: '/x/checkout' is already checked out at '/y'\n"
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	s.Runner = gitErrRunner{Runner: runner, fails: func(args []string) bool {
		return slices.Contains(args, "worktree") && slices.Contains(args, "add")
	}, err: gitRefused(stderr)}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want ErrReviewUnavailable", err)
	}
	sk := firstSkip(t, s, r)
	if want := "checkout PR head " + shaA[:7] + ": git failed: exit status 128"; sk["reason"] != want {
		t.Fatalf("reason=%q, want %q", sk["reason"], want)
	}
	if detail, _ := sk["detail"].(string); !strings.Contains(detail, "already checked out") {
		t.Fatalf("skip detail=%q, want the git stderr", detail)
	}
	released := auditRows(t, s, r, "review-lease-released")
	if len(released) != 1 || !strings.Contains(rowDetail(released[0]), "already checked out") {
		t.Fatalf("lease released=%v", released)
	}
	failed := auditRows(t, s, r, "review-failed")
	if len(failed) != 1 {
		t.Fatalf("review-failed rows=%v", failed)
	}
	if detail := rowDetail(failed[0]); !strings.Contains(detail, "already checked out") {
		t.Fatalf("review-failed detail=%q, want the git stderr", detail)
	}
	if text, _ := failed[0]["error"].(string); strings.Contains(text, "already checked out") {
		t.Fatalf("review-failed error=%q: the git stderr must stay out of the error text", text)
	}
}

// A fetch that fails with a credentialed remote URL in its stderr records the
// URL with its userinfo redacted, and never the secret.
func TestFetchFailureRedactsCredentialedURL(t *testing.T) {
	secret := "SECRET" + "VALUE"
	stderr := "fatal: unable to access 'https://x-access-token:" + secret + "@github.com/o/r/': 403\n"
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	s.Runner = gitErrRunner{Runner: runner, fails: func(args []string) bool {
		return slices.Contains(args, "fetch")
	}, err: gitRefused(stderr)}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want ErrReviewUnavailable", err)
	}
	sk := firstSkip(t, s, r)
	detail, _ := sk["detail"].(string)
	if !strings.Contains(detail, "github.com/o/r") || !strings.Contains(detail, "[redacted]") {
		t.Fatalf("skip detail=%q, want the URL with its userinfo redacted", detail)
	}
	for _, field := range []string{detail, sk["reason"].(string), rowDetail(auditRows(t, s, r, "review-failed")[0])} {
		if strings.Contains(field, secret) {
			t.Fatalf("a recorded field leaks the secret: %q", field)
		}
	}
}

// The fallback fetches the branch after pull/<n>/head fails. Its stderr is the
// one a skip records, and the secret stays out of it too.
func TestFallbackFetchFailureRedactsCredentialedURL(t *testing.T) {
	secret := "SECRET" + "VALUE"
	stderr := "fatal: unable to access 'https://x-access-token:" + secret + "@github.com/o/r/': 403\n"
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	g.pull.Head.Ref = "feature"
	g.pull.Head.Repo.FullName = r.Name
	s.Runner = gitErrRunner{Runner: runner, fails: func(args []string) bool {
		return slices.Contains(args, "fetch")
	}, err: gitRefused(stderr)}
	if err := s.RetryReview(context.Background(), r, g.pull, Options{}); !errors.Is(err, ErrReviewUnavailable) {
		t.Fatalf("err=%v, want ErrReviewUnavailable", err)
	}
	sk := firstSkip(t, s, r)
	reason, _ := sk["reason"].(string)
	if !strings.HasPrefix(reason, "fetch PR head") || strings.Contains(reason, secret) {
		t.Fatalf("reason=%q, want the fetch failure without the secret", reason)
	}
	detail, _ := sk["detail"].(string)
	if !strings.Contains(detail, "github.com/o/r") || strings.Contains(detail, secret) {
		t.Fatalf("skip detail=%q, want the redacted URL", detail)
	}
}
