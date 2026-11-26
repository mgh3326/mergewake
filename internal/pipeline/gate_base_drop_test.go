package pipeline

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

func baseDropGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, b)
	}
	return strings.TrimSpace(string(b))
}

// baseDropRepo rebuilds the incident shape: base adds one line after the
// fork, then the PR either drops it in an ours merge, keeps both sides in a
// normal merge, or never merges base.
func baseDropRepo(t *testing.T, mode string) (root, base, head string) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(src, 0700); err != nil {
		t.Fatal(err)
	}
	baseDropGit(t, src, "init", "-b", "main")
	baseDropGit(t, src, "config", "user.email", "test@example.com")
	baseDropGit(t, src, "config", "user.name", "test")
	write := func(text string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(src, "tracked.txt"), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	baseText := "legacy line\n"
	if mode == "combine" {
		baseText = "target line\n"
	}
	write(baseText)
	baseDropGit(t, src, "add", ".")
	baseDropGit(t, src, "commit", "-m", "fork point")
	fork := baseDropGit(t, src, "rev-parse", "HEAD")
	if mode == "combine" {
		write("base side\n")
	} else {
		write("legacy line\nbase addition\n")
	}
	baseDropGit(t, src, "commit", "-am", "base adds line")
	base = baseDropGit(t, src, "rev-parse", "HEAD")
	baseDropGit(t, src, "switch", "-c", "feature", fork)
	switch mode {
	case "drop":
		baseDropGit(t, src, "merge", "--no-ff", "-s", "ours", "main", "-m", "drop base addition")
	case "both":
		if err := os.WriteFile(filepath.Join(src, "feature.txt"), []byte("feature work\n"), 0600); err != nil {
			t.Fatal(err)
		}
		baseDropGit(t, src, "add", "feature.txt")
		baseDropGit(t, src, "commit", "-m", "feature work")
		baseDropGit(t, src, "merge", "main", "-m", "keep both sides")
		parents := strings.Fields(baseDropGit(t, src, "rev-list", "--parents", "-n", "1", "HEAD"))
		if len(parents) != 3 {
			t.Fatalf("keep-both fixture did not create a merge commit: %v", parents)
		}
	case "edit_after_merge":
		if err := os.WriteFile(filepath.Join(src, "feature.txt"), []byte("feature work\n"), 0600); err != nil {
			t.Fatal(err)
		}
		baseDropGit(t, src, "add", "feature.txt")
		baseDropGit(t, src, "commit", "-m", "feature work")
		baseDropGit(t, src, "merge", "main", "-m", "clean base merge")
		write("legacy line\nauthor edit\n")
		baseDropGit(t, src, "commit", "-am", "edit merged line later")
	case "rename_after_merge":
		if err := os.WriteFile(filepath.Join(src, "feature.txt"), []byte("feature work\n"), 0600); err != nil {
			t.Fatal(err)
		}
		baseDropGit(t, src, "add", "feature.txt")
		baseDropGit(t, src, "commit", "-m", "feature work")
		baseDropGit(t, src, "merge", "main", "-m", "clean base merge")
		baseDropGit(t, src, "mv", "tracked.txt", "renamed.txt")
		baseDropGit(t, src, "commit", "-m", "rename merged file")
	case "combine":
		write("feature side\n")
		baseDropGit(t, src, "commit", "-am", "feature changes target")
		cmd := exec.Command("git", "-C", src, "merge", "main", "-m", "combine both sides")
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("expected merge conflict, got success: %s", out)
		}
		write("combined base and feature sides\n")
		baseDropGit(t, src, "add", "tracked.txt")
		baseDropGit(t, src, "commit", "-m", "resolve by combining both sides")
	case "no_merge":
		write("branch edit\n")
		baseDropGit(t, src, "commit", "-am", "edit pre-fork line")
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	head = baseDropGit(t, src, "rev-parse", "HEAD")
	baseDropGit(t, src, "update-ref", "refs/pull/7/head", head)
	root = filepath.Join(t.TempDir(), "checkout")
	if out, err := exec.Command("git", "clone", src, root).CombinedOutput(); err != nil {
		t.Fatalf("clone test repo: %v\n%s", err, out)
	}
	return root, base, head
}

func TestGateBaseChangeDropped(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want string
	}{
		{mode: "drop", want: "base_change_dropped:1"},
		{mode: "both", want: ""},
		{mode: "edit_after_merge", want: ""},
		{mode: "rename_after_merge", want: ""},
		// The base's exact added line is absent from the merge tree. Under the
		// gate's line-survival definition, replacing it with a combined line is
		// a drop even though the author intended to preserve its meaning.
		{mode: "combine", want: "base_change_dropped:1"},
		{mode: "no_merge", want: ""},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			root, base, head := baseDropRepo(t, tc.mode)
			s, g, _, _, r := setup(t)
			r.Root = root
			runner := s.Runner.(*fakeRunner)
			s.Runner = &realGitRunner{fakeRunner: *runner}
			g.pull.Base = github.Ref{Ref: "main", SHA: base}
			g.pull.Head = github.Ref{Ref: "feature", SHA: head}
			g.pull.Head.Repo.FullName = r.Name
			g.pull.ChangedFiles = 1
			p, err := s.Policy(context.Background(), r, base)
			if err != nil {
				t.Fatal(err)
			}
			v, err := s.mergeGate(context.Background(), r, g.pull, p, base)
			if err != nil {
				t.Fatal(err)
			}
			got := ""
			for _, reason := range v.Reasons {
				if strings.HasPrefix(reason, "base_change_dropped:") {
					got = reason
				}
			}
			if got != tc.want {
				t.Fatalf("base-drop reason = %q, want %q; all reasons=%v", got, tc.want, v.Reasons)
			}
			if tc.mode == "drop" && (v.State != "blocked" || len(v.BaseChangesDropped) != 1 || v.BaseChangesDropped[0] == "") {
				t.Fatalf("dropped base change verdict = %+v", v)
			}
		})
	}
}

func TestGateBaseChangeDroppedUnknownFailsClosed(t *testing.T) {
	root, base, head := baseDropRepo(t, "drop")
	s, g, _, _, r := setup(t)
	r.Root = root
	s.Runner = &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}
	s.Runner = gitErrRunner{Runner: s.Runner, fails: func(args []string) bool {
		return strings.Contains(strings.Join(args, " "), "rev-parse --git-dir")
	}, err: errors.New("git checkout unavailable")}
	g.pull.Base = github.Ref{Ref: "main", SHA: base}
	g.pull.Head = github.Ref{Ref: "feature", SHA: head}
	g.pull.Head.Repo.FullName = r.Name
	p, err := s.Policy(context.Background(), r, base)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.mergeGate(context.Background(), r, g.pull, p, base)
	if err != nil {
		t.Fatal(err)
	}
	if v.State != "blocked" || !strings.Contains(strings.Join(v.Reasons, ","), "base_change_dropped:unknown") {
		t.Fatalf("failed local check did not hold gate: %+v", v)
	}
}

type baseDropCountingRunner struct {
	next  *realGitRunner
	calls int
}

func (r *baseDropCountingRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" {
		r.calls++
	}
	return r.next.Run(ctx, command, args, input, dir)
}

func TestGateBaseChangeDroppedCachesUnchangedVerdict(t *testing.T) {
	root, base, head := baseDropRepo(t, "drop")
	s, g, _, _, r := setup(t)
	r.Root = root
	runner := &baseDropCountingRunner{next: &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}}
	s.Runner = runner
	g.pull.Base = github.Ref{Ref: "main", SHA: base}
	g.pull.Head = github.Ref{Ref: "feature", SHA: head}
	g.pull.Head.Repo.FullName = r.Name
	first, err := s.baseChangesDropped(context.Background(), r, g.pull)
	if err != nil || len(first) != 1 {
		t.Fatalf("first check = %v, %v", first, err)
	}
	calls := runner.calls
	second, err := s.baseChangesDropped(context.Background(), r, g.pull)
	if err != nil || len(second) != 1 || runner.calls != calls {
		t.Fatalf("cached check = %v, %v; git calls grew %d -> %d", second, err, calls, runner.calls)
	}
}

func TestBaseChangeDropNoticeNamesCommitsAndFix(t *testing.T) {
	commit := strings.Repeat("c", 40)
	v := gateVerdict{Reasons: []string{"base_change_dropped:1"}, BaseChangesDropped: []string{commit}}
	notice := baseChangeDropNotice(v)
	for _, want := range []string{commit, "re-merging origin/main", "keeping both sides"} {
		if !strings.Contains(notice, want) {
			t.Fatalf("notice %q does not contain %q", notice, want)
		}
	}
	if !authorAction("base_change_dropped:1 | " + notice) {
		t.Fatal("base-change-drop reason is not routed as an author action")
	}
}
