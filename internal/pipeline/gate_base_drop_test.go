package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
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
	if mode == "duplicate" {
		baseText = "return nil\n"
	}
	write(baseText)
	baseDropGit(t, src, "add", ".")
	baseDropGit(t, src, "commit", "-m", "fork point")
	fork := baseDropGit(t, src, "rev-parse", "HEAD")
	if mode == "combine" {
		write("base side\n")
	} else if mode == "duplicate" {
		write("return nil\nreturn nil\n")
	} else if mode == "doubleplus" {
		write("legacy line\n++counter;\n")
	} else if mode == "paths" {
		for _, name := range []string{"quoted \"name\".txt", "has space.txt", "tab\t"} {
			if err := os.WriteFile(filepath.Join(src, name), []byte("base path line\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	} else if mode == "large" {
		if err := os.WriteFile(filepath.Join(src, "large.txt"), []byte(strings.Repeat("z", (4<<20)+1024)+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	} else {
		write("legacy line\nbase addition\n")
	}
	baseDropGit(t, src, "add", "-A")
	baseDropGit(t, src, "commit", "-m", "base adds line")
	base = baseDropGit(t, src, "rev-parse", "HEAD")
	baseDropGit(t, src, "switch", "-c", "feature", fork)
	switch mode {
	case "drop":
		baseDropGit(t, src, "merge", "--no-ff", "-s", "ours", "main", "-m", "drop base addition")
	case "duplicate", "doubleplus", "paths", "large":
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
	s, g, n, r := notifySetup(t)
	r.Root = root
	s.Runner = gitErrRunner{Runner: &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}, fails: func(args []string) bool {
		return slices.Contains(args, "fetch")
	}, err: errors.New("transient fetch failure")}
	g.head = base
	g.pull.Base = github.Ref{Ref: "main", SHA: base}
	g.pull.Head = github.Ref{Ref: "feature", SHA: head}
	g.pull.Head.Repo.FullName = r.Name
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 {
		t.Fatalf("verification fault sent an author notice: %+v", n.events)
	}
	if rows := auditRows(t, s, r, "base-drop-error"); len(rows) != 1 || !strings.Contains(fmt.Sprint(rows[0]["error"]), "transient fetch failure") {
		t.Fatalf("base-drop error was not audited: %v", rows)
	}
	if reasons, waiting := splitInternalWaits([]string{"base_change_dropped:unknown"}, ""); !waiting || len(reasons) != 0 {
		t.Fatalf("verification fault was not an internal wait: reasons=%v waiting=%v", reasons, waiting)
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
	for _, want := range []string{commit, "origin/main", "ask the desk"} {
		if !strings.Contains(notice, want) {
			t.Fatalf("notice %q does not contain %q", notice, want)
		}
	}
	if !authorAction("base_change_dropped:1 | " + notice) {
		t.Fatal("base-change-drop reason is not routed as an author action")
	}
	many := make([]string, 12)
	for i := range many {
		many[i] = fmt.Sprintf("%040x", i+1)
	}
	notice = baseChangeDropNotice(gateVerdict{BaseChangesDropped: many})
	if !strings.Contains(notice, "and 7 more") || strings.Contains(notice, many[5]) {
		t.Fatalf("commit list was not capped: %q", notice)
	}
}

func TestBaseDropNoticeUsesConfiguredBranch(t *testing.T) {
	root, base, head := baseDropRepo(t, "drop")
	s, g, n, r := notifySetup(t)
	r.Root, r.Branch = root, "trunk"
	s.Runner = &localBaseDropRunner{&realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}}
	g.head = base
	g.pull.Base = github.Ref{Ref: r.Branch, SHA: base}
	g.pull.Head = github.Ref{Ref: "feature", SHA: head}
	g.pull.Head.Repo.FullName = r.Name
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "origin/trunk") || !strings.Contains(n.events[0].Text, "ask the desk to merge") {
		t.Fatalf("configured branch notice=%+v", n.events)
	}
}

func TestBaseDropCacheIncludesBaseTip(t *testing.T) {
	root, base, head := baseDropRepo(t, "drop")
	mergeBase := baseDropGit(t, root, "merge-base", base, head)
	s, _, _, _, r := setup(t)
	r.Root = root
	s.Runner = &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}
	// A merge-base/head aggregate can be stale for a different base tip that
	// shares the merge base. Seed the shape the old cross-pair cache reused.
	s.baseDrops = map[string]baseDropResult{baseDropCacheKey("merge", mergeBase, head): {commits: nil}}
	pr := github.Pull{Number: 7, Base: github.Ref{Ref: "main", SHA: base}, Head: github.Ref{Ref: "feature", SHA: head}}
	pr.Head.Repo.FullName = r.Name
	got, err := s.baseChangesDropped(context.Background(), r, pr)
	if err != nil || len(got) != 1 {
		t.Fatalf("base-tip verdict=%v err=%v", got, err)
	}
}

func TestBaseDropCountsDuplicateLines(t *testing.T) {
	root, base, head := baseDropRepo(t, "duplicate")
	got := baseDropCheck(t, root, base, head)
	if len(got) != 1 {
		t.Fatalf("dropped duplicate verdict=%v, want one commit", got)
	}
}

func TestBaseDropHandlesQuotedSpaceAndTabPaths(t *testing.T) {
	root, base, head := baseDropRepo(t, "paths")
	got := baseDropCheck(t, root, base, head)
	if len(got) != 1 {
		t.Fatalf("dropped path verdict=%v, want one commit", got)
	}
}

func TestBaseDropParsesAddedLineStartingWithPlus(t *testing.T) {
	root, base, head := baseDropRepo(t, "doubleplus")
	got := baseDropCheck(t, root, base, head)
	if len(got) != 1 {
		t.Fatalf("dropped ++ line verdict=%v, want one commit", got)
	}
}

func TestBaseDropFailsClosedForShallowCheckout(t *testing.T) {
	root, base, head := baseDropRepo(t, "drop")
	if err := os.WriteFile(filepath.Join(root, ".git", "shallow"), []byte(base+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := baseDropCheckErr(t, root, base, head); err == nil || !strings.Contains(err.Error(), "shallow") {
		t.Fatalf("shallow check err=%v", err)
	}
}

type localBaseDropRunner struct{ *realGitRunner }

func (r *localBaseDropRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "git" && slices.Contains(args, "fetch") {
		return nil, nil
	}
	return r.realGitRunner.Run(ctx, command, args, input, dir)
}

func TestBaseDropLargeDiffIsOperatorWait(t *testing.T) {
	root, base, head := baseDropRepo(t, "large")
	s, g, n, r := notifySetup(t)
	r.Root = root
	s.Runner = &localBaseDropRunner{&realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}}
	g.head = base
	g.pull.Base = github.Ref{Ref: "main", SHA: base}
	g.pull.Head = github.Ref{Ref: "feature", SHA: head}
	g.pull.Head.Repo.FullName = r.Name
	if err := fsutil.WriteJSON(s.reviewWaitsFile(r.Name), map[string]reviewWait{"7 " + head: {First: s.now().Add(-reviewStallAfter - time.Minute)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.NotifyPR(context.Background(), r, 7, Options{}); err != nil {
		t.Fatal(err)
	}
	if n.count != 1 || !strings.Contains(n.events[0].Text, "internal base-change verification failed") {
		t.Fatalf("oversized diff did not produce one operator notice: %+v", n.events)
	}
	if rows := auditRows(t, s, r, "base-drop-error"); len(rows) != 1 || !strings.Contains(fmt.Sprint(rows[0]["error"]), "runner output limit") {
		t.Fatalf("oversized diff reason was not audited: %v", rows)
	}
}

func TestBaseDropUnknownIsAuditedInternalWait(t *testing.T) {
	reasons, waiting := splitInternalWaits([]string{"base_change_dropped:unknown"}, "")
	if !waiting || len(reasons) != 0 || authorAction("base_change_dropped:unknown") {
		t.Fatalf("unknown wait reasons=%v waiting=%v", reasons, waiting)
	}
}

func baseDropCheck(t *testing.T, root, base, head string) []string {
	t.Helper()
	got, err := baseDropCheckErr(t, root, base, head)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func baseDropCheckErr(t *testing.T, root, base, head string) ([]string, error) {
	t.Helper()
	s, _, _, _, r := setup(t)
	r.Root = root
	s.Runner = &realGitRunner{fakeRunner: *s.Runner.(*fakeRunner)}
	pr := github.Pull{Base: github.Ref{Ref: "main", SHA: base}, Head: github.Ref{Ref: "feature", SHA: head}}
	pr.Head.Repo.FullName = r.Name
	return s.baseChangesDropped(context.Background(), r, pr)
}
