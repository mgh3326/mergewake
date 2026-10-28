package reap

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeRunner struct {
	outs  map[string]string
	errs  map[string]error
	calls []string
}

func key(command string, args []string) string { return command + "\x00" + strings.Join(args, "\x00") }

func (f *fakeRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	k := key(command, args)
	f.calls = append(f.calls, k)
	if e, ok := f.errs[k]; ok {
		return nil, e
	}
	if out, ok := f.outs[k]; ok {
		return []byte(out), nil
	}
	return nil, errors.New("unexpected command: " + k)
}

func testExec(outs map[string]string, errs map[string]error) (*Exec, *fakeRunner) {
	f := &fakeRunner{outs: outs, errs: errs}
	return &Exec{Runner: f, Host: Host{Name: "local"}}, f
}

const paneList = `{"id":"cli:agent:list","result":{"type":"agent_list","agents":[` +
	`{"pane_id":"w1:p1","name":"wk-7","agent":"devin","agent_status":"idle","cwd":"/repo.wt"},` +
	`{"pane_id":"w1:p2","agent":"claude","agent_status":"working","cwd":"/repo"}]}}`

func TestPanesLocalAndRemote(t *testing.T) {
	outs := map[string]string{
		key("herdr", []string{"agent", "list"}): paneList,
		key("ssh", []string{"-o", "BatchMode=yes", "-T", "-n", "ops@node.example", "'herdr' 'agent' 'list'"}): paneList,
		key("herdr", []string{"pane", "list"}): `{"result":{"panes":[{"pane_id":"w1:p9","cwd":"/repo.wt","agent_status":"unknown"}]}}`,
	}
	e, f := testExec(outs, nil)
	panes, err := e.Panes(context.Background())
	if err != nil || len(panes) != 2 {
		t.Fatalf("%v %v", panes, err)
	}
	if panes[0].ID != "w1:p1" || panes[0].Name != "wk-7" || panes[0].Status != "idle" || panes[0].Cwd != "/repo.wt" {
		t.Fatalf("%+v", panes[0])
	}
	if panes[1].Name != "" || panes[1].Status != "working" {
		t.Fatalf("%+v", panes[1])
	}

	remote := &Exec{Runner: f, Host: Host{Name: "node", SSH: "ops@node.example"}}
	if _, err = remote.Panes(context.Background()); err != nil {
		t.Fatal(err)
	}
	last := f.calls[len(f.calls)-1]
	if !strings.HasPrefix(last, "ssh\x00-o\x00BatchMode=yes\x00-T\x00-n\x00ops@node.example\x00'herdr' 'agent' 'list'") {
		t.Fatalf("ssh invocation %q", last)
	}

	all, err := e.AllPanes(context.Background())
	if err != nil || len(all) != 1 || all[0].Cwd != "/repo.wt" {
		t.Fatalf("%+v %v", all, err)
	}
}

func TestPanesMalformed(t *testing.T) {
	for _, out := range []string{`not json`, `{"result":{}}`, `{"agents":[]}`} {
		e, _ := testExec(map[string]string{key("herdr", []string{"agent", "list"}): out}, nil)
		if _, err := e.Panes(context.Background()); err == nil {
			t.Fatalf("accepted %q", out)
		}
	}
}

func TestInspectLinkedCanonicalAndPlain(t *testing.T) {
	outs := map[string]string{
		key("git", []string{"-C", "/repo.wt", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"}): "/repo/.git/worktrees/repo.wt\n/repo/.git\n/repo.wt\n",
		key("git", []string{"-C", "/repo.wt", "branch", "--show-current"}):                                               "wk/session-reap\n",
		key("git", []string{"-C", "/repo.wt", "remote", "get-url", "origin"}):                                            "git@github.com:example/project.git\n",
		key("git", []string{"-C", "/repo", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"}):    "/repo/.git\n/repo/.git\n/repo\n",
		key("git", []string{"-C", "/sub", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"}):     "/repo/.git/modules/sub\n/repo/.git/modules/sub\n/repo/sub\n",
	}
	errs := map[string]error{
		key("git", []string{"-C", "/plain", "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"}): errors.New("not a git repository"),
	}
	e, _ := testExec(outs, errs)
	w, err := e.Inspect(context.Background(), "/repo.wt")
	if err != nil || w == nil {
		t.Fatalf("%+v %v", w, err)
	}
	if w.Path != "/repo.wt" || w.Branch != "wk/session-reap" || w.Repo != "example/project" || w.Common != "/repo/.git" {
		t.Fatalf("%+v", w)
	}
	for _, dir := range []string{"/repo", "/sub", "/plain"} {
		if w, err = e.Inspect(context.Background(), dir); err != nil || w != nil {
			t.Fatalf("%s: %+v %v", dir, w, err)
		}
	}
}

func TestRepoFromRemote(t *testing.T) {
	for _, tt := range []struct{ in, want string }{
		{"git@github.com:example/project.git", "example/project"},
		{"https://github.com/example/project.git", "example/project"},
		{"https://github.com/example/project", "example/project"},
		{"ssh://git@github.com/example/project.git", "example/project"},
		{"git@github.com:example/project/", "example/project"},
		{"/home/ops/project", ""},
		{"file:///home/ops/project.git", ""},
		{"https://gitlab.example/group/sub/project", ""},
		{"https://github.com/example", ""},
		{"", ""},
	} {
		if got := RepoFromRemote(tt.in); got != tt.want {
			t.Errorf("%q: %q != %q", tt.in, got, tt.want)
		}
	}
}

func TestEntriesAndClean(t *testing.T) {
	list := "worktree /repo\nHEAD " + strings.Repeat("a", 40) + "\nbranch refs/heads/main\n\n" +
		"worktree /repo.wt\nHEAD " + strings.Repeat("b", 40) + "\nbranch refs/heads/wk/x\n\n" +
		"worktree /repo.old\nHEAD " + strings.Repeat("c", 40) + "\ndetached\nlocked reason\n"
	entries, err := Entries([]byte(list))
	if err != nil || len(entries) != 3 {
		t.Fatalf("%+v %v", entries, err)
	}
	if entries[0].Path != "/repo" || entries[0].Branch != "main" || entries[0].Locked {
		t.Fatalf("%+v", entries[0])
	}
	if entries[1].Path != "/repo.wt" || entries[1].Branch != "wk/x" {
		t.Fatalf("%+v", entries[1])
	}
	if !entries[2].Detached || !entries[2].Locked || entries[2].Branch != "" {
		t.Fatalf("%+v", entries[2])
	}
	if _, err = Entries([]byte("HEAD abc\n")); err == nil {
		t.Fatal("malformed list accepted")
	}

	e, _ := testExec(map[string]string{
		key("git", []string{"-C", "/repo.wt", "status", "--porcelain"}):           "",
		key("git", []string{"-C", "/repo.dirty", "status", "--porcelain"}):        " M tool\n?? note.txt\n",
		key("git", []string{"-C", "/repo.wt", "worktree", "list", "--porcelain"}): list,
	}, nil)
	for _, tt := range []struct {
		path string
		want bool
	}{{"/repo.wt", true}, {"/repo.dirty", false}} {
		if clean, err := e.Clean(context.Background(), tt.path); err != nil || clean != tt.want {
			t.Fatalf("%s: %v %v", tt.path, clean, err)
		}
	}
	got, err := e.WorktreeList(context.Background(), "/repo.wt")
	if err != nil || len(got) != 3 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestJoinQuoting(t *testing.T) {
	got := Join([]string{"git", "-C", "/a b/c'd", "status"})
	if got != `'git' '-C' '/a b/c'\''d' 'status'` {
		t.Fatal(got)
	}
}

func TestExistsCaptureAndPushed(t *testing.T) {
	report := "/jobs/wk-7/report.md"
	screen := `mkdir -p -- "$1" && herdr pane read "$2" > "$1/pane-screen.txt"`
	outs := map[string]string{
		key("sh", []string{"-c", `test -f "$1" && echo yes || echo no`, "sh", report}):                       "yes\n",
		key("sh", []string{"-c", `test -f "$1" && echo yes || echo no`, "sh", "/jobs/wk-8/report.md"}):       "no\n",
		key("sh", []string{"-c", screen, "sh", "/jobs/wk-8", "w1:p2"}):                                       "",
		key("git", []string{"-C", "/repo", "merge-base", "--is-ancestor", "wk/x", "origin/main"}):            "",
		key("git", []string{"-C", "/repo", "rev-list", "--count", "--left-right", "wk/y...wk/y@{upstream}"}): "0\t2\n",
	}
	errs := map[string]error{
		key("git", []string{"-C", "/repo", "merge-base", "--is-ancestor", "wk/y", "origin/main"}):            errors.New("exit 1"),
		key("git", []string{"-C", "/repo", "merge-base", "--is-ancestor", "wk/y", "main"}):                   errors.New("exit 1"),
		key("git", []string{"-C", "/repo", "rev-list", "--count", "--left-right", "wk/z...wk/z@{upstream}"}): errors.New("no upstream"),
		key("git", []string{"-C", "/repo", "rev-list", "--count", "--left-right", "wk/z...origin/wk/z"}):     errors.New("unknown revision"),
		key("git", []string{"-C", "/repo", "merge-base", "--is-ancestor", "wk/z", "origin/main"}):            errors.New("exit 1"),
		key("git", []string{"-C", "/repo", "merge-base", "--is-ancestor", "wk/z", "main"}):                   errors.New("exit 1"),
	}
	e, _ := testExec(outs, errs)
	if ok, err := e.Exists(context.Background(), report); err != nil || !ok {
		t.Fatal("report should exist")
	}
	if saved, err := e.Capture(context.Background(), "w1:p1", "/jobs/wk-7"); err != nil || saved != report {
		t.Fatalf("%q %v", saved, err)
	}
	if saved, err := e.Capture(context.Background(), "w1:p2", "/jobs/wk-8"); err != nil || saved != "/jobs/wk-8/pane-screen.txt" {
		t.Fatalf("%q %v", saved, err)
	}
	if !e.Pushed(context.Background(), "/repo", "wk/x", "main") {
		t.Fatal("merged branch rejected")
	}
	if !e.Pushed(context.Background(), "/repo", "wk/y", "main") {
		t.Fatal("pushed branch rejected")
	}
	if e.Pushed(context.Background(), "/repo", "wk/z", "main") {
		t.Fatal("unpushed branch accepted")
	}
}

func TestJobDirAndValidJobName(t *testing.T) {
	h := Host{Name: "local", Jobs: "/jobs"}
	if d := h.JobDir("wk-7"); d != "/jobs/wk-7" {
		t.Fatal(d)
	}
	for _, bad := range []string{"", "../x", "a/b", "..", ".x", "-x", "a b", "a..b"} {
		if ValidJobName(bad) {
			t.Fatal(bad)
		}
		if h.JobDir(bad) != "" {
			t.Fatal(bad)
		}
	}
	if (Host{Name: "local"}).JobDir("wk-7") != "" {
		t.Fatal("job dir without jobs base")
	}
}

func TestProtected(t *testing.T) {
	globs := []string{"*-integrator", "director-*", "fleet-tooling-*", "consult-*", "cmd"}
	for _, name := range []string{"at-integrator", "director-1", "fleet-tooling-2", "consult-g90", "cmd"} {
		if !Protected(name, globs) {
			t.Fatal(name)
		}
	}
	for _, name := range []string{"mw-session-reap", "b2103-luna", "wk-7"} {
		if Protected(name, globs) {
			t.Fatal(name)
		}
	}
}
