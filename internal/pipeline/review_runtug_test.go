package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// runtugExit is a command error carrying an exit status, like exec.ExitError.
type runtugExit int

func (e runtugExit) Error() string { return "exit status" }
func (e runtugExit) ExitCode() int { return int(e) }

// runtugRunner fakes git, the quota tool, headless reviewers, panewire and
// runtug. A spawn writes the configured findings file and spawn receipt into
// the job dir named by its brief path, unless refuse lists its role; a remote
// spawn instead uploads the findings to sink and announces them on the inbox.
type runtugRunner struct {
	fakeRunner
	refuse   map[string]int // runtug --role → exit status
	findings string         // "" writes no findings file
	family   string
	quotaErr error
	usageErr error
	spawns   [][]string
	git      [][]string
	headless int
	remote   *taskJobSink
	inbox    []string // lane.event items, as panewire inbox list prints them
	inboxErr error
	acks     [][]string
	late     int // inbox looks that still show nothing
	looks    int
	addFails bool
	realGit  bool   // run git for real (against a test repository)
	afterRun func() // called once runtug returns
	sink     *taskJobSink
}

func (r *runtugRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	switch command {
	case "git":
		r.git = append(r.git, args)
		if r.realGit {
			b, e := exec.CommandContext(ctx, "git", args...).CombinedOutput()
			if e != nil {
				return b, runtugExit(1)
			}
			return b, nil
		}
		if r.addFails && slices.Contains(args, "add") {
			return nil, runtugExit(128)
		}
		return nil, nil
	case "panewire":
		if args[1] == "ack" {
			r.acks = append(r.acks, args)
			return []byte(`{"acked":1}`), nil
		}
		r.looks++
		if r.inboxErr != nil {
			return nil, r.inboxErr
		}
		if r.looks <= r.late {
			return []byte("[]"), nil
		}
		return []byte("[" + strings.Join(r.inbox, ",") + "]"), nil
	case "scopefuel":
		if r.quotaErr != nil {
			return nil, r.quotaErr
		}
		if r.usageErr != nil && args[0] == "--json" {
			return nil, r.usageErr
		}
		return r.fakeRunner.Run(ctx, command, args, input, dir)
	case "runtug":
		r.spawns = append(r.spawns, args)
		role := args[slices.Index(args, "--role")+1]
		if code, ok := r.refuse[role]; ok {
			return nil, runtugExit(code)
		}
		job := filepath.Dir(args[slices.Index(args, "-p")+1])
		label := arg(args, "-l")
		switch {
		case r.findings == "":
		case r.remote != nil:
			sum := sha256.Sum256([]byte(r.findings))
			sha := hex.EncodeToString(sum[:])
			r.remote.files[sha] = []byte(r.findings)
			r.inbox = append(r.inbox, `{"id":8,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings"}}`)
			r.inbox = append(r.inbox, `{"id":9,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings attachment=`+sha+`"}}`)
		default:
			if e := os.WriteFile(filepath.Join(job, "findings.json"), []byte(r.findings), 0600); e != nil {
				return nil, e
			}
		}
		if e := os.MkdirAll(filepath.Join(job, "events"), 0700); e != nil {
			return nil, e
		}
		if e := os.WriteFile(filepath.Join(job, "events", "0002-spawned.json"), []byte(`{"kind":"job.spawned","payload":{"family":"`+r.family+`"}}`), 0600); e != nil {
			return nil, e
		}
		if r.afterRun != nil {
			r.afterRun()
		}
		return []byte("OK pane=p1 model=seat-x label=l status=working landed=yes job=l quota_record=x/quota_pool\nwait job=l outcome=completed report= pane=p1\n"), nil
	}
	r.headless++
	return r.fakeRunner.Run(ctx, command, args, input, dir)
}

type taskJobSink struct {
	fakeLaneSink
	job, title string
	files      map[string][]byte
	jobs       map[string]integrations.JobRecord // per-id records
	missing    *integrations.JobRecord           // record for any unlisted id
	jobErr     error
	gets       []string
}

func (s *taskJobSink) TaskJob(context.Context, string) (string, string, error) {
	return s.job, s.title, nil
}

func (s *taskJobSink) Job(_ context.Context, id string) (integrations.JobRecord, error) {
	s.gets = append(s.gets, id)
	if s.jobErr != nil {
		return integrations.JobRecord{}, s.jobErr
	}
	if rec, ok := s.jobs[id]; ok {
		return rec, nil
	}
	if s.missing != nil {
		return *s.missing, nil
	}
	return integrations.JobRecord{}, integrations.ErrJobNotFound
}

func (s *taskJobSink) Attachment(_ context.Context, sha string) ([]byte, error) {
	if b, ok := s.files[sha]; ok {
		return b, nil
	}
	return nil, errors.New("not_found")
}

func runtugSetup(t *testing.T, runner *runtugRunner) (*Service, *fakeGH, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	s.Runner = runner
	sink := &taskJobSink{job: "author-job", title: "Fix the thing (grade A, T2)", files: map[string][]byte{}, jobs: map[string]integrations.JobRecord{}}
	s.Sink = sink
	runner.sink = sink
	if runner.remote != nil {
		runner.remote = sink
	}
	grace := findingsGrace
	findingsGrace = 0
	t.Cleanup(func() { findingsGrace = grace })
	s.Config.Review.Executor = "runtug"
	dir := t.TempDir()
	s.Config.Review.Runtug = config.RuntugReview{Command: "runtug", Workspace: "reviews", Host: "auto", Jobs: filepath.Join(dir, "jobs"), Checkouts: filepath.Join(dir, "checkouts")}
	g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Family: anthropic\n"
	g.pull.Base.Ref = "main"
	g.pull.Head.Ref = "feat/thing"
	g.pull.Head.Repo.FullName = "example/project"
	r.Root = filepath.Join(dir, "repo")
	return s, g, r
}

func arg(args []string, flag string) string {
	if i := slices.Index(args, flag); i >= 0 && i+1 < len(args) {
		return args[i+1]
	}
	return ""
}

func TestRuntugPassPostsPaneFindings(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`, family: "openai"}
	s, g, r := runtugSetup(t, runner)
	g.pull.Body += "\n## Operator decisions\n\n- Headless stays the default executor.\n"
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 || runner.headless != 0 || g.postCount != 1 {
		t.Fatal(runner.spawns, runner.headless, g.postCount)
	}
	sp := runner.spawns[0]
	for flag, want := range map[string]string{"-m": "auto", "--builder-job": "author-job", "--builder-family": "anthropic", "--task": "42", "--role": "reviewer", "--t": "T2", "--host": "auto", "-w": "reviews"} {
		if arg(sp, flag) != want {
			t.Fatalf("%s=%q in %v", flag, arg(sp, flag), sp)
		}
	}
	if !slices.Contains(sp, "--wait") || arg(sp, "--wait-timeout") == "" || slices.Contains(sp, "--override-reason") {
		t.Fatal(sp)
	}
	checkout := arg(sp, "-c")
	if !strings.HasPrefix(checkout, s.Config.Review.Runtug.Checkouts) {
		t.Fatal(checkout)
	}
	// The checkout is detached at the reviewed head from pull/7/head; no
	// branch is created and the worktree is removed afterwards.
	want := []string{
		"fetch --no-tags origin pull/7/head main",
		"worktree add --detach " + checkout + " " + shaA,
		"worktree remove --force " + checkout,
	}
	if got := gitCalls(runner, r.Root); !slices.Equal(got, want) {
		t.Fatalf("%q", got)
	}
	brief, e := os.ReadFile(arg(sp, "-p"))
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"example/project#7", shaA, "detached at the head", "default lens", "Author model family: anthropic", "J='" + filepath.Dir(arg(sp, "-p")) + "'", "$J/findings.json", "git rev-parse HEAD", `{"refused":`, "runtug done " + arg(sp, "-l"), "## PR context", "OPERATOR DECISIONS", "Headless stays the default executor."} {
		if !strings.Contains(string(brief), want) {
			t.Fatalf("brief lacks %q:\n%s", want, brief)
		}
	}
	// Without a findings lane the pane is not told to upload anything.
	if strings.Contains(string(brief), "attach put") || slices.Contains(sp, "--exclude-host") {
		t.Fatalf("%v\n%s", sp, brief)
	}
	got := g.passes[shaA][0]
	if (got != github.PassReceipt{Pass: 1, Lens: "default", Reviewer: "runtug:seat-x", Family: "openai", Executor: "runtug", Fallback: "findings returned through the legacy lane/file path (inbox fallback 1)"}) {
		t.Fatalf("%+v", got)
	}
}

// The builder's hk job record supplies --builder-family; a builder without a
// record (pre-records spawn) falls back to the trailer/commit inference.
func TestRuntugBuilderFamilyFromRecord(t *testing.T) {
	for name, tc := range map[string]struct {
		record *integrations.JobRecord
		want   string
	}{
		"record family":       {&integrations.JobRecord{State: "completed", Family: "google"}, "google"},
		"no record":           {nil, "anthropic"},
		"no family on record": {&integrations.JobRecord{State: "completed"}, "anthropic"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai"}
			s, _, r := runtugSetup(t, runner)
			if tc.record != nil {
				runner.sink.jobs["author-job"] = *tc.record
			}
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			sp := runner.spawns[0]
			if arg(sp, "--builder-family") != tc.want || arg(sp, "--builder-job") != "author-job" {
				t.Fatal(sp)
			}
			brief, _ := os.ReadFile(arg(sp, "-p"))
			if !strings.Contains(string(brief), "Author model family: "+tc.want) {
				t.Fatalf("brief lacks family %q:\n%s", tc.want, brief)
			}
		})
	}
}

// gitCalls lists the git invocations against root without the -C prefix.
func gitCalls(r *runtugRunner, root string) []string {
	out := []string{}
	for _, c := range r.git {
		out = append(out, strings.TrimPrefix(strings.Join(c, " "), "-C "+root+" "))
	}
	return out
}

// A pane on another host returns its findings on its hk job record. A pane
// from before `runtug done --result` leaves a record without one and its
// findings still arrive as an attachment announced on the findings lane:
// mergewake fetches, validates and posts them through the legacy fallback,
// counts the use and acknowledges the event.
func TestRuntugRemotePaneReturnsFindingsByAttachment(t *testing.T) {
	for name, record := range map[string]integrations.JobRecord{
		"record result":      {State: "completed", Family: "google", Result: json.RawMessage(`{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`)},
		"legacy inbox reply": {State: "completed", Family: "google"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &runtugRunner{findings: `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`, family: "openai", remote: &taskJobSink{}}
			s, g, r := runtugSetup(t, runner)
			s.Config.Review.Runtug.Lane = "review-desk"
			s.Config.Review.Runtug.Panewire = "panewire"
			s.Config.Review.Runtug.ExcludeHosts = []string{"slow-host", "other-host"}
			runner.sink.missing = &record
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			got := g.passes[shaA]
			if runner.headless != 0 || len(got) != 1 || got[0].Executor != "runtug" || got[0].Reviewer != "runtug:seat-x" || g.postCount != 1 {
				t.Fatalf("headless=%d %+v", runner.headless, got)
			}
			// The record's family wins over the guessed remote family.
			if got[0].Family != "google" {
				t.Fatalf("receipt family %+v", got[0])
			}
			sp := runner.spawns[0]
			if arg(sp, "--exclude-host") != "slow-host,other-host" {
				t.Fatal(sp)
			}
			label := arg(sp, "-l")
			brief, _ := os.ReadFile(arg(sp, "-p"))
			if !strings.Contains(string(brief), "runtug done "+label+` --result "$J/findings.json"`) {
				t.Fatalf("brief lacks runtug done --result:\n%s", brief)
			}
			if strings.Contains(string(brief), "attach put") || strings.Contains(string(brief), "panewire emit") {
				t.Fatalf("brief still teaches the lane return:\n%s", brief)
			}
			if name == "record result" {
				if runner.looks != 0 || s.inboxFallbacks != 0 || got[0].Fallback != "" {
					t.Fatalf("looks=%d fallbacks=%d receipt=%+v", runner.looks, s.inboxFallbacks, got[0])
				}
				return
			}
			// A malformed announcement ahead of the real one is consumed and
			// skipped, and the fallback use is counted.
			if len(runner.acks) != 2 || strings.Join(runner.acks[0], " ") != "inbox ack 8 --lane review-desk --json" || strings.Join(runner.acks[1], " ") != "inbox ack 9 --lane review-desk --json" {
				t.Fatal(runner.acks)
			}
			if s.inboxFallbacks != 1 || !strings.Contains(got[0].Fallback, "inbox fallback 1") {
				t.Fatalf("count=%d fallback=%q", s.inboxFallbacks, got[0].Fallback)
			}
		})
	}
}

// A failed `worktree add --detach` is cleaned up and falls back; nothing
// ever touches a local branch.
func TestRuntugFailedCheckoutFallsBack(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", addFails: true}
	s, g, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	got := gitCalls(runner, r.Root)
	if len(got) != 3 || !strings.HasPrefix(got[1], "worktree add --detach ") || !strings.HasPrefix(got[2], "worktree remove --force ") {
		t.Fatalf("%q", got)
	}
	if p := g.passes[shaA]; len(runner.spawns) != 0 || runner.headless != 1 || !strings.Contains(p[0].Fallback, "checkout PR head "+shaA[:7]) {
		t.Fatalf("spawns=%d %+v", len(runner.spawns), p)
	}
}

// A review cancelled while the findings are awaited ends with the context
// error once, not joined with itself.
func TestRuntugCancelWhileCollectingWrapsOnce(t *testing.T) {
	runner := &runtugRunner{family: "openai"}
	s, g, r := runtugSetup(t, runner)
	findingsGrace = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	runner.afterRun = cancel
	diff, _ := g.Diff(ctx, r.Name, 7)
	x := &runtugExecutor{s: s, repo: r, pr: g.pull, diff: diff, authorFamily: "anthropic"}
	_, e := x.runPass(ctx, &passPlan{pass: 1, lens: "default"}, func() (bool, error) { return true, nil })
	if !errors.Is(e, context.Canceled) || strings.Count(e.Error(), context.Canceled.Error()) != 1 {
		t.Fatalf("%v", e)
	}
}

// GitHub reports a deleted fork's head as "repo": null; the pass falls back to
// headless before the claim, without touching git or runtug.
func TestRuntugDeletedForkFallsBackToHeadless(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := runtugSetup(t, runner)
	var pull github.Pull
	if e := json.Unmarshal([]byte(`{"number":7,"state":"open","head":{"sha":"`+shaA+`","ref":"feat/thing","repo":null},"base":{"sha":"`+shaB+`","ref":"main","repo":{"full_name":"example/project"}}}`), &pull); e != nil {
		t.Fatal(e)
	}
	g.pull.Head = pull.Head
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	got := g.passes[shaA]
	if len(runner.git) != 0 || len(runner.spawns) != 0 || runner.headless != 1 || len(got) != 1 || !strings.Contains(got[0].Fallback, "deleted fork") {
		t.Fatalf("git=%v spawns=%d headless=%d %+v", runner.git, len(runner.spawns), runner.headless, got)
	}
}

// An announcement that trails runtug's completion is waited for.
func TestRuntugWaitsForLateAnnouncement(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", remote: &taskJobSink{}, late: 2}
	s, g, r := runtugSetup(t, runner)
	s.Config.Review.Runtug.Lane = "review-desk"
	s.Config.Review.Runtug.Panewire = "panewire"
	runner.sink.missing = &integrations.JobRecord{State: "completed"}
	poll := findingsPoll
	findingsGrace, findingsPoll = time.Minute, time.Millisecond
	t.Cleanup(func() { findingsPoll = poll })
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if got := g.passes[shaA]; runner.looks != 3 || runner.headless != 0 || got[0].Executor != "runtug" {
		t.Fatalf("looks=%d headless=%d %+v", runner.looks, runner.headless, got)
	}
}

// With a findings lane configured, a pane on this host that left only the
// local file is still read; a lane that cannot be read falls back to it too.
func TestRuntugLocalFindingsFileStillRead(t *testing.T) {
	for name, inboxErr := range map[string]error{"no event": nil, "inbox down": errors.New("hub unavailable")} {
		t.Run(name, func(t *testing.T) {
			runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", inboxErr: inboxErr}
			s, g, r := runtugSetup(t, runner)
			s.Config.Review.Runtug.Lane = "review-desk"
			s.Config.Review.Runtug.Panewire = "panewire"
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			if got := g.passes[shaA]; runner.headless != 0 || len(got) != 1 || got[0].Executor != "runtug" || len(runner.acks) != 0 {
				t.Fatalf("headless=%d %+v", runner.headless, got)
			}
		})
	}
}

// A pane whose worktree is not at the head refuses; the refusal is never
// posted as a clean review.
func TestRuntugHeadRefusalFallsBackToHeadless(t *testing.T) {
	runner := &runtugRunner{findings: `{"refused":"HEAD is 1234567, expected ` + shaA + `"}`, family: "openai"}
	s, g, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	got := g.passes[shaA]
	if runner.headless != 1 || len(got) != 1 || got[0].Executor != "" || !strings.Contains(got[0].Fallback, "refused: HEAD is 1234567") {
		t.Fatalf("headless=%d %+v", runner.headless, got)
	}
}

// The detached checkout never names the author's branch, so a host where
// that branch is checked out in another worktree is not refused. A fork PR
// still falls back: runtug replicates only a spec on the repo's origin.
func TestRuntugCheckoutDetached(t *testing.T) {
	for name, tc := range map[string]struct {
		fork   string
		want   []string
		reason string
	}{
		"same repo": {want: []string{"worktree add --detach <checkout> " + shaA, "worktree remove --force <checkout>"}},
		"fork":      {fork: "someone/project", want: []string{}, reason: "head branch is not in example/project (fork PR)"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai"}
			s, g, r := runtugSetup(t, runner)
			if tc.fork != "" {
				g.pull.Head.Repo.FullName = tc.fork
			}
			if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
				t.Fatal(e)
			}
			got := gitCalls(runner, r.Root)
			if len(got) > 0 {
				got = got[1:] // fetch
			}
			if len(runner.spawns) > 0 {
				for i := range got {
					got[i] = strings.ReplaceAll(got[i], arg(runner.spawns[0], "-c"), "<checkout>")
				}
			}
			p := g.passes[shaA]
			if !slices.Equal(got, tc.want) {
				t.Fatalf("%q", got)
			}
			if tc.reason == "" {
				if p[0].Executor != "runtug" {
					t.Fatalf("%+v", p)
				}
				return
			}
			if len(runner.spawns) != 0 || runner.headless != 1 || !strings.Contains(p[0].Fallback, tc.reason) {
				t.Fatalf("spawns=%d %+v", len(runner.spawns), p)
			}
		})
	}
}

// Against a real repository the pane's worktree is detached at the reviewed
// head fetched from pull/<n>/head — no local branch appears before, during or
// after the pass, and the worktree is gone once the wait ends. The author's
// branch checked out in the clone's own worktree does not refuse the pass.
func TestRuntugDetachedCheckoutRealRepo(t *testing.T) {
	gitRun := func(dir string, args ...string) string {
		t.Helper()
		b, e := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
		if e != nil {
			t.Fatalf("git %v: %v\n%s", args, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	src := filepath.Join(t.TempDir(), "src")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	gitRun(src, "init", "-b", "main")
	gitRun(src, "config", "user.email", "t@t")
	gitRun(src, "config", "user.name", "t")
	if e := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	gitRun(src, "add", ".")
	gitRun(src, "commit", "-m", "c")
	head := gitRun(src, "rev-parse", "HEAD")
	gitRun(src, "update-ref", "refs/pull/7/head", "HEAD")
	root := filepath.Join(t.TempDir(), "repo")
	if b, e := exec.Command("git", "clone", src, root).CombinedOutput(); e != nil {
		t.Fatalf("%v\n%s", e, b)
	}
	// The author's branch, checked out in the clone's only worktree.
	gitRun(root, "checkout", "-b", "feat/thing")
	branches := gitRun(root, "branch", "--list")

	var during []string
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", realGit: true}
	runner.afterRun = func() {
		checkout := arg(runner.spawns[0], "-c")
		during = append(during,
			gitRun(root, "worktree", "list", "--porcelain"),
			gitRun(checkout, "rev-parse", "HEAD"),
			gitRun(root, "branch", "--list"),
		)
	}
	s, g, r := runtugSetup(t, runner)
	r.Root = root
	g.pull.Head.SHA = head
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if p := g.passes[head]; len(p) != 1 || p[0].Executor != "runtug" {
		t.Fatalf("%+v", p)
	}
	if len(during) != 3 || !strings.Contains(during[0], arg(runner.spawns[0], "-c")) || during[1] != head || during[2] != branches {
		t.Fatalf("%q", during)
	}
	if w := gitRun(root, "worktree", "list", "--porcelain"); strings.Count(w, "worktree ") != 1 {
		t.Fatalf("worktree left on the host:\n%s", w)
	}
	if b := gitRun(root, "branch", "--list"); b != branches {
		t.Fatalf("branches changed: %q -> %q", branches, b)
	}
}

func TestRuntugFamilyRefusalRetriesAsWorker(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "anthropic", refuse: map[string]int{"reviewer": runtugRefused}}
	s, g, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 || arg(runner.spawns[1], "--role") != "worker" || runner.headless != 0 {
		t.Fatal(runner.spawns, runner.headless)
	}
	got := g.passes[shaA][0]
	if got.Executor != "runtug" || !got.SameFamily || got.Family != "anthropic" || !strings.Contains(got.Fallback, "outside the author family") {
		t.Fatalf("%+v", got)
	}
}

func TestRuntugFallsBackToHeadless(t *testing.T) {
	for name, tc := range map[string]struct {
		body   string
		runner *runtugRunner
		reason string
	}{
		"no task trailer":   {"Mergewake-Author-Family: openai\n", &runtugRunner{findings: `{"findings":[]}`}, "no Mergewake-Task trailer"},
		"spawn refused":     {"", &runtugRunner{refuse: map[string]int{"reviewer": 70}}, "runtug spawn exit 70"},
		"refused twice":     {"", &runtugRunner{refuse: map[string]int{"reviewer": runtugRefused, "worker": runtugRefused}}, "runtug spawn exit 77"},
		"no findings file":  {"", &runtugRunner{}, "wrote no findings file"},
		"findings off diff": {"", &runtugRunner{findings: `{"findings":[{"path":"other","line":9,"body":"x"}]}`}, "findings rejected"},
	} {
		t.Run(name, func(t *testing.T) {
			s, g, r := runtugSetup(t, tc.runner)
			if tc.body != "" {
				g.pull.Body = tc.body
			}
			if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
				t.Fatal(e)
			}
			got := g.passes[shaA]
			if tc.runner.headless != 1 || len(got) != 1 || got[0].Reviewer != "claude" || got[0].Executor != "" || !strings.HasPrefix(got[0].Fallback, "runtug: ") || !strings.Contains(got[0].Fallback, tc.reason) {
				t.Fatalf("headless=%d %+v", tc.runner.headless, got)
			}
			// One lease covers both executors of the pass.
			if g.claimCount != 2 { // claim ref + done marker
				t.Fatalf("claims=%d", g.claimCount)
			}
		})
	}
}

func TestRuntugNeedsNoQuotaTool(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", quotaErr: errors.New("scopefuel: not found")}
	s, g, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if g.postCount != 1 || g.passes[shaA][0].Executor != "runtug" {
		t.Fatal(g.passes)
	}
	// Without quota or a working pane the pass fails with both reasons.
	runner = &runtugRunner{quotaErr: errors.New("scopefuel: not found")}
	s, g, r = runtugSetup(t, runner)
	e := s.Review(context.Background(), r, 7, "", Options{})
	if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "runtug: no findings") || !strings.Contains(e.Error(), "headless: review unavailable: quota recommendation") || g.postCount != 0 {
		t.Fatal(e, g.postCount)
	}
}

func TestHeadlessExecutorUnchangedWithoutRuntug(t *testing.T) {
	runner := &runtugRunner{}
	s, g, r := runtugSetup(t, runner)
	s.Config.Review.Executor = "headless"
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 0 || runner.headless != 1 || g.passes[shaA][0].Fallback != "" || g.passes[shaA][0].Executor != "" {
		t.Fatal(runner.spawns, g.passes)
	}
}

// A headless skip before the fallback succeeds must not replace the runtug
// reason recorded in the receipt.
func TestRuntugFallbackReasonSurvivesHeadlessSkips(t *testing.T) {
	runner := &runtugRunner{fakeRunner: fakeRunner{recommend: []string{"bad", "claude"}}, refuse: map[string]int{"reviewer": 70}}
	s, g, r := runtugSetup(t, runner)
	s.Config.Review.Pairings = map[string][]string{"openai": {"bad", "claude"}}
	s.Config.Review.Profiles["bad"] = config.Reviewer{Family: "xai", Command: "grok", Args: []string{"--unknown", "x"}}
	out := &bytes.Buffer{}
	s.Out = out
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	got := g.passes[shaA][0]
	if !strings.Contains(out.String(), `"reviewer":"bad","reason":"reviewer profile rejected"`) || got.Reviewer != "claude" || got.Fallback != "runtug: runtug spawn exit 70: exit status" {
		t.Fatalf("%+v", got)
	}
}

// A pass run without a context deadline waits up to review.timeout instead of
// skipping the pane for lack of time.
func TestRuntugPassWithoutDeadlineUsesReviewTimeout(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := runtugSetup(t, runner)
	s.Config.Review.Timeout = "10m"
	runner.sink.missing = &integrations.JobRecord{State: "completed"}
	diff, _ := g.Diff(context.Background(), r.Name, 7)
	x := &runtugExecutor{s: s, repo: r, pr: g.pull, diff: diff, authorFamily: "anthropic"}
	out, e := x.runPass(context.Background(), &passPlan{pass: 1, lens: "default"}, func() (bool, error) { return true, nil })
	if e != nil || out.executor != "runtug" || len(out.skips) != 0 || len(runner.spawns) != 1 {
		t.Fatalf("%+v %v %v", out, e, runner.spawns)
	}
	// A minute of the ten is kept back for posting.
	if w := arg(runner.spawns[0], "--wait-timeout"); w != "539" && w != "540" {
		t.Fatalf("--wait-timeout %s", w)
	}
}

// In runtug mode a failed usage read keeps the recommendation: the headless
// fallback still has its ranked candidates, ordered without pace.
func TestRuntugUsageFailureKeepsRankedFallback(t *testing.T) {
	runner := &runtugRunner{usageErr: errors.New("usage snapshot unavailable"), refuse: map[string]int{"reviewer": 70}}
	s, g, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	got := g.passes[shaA]
	if runner.headless != 1 || len(got) != 1 || got[0].Reviewer != "claude" || !strings.Contains(got[0].Fallback, "runtug spawn exit 70") {
		t.Fatalf("headless=%d %+v", runner.headless, got)
	}
}

// A failed add -b that left no branch at the PR head never deletes a branch:
// a rev-parse failure is not proof the branch was absent.
func TestRuntugFailedCheckoutKeepsUnprovenBranch(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", addFails: true}
	s, _, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	for _, c := range gitCalls(runner, r.Root) {
		if strings.HasPrefix(c, "branch -D") {
			t.Fatalf("deleted a branch it did not prove it created: %q", gitCalls(runner, r.Root))
		}
	}
}

// The W4 rule decides what a finished reviewer pane's hk record means:
// completed with a result uses it; completed without one, a pending or
// missing record, and an unreadable hk fall back after the bounded poll;
// escalated and lost fall back immediately and the receipt keeps the state.
func TestRuntugJobRecordResultW4(t *testing.T) {
	findings := `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`
	for name, tc := range map[string]struct {
		record   *integrations.JobRecord
		jobErr   error
		result   string
		executor string // pass executor: "runtug" or "" (headless)
		reason   string
		polled   bool // the record was asked for more than once
	}{
		"completed with result":    {record: &integrations.JobRecord{State: "completed", Family: "google", Result: json.RawMessage(findings)}, executor: "runtug"},
		"refusal in the result":    {record: &integrations.JobRecord{State: "completed", Result: json.RawMessage(`{"refused":"HEAD is 1234567"}`)}, reason: "refused: HEAD is 1234567"},
		"completed without result": {record: &integrations.JobRecord{State: "completed"}, reason: "completed without a result"},
		"escalated":                {record: &integrations.JobRecord{State: "escalated"}, reason: "reviewer job record escalated"},
		"lost":                     {record: &integrations.JobRecord{State: "lost"}, reason: "reviewer job record lost"},
		"still spawned":            {record: &integrations.JobRecord{State: "spawned"}, reason: "job record state spawned", polled: true},
		"no record":                {reason: "no job record", polled: true},
		"hk unreadable":            {jobErr: errors.New("connection refused"), reason: "hk-unavailable", polled: true},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &runtugRunner{family: "openai"}
			s, g, r := runtugSetup(t, runner)
			findingsGrace, findingsPoll = 30*time.Millisecond, time.Millisecond
			if tc.record != nil {
				runner.sink.missing = tc.record
			}
			runner.sink.jobErr = tc.jobErr
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			got := g.passes[shaA]
			if len(got) != 1 || got[0].Executor != tc.executor {
				t.Fatalf("%+v", got)
			}
			if tc.executor == "runtug" {
				if got[0].Family != "google" || runner.headless != 0 {
					t.Fatalf("%+v headless=%d", got[0], runner.headless)
				}
				return
			}
			if runner.headless != 1 || !strings.Contains(got[0].Fallback, tc.reason) {
				t.Fatalf("headless=%d %+v", runner.headless, got[0])
			}
			reads := 0
			for _, id := range runner.sink.gets {
				if id != "author-job" {
					reads++
				}
			}
			if tc.polled && reads < 2 {
				t.Fatalf("record reads=%d, want a poll loop", reads)
			}
		})
	}
}
