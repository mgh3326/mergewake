package pipeline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// runtugExit is a command error carrying an exit status, like exec.ExitError.
type runtugExit int

func (e runtugExit) Error() string { return "exit status" }
func (e runtugExit) ExitCode() int { return int(e) }

// runtugRunner fakes git, the quota tool, headless reviewers and runtug. A
// spawn writes the configured findings file and spawn receipt into the job dir
// named by its brief path, unless refuse lists its role.
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
}

func (r *runtugRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	switch command {
	case "git":
		r.git = append(r.git, args)
		return nil, nil
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
		if r.findings != "" {
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
		return []byte("OK pane=p1 model=seat-x label=l status=working landed=yes job=l quota_record=x/quota_pool\nwait job=l outcome=completed report= pane=p1\n"), nil
	}
	r.headless++
	return r.fakeRunner.Run(ctx, command, args, input, dir)
}

type taskJobSink struct {
	fakeLaneSink
	job, title string
}

func (s *taskJobSink) TaskJob(context.Context, string) (string, string, error) {
	return s.job, s.title, nil
}

func runtugSetup(t *testing.T, runner *runtugRunner) (*Service, *fakeGH, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	s.Runner = runner
	s.Sink = &taskJobSink{job: "author-job", title: "Fix the thing (grade A, T2)"}
	s.Config.Review.Executor = "runtug"
	dir := t.TempDir()
	s.Config.Review.Runtug = config.RuntugReview{Command: "runtug", Workspace: "reviews", Host: "auto", Jobs: filepath.Join(dir, "jobs"), Checkouts: filepath.Join(dir, "checkouts")}
	g.pull.Body = "Mergewake-Task: 42\nMergewake-Author-Family: anthropic\n"
	g.pull.Base.Ref = "main"
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
	for flag, want := range map[string]string{"-m": "auto", "--parent-job": "author-job", "--role": "reviewer", "--t": "T2", "--host": "auto", "-w": "reviews"} {
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
	// The checkout is fetched, added at the head, and removed afterwards.
	gitCalls := []string{}
	for _, c := range runner.git {
		gitCalls = append(gitCalls, strings.Join(c, " "))
	}
	want := []string{"-C " + r.Root + " fetch --no-tags origin pull/7/head main", "-C " + r.Root + " worktree add --detach " + checkout + " " + shaA, "-C " + r.Root + " worktree remove --force " + checkout}
	if !slices.Equal(gitCalls, want) {
		t.Fatalf("%q", gitCalls)
	}
	brief, e := os.ReadFile(arg(sp, "-p"))
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"example/project#7", shaA, "default lens", "Author model family: anthropic", filepath.Join(filepath.Dir(arg(sp, "-p")), "findings.json"), "runtug done " + arg(sp, "-l"), "## PR context", "OPERATOR DECISIONS", "Headless stays the default executor."} {
		if !strings.Contains(string(brief), want) {
			t.Fatalf("brief lacks %q:\n%s", want, brief)
		}
	}
	got := g.passes[shaA][0]
	if (got != github.PassReceipt{Pass: 1, Lens: "default", Reviewer: "runtug:seat-x", Family: "openai", Executor: "runtug"}) {
		t.Fatalf("%+v", got)
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
	if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "runtug: reviewer pane") || !strings.Contains(e.Error(), "headless: review unavailable: quota recommendation") || g.postCount != 0 {
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
