package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
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
	branchAt string // local head branch tip; "" means the branch is absent
	remote   *taskJobSink
	inbox    []string // lane.event items, as panewire inbox list prints them
	inboxErr error
	acks     [][]string
	late     int // inbox looks that still show nothing
	looks    int
	addFails bool
	// branchAfterAdd is where a failed `worktree add -b` leaves the branch
	// ("" = not created).
	branchAfterAdd string
	addTried       bool
	afterRun       func() // called once runtug returns
}

func (r *runtugRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	switch command {
	case "git":
		r.git = append(r.git, args)
		if r.addFails && slices.Contains(args, "add") {
			r.addTried = true
			return nil, runtugExit(128)
		}
		if slices.Contains(args, "rev-parse") {
			if r.addTried && r.branchAfterAdd != "" {
				return []byte(r.branchAfterAdd + "\n"), nil
			}
			if r.branchAt == "" {
				return nil, runtugExit(1)
			}
			return []byte(r.branchAt + "\n"), nil
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
}

func (s *taskJobSink) TaskJob(context.Context, string) (string, string, error) {
	return s.job, s.title, nil
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
	sink := &taskJobSink{job: "author-job", title: "Fix the thing (grade A, T2)", files: map[string][]byte{}}
	s.Sink = sink
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
	// The checkout is the head branch, created at the head and removed with
	// its branch afterwards.
	want := []string{
		"fetch --no-tags origin pull/7/head main +refs/heads/feat/thing:refs/remotes/origin/feat/thing",
		"rev-parse --verify --quiet refs/heads/feat/thing",
		"worktree add -b feat/thing " + checkout + " " + shaA,
		"worktree remove --force " + checkout,
		"branch -D feat/thing",
	}
	if got := gitCalls(runner, r.Root); !slices.Equal(got, want) {
		t.Fatalf("%q", got)
	}
	brief, e := os.ReadFile(arg(sp, "-p"))
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"example/project#7", shaA, "on branch feat/thing", "default lens", "Author model family: anthropic", "J='" + filepath.Dir(arg(sp, "-p")) + "'", "$J/findings.json", "git rev-parse HEAD", `{"refused":`, "runtug done " + arg(sp, "-l"), "## PR context", "OPERATOR DECISIONS", "Headless stays the default executor."} {
		if !strings.Contains(string(brief), want) {
			t.Fatalf("brief lacks %q:\n%s", want, brief)
		}
	}
	// Without a findings lane the pane is not told to upload anything.
	if strings.Contains(string(brief), "attach put") || slices.Contains(sp, "--exclude-host") {
		t.Fatalf("%v\n%s", sp, brief)
	}
	got := g.passes[shaA][0]
	if (got != github.PassReceipt{Pass: 1, Lens: "default", Reviewer: "runtug:seat-x", Family: "openai", Executor: "runtug"}) {
		t.Fatalf("%+v", got)
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

// A pane on another host returns its findings as an attachment announced on
// the findings lane; mergewake fetches, validates and posts them, and
// acknowledges the event.
func TestRuntugRemotePaneReturnsFindingsByAttachment(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`, family: "openai", remote: &taskJobSink{}}
	s, g, r := runtugSetup(t, runner)
	s.Config.Review.Runtug.Lane = "review-desk"
	s.Config.Review.Runtug.Panewire = "panewire"
	s.Config.Review.Runtug.ExcludeHosts = []string{"slow-host", "other-host"}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	got := g.passes[shaA]
	if runner.headless != 0 || len(got) != 1 || got[0].Executor != "runtug" || got[0].Reviewer != "runtug:seat-x" || g.postCount != 1 {
		t.Fatalf("headless=%d %+v", runner.headless, got)
	}
	sp := runner.spawns[0]
	if arg(sp, "--exclude-host") != "slow-host,other-host" {
		t.Fatal(sp)
	}
	// A malformed announcement ahead of the real one is consumed and skipped.
	if len(runner.acks) != 2 || strings.Join(runner.acks[0], " ") != "inbox ack 8 --lane review-desk --json" || strings.Join(runner.acks[1], " ") != "inbox ack 9 --lane review-desk --json" {
		t.Fatal(runner.acks)
	}
	brief, _ := os.ReadFile(arg(sp, "-p"))
	label := arg(sp, "-l")
	for _, want := range []string{`handoffkeep attach put "$J/findings.json" --name ` + label + "-findings.json", "panewire emit --kind lane.event --lane review-desk --deliver inbox --type report --event-id " + label + "-findings --text '[" + label + "] findings attachment=<sha256>'"} {
		if !strings.Contains(string(brief), want) {
			t.Fatalf("brief lacks %q:\n%s", want, brief)
		}
	}
}

// A failed `worktree add -b` may already have created the branch; it is
// deleted so later heads of the PR are not refused as "left untouched".
func TestRuntugFailedCheckoutDeletesCreatedBranch(t *testing.T) {
	runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", addFails: true, branchAfterAdd: shaA}
	s, g, r := runtugSetup(t, runner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	got := gitCalls(runner, r.Root)
	if len(got) != 6 || !strings.HasPrefix(got[2], "worktree add -b feat/thing ") || got[3] != "rev-parse --verify --quiet refs/heads/feat/thing" || !strings.HasPrefix(got[4], "worktree remove --force ") || got[5] != "branch -D feat/thing" {
		t.Fatalf("%q", got)
	}
	if p := g.passes[shaA]; len(runner.spawns) != 0 || runner.headless != 1 || !strings.Contains(p[0].Fallback, "checkout PR head branch feat/thing") {
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

func TestRuntugCheckoutBranch(t *testing.T) {
	for name, tc := range map[string]struct {
		branchAt, fork string
		want           []string
		reason         string
	}{
		"local branch at head":   {branchAt: shaA, want: []string{"rev-parse --verify --quiet refs/heads/feat/thing", "worktree add <checkout> feat/thing", "worktree remove --force <checkout>"}},
		"local branch elsewhere": {branchAt: shaB, want: []string{"rev-parse --verify --quiet refs/heads/feat/thing"}, reason: "local branch feat/thing is at " + shaB[:7] + ", not the PR head; left untouched"},
		"fork":                   {fork: "someone/project", want: []string{}, reason: "head branch is not in example/project (fork PR)"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &runtugRunner{findings: `{"findings":[]}`, family: "openai", branchAt: tc.branchAt}
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
