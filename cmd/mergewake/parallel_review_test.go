package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// reviewHub is a stateful fake GitHub for one repository with n open PRs:
// review leases, done markers and posted reviews persist across requests, so
// a review runs end to end through the real Service and github.Client.
type reviewHub struct {
	mu sync.Mutex
	n  int
	// first is the number of the first PR; zero means 1. Repositories in one
	// test take different numbers, so their heads differ as real ones do.
	first   int
	repo    string
	refs    map[string]string // branch → sha
	commits map[string]string // sha → message
	reviews map[int][]map[string]any
	seq     int
	trace   *eventLog
	// taskPerPR gives PR n the trailer Mergewake-Task: 1000+n, else every PR
	// links task 42.
	taskPerPR bool
	// changedFiles is the changed_files every PR reports; zero means 1, the
	// number of files the hub lists.
	changedFiles int
}

func (h *reviewHub) repoName() string {
	if h.repo == "" {
		return "o/one"
	}
	return h.repo
}

const hubBase = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func hubHead(n int) string { return fmt.Sprintf("%040x", n) }

var hubPolicy = []byte("version: 1\nenabled: true\nrequired_checks: [CI]\n")

func (h *reviewHub) pull(n int) string {
	task := 42
	if h.taskPerPR {
		task = 1000 + n
	}
	return fmt.Sprintf(`{"number":%d,"state":"open","title":"change %d","body":"Mergewake-Author-Family: openai\nMergewake-Task: %d\n","changed_files":%d,"head":{"sha":%q,"ref":"b%d","repo":{"full_name":%q}},"base":{"ref":"main","sha":%q},"user":{"login":"author","type":"User"},"mergeable":true,"mergeable_state":"clean"}`, n, n, task, max(h.changedFiles, 1), hubHead(n), n, h.repoName(), hubBase)
}

var prPath = regexp.MustCompile(`/(?:pulls|issues)/(\d+)(/|$)`)

func (h *reviewHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	path := r.URL.Path
	body, _ := io.ReadAll(r.Body)
	number := 0
	if m := prPath.FindStringSubmatch(path); m != nil {
		number, _ = strconv.Atoi(m[1])
	}
	switch {
	case path == "/graphql":
		head := hubBase
		if m := regexp.MustCompile(`"number":(\d+)`).FindSubmatch(body); m != nil {
			n, _ := strconv.Atoi(string(m[1]))
			head = hubHead(n)
		}
		fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"author":{"login":"author","__typename":"User"},"headRefOid":%q,"reviewDecision":"","reviews":{"nodes":[]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`, head)
	case strings.HasSuffix(path, "/pulls") && r.Method == "GET":
		if r.URL.Query().Get("state") == "closed" {
			fmt.Fprint(w, `[]`)
			return
		}
		var all []string
		for i := max(h.first, 1); i < max(h.first, 1)+h.n; i++ {
			all = append(all, h.pull(i))
		}
		fmt.Fprintf(w, "[%s]", strings.Join(all, ","))
	case strings.HasSuffix(path, "/reviews") && number > 0:
		if r.Method == "POST" {
			var in struct {
				CommitID string `json:"commit_id"`
				Body     string `json:"body"`
			}
			_ = json.Unmarshal(body, &in)
			h.seq++
			h.reviews[number] = append(h.reviews[number], map[string]any{"id": h.seq, "user": map[string]string{"login": "reviewer"}, "commit_id": in.CommitID, "body": in.Body, "state": "COMMENTED"})
			fmt.Fprint(w, `{}`)
			return
		}
		b, _ := json.Marshal(append([]map[string]any{}, h.reviews[number]...))
		w.Write(b)
	case strings.HasSuffix(path, "/files") && number > 0:
		fmt.Fprint(w, `[{"filename":"tool","status":"modified","additions":1,"deletions":0,"changes":1}]`)
	case strings.HasSuffix(path, "/merge") && number > 0:
		h.trace.add(fmt.Sprintf("merge %d", number))
		http.Error(w, `{"message":"Method Not Allowed"}`, 405)
	case regexp.MustCompile(`/pulls/\d+$`).MatchString(path):
		if strings.Contains(r.Header.Get("Accept"), "diff") {
			fmt.Fprintf(w, "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old %d\n+new %d\n", number, number)
			return
		}
		fmt.Fprint(w, h.pull(number))
	case strings.HasPrefix(path, "/repos/"+h.repoName()+"/git/ref/heads/"):
		branch := strings.TrimPrefix(path, "/repos/"+h.repoName()+"/git/ref/heads/")
		if branch == "main" {
			fmt.Fprintf(w, `{"object":{"sha":%q}}`, hubBase)
			return
		}
		sha, ok := h.refs[branch]
		if !ok {
			http.Error(w, `{"message":"Not Found"}`, 404)
			return
		}
		fmt.Fprintf(w, `{"object":{"sha":%q}}`, sha)
	case path == "/repos/"+h.repoName()+"/git/refs" && r.Method == "POST":
		var in struct{ Ref, SHA string }
		_ = json.Unmarshal(body, &in)
		branch := strings.TrimPrefix(in.Ref, "refs/heads/")
		if _, ok := h.refs[branch]; ok {
			http.Error(w, `{"message":"Reference already exists"}`, 422)
			return
		}
		h.refs[branch] = in.SHA
		fmt.Fprint(w, `{}`)
	case strings.HasPrefix(path, "/repos/"+h.repoName()+"/git/refs/heads/") && r.Method == "PATCH":
		var in struct{ SHA string }
		_ = json.Unmarshal(body, &in)
		h.refs[strings.TrimPrefix(path, "/repos/"+h.repoName()+"/git/refs/heads/")] = in.SHA
		fmt.Fprint(w, `{}`)
	case path == "/repos/"+h.repoName()+"/git/commits" && r.Method == "POST":
		var in struct{ Message string }
		_ = json.Unmarshal(body, &in)
		h.seq++
		sha := fmt.Sprintf("%040x", 1<<40+h.seq)
		h.commits[sha] = in.Message
		fmt.Fprintf(w, `{"sha":%q}`, sha)
	case strings.HasPrefix(path, "/repos/"+h.repoName()+"/commits/") && strings.HasSuffix(path, "/pulls"):
		fmt.Fprint(w, `[]`)
	case strings.HasPrefix(path, "/repos/"+h.repoName()+"/commits/") && strings.HasSuffix(path, "/check-runs"):
		fmt.Fprint(w, `{"check_runs":[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]}`)
	case strings.HasPrefix(path, "/repos/"+h.repoName()+"/commits/") && strings.HasSuffix(path, "/statuses"):
		fmt.Fprint(w, `[]`)
	case strings.HasPrefix(path, "/repos/"+h.repoName()+"/commits/"):
		sha := strings.TrimPrefix(path, "/repos/"+h.repoName()+"/commits/")
		msg := h.commits[sha]
		if msg == "" {
			msg = "change"
		}
		fmt.Fprintf(w, `{"sha":%q,"commit":{"tree":{"sha":"tree"},"message":%q,"committer":{"date":%q}}}`, sha, msg, time.Now().UTC().Format(time.RFC3339))
	case strings.HasSuffix(path, "/git/trees/tree"):
		fmt.Fprint(w, `{"tree":[{"path":"policy.yaml","mode":"100644","type":"blob","sha":"blob"}],"truncated":false}`)
	case strings.HasSuffix(path, "/git/blobs/blob"):
		fmt.Fprintf(w, `{"content":%q,"encoding":"base64","size":%d}`, base64.StdEncoding.EncodeToString(hubPolicy), len(hubPolicy))
	case strings.HasSuffix(path, "/actions/runs"):
		fmt.Fprint(w, `{"workflow_runs":[]}`)
	case strings.Contains(path, "/compare/"):
		fmt.Fprintf(w, `{"status":"ahead","merge_base_commit":{"sha":%q}}`, hubBase)
	case strings.HasSuffix(path, "/comments"):
		fmt.Fprint(w, `[]`)
	default:
		h.trace.add("other " + r.Method + " " + path)
		http.Error(w, `{"message":"Not Found"}`, 404)
	}
}

// eventLog records the order of reviews and settles across goroutines.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// blockingRunner holds each fake panewire spawn until released.
type blockingRunner struct {
	trace    *eventLog
	release  chan struct{}
	mu       sync.Mutex
	inFlight int
	peak     int
	started  int
	returned int
	// startedAfter[i] is how many reviews had returned when review i+1 began.
	startedAfter []int
	changed      chan struct{}
}

var reviewedBrief = regexp.MustCompile(`#(\d+) pass`)

func (b *blockingRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command != "panewire" || len(args) == 0 || args[0] != "spawn" {
		return nil, nil
	}
	i := slices.Index(args, "-p")
	if i < 0 || i+1 >= len(args) {
		return nil, errors.New("missing brief")
	}
	brief, err := os.ReadFile(args[i+1])
	if err != nil {
		return nil, err
	}
	pr := "?"
	if m := reviewedBrief.FindSubmatch(brief); m != nil {
		pr = string(m[1])
	}
	b.mu.Lock()
	b.inFlight++
	b.started++
	b.peak = max(b.peak, b.inFlight)
	b.startedAfter = append(b.startedAfter, b.returned)
	b.mu.Unlock()
	b.trace.add("review " + pr)
	b.notify()
	<-b.release
	if err := os.WriteFile(filepath.Join(filepath.Dir(args[i+1]), "findings.json"), []byte(`{"findings":[]}`), 0600); err != nil {
		return nil, err
	}
	b.trace.add("reviewed " + pr)
	b.mu.Lock()
	b.inFlight--
	b.returned++
	b.mu.Unlock()
	b.notify()
	return []byte("OK pane=p1 model=seat-x status=working landed=yes\nwait outcome=completed\n"), nil
}

func (b *blockingRunner) notify() {
	select {
	case b.changed <- struct{}{}:
	default:
	}
}

func (b *blockingRunner) state() (inFlight, peak, started, returned int, after []int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight, b.peak, b.started, b.returned, append([]int(nil), b.startedAfter...)
}

// waitFor polls the runner state until cond holds, failing after 20s.
func (b *blockingRunner) waitFor(t *testing.T, what string, cond func(inFlight, started, returned int) bool) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		in, _, started, returned, _ := b.state()
		if cond(in, started, returned) {
			return
		}
		select {
		case <-b.changed:
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for %s: in flight %d, started %d, returned %d", what, in, started, returned)
		}
	}
}

// recordingNotifier records each lane event; a notify is the settle step
// NotifyPR takes for a READY or BLOCKED verdict.
type recordingNotifier struct{ trace *eventLog }

var eventPR = regexp.MustCompile(`#(\d+) `)

func (n recordingNotifier) Notify(_ context.Context, e integrations.Event) error {
	pr := "?"
	if m := eventPR.FindStringSubmatch(e.Text); m != nil {
		pr = m[1]
	}
	n.trace.add("notify " + pr)
	return nil
}

type parallelFixture struct {
	hub    *reviewHub
	s      *pipeline.Service
	gh     *github.Client
	repo   config.Repo
	runner *blockingRunner
	trace  *eventLog
	out    *bytes.Buffer
}

type parallelSink struct{ integrations.Noop }

func (parallelSink) TaskJob(context.Context, string) (string, string, error) {
	return "", "fixture (grade B, T2)", nil
}

// fixtureJob is the hk job the fixture task binds to: the PR author's worker
// lane, where READY and MERGED go.
const fixtureJob = "w-fixture"

// workerSink is parallelSink with an author job binding. It reads job records
// (JobReader), so the engine may schedule revisits; a fixture that must model
// a sink without job records uses parallelSink instead.
type workerSink struct{ parallelSink }

func (workerSink) TaskJob(context.Context, string) (string, string, error) {
	return fixtureJob, "fixture (grade B, T2)", nil
}

func (workerSink) Job(_ context.Context, id string) (integrations.JobRecord, error) {
	if id == fixtureJob {
		return integrations.JobRecord{State: "spawned", Role: "worker", Label: "fixture-lane"}, nil
	}
	return integrations.JobRecord{}, integrations.ErrJobNotFound
}

func newParallelFixture(t *testing.T, prs, parallel int) *parallelFixture {
	return newParallelFixtureForRepo(t, prs, parallel, "o/one")
}

func newParallelFixtureForRepo(t *testing.T, prs, parallel int, name string) *parallelFixture {
	t.Helper()
	trace := &eventLog{}
	hub := &reviewHub{n: prs, repo: name, refs: map[string]string{}, commits: map[string]string{}, reviews: map[int][]map[string]any{}, trace: trace}
	srv := httptest.NewServer(hub)
	t.Cleanup(srv.Close)
	repo := config.Repo{Name: name, Branch: "main", Policy: "policy.yaml", Root: t.TempDir()}
	cfg := config.Config{StateDir: t.TempDir(), Repos: []config.Repo{repo}, WatchLimit: 10,
		Review: config.Review{Login: "reviewer", Timeout: "60s", Executor: "panewire", Panewire: config.PanewireReview{Command: "panewire", Workspace: "reviews", Host: "auto", Jobs: filepath.Join(t.TempDir(), "jobs"), Checkouts: filepath.Join(t.TempDir(), "checkouts")},
			Pairings: map[string][]string{"openai": {"claude"}},
			Profiles: map[string]config.Reviewer{"claude": {Family: "anthropic", Command: "pi", Args: []string{"--model", "example-model"}}}},
		Notifier: config.Notifier{Lane: "desk"}}
	cfg.Daemon.PR.ReviewParallel = parallel
	on := true
	cfg.Daemon.PR.Notify.Enabled = &on
	gh := github.New("")
	gh.BaseURL = srv.URL
	gh.Stderr = io.Discard
	runner := &blockingRunner{trace: trace, release: make(chan struct{}), changed: make(chan struct{}, 1)}
	out := &bytes.Buffer{}
	s := &pipeline.Service{Config: cfg, GH: gh, Runner: runner, Sink: parallelSink{}, Notifier: recordingNotifier{trace}, Out: out}
	return &parallelFixture{hub: hub, s: s, gh: gh, repo: repo, runner: runner, trace: trace, out: out}
}

func (f *parallelFixture) poll(ctx context.Context) chan error {
	done := make(chan error, 1)
	go func() { done <- pollPRs(ctx, f.s, f.gh, f.repo, pipeline.Options{}) }()
	return done
}

// steps keeps the review and settle events of a trace, in order.
func steps(events []string) []string {
	var out []string
	for _, e := range events {
		if strings.HasPrefix(e, "review ") || strings.HasPrefix(e, "reviewed ") || strings.HasPrefix(e, "notify ") || strings.HasPrefix(e, "merge ") {
			out = append(out, e)
		}
	}
	return out
}

// With review_parallel 4, four of five blocked reviews run at once; the
// fifth starts only after one of them returns.
func TestPollPRsBoundsParallelReviews(t *testing.T) {
	f := newParallelFixture(t, 5, 4)
	done := f.poll(context.Background())
	f.runner.waitFor(t, "four reviews in flight", func(in, started, _ int) bool { return in == 4 && started == 4 })
	// Give a fifth review time to start if the bound were missing: each
	// review reaches its reviewer within a few tens of milliseconds.
	time.Sleep(300 * time.Millisecond)
	if _, _, started, _, _ := f.runner.state(); started != 4 {
		t.Fatalf("%d reviews started before any returned, want 4", started)
	}
	f.runner.release <- struct{}{}
	f.runner.waitFor(t, "the fifth review", func(_, started, _ int) bool { return started == 5 })
	close(f.runner.release)
	if err := <-done; err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
	_, peak, started, returned, after := f.runner.state()
	if peak != 4 || started != 5 || returned != 5 {
		t.Fatalf("peak %d started %d returned %d, want 4 5 5", peak, started, returned)
	}
	if after[4] < 1 {
		t.Fatalf("the fifth review started before any review returned: %v", after)
	}
	// Every PR reached its land decision, which is what the notifier would
	// announce: READY itself goes only to a worker lane, and this fixture has
	// no hk job binding.
	settled := 0
	for _, e := range f.trace.snapshot() {
		if strings.HasPrefix(e, "merge ") {
			settled++
		}
	}
	if settled != 5 {
		t.Fatalf("%d PRs settled, want 5: %q", settled, f.trace.snapshot())
	}
}

// review_parallel 1 keeps the sequential order: each PR is reviewed and
// settled before the next PR's review starts.
func TestPollPRsParallelOneKeepsOrder(t *testing.T) {
	f := newParallelFixture(t, 3, 1)
	close(f.runner.release)
	if err := <-f.poll(context.Background()); err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
	var want []string
	for n := 1; n <= 3; n++ {
		want = append(want, fmt.Sprintf("review %d", n), fmt.Sprintf("reviewed %d", n), fmt.Sprintf("merge %d", n))
	}
	if got := steps(f.trace.snapshot()); !slices.Equal(got, want) {
		t.Fatalf("order %q, want %q", got, want)
	}
	if _, peak, _, _, _ := f.runner.state(); peak != 1 {
		t.Fatalf("peak %d reviews in flight, want 1", peak)
	}
}

// A pass cancelled mid-review starts no further PR and returns only after
// the running reviews have.
func TestPollPRsCancelStartsNoNewPR(t *testing.T) {
	f := newParallelFixture(t, 5, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := f.poll(ctx)
	f.runner.waitFor(t, "two reviews in flight", func(in, _, _ int) bool { return in == 2 })
	cancel()
	select {
	case err := <-done:
		t.Fatalf("pollPRs returned while two reviews were running: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(f.runner.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v, want context.Canceled", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("pollPRs did not return after the running reviews did")
	}
	if _, _, started, returned, _ := f.runner.state(); started != 2 || returned != 2 {
		t.Fatalf("started %d returned %d, want 2 2", started, returned)
	}
	for _, e := range f.trace.snapshot() {
		if e == "review 3" || e == "review 4" || e == "review 5" {
			t.Fatalf("a PR started after cancel: %q", f.trace.snapshot())
		}
	}
}

// Concurrent reviews through the real Service and GitHub client: run under
// -race, this exercises their shared state. Every PR is reviewed once, the
// decisions log and Out stay whole JSON lines, and a second pass finds every
// READY's sent key (this fixture has no worker lane, so READY is suppressed
// and only its key is written), so no ledger write was lost.
func TestPollPRsParallelReviewsShareServiceState(t *testing.T) {
	const prs = 6
	f := newParallelFixture(t, prs, 4)
	first := f.poll(context.Background())
	// Hold the reviews until four overlap, then let all of them return at once.
	f.runner.waitFor(t, "four reviews in flight", func(in, _, _ int) bool { return in == 4 })
	close(f.runner.release)
	if err := <-first; err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatal(err)
	}
	if err := <-f.poll(context.Background()); err != nil && !strings.Contains(err.Error(), "land") {
		t.Fatalf("second pass: %v", err)
	}
	if _, peak, started, _, _ := f.runner.state(); started != prs || peak != 4 {
		t.Fatalf("started %d reviews with peak %d, want %d with peak 4: %q", started, peak, prs, f.trace.snapshot())
	}
	count := map[string]int{}
	for _, e := range steps(f.trace.snapshot()) {
		count[strings.Fields(e)[0]]++
	}
	if count["review"] != prs || count["reviewed"] != prs {
		t.Fatalf("counts %v, want %d reviews: %q", count, prs, f.trace.snapshot())
	}
	whole := func(name string, b []byte) int {
		lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
		for _, line := range lines {
			if !json.Valid([]byte(line)) {
				t.Fatalf("%s holds a broken line: %q", name, line)
			}
		}
		return len(lines)
	}
	decisions, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "o/one", "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := whole("decisions.jsonl", decisions); n < 2*prs {
		t.Fatalf("decisions.jsonl has %d records", n)
	}
	whole("Out", f.out.Bytes())
	sent, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "o/one", "pr-notify-sent"))
	if err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= prs; n++ {
		if !strings.Contains(string(sent), fmt.Sprintf("ready %d %s", n, hubHead(n))) {
			t.Fatalf("pr-notify-sent lacks PR %d:\n%s", n, sent)
		}
	}
}

type reviewerWaitExit struct{}

func (reviewerWaitExit) Error() string { return "wait timed out" }
func (reviewerWaitExit) ExitCode() int { return 85 }

type slowPassRunner struct {
	wait    string
	running bool
}

func (r *slowPassRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" {
		i := slices.Index(args, "--wait-timeout")
		if i >= 0 && i+1 < len(args) {
			r.wait = args[i+1]
		}
		r.running = true
		return nil, reviewerWaitExit{}
	}
	return nil, nil
}

func TestSlowReviewerDoesNotDelayNextRepo(t *testing.T) {
	a := newParallelFixtureForRepo(t, 1, 1, "o/one")
	b := newParallelFixtureForRepo(t, 1, 1, "o/two")
	a.s.Config.Review.Timeout = "25m"
	slow := &slowPassRunner{}
	a.s.Runner = slow
	close(b.runner.release)
	for _, f := range []*parallelFixture{a, b} {
		if err := pollPRs(context.Background(), f.s, f.gh, f.repo, pipeline.Options{}); err != nil && !strings.Contains(err.Error(), "land") {
			t.Fatal(err)
		}
	}
	if slow.wait != "30" || !slow.running {
		t.Fatalf("wait=%q running=%v", slow.wait, slow.running)
	}
	if _, err := os.Stat(filepath.Join(a.s.Config.StateDir, a.repo.Name, "reviewer-jobs", "1-p1.json")); err != nil {
		t.Fatalf("reviewer record: %v", err)
	}
	if !slices.Contains(b.trace.snapshot(), "merge 1") {
		t.Fatalf("second repo did not settle: %v", b.trace.snapshot())
	}
}
