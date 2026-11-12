package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// repoRunner is the fake panewire for several repositories. A spawn for a
// repository with a gate holds until the gate is closed; the others return at
// once. The brief names the repository and PR it reviews.
type repoRunner struct {
	results  *paneResults // where a returning pane publishes its findings
	mu       sync.Mutex
	gates    map[string]chan struct{}
	started  map[string]int
	returned map[string]int
	changed  chan struct{}
}

var repoBrief = regexp.MustCompile(`# Review — (\S+)#(\d+) pass`)

func (r *repoRunner) Run(ctx context.Context, command string, args []string, _, _ string) ([]byte, error) {
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
	m := repoBrief.FindSubmatch(brief)
	if m == nil {
		return nil, fmt.Errorf("brief names no repository: %.80s", brief)
	}
	repo := string(m[1])
	r.mu.Lock()
	r.started[repo]++
	gate := r.gates[repo]
	r.mu.Unlock()
	r.notify()
	if gate != nil {
		<-gate
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(args[i+1]), "findings.json"), []byte(`{"findings":[]}`), 0600); err != nil {
		return nil, err
	}
	r.results.publish(args, `{"findings":[]}`)
	r.mu.Lock()
	r.returned[repo]++
	r.mu.Unlock()
	r.notify()
	return []byte("OK pane=p1 model=seat-x status=working landed=yes\nwait outcome=completed\n"), nil
}

func (r *repoRunner) notify() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *repoRunner) counts(repo string) (started, returned int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started[repo], r.returned[repo]
}

// waitFor polls cond until it holds, failing after 20s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for !cond() {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func waitForBriefly(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(750 * time.Millisecond)
	for !cond() {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// repoRouter sends each request to the hub of the repository its path names.
// A repository in failList answers its pull list with a server error.
type repoRouter struct {
	hubs      map[string]*reviewHub
	failList  map[string]bool
	holdList  map[string]chan struct{}
	ignoreCtx map[string]bool
	mu        *sync.Mutex
	listCalls map[string]int
	active    map[string]int
}

func (rt repoRouter) RoundTrip(r *http.Request) (*http.Response, error) {
	for name, hub := range rt.hubs {
		if strings.HasPrefix(r.URL.Path, "/repos/"+name+"/") {
			if strings.HasSuffix(r.URL.Path, "/pulls") {
				rt.mu.Lock()
				rt.listCalls[name]++
				gate := rt.holdList[name]
				ignoreCtx := rt.ignoreCtx[name]
				if gate != nil {
					rt.active[name]++
				}
				rt.mu.Unlock()
				if gate != nil {
					defer func() {
						rt.mu.Lock()
						rt.active[name]--
						rt.mu.Unlock()
					}()
					if ignoreCtx {
						<-gate
					} else {
						select {
						case <-gate:
						case <-r.Context().Done():
							return nil, r.Context().Err()
						}
					}
				}
			}
			if rt.failList[name] && strings.HasSuffix(r.URL.Path, "/pulls") {
				return &http.Response{StatusCode: 500, Status: "500", Body: io.NopCloser(strings.NewReader(`{"message":"down"}`)), Header: http.Header{}, Request: r}, nil
			}
			return handlerTransport{hub}.RoundTrip(r)
		}
	}
	return handlerTransport{rt.hubs[slices.Sorted(maps.Keys(rt.hubs))[0]]}.RoundTrip(r)
}

func (rt *repoRouter) counts(repo string) (calls, active int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.listCalls[repo], rt.active[repo]
}

type repoLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *repoLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *repoLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

// multiRepo is one Service over several repositories, each with its own fake
// GitHub, run through the real pr role of the daemon.
type multiRepo struct {
	s      *pipeline.Service
	gh     *github.Client
	runner *repoRunner
	hubs   map[string]*reviewHub
	router repoRouter
	out    *bytes.Buffer
	stderr repoLogBuffer
}

func newMultiRepo(t *testing.T, names []string, prs, parallel int) *multiRepo {
	t.Helper()
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	hubs := map[string]*reviewHub{}
	var repos []config.Repo
	for i, name := range names {
		hubs[name] = &reviewHub{n: prs, first: 1 + 100*i, repo: name, refs: map[string]string{}, commits: map[string]string{}, reviews: map[int][]map[string]any{}, trace: &eventLog{}}
		repos = append(repos, config.Repo{Name: name, Branch: "main", Policy: "policy.yaml", Root: t.TempDir()})
	}
	cfg := config.Config{StateDir: t.TempDir(), Repos: repos, WatchLimit: 10,
		Review: config.Review{Login: "reviewer", Timeout: "60s", Executor: "panewire", Panewire: config.PanewireReview{Command: "panewire", Workspace: "reviews", Host: "auto", Jobs: filepath.Join(t.TempDir(), "jobs"), Checkouts: filepath.Join(t.TempDir(), "checkouts")},
			Pairings: map[string][]string{"openai": {"claude"}},
			Profiles: map[string]config.Reviewer{"claude": {Family: "anthropic", Command: "pi", Args: []string{"--model", "example-model"}}}},
		Notifier: config.Notifier{Lane: "desk"}}
	cfg.Daemon.PR.ReviewParallel = parallel
	cfg.Daemon.PR.Loop = config.Loop{Enabled: true, Interval: "1h"}
	on := true
	cfg.Daemon.PR.Notify.Enabled = &on
	router := repoRouter{hubs: hubs, failList: map[string]bool{}, holdList: map[string]chan struct{}{}, ignoreCtx: map[string]bool{}, mu: &sync.Mutex{}, listCalls: map[string]int{}, active: map[string]int{}}
	gh := github.New("")
	gh.BaseURL = "https://fake.invalid"
	gh.HTTP = &http.Client{Transport: router}
	gh.Stderr = io.Discard
	results := &paneResults{}
	runner := &repoRunner{results: results, gates: map[string]chan struct{}{}, started: map[string]int{}, returned: map[string]int{}, changed: make(chan struct{}, 1)}
	out := &bytes.Buffer{}
	s := &pipeline.Service{Config: cfg, GH: gh, Runner: runner, Sink: resultSink{results: results}, Notifier: recordingNotifier{&eventLog{}}, Out: out}
	return &multiRepo{s: s, gh: gh, runner: runner, hubs: hubs, router: router, out: out}
}

func (m *multiRepo) merged(repo string) []string {
	var merges []string
	for _, e := range m.hubs[repo].trace.snapshot() {
		if strings.HasPrefix(e, "merge ") {
			merges = append(merges, e)
		}
	}
	return merges
}

// daemonPass runs the pr role for one pass; stop cancels the daemon, and
// wait returns the status the pass recorded once the daemon has stopped.
func (m *multiRepo) daemonPass(t *testing.T) (cancel context.CancelFunc, passDone func() bool, wait func() daemon.Status) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	exit := make(chan error, 1)
	go func() { exit <- runDaemonLoops(ctx, m.s, m.gh, "", pipeline.Options{}, &m.stderr) }()
	status := func() daemon.Status {
		var st daemon.Status
		if b, err := os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", "pr.json")); err == nil {
			_ = json.Unmarshal(b, &st)
		}
		return st
	}
	passDone = func() bool {
		st := status()
		return !st.Running && !st.LastFinish.IsZero()
	}
	wait = func() daemon.Status {
		select {
		case err := <-exit:
			if err != nil {
				t.Fatalf("daemon: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("daemon did not stop")
		}
		return status()
	}
	return cancel, passDone, wait
}

// A reviewer that never returns in repo A must not hold repo B: B settles its
// PR while A is still waiting. The pass is a sweep that queues the PRs, so it
// ends without waiting for A either.
func TestPRRoleRunsReposInParallel(t *testing.T) {
	m := newMultiRepo(t, []string{"o/one", "o/two"}, 1, 1)
	gate := make(chan struct{})
	m.runner.gates["o/one"] = gate
	cancel, passDone, wait := m.daemonPass(t)
	defer cancel()
	waitFor(t, "repo B to settle", func() bool { return len(m.merged("o/two")) == 1 })
	if _, returned := m.runner.counts("o/one"); returned != 0 || len(m.merged("o/one")) != 0 {
		t.Fatal("repo A finished before its reviewer returned")
	}
	waitFor(t, "the sweep to end while repo A is still reviewing", passDone)
	if _, returned := m.runner.counts("o/one"); returned != 0 {
		t.Fatal("repo A finished before its reviewer returned")
	}
	close(gate)
	waitFor(t, "repo A to settle", func() bool { return len(m.merged("o/one")) == 1 })
	cancel()
	if st := wait(); st.Failures != 0 && !strings.Contains(st.LastError, "land") {
		t.Fatalf("pass error: %s", st.LastError)
	}
}

// Repositories share one Service and GitHub client. Under -race, with
// reviews of several repos overlapping, every PR is reviewed and landed once
// and the shared logs stay whole JSON lines.
func TestPRRoleParallelReposShareServiceState(t *testing.T) {
	names := []string{"o/one", "o/two", "o/three"}
	const prs = 3
	m := newMultiRepo(t, names, prs, 2)
	gate := make(chan struct{})
	for _, name := range names {
		m.runner.gates[name] = gate
	}
	cancel, passDone, wait := m.daemonPass(t)
	defer cancel()
	// Hold every repo's first reviews until all repos have reviews in flight.
	waitFor(t, "reviews of every repo in flight", func() bool {
		for _, name := range names {
			if started, _ := m.runner.counts(name); started < 2 {
				return false
			}
		}
		return true
	})
	close(gate)
	waitFor(t, "the pass to end", passDone)
	waitFor(t, "every PR to land", func() bool {
		for _, name := range names {
			if len(m.merged(name)) != prs {
				return false
			}
		}
		return true
	})
	cancel()
	wait()
	whole := func(name string, b []byte) int {
		lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
		for _, line := range lines {
			if !json.Valid([]byte(line)) {
				t.Fatalf("%s holds a broken line: %q", name, line)
			}
		}
		return len(lines)
	}
	for _, name := range names {
		if started, returned := m.runner.counts(name); started != prs || returned != prs {
			t.Fatalf("%s: started %d returned %d, want %d", name, started, returned, prs)
		}
		if got := m.merged(name); len(got) != prs {
			t.Fatalf("%s landed %q, want %d PRs", name, got, prs)
		}
		decisions, err := os.ReadFile(filepath.Join(m.s.Config.StateDir, name, "decisions.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		if n := whole(name+" decisions", decisions); n < 2*prs {
			t.Fatalf("%s: %d decision records", name, n)
		}
	}
	whole("Out", m.out.Bytes())
}

// Cancelling the pass starts no new PR in any repo: each repo finishes the
// review it is running and no more.
func TestPRRoleCancelStartsNoNewPRInAnyRepo(t *testing.T) {
	names := []string{"o/one", "o/two"}
	m := newMultiRepo(t, names, 4, 1)
	gate := make(chan struct{})
	for _, name := range names {
		m.runner.gates[name] = gate
	}
	cancel, _, wait := m.daemonPass(t)
	defer cancel()
	waitFor(t, "a review in flight in every repo", func() bool {
		for _, name := range names {
			if started, _ := m.runner.counts(name); started != 1 {
				return false
			}
		}
		return true
	})
	cancel()
	close(gate)
	wait()
	for _, name := range names {
		if started, _ := m.runner.counts(name); started != 1 {
			t.Fatalf("%s started %d reviews, want 1", name, started)
		}
		if got := m.merged(name); len(got) > 1 {
			t.Fatalf("%s landed %q after cancel", name, got)
		}
	}
}

// A pass cancelled before it reaches the repos starts none of them.
func TestPRRoleCancelledPassStartsNoRepo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var mu sync.Mutex
	var ran []string
	err := forRepos(ctx, []config.Repo{{Name: "o/one"}, {Name: "o/two"}}, true, func(_ context.Context, r config.Repo) error {
		mu.Lock()
		ran = append(ran, r.Name)
		mu.Unlock()
		return nil
	})
	if err != nil || len(ran) != 0 {
		t.Fatalf("ran %q, err %v", ran, err)
	}
}

// Both repos failing in one pass: both errors are returned, each named by its
// repository, in config order.
func TestPRRoleJoinsErrorsFromEveryRepo(t *testing.T) {
	m := newMultiRepo(t, []string{"o/one", "o/two", "o/three"}, 1, 1)
	m.router.failList["o/one"] = true
	m.router.failList["o/three"] = true
	cancel, passDone, wait := m.daemonPass(t)
	defer cancel()
	waitFor(t, "the pass to end", passDone)
	waitFor(t, "the healthy repo to land its PR", func() bool { return len(m.merged("o/two")) == 1 })
	cancel()
	st := wait()
	one, three := strings.Index(st.LastError, "o/one: GitHub GET /repos/o/one/pulls"), strings.Index(st.LastError, "o/three: GitHub GET /repos/o/three/pulls")
	if one < 0 || three < one || strings.Contains(st.LastError, "o/two: GitHub GET") {
		t.Fatalf("last_error %q, want the o/one then the o/three listing failure", st.LastError)
	}
	if len(m.merged("o/two")) != 1 {
		t.Fatal("a failing repo stopped the healthy one")
	}
}

func TestPRRoleSweepDeadlineLetsOtherReposRunAndSkipsUntilReturn(t *testing.T) {
	oldTimeout := prSweepTimeout
	prSweepTimeout = 2 * time.Second
	t.Cleanup(func() { prSweepTimeout = oldTimeout })
	m := newMultiRepo(t, []string{"o/one", "o/two"}, 1, 1)
	m.s.Config.Daemon.PR.Loop.Interval = "500ms"
	for _, name := range []string{"o/one", "o/two"} {
		m.hubs[name].closeOnMerge = true
		m.hubs[name].closed = map[int]bool{}
	}
	gate := make(chan struct{})
	m.router.holdList["o/one"] = gate
	m.router.ignoreCtx["o/one"] = true
	released := false
	var cancel context.CancelFunc
	var wait func() daemon.Status
	stopped := false
	t.Cleanup(func() {
		if !released {
			close(gate)
		}
		if cancel != nil {
			cancel()
			if !stopped {
				wait()
			}
		}
	})
	cancel, _, wait = m.daemonPass(t)

	waitFor(t, "repo A pull list to block", func() bool {
		calls, active := m.router.counts("o/one")
		return calls == 1 && active == 1
	})
	waitFor(t, "repo B to merge during repo A's held pull list", func() bool { return len(m.merged("o/two")) == 1 })
	waitFor(t, "the first pass to record its deadline", func() bool {
		var st daemon.Status
		b, err := os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", "pr.json"))
		return err == nil && json.Unmarshal(b, &st) == nil && strings.Contains(st.LastError, "open_pulls")
	})
	var st daemon.Status
	statusBytes, _ := os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", "pr.json"))
	_ = json.Unmarshal(statusBytes, &st)
	if st.Running || !strings.Contains(st.LastError, "o/one") || !strings.Contains(st.LastError, "deadline") || !strings.Contains(st.LastError, "context deadline exceeded") {
		t.Fatalf("pr.json pass status: %+v", st)
	}
	firstFinish := st.LastFinish

	// A PR arriving in another repository remains eligible while the stuck
	// repository is skipped on later passes.
	m.hubs["o/two"].mu.Lock()
	m.hubs["o/two"].n = 2
	m.hubs["o/two"].mu.Unlock()
	waitFor(t, "repo B's later PR to merge", func() bool { return len(m.merged("o/two")) == 2 })
	waitFor(t, "the still-running sweep skip log", func() bool { return strings.Contains(m.stderr.String(), "pr sweep still running") })
	logLine := m.stderr.String()
	if !strings.Contains(logLine, `"repo":"o/one"`) || !strings.Contains(logLine, `"step":"open_pulls"`) || !strings.Contains(logLine, `"age":`) {
		t.Fatalf("still-running log lacks repo, step or age: %s", logLine)
	}
	waitFor(t, "a later pass to finish", func() bool {
		var state daemon.Status
		b, err := os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", "pr.json"))
		return err == nil && json.Unmarshal(b, &state) == nil && state.LastFinish.After(firstFinish)
	})
	statusBytes, _ = os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", "pr.json"))
	_ = json.Unmarshal(statusBytes, &st)
	if st.Running || st.Failures != 0 || st.LastError != "" {
		t.Fatalf("skipped pass recorded a role failure: %+v; log=%s", st, m.stderr.String())
	}
	if calls, active := m.router.counts("o/one"); calls != 1 || active != 1 {
		t.Fatalf("repo A pull-list calls=%d active=%d, want one active call", calls, active)
	}

	close(gate)
	released = true
	waitFor(t, "repo A to merge after its held sweep returns", func() bool { return len(m.merged("o/one")) == 1 })
	waitFor(t, "repo A's sweep goroutine to return", func() bool {
		_, active := m.router.counts("o/one")
		return active == 0
	})
	waitFor(t, "a later sweep of repo A", func() bool {
		calls, _ := m.router.counts("o/one")
		return calls >= 2
	})
	if calls, _ := m.router.counts("o/one"); calls < 2 {
		t.Fatalf("repo A pull-list calls=%d, want another sweep after release", calls)
	}
	cancel()
	wait()
	stopped = true
}

func TestPRRoleSweepDeadlineCancelsContextHonoringOperation(t *testing.T) {
	oldTimeout := prSweepTimeout
	prSweepTimeout = 35 * time.Millisecond
	t.Cleanup(func() { prSweepTimeout = oldTimeout })
	m := newMultiRepo(t, []string{"o/one"}, 1, 1)
	gate := make(chan struct{})
	m.router.holdList["o/one"] = gate
	released := false
	var cancel context.CancelFunc
	var wait func() daemon.Status
	stopped := false
	t.Cleanup(func() {
		if !released {
			close(gate)
		}
		if cancel != nil {
			cancel()
			if !stopped {
				wait()
			}
		}
	})
	cancel, _, wait = m.daemonPass(t)
	waitFor(t, "repo A pull-list request to start", func() bool {
		_, active := m.router.counts("o/one")
		return active == 1
	})
	waitForBriefly(t, "the deadline error to be recorded", func() bool {
		var st daemon.Status
		b, err := os.ReadFile(filepath.Join(m.s.Config.StateDir, "daemon", "pr.json"))
		return err == nil && json.Unmarshal(b, &st) == nil && strings.Contains(st.LastError, "context deadline exceeded")
	})
	if calls, active := m.router.counts("o/one"); calls != 1 || active != 0 {
		t.Fatalf("context-honoring pull-list calls=%d active=%d, want one returned call", calls, active)
	}
	released = true
	cancel()
	wait()
	stopped = true
}
