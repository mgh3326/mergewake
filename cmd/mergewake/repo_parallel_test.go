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

// repoRouter sends each request to the hub of the repository its path names.
// A repository in failList answers its pull list with a server error.
type repoRouter struct {
	hubs     map[string]*reviewHub
	failList map[string]bool
}

func (rt repoRouter) RoundTrip(r *http.Request) (*http.Response, error) {
	for name, hub := range rt.hubs {
		if strings.HasPrefix(r.URL.Path, "/repos/"+name+"/") {
			if rt.failList[name] && strings.HasSuffix(r.URL.Path, "/pulls") {
				return &http.Response{StatusCode: 500, Status: "500", Body: io.NopCloser(strings.NewReader(`{"message":"down"}`)), Header: http.Header{}, Request: r}, nil
			}
			return handlerTransport{hub}.RoundTrip(r)
		}
	}
	return handlerTransport{rt.hubs[slices.Sorted(maps.Keys(rt.hubs))[0]]}.RoundTrip(r)
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
	router := repoRouter{hubs: hubs, failList: map[string]bool{}}
	gh := github.New("")
	gh.BaseURL = "https://fake.invalid"
	gh.HTTP = &http.Client{Transport: router}
	gh.Stderr = io.Discard
	runner := &repoRunner{gates: map[string]chan struct{}{}, started: map[string]int{}, returned: map[string]int{}, changed: make(chan struct{}, 1)}
	out := &bytes.Buffer{}
	s := &pipeline.Service{Config: cfg, GH: gh, Runner: runner, Sink: parallelSink{}, Notifier: recordingNotifier{&eventLog{}}, Out: out}
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
	go func() { exit <- runDaemonLoops(ctx, m.s, m.gh, "", pipeline.Options{}, io.Discard) }()
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
