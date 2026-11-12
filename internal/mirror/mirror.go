// Package mirror keeps one bare git mirror per repository and builds a PR
// patch from it. The pipeline compares that patch with the one GitHub returns
// before any review trusts the local copy.
package mirror

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/integrations"
)

const (
	DefaultFetchTimeout   = 60 * time.Second
	DefaultMinInterval    = 10 * time.Minute
	DefaultCompareTimeout = 10 * time.Second

	// fetchFailureLimit is how many consecutive failures put one repository's
	// mirror on cooldown, and skipCooldown is how long it stays there.
	fetchFailureLimit = 3
	skipCooldown      = time.Hour
	// warnInterval bounds the cooldown warning to one per repository per hour.
	warnInterval = time.Hour
)

var (
	errNoToken = errors.New("no GitHub App token for this repository")
	errSkipped = errors.New("mirror fetch skipped after repeated failures")
)

var (
	fullSHA  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	repoName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

// Runner runs one command with a replaced environment. integrations.Exec
// implements it.
type Runner interface {
	RunEnv(ctx context.Context, command string, args []string, input, dir string, env []string) ([]byte, error)
}

// Options configure a Mirror. Dir, Token and Run are required.
type Options struct {
	// Dir is the root the mirrors live under: <dir>/<owner>/<name>.git.
	Dir string
	// Token returns the GitHub App installation token for a repository. It
	// must return false, never the user token, when the App cannot serve the
	// repository; that repository is then never fetched.
	Token func(ctx context.Context, repo string) (string, bool)
	Run   Runner
	// URL maps owner/name to a fetch URL; nil means https://github.com/<repo>.git.
	URL func(repo string) string
	// BaseEnv is the environment a git command starts from; nil means
	// os.Environ.
	BaseEnv func() []string
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Log receives fetch and skip lines; nil discards them.
	Log *slog.Logger
	// FetchTimeout bounds one fetch, MinInterval the time between refreshes
	// and CompareTimeout one diff build; zero means the defaults.
	FetchTimeout, MinInterval, CompareTimeout time.Duration
}

// Mirror is the set of bare mirrors under one directory. It is safe for
// concurrent use: one fetch per repository runs at a time and a second caller
// joins it.
type Mirror struct {
	dir     string
	url     func(string) string
	token   func(context.Context, string) (string, bool)
	run     Runner
	baseEnv func() []string
	now     func() time.Time
	log     *slog.Logger
	fetch   time.Duration
	refresh time.Duration
	compare time.Duration

	mu       sync.Mutex
	repos    map[string]*repoState
	fetches  int
	failures int
	fetchMS  int64
}

// repoState is what one repository's mirror remembers between calls.
type repoState struct {
	mu        sync.Mutex
	running   bool
	done      chan struct{}
	result    error
	last      time.Time
	failures  int
	skipUntil time.Time
	warned    time.Time
}

// New returns a Mirror. A fetch or compare bound that is zero or negative
// takes its default.
func New(o Options) *Mirror {
	m := &Mirror{
		dir: o.Dir, url: o.URL, token: o.Token, run: o.Run, baseEnv: o.BaseEnv,
		now: o.Now, log: o.Log, fetch: o.FetchTimeout, refresh: o.MinInterval, compare: o.CompareTimeout,
		repos: map[string]*repoState{},
	}
	if m.url == nil {
		m.url = func(repo string) string { return "https://github.com/" + repo + ".git" }
	}
	if m.baseEnv == nil {
		m.baseEnv = os.Environ
	}
	if m.now == nil {
		m.now = time.Now
	}
	if m.log == nil {
		m.log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if m.fetch <= 0 {
		m.fetch = DefaultFetchTimeout
	}
	if m.refresh <= 0 {
		m.refresh = DefaultMinInterval
	}
	if m.compare <= 0 {
		m.compare = DefaultCompareTimeout
	}
	return m
}

// Dir is the directory the mirrors live under.
func (m *Mirror) Dir() string { return m.dir }

// Path is where one repository's bare mirror lives.
func (m *Mirror) Path(repo string) string {
	owner, name, _ := strings.Cut(repo, "/")
	return filepath.Join(m.dir, owner, name+".git")
}

// Stats is the fetch counters one shadow log line reports.
type Stats struct {
	Fetches  int   `json:"fetches"`
	Failures int   `json:"failures"`
	FetchMS  int64 `json:"fetch_ms"`
}

// Stats returns the fetch counters since the process started.
func (m *Mirror) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Stats{Fetches: m.fetches, Failures: m.failures, FetchMS: m.fetchMS}
}

func (m *Mirror) state(repo string) *repoState {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.repos[repo]
	if st == nil {
		st = &repoState{}
		m.repos[repo] = st
	}
	return st
}

// Fetch brings the repository's mirror up to date with the refs the pipeline
// reads: every branch head and every PR head. One fetch per repository runs at
// a time; a caller that arrives during one joins it and gets its result. A
// repository the App cannot serve is never fetched: the user token is not used
// for mirrors.
func (m *Mirror) Fetch(ctx context.Context, repo string) error {
	if !repoName.MatchString(repo) {
		return fmt.Errorf("invalid repository %q", repo)
	}
	st := m.state(repo)
	st.mu.Lock()
	if st.running {
		done := st.done
		st.mu.Unlock()
		select {
		case <-done:
			st.mu.Lock()
			e := st.result
			st.mu.Unlock()
			return e
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	st.running = true
	st.done = make(chan struct{})
	st.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, m.fetch)
	e := m.fetchRepo(ctx, repo, st)
	cancel()

	st.mu.Lock()
	st.running = false
	st.result = e
	st.last = m.now()
	close(st.done)
	st.mu.Unlock()
	return e
}

func (m *Mirror) fetchRepo(ctx context.Context, repo string, st *repoState) error {
	if e := m.cooldown(repo, st); e != nil {
		return e
	}
	token, ok := m.token(ctx, repo)
	if !ok {
		return errNoToken
	}
	start := m.now()
	e := m.fetchInto(ctx, repo, token)
	m.recordFetch(repo, token, e, m.now().Sub(start))
	if e != nil {
		// The error leaves this package redacted: git can echo the URL or the
		// credential it was given, and callers log what they get.
		e = errors.New(m.scrub(e, token))
		m.failed(repo, token, st, e)
		return e
	}
	st.mu.Lock()
	st.failures = 0
	st.skipUntil = time.Time{}
	st.mu.Unlock()
	return nil
}

// cooldown refuses a fetch of a repository whose mirror keeps failing. It logs
// at most one WARN per repository per hour.
func (m *Mirror) cooldown(repo string, st *repoState) error {
	now := m.now()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !now.Before(st.skipUntil) {
		return nil
	}
	if now.Sub(st.warned) >= warnInterval {
		st.warned = now
		m.log.Warn("mirror fetch skipped after repeated failures", "repo", repo, "failures", st.failures, "retry_after", st.skipUntil.UTC().Format(time.RFC3339))
	}
	return errSkipped
}

// failed counts one failed fetch and puts the repository on cooldown after
// fetchFailureLimit failures in a row.
func (m *Mirror) failed(repo, token string, st *repoState, e error) {
	st.mu.Lock()
	st.failures++
	skip := st.failures >= fetchFailureLimit
	now := m.now()
	if skip {
		st.skipUntil = now.Add(skipCooldown)
		st.warned = now
	}
	failures, until := st.failures, st.skipUntil
	st.mu.Unlock()
	if skip {
		m.log.Warn("mirror fetch skipped after repeated failures", "repo", repo, "failures", failures, "retry_after", until.UTC().Format(time.RFC3339), "error", m.scrub(e, token))
	}
}

// recordFetch counts one fetch attempt and logs its result and duration.
func (m *Mirror) recordFetch(repo, token string, e error, d time.Duration) {
	m.mu.Lock()
	m.fetches++
	m.fetchMS += d.Milliseconds()
	if e != nil {
		m.failures++
	}
	fetches, failures, total := m.fetches, m.failures, m.fetchMS
	m.mu.Unlock()
	result := "ok"
	if e != nil {
		result = "failed"
	}
	fields := []any{"repo", repo, "result", result, "duration_ms", d.Milliseconds(), "fetches", fetches, "failures", failures, "fetch_ms", total}
	if e != nil {
		fields = append(fields, "error", m.scrub(e, token))
	}
	m.log.Info("mirror fetch", fields...)
}

// fetchInto creates the bare mirror when it is missing, fetches the refs the
// pipeline reads, and asks git to collect garbage when it is due.
func (m *Mirror) fetchInto(ctx context.Context, repo, token string) error {
	path := m.Path(repo)
	if e := os.MkdirAll(path, 0o755); e != nil {
		return fmt.Errorf("create mirror directory: %w", e)
	}
	if _, e := os.Stat(filepath.Join(path, "HEAD")); e != nil {
		if _, e := m.run.RunEnv(ctx, "git", []string{"init", "--bare", "--quiet", path}, "", "", m.env("")); e != nil {
			return fmt.Errorf("init mirror: %w", e)
		}
	}
	args := []string{"-C", path, "fetch", "--prune", "--quiet", "--no-tags", m.url(repo),
		"+refs/heads/*:refs/heads/*", "+refs/pull/*/head:refs/pull/*/head"}
	if _, e := m.run.RunEnv(ctx, "git", args, "", "", m.env(token)); e != nil {
		return fmt.Errorf("fetch mirror: %w", e)
	}
	if _, e := m.run.RunEnv(ctx, "git", []string{"-C", path, "gc", "--auto", "--quiet"}, "", "", m.env("")); e != nil {
		m.log.Debug("mirror gc failed", "repo", repo, "error", m.scrub(e, ""))
	}
	return nil
}

// Request starts a fetch of repo when none is running. It returns at once; a
// request that arrives during a fetch coalesces with it.
func (m *Mirror) Request(repo string) {
	if !repoName.MatchString(repo) {
		return
	}
	st := m.state(repo)
	st.mu.Lock()
	running := st.running
	st.mu.Unlock()
	if running {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), m.fetch)
		defer cancel()
		_ = m.Fetch(ctx, repo)
	}()
}

// Refresh fetches every repository whose mirror is due. Each repository is
// fetched at most once per MinInterval.
func (m *Mirror) Refresh(ctx context.Context, repos []string) {
	for _, repo := range repos {
		if !m.due(repo) {
			continue
		}
		fctx, cancel := context.WithTimeout(ctx, m.fetch)
		_ = m.Fetch(fctx, repo)
		cancel()
		if ctx.Err() != nil {
			return
		}
	}
}

// RefreshLoop refreshes the mirrors now and then once per MinInterval until
// ctx ends.
func (m *Mirror) RefreshLoop(ctx context.Context, repos []string) {
	m.Refresh(ctx, repos)
	t := time.NewTicker(m.refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Refresh(ctx, repos)
		}
	}
}

func (m *Mirror) due(repo string) bool {
	st := m.state(repo)
	st.mu.Lock()
	defer st.mu.Unlock()
	return m.now().Sub(st.last) >= m.refresh
}

// Diff builds the PR patch from the mirror: the merge base of base and head to
// head. A missing object is reported as reason "missing object" (a missing
// head also asks for a fetch); a build that runs past CompareTimeout is reason
// "timeout". text is empty whenever reason is not.
func (m *Mirror) Diff(ctx context.Context, repo, base, head string) (text, reason string, err error) {
	if !repoName.MatchString(repo) || !fullSHA.MatchString(base) || !fullSHA.MatchString(head) {
		return "", "missing object", nil
	}
	ctx, cancel := context.WithTimeout(ctx, m.compare)
	defer cancel()
	path := m.Path(repo)
	if !m.has(ctx, path, head) {
		if ctx.Err() != nil {
			return "", "timeout", nil
		}
		m.Request(repo)
		return "", "missing object", nil
	}
	if !m.has(ctx, path, base) {
		if ctx.Err() != nil {
			return "", "timeout", nil
		}
		return "", "missing object", nil
	}
	mergeBase, e := m.out(ctx, path, "merge-base", base, head)
	if e != nil || mergeBase == "" {
		if ctx.Err() != nil {
			return "", "timeout", nil
		}
		return "", "missing object", nil
	}
	text, e = m.out(ctx, path, "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--find-renames", mergeBase+"..."+head)
	if e != nil {
		if ctx.Err() != nil {
			return "", "timeout", nil
		}
		return "", "", errors.New(m.scrub(fmt.Errorf("build mirror diff: %w", e), ""))
	}
	return text, "", nil
}

func (m *Mirror) has(ctx context.Context, path, sha string) bool {
	if _, e := os.Stat(filepath.Join(path, "HEAD")); e != nil {
		return false
	}
	_, e := m.run.RunEnv(ctx, "git", []string{"-C", path, "cat-file", "-e", sha + "^{commit}"}, "", "", m.env(""))
	return e == nil
}

// out runs one read-only git command in the mirror and returns its trimmed
// output. The token is not part of it: only a fetch needs the App credential.
func (m *Mirror) out(ctx context.Context, path string, args ...string) (string, error) {
	b, e := m.run.RunEnv(ctx, "git", append([]string{"-C", path}, args...), "", "", m.env(""))
	return strings.TrimSpace(string(b)), e
}

// env is the environment one git command runs with: the parent environment
// without any inherited git config overrides, the user credential helper
// disabled, no terminal prompt, and (for a fetch) the App installation token
// as an extra HTTP header. The token is never on argv or on disk.
func (m *Mirror) env(token string) []string {
	overrides := []string{"GIT_CONFIG_KEY_0=credential.helper", "GIT_CONFIG_VALUE_0="}
	if token != "" {
		overrides = append(overrides,
			"GIT_CONFIG_KEY_1=http.extraHeader",
			"GIT_CONFIG_VALUE_1=Authorization: Basic "+basicAuth(token))
	}
	out := filterEnv(m.baseEnv(), func(k string) bool {
		return k == "GIT_CONFIG_COUNT" || k == "GIT_CONFIG_PARAMETERS" || k == "GIT_TERMINAL_PROMPT" || k == "GIT_ASKPASS" ||
			strings.HasPrefix(k, "GIT_CONFIG_KEY_") || strings.HasPrefix(k, "GIT_CONFIG_VALUE_")
	})
	out = append(out, fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(overrides)/2))
	out = append(out, overrides...)
	return append(out, "GIT_TERMINAL_PROMPT=0")
}

func filterEnv(env []string, drop func(string) bool) []string {
	out := env[:0:0]
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		if drop(k) {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func basicAuth(token string) string {
	return base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))
}

// scrub is an error as a log line carries it: the App token and its basic
// credential form are removed first, then fleetevent's redaction runs.
func (m *Mirror) scrub(e error, token string) string {
	if e == nil {
		return ""
	}
	s := integrations.Detail(e)
	if token != "" {
		s = strings.ReplaceAll(s, token, "[redacted]")
		s = strings.ReplaceAll(s, basicAuth(token), "[redacted]")
	}
	return fleetevent.Redact(s)
}
