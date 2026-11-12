package mirror

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

type fakeCall struct {
	args []string
	env  []string
}

// fakeRunner records every command and delegates to run when set.
type fakeRunner struct {
	mu    sync.Mutex
	calls []fakeCall
	run   func(ctx context.Context, args []string) ([]byte, error)
}

func (f *fakeRunner) RunEnv(ctx context.Context, command string, args []string, input, dir string, env []string) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeCall{args: append([]string(nil), args...), env: append([]string(nil), env...)})
	run := f.run
	f.mu.Unlock()
	if run != nil {
		return run(ctx, args)
	}
	return nil, nil
}

func (f *fakeRunner) fetchCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if slices.Contains(c.args, "fetch") {
			n++
		}
	}
	return n
}

func (f *fakeRunner) callsNamed(sub string) []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeCall
	for _, c := range f.calls {
		if slices.Contains(c.args, sub) {
			out = append(out, c)
		}
	}
	return out
}

func testBaseEnv(t *testing.T) func() []string {
	t.Helper()
	home := t.TempDir()
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home}
	return func() []string { return env }
}

// seedOrigin makes a bare origin with main at base, feat/thing at head and
// refs/pull/7/head at the head, plus the working clone that made them.
func seedOrigin(t *testing.T) (origin, work, base, head string) {
	t.Helper()
	if _, e := exec.LookPath("git"); e != nil {
		t.Skipf("git is not installed: %v", e)
	}
	dir := t.TempDir()
	work, origin = filepath.Join(dir, "work"), filepath.Join(dir, "origin.git")
	if e := os.MkdirAll(work, 0o755); e != nil {
		t.Fatal(e)
	}
	gitAt(t, work, "init", "-b", "main")
	gitAt(t, work, "config", "user.email", "t@example.com")
	gitAt(t, work, "config", "user.name", "t")
	if e := os.WriteFile(filepath.Join(work, "f"), []byte("base\n"), 0o644); e != nil {
		t.Fatal(e)
	}
	gitAt(t, work, "add", "f")
	gitAt(t, work, "commit", "-m", "base")
	base = gitAt(t, work, "rev-parse", "HEAD")
	gitAt(t, work, "checkout", "-b", "feat/thing")
	if e := os.WriteFile(filepath.Join(work, "f"), []byte("head\n"), 0o644); e != nil {
		t.Fatal(e)
	}
	gitAt(t, work, "commit", "-am", "head")
	head = gitAt(t, work, "rev-parse", "HEAD")
	if b, e := exec.Command("git", "init", "--bare", origin).CombinedOutput(); e != nil {
		t.Fatalf("init origin: %v\n%s", e, b)
	}
	gitAt(t, work, "push", origin, "main", "feat/thing")
	gitAt(t, work, "push", origin, head+":refs/pull/7/head")
	return origin, work, base, head
}

func gitAt(t *testing.T, dir string, args ...string) string {
	t.Helper()
	b, e := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if e != nil {
		t.Fatalf("git %v: %v\n%s", args, e, b)
	}
	return strings.TrimSpace(string(b))
}

func realMirror(t *testing.T, origin string) *Mirror {
	t.Helper()
	return New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "ghs_testtokenvalue1234567890", true },
		Run:     integrations.Exec{},
		URL:     func(string) string { return origin },
		BaseEnv: testBaseEnv(t),
	})
}

func TestFetchPublishesBranchesAndPRHeads(t *testing.T) {
	origin, _, base, head := seedOrigin(t)
	m := realMirror(t, origin)
	if e := m.Fetch(context.Background(), "o/r"); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	path := m.Path("o/r")
	if got := gitAt(t, path, "rev-parse", "refs/heads/main"); got != base {
		t.Fatalf("refs/heads/main = %s, want %s", got, base)
	}
	if got := gitAt(t, path, "rev-parse", "refs/pull/7/head"); got != head {
		t.Fatalf("refs/pull/7/head = %s, want %s", got, head)
	}
	if got := gitAt(t, path, "rev-parse", "refs/heads/feat/thing"); got != head {
		t.Fatalf("refs/heads/feat/thing = %s, want %s", got, head)
	}
	text, reason, e := m.Diff(context.Background(), "o/r", base, head)
	if e != nil || reason != "" {
		t.Fatalf("diff: %v reason=%q", e, reason)
	}
	if !strings.Contains(text, "diff --git a/f b/f") || !strings.Contains(text, "+head") {
		t.Fatalf("diff does not describe the change:\n%s", text)
	}
	if !strings.HasSuffix(text, "\n") {
		t.Fatalf("diff lost its final newline: %q", text)
	}
	if st := m.Stats(); st.Fetches != 1 || st.Failures != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

// A patch's final newline and trailing spaces are part of what the shadow
// compare hashes; Diff must return git's bytes untouched.
func TestDiffKeepsTrailingWhitespace(t *testing.T) {
	origin, work, base, _ := seedOrigin(t)
	if e := os.WriteFile(filepath.Join(work, "f"), []byte("head   \n"), 0o644); e != nil {
		t.Fatal(e)
	}
	gitAt(t, work, "commit", "-am", "trailing")
	head := gitAt(t, work, "rev-parse", "HEAD")
	gitAt(t, work, "push", origin, "feat/thing", "HEAD:refs/pull/7/head")
	m := realMirror(t, origin)
	if e := m.Fetch(context.Background(), "o/r"); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	text, reason, e := m.Diff(context.Background(), "o/r", base, head)
	if e != nil || reason != "" {
		t.Fatalf("diff: %v reason=%q", e, reason)
	}
	if !strings.HasSuffix(text, "\n") {
		t.Fatalf("diff lost its final newline: %q", text)
	}
	if !strings.Contains(text, "+head   \n") {
		t.Fatalf("diff lost a line's trailing spaces:\n%s", text)
	}
}

func TestFetchEnvCarriesOnlyTheAppToken(t *testing.T) {
	const token = "token-not-shaped-like-a-github-token"
	runner := &fakeRunner{}
	base := []string{
		"PATH=/bin",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=credential.helper",
		"GIT_CONFIG_VALUE_0=store",
		"GIT_CONFIG_PARAMETERS='credential.helper=osxkeychain'",
		"GIT_ASKPASS=/usr/bin/askpass",
		"GIT_TERMINAL_PROMPT=1",
	}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return token, true },
		Run:     runner,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: func() []string { return base },
	})
	if e := m.Fetch(context.Background(), "o/r"); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	fetches := runner.callsNamed("fetch")
	if len(fetches) != 1 {
		t.Fatalf("fetch calls = %d", len(fetches))
	}
	call := fetches[0]
	for _, arg := range call.args {
		if strings.Contains(arg, token) {
			t.Fatalf("token on argv: %q", call.args)
		}
	}
	env := map[string]string{}
	for _, kv := range call.env {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	if env["GIT_CONFIG_COUNT"] != "2" {
		t.Fatalf("GIT_CONFIG_COUNT = %q, want 2", env["GIT_CONFIG_COUNT"])
	}
	if env["GIT_CONFIG_KEY_0"] != "credential.helper" || env["GIT_CONFIG_VALUE_0"] != "" {
		t.Fatalf("user credential helper not disabled: %q=%q", env["GIT_CONFIG_KEY_0"], env["GIT_CONFIG_VALUE_0"])
	}
	if env["GIT_TERMINAL_PROMPT"] != "0" {
		t.Fatalf("GIT_TERMINAL_PROMPT = %q", env["GIT_TERMINAL_PROMPT"])
	}
	if _, ok := env["GIT_ASKPASS"]; ok {
		t.Fatal("GIT_ASKPASS survived")
	}
	if _, ok := env["GIT_CONFIG_PARAMETERS"]; ok {
		t.Fatal("inherited GIT_CONFIG_PARAMETERS survived")
	}
	want := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
	if env["GIT_CONFIG_KEY_1"] != "http.extraHeader" || env["GIT_CONFIG_VALUE_1"] != want {
		t.Fatalf("extra header = %q=%q", env["GIT_CONFIG_KEY_1"], env["GIT_CONFIG_VALUE_1"])
	}
	// A read-only command never gets the token.
	for _, c := range runner.calls {
		if slices.Contains(c.args, "fetch") {
			continue
		}
		for _, kv := range c.env {
			if strings.Contains(kv, token) {
				t.Fatalf("token in a non-fetch command env: %q", c.args)
			}
		}
	}
}

func TestFetchWithoutAppTokenFetchesNothing(t *testing.T) {
	runner := &fakeRunner{}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "", false },
		Run:     runner,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: testBaseEnv(t),
	})
	e := m.Fetch(context.Background(), "o/r")
	if !errors.Is(e, errNoToken) {
		t.Fatalf("err = %v, want errNoToken", e)
	}
	if n := len(runner.calls); n != 0 {
		t.Fatalf("runner ran %d commands on fallback: %+v", n, runner.calls)
	}
	if _, e := os.Stat(m.Path("o/r")); !os.IsNotExist(e) {
		t.Fatalf("mirror directory exists on fallback: %v", e)
	}
	if st := m.Stats(); st.Fetches != 0 || st.Failures != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestFetchErrorRedactsTheToken(t *testing.T) {
	const token = "plain-secret-value-123456789"
	var logs bytes.Buffer
	runner := &fakeRunner{run: func(ctx context.Context, args []string) ([]byte, error) {
		if slices.Contains(args, "fetch") {
			return nil, &integrations.CommandError{
				Command: "git",
				Err:     errors.New("exit status 128"),
				Stderr:  "fatal: unable to access https://x-access-token:" + token + "@example.invalid/o/r.git",
			}
		}
		return nil, nil
	}}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return token, true },
		Run:     runner,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: testBaseEnv(t),
		Log:     slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	e := m.Fetch(context.Background(), "o/r")
	if e == nil {
		t.Fatal("fetch succeeded")
	}
	if strings.Contains(e.Error(), token) {
		t.Fatalf("token in error text: %v", e)
	}
	if !strings.Contains(e.Error(), "exit status 128") {
		t.Fatalf("error lost the command detail: %v", e)
	}
	if strings.Contains(logs.String(), token) {
		t.Fatalf("token in log line: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "mirror fetch") {
		t.Fatalf("no fetch log line: %s", logs.String())
	}
}

func TestConcurrentFetchesRunOne(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	runner := &fakeRunner{run: func(ctx context.Context, args []string) ([]byte, error) {
		if !slices.Contains(args, "fetch") {
			return nil, nil
		}
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, nil
	}}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "ghs_testtokenvalue1234567890", true },
		Run:     runner,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: testBaseEnv(t),
	})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = m.Fetch(context.Background(), "o/r")
		}()
	}
	<-started
	time.Sleep(100 * time.Millisecond)
	if n := runner.fetchCalls(); n != 1 {
		t.Fatalf("fetch calls while one runs = %d, want 1", n)
	}
	close(release)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("errs = %v", errs)
	}
	if n := runner.fetchCalls(); n != 1 {
		t.Fatalf("fetch calls = %d, want 1", n)
	}
}

func TestFetchFailureCooldownWarnsOnceAnHour(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	runner := &fakeRunner{run: func(ctx context.Context, args []string) ([]byte, error) {
		if slices.Contains(args, "fetch") {
			return nil, errors.New("boom")
		}
		return nil, nil
	}}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "ghs_testtokenvalue1234567890", true },
		Run:     runner,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: testBaseEnv(t),
		Now:     func() time.Time { return now },
		Log:     slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	for range fetchFailureLimit {
		if e := m.Fetch(context.Background(), "o/r"); e == nil {
			t.Fatal("fetch succeeded")
		}
	}
	if n := runner.fetchCalls(); n != fetchFailureLimit {
		t.Fatalf("fetch calls = %d", n)
	}
	// On cooldown: no command runs, and the warning is not repeated.
	if e := m.Fetch(context.Background(), "o/r"); !errors.Is(e, errSkipped) {
		t.Fatalf("err = %v, want errSkipped", e)
	}
	if n := runner.fetchCalls(); n != fetchFailureLimit {
		t.Fatalf("fetch calls during cooldown = %d", n)
	}
	now = now.Add(30 * time.Minute)
	_ = m.Fetch(context.Background(), "o/r")
	if n := runner.fetchCalls(); n != fetchFailureLimit {
		t.Fatalf("fetch calls during cooldown = %d", n)
	}
	if got := strings.Count(logs.String(), "mirror fetch skipped after repeated failures"); got != 1 {
		t.Fatalf("cooldown warnings in the first hour = %d, want 1:\n%s", got, logs.String())
	}
	// Past the cooldown the repository is tried again, once.
	now = now.Add(31 * time.Minute)
	_ = m.Fetch(context.Background(), "o/r")
	if n := runner.fetchCalls(); n != fetchFailureLimit+1 {
		t.Fatalf("fetch calls after cooldown = %d", n)
	}
}

func TestDiffMissingObjectAsksForFetch(t *testing.T) {
	runner := &fakeRunner{}
	m := New(Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "ghs_testtokenvalue1234567890", true },
		Run:     runner,
		URL:     func(string) string { return "https://example.invalid/o/r.git" },
		BaseEnv: testBaseEnv(t),
	})
	base := strings.Repeat("a", 40)
	head := strings.Repeat("b", 40)
	text, reason, e := m.Diff(context.Background(), "o/r", base, head)
	if e != nil || text != "" || reason != "missing object" {
		t.Fatalf("diff = %q %q %v", text, reason, e)
	}
	// The missing head asks for a fetch, which starts in the background.
	deadline := time.Now().Add(2 * time.Second)
	for runner.fetchCalls() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runner.fetchCalls(); n == 0 {
		t.Fatal("no fetch was requested")
	}
}

func TestPathRejectsUnsafeRepoNames(t *testing.T) {
	m := New(Options{Dir: t.TempDir(), Token: func(context.Context, string) (string, bool) { return "", false }, Run: &fakeRunner{}})
	for _, repo := range []string{"", "o", "../o/r", "o/..", "o/r/extra", "o/r\n"} {
		if e := m.Fetch(context.Background(), repo); e == nil || !strings.Contains(e.Error(), "invalid repository") {
			t.Errorf("Fetch(%q) = %v", repo, e)
		}
	}
}
