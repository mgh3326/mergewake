package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/mirror"
)

const shadowRepo = "o/r"

func shadowPull(base, head string) github.Pull {
	var p github.Pull
	p.Number = 7
	p.Base.SHA = base
	p.Head.SHA = head
	return p
}

func shadowMirror(t *testing.T, origin string, runner mirror.Runner) *mirror.Mirror {
	t.Helper()
	home := t.TempDir()
	return mirror.New(mirror.Options{
		Dir:     t.TempDir(),
		Token:   func(context.Context, string) (string, bool) { return "ghs_testtokenvalue1234567890", true },
		Run:     runner,
		URL:     func(string) string { return origin },
		BaseEnv: func() []string { return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home} },
	})
}

// shadowOrigin makes a bare origin whose main is base and whose PR head is
// head, and returns the working clone that produced them.
func shadowOrigin(t *testing.T, edit func(work, base string) string) (origin, work, base, head string) {
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
	if edit != nil {
		head = edit(work, base)
	} else {
		if e := os.WriteFile(filepath.Join(work, "f"), []byte("head\n"), 0o644); e != nil {
			t.Fatal(e)
		}
		gitAt(t, work, "commit", "-am", "head")
		head = gitAt(t, work, "rev-parse", "HEAD")
	}
	if b, e := exec.Command("git", "init", "--bare", origin).CombinedOutput(); e != nil {
		t.Fatalf("init origin: %v\n%s", e, b)
	}
	gitAt(t, work, "push", origin, "main", "feat/thing")
	gitAt(t, work, "push", origin, head+":refs/pull/7/head")
	return origin, work, base, head
}

// gitRaw is git's stdout untouched: GitHub's diff body keeps its final newline
// and any trailing spaces, so a fixture that trims them is not the API's text.
func gitRaw(t *testing.T, dir string, args ...string) string {
	t.Helper()
	b, e := exec.Command("git", append([]string{"-C", dir}, args...)...).Output()
	if e != nil {
		t.Fatalf("git %v: %v", args, e)
	}
	return string(b)
}

func TestMirrorShadowEqual(t *testing.T) {
	origin, work, base, head := shadowOrigin(t, nil)
	m := shadowMirror(t, origin, integrations.Exec{})
	if e := m.Fetch(context.Background(), shadowRepo); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	api := gitRaw(t, work, "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--find-renames", base+"..."+head)
	var logs bytes.Buffer
	s := &Service{GH: &fakeGH{pull: shadowPull(base, head)}, Mirror: m, Log: slog.New(slog.NewJSONHandler(&logs, nil))}
	s.ShadowDiff(context.Background(), shadowRepo, 7, api)
	st := s.ShadowStats()
	if st.Equal != 1 || st.Mismatch != 0 {
		t.Fatalf("stats = %+v", st)
	}
	line := logs.String()
	if !strings.Contains(line, `"msg":"mirror shadow"`) || !strings.Contains(line, `"result":"equal"`) {
		t.Fatalf("shadow log = %s", line)
	}
	if !strings.Contains(line, `"mirror_fetches":1`) {
		t.Fatalf("shadow log lacks mirror fetch counters: %s", line)
	}
}

func TestMirrorShadowRenameMismatch(t *testing.T) {
	origin, work, base, head := shadowOrigin(t, func(work, _ string) string {
		gitAt(t, work, "mv", "f", "g")
		gitAt(t, work, "commit", "-am", "rename")
		return gitAt(t, work, "rev-parse", "HEAD")
	})
	m := shadowMirror(t, origin, integrations.Exec{})
	if e := m.Fetch(context.Background(), shadowRepo); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	// GitHub did not detect the rename; the mirror's git did.
	api := gitRaw(t, work, "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--no-renames", base+"..."+head)
	s := &Service{GH: &fakeGH{pull: shadowPull(base, head)}, Mirror: m}
	s.ShadowDiff(context.Background(), shadowRepo, 7, api)
	st := s.ShadowStats()
	if st.Mismatch != 1 || st.Reasons["rename"] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestMirrorShadowBinaryMismatch(t *testing.T) {
	origin, _, base, head := shadowOrigin(t, func(work, _ string) string {
		if e := os.WriteFile(filepath.Join(work, "blob.bin"), []byte{0, 1, 2, 3}, 0o644); e != nil {
			t.Fatal(e)
		}
		gitAt(t, work, "add", "blob.bin")
		gitAt(t, work, "commit", "-m", "binary")
		return gitAt(t, work, "rev-parse", "HEAD")
	})
	m := shadowMirror(t, origin, integrations.Exec{})
	if e := m.Fetch(context.Background(), shadowRepo); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	api := "diff --git a/blob.bin b/blob.bin\nindex 1111111..2222222 100644\nBinary files a/blob.bin and b/blob.bin differ\n"
	s := &Service{GH: &fakeGH{pull: shadowPull(base, head)}, Mirror: m}
	s.ShadowDiff(context.Background(), shadowRepo, 7, api)
	st := s.ShadowStats()
	if st.Mismatch != 1 || st.Reasons["binary"] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestCompareDiffNeverPanics(t *testing.T) {
	for _, tc := range []struct{ api, local string }{
		{"", ""},
		{"garbage", ""},
		{"garbage", "other garbage"},
		{"diff --git a/x b/x\n@@ -1 +1 @@\n-a\n+b\n", "diff --git a/y b/y\n"},
		{"\x00\xff", "diff --git a/x b/x\n"},
	} {
		result, reason := compareDiff(tc.api, tc.local)
		if result != "equal" && result != "mismatch" {
			t.Fatalf("compareDiff(%q, %q) = %q", tc.api, tc.local, result)
		}
		if result == "mismatch" && reason == "" {
			t.Fatalf("mismatch without a reason: %q vs %q", tc.api, tc.local)
		}
	}
}

// slowRunner delays every command, so a shadow compare that ran on the
// caller's path would show up in Diff's latency.
type slowRunner struct {
	mu    sync.Mutex
	delay time.Duration
}

func (r *slowRunner) setDelay(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delay = d
}

func (r *slowRunner) RunEnv(ctx context.Context, command string, args []string, input, dir string, env []string) ([]byte, error) {
	r.mu.Lock()
	d := r.delay
	r.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	return integrations.Exec{}.RunEnv(ctx, command, args, input, dir, env)
}

func shadowAPI(t *testing.T, api, base, head string) *github.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/pulls/7") && strings.Contains(r.Header.Get("Accept"), "diff"):
			_, _ = fmt.Fprint(w, api)
		case strings.HasSuffix(r.URL.Path, "/pulls/7"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"number":7,"base":{"sha":%q},"head":{"sha":%q}}`, base, head)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	gh := github.New("token")
	gh.BaseURL = srv.URL
	gh.HTTP = srv.Client()
	return gh
}

func waitShadow(t *testing.T, s *Service) ShadowStats {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		st := s.ShadowStats()
		if st.Equal+st.Mismatch > 0 {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("shadow compare did not finish")
	return ShadowStats{}
}

func TestDiffReturnsBeforeShadowCompare(t *testing.T) {
	origin, work, base, head := shadowOrigin(t, nil)
	runner := &slowRunner{}
	m := shadowMirror(t, origin, runner)
	if e := m.Fetch(context.Background(), shadowRepo); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	api := gitRaw(t, work, "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--find-renames", base+"..."+head)
	gh := shadowAPI(t, api, base, head)
	s := &Service{GH: gh, Mirror: m}
	gh.DiffObserver = s.ShadowDiff
	runner.setDelay(500 * time.Millisecond)

	start := time.Now()
	text, e := gh.Diff(context.Background(), shadowRepo, 7)
	elapsed := time.Since(start)
	if e != nil {
		t.Fatalf("diff: %v", e)
	}
	if text != api {
		t.Fatalf("Diff returned other bytes:\n%s", text)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Diff took %s with a 500ms mirror command: the shadow compare is on its path", elapsed)
	}
	if st := waitShadow(t, s); st.Equal != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// blockRunner blocks every command until the caller's context ends.
type blockRunner struct {
	mu    sync.Mutex
	block bool
}

func (r *blockRunner) setBlock(b bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.block = b
}

func (r *blockRunner) RunEnv(ctx context.Context, command string, args []string, input, dir string, env []string) ([]byte, error) {
	r.mu.Lock()
	block := r.block
	r.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return integrations.Exec{}.RunEnv(ctx, command, args, input, dir, env)
}

func TestMirrorFetchTimeoutLeavesDiffUnaffected(t *testing.T) {
	origin, work, base, head := shadowOrigin(t, nil)
	runner := &blockRunner{}
	m := shadowMirror(t, origin, runner)
	if e := m.Fetch(context.Background(), shadowRepo); e != nil {
		t.Fatalf("fetch: %v", e)
	}
	api := gitRaw(t, work, "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--find-renames", base+"..."+head)
	gh := shadowAPI(t, api, base, head)
	s := &Service{GH: gh, Mirror: m}
	gh.DiffObserver = s.ShadowDiff

	runner.setBlock(true)
	fctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if e := m.Fetch(fctx, shadowRepo); e == nil {
		t.Fatal("fetch with a blocked runner succeeded")
	}
	cancel()
	runner.setBlock(false)

	start := time.Now()
	text, e := gh.Diff(context.Background(), shadowRepo, 7)
	if e != nil || text != api {
		t.Fatalf("diff after timed-out fetch: %v", e)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Diff took %s after a timed-out fetch", elapsed)
	}
	if st := waitShadow(t, s); st.Equal != 1 {
		t.Fatalf("stats after a timed-out fetch = %+v", st)
	}
}
