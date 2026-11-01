package crashreport

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var fleetEventFakeSecrets = []string{"ghp_FAKEfakeFAKEfakeFAKEfake0123456789", "hunter2pass", "tok3nVALUEfake99"}

const fleetEventFakeSecretText = "token=tok3nVALUEfake99 ghp_FAKEfakeFAKEfakeFAKEfake0123456789 https://ops:hunter2pass@reviewer.invalid/v1"

func TestCrashReportingIsOffWithoutDSN(t *testing.T) {
	state := t.TempDir()
	t.Setenv(DSNEnv, "")
	Start(state, "mergewake", "v-test")()
	if _, err := os.Stat(filepath.Join(state, "crash")); !os.IsNotExist(err) {
		t.Fatalf("crash dir created without a DSN: %v", err)
	}
}

// deadPID returns the pid of a process that has exited.
func deadPID(t *testing.T) int {
	t.Helper()
	command := exec.Command("true")
	if err := command.Run(); err != nil {
		t.Skipf("cannot run true: %v", err)
	}
	return command.Process.Pid
}

func writeCrashFile(t *testing.T, dir string, nanos int64, pid int, contents string) string {
	t.Helper()
	path := filepath.Join(dir, "crash-"+strconv.FormatInt(nanos, 10)+"-"+strconv.Itoa(pid)+".log")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCollectCrashFilesKeepsLiveProcessesAndSamplesALoop(t *testing.T) {
	dir := t.TempDir()
	dead := deadPID(t)
	live := writeCrashFile(t, dir, 1, os.Getpid(), "")
	emptyDead := writeCrashFile(t, dir, 2, dead, "")
	var crashed []string
	for i := int64(10); i < 15; i++ {
		crashed = append(crashed, writeCrashFile(t, dir, i, dead, "panic: boom\n"))
	}
	got := collectCrashFiles(dir)
	want := []string{crashed[4], crashed[3], crashed[2]}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("pending=%v want %v", got, want)
	}
	for _, path := range []string{emptyDead, crashed[0], crashed[1]} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s should be removed", path)
		}
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("a live process's crash file must stay: %v", err)
	}
}

func TestCrashReportUploadIsScrubbedAndDeletesTheFile(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, r.URL.Path+"\n"+string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	dsn := strings.Replace(server.URL, "http://", "http://publickey@", 1) + "/42"
	home, _ := os.UserHomeDir()
	dir := t.TempDir()
	path := writeCrashFile(t, dir, 1, deadPID(t), "panic: boom "+fleetEventFakeSecretText+"\n\ngoroutine 7 [running]:\nmain.f()\n\t"+home+"/work/x/main.go:10 +0x1d\n")
	if _, err := uploadCrashReports(dsn, "mergewake", "v-test", []string{path}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	all := strings.Join(bodies, "\n")
	if len(bodies) != 1 || !strings.Contains(bodies[0], "/api/42/") || !strings.Contains(all, "panic: boom") || !strings.Contains(all, `"level":"fatal"`) {
		t.Fatalf("sentry received: %s", all)
	}
	for _, secret := range fleetEventFakeSecrets {
		if strings.Contains(all, secret) {
			t.Fatalf("secret %q reached Sentry", secret)
		}
	}
	if len(home) > 1 && strings.Contains(all, home+"/work") {
		t.Fatal("the home directory reached Sentry")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("a sent crash file must be deleted")
	}
}

// TestCrashOutputCapturesAGoroutinePanic runs a child that arms crash
// reporting and panics off the main goroutine; the runtime must leave the
// traceback in the child's crash file for the next start to send.
func TestCrashOutputCapturesAGoroutinePanic(t *testing.T) {
	if state := os.Getenv("MERGEWAKE_CRASH_MAIN_CHILD"); state != "" {
		// The production entry: a panic on the goroutine that called Run.
		_ = Run(state, "mergewake", "v-test", func() error { panic("crash-child-main-boom") })
		return
	}
	if state := os.Getenv("MERGEWAKE_CRASH_CHILD"); state != "" {
		Start(state, "mergewake", "v-test")
		done := make(chan struct{})
		go func() {
			defer close(done)
			panic("crash-child-boom")
		}()
		<-done
		return
	}
	state := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestCrashOutputCapturesAGoroutinePanic$")
	command.Env = append(os.Environ(), "MERGEWAKE_CRASH_CHILD="+state, DSNEnv+"=http://publickey@127.0.0.1:9/1")
	if err := command.Run(); err == nil {
		t.Fatal("the child must crash")
	}
	pending := collectCrashFiles(filepath.Join(state, "crash"))
	if len(pending) != 1 {
		t.Fatalf("pending crash files: %v", pending)
	}
	raw, err := os.ReadFile(pending[0])
	if err != nil || !strings.Contains(string(raw), "panic: crash-child-boom") || !strings.Contains(string(raw), "goroutine") {
		t.Fatalf("crash file: %q %v", raw, err)
	}
}

func TestCrashBudgetCountsOnlyAcceptedReports(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	recordCrashSends(dir, start, 2)
	if got := crashBudgetLeft(dir, start.Add(time.Minute)); got != 1 {
		t.Fatalf("one report left: %d", got)
	}
	recordCrashSends(dir, start.Add(time.Minute), 1)
	if got := crashBudgetLeft(dir, start.Add(2*time.Minute)); got != 0 {
		t.Fatalf("a crash loop must be cut off: %d", got)
	}
	if got := crashBudgetLeft(dir, start.Add(25*time.Hour)); got != crashReportDailyBudget {
		t.Fatalf("the budget renews after 24 hours: %d", got)
	}
}

func TestReportPendingCrashesDropsWhatTheBudgetRefuses(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	recordCrashSends(dir, now, crashReportDailyBudget)
	path := writeCrashFile(t, dir, 1, deadPID(t), "panic: boom\n")
	reportPendingCrashes("http://publickey@127.0.0.1:9/1", "tool", "v-test", dir, now)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("a crash over budget must be dropped")
	}
}

func TestUnreachableSentryKeepsFileAndBudget(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	path := writeCrashFile(t, dir, 1, deadPID(t), "panic: boom\n")
	reportPendingCrashes("http://publickey@127.0.0.1:9/1", "tool", "v-test", dir, now)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("an undelivered crash must stay for the next start: %v", err)
	}
	if got := crashBudgetLeft(dir, now); got != crashReportDailyBudget {
		t.Fatalf("an undelivered crash must not spend the budget: %d left", got)
	}
}

func TestCrashReportRejectedBySentryIsKept(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	dsn := strings.Replace(server.URL, "http://", "http://publickey@", 1) + "/42"
	path := writeCrashFile(t, t.TempDir(), 1, deadPID(t), "panic: boom\n")
	if _, err := uploadCrashReports(dsn, "tool", "v-test", []string{path}); err == nil {
		t.Fatal("a rejected report must be an error")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a rejected report's file must stay for the next start: %v", err)
	}
}

func TestCrashReportScrubRemovesEmails(t *testing.T) {
	if got := crashReportScrub("panic: user alice@example.org not found"); strings.Contains(got, "alice@example.org") {
		t.Fatalf("email reached the report: %q", got)
	}
}

func TestRunCapturesAPanicOnItsOwnGoroutine(t *testing.T) {
	state := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestCrashOutputCapturesAGoroutinePanic$")
	command.Env = append(os.Environ(), "MERGEWAKE_CRASH_MAIN_CHILD="+state, DSNEnv+"=http://publickey@127.0.0.1:9/1")
	if err := command.Run(); err == nil {
		t.Fatal("the child must crash")
	}
	pending := collectCrashFiles(filepath.Join(state, "crash"))
	if len(pending) != 1 {
		t.Fatalf("pending crash files: %v", pending)
	}
	raw, err := os.ReadFile(pending[0])
	if err != nil || !strings.Contains(string(raw), "panic: crash-child-main-boom") {
		t.Fatalf("crash file: %q %v", raw, err)
	}
}

func TestCrashReportSendLogUnwritableSendsNothing(t *testing.T) {
	var requests int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
	}))
	defer server.Close()
	dir := t.TempDir()
	path := writeCrashFile(t, dir, 1, deadPID(t), "panic: boom\n")
	if err := os.Mkdir(filepath.Join(dir, "sent"), 0o700); err != nil {
		t.Fatal(err)
	}
	dsn := strings.Replace(server.URL, "http://", "http://publickey@", 1) + "/42"
	reportPendingCrashes(dsn, "tool", "v-test", dir, time.Now())
	mu.Lock()
	defer mu.Unlock()
	if _, err := os.Stat(path); err != nil || requests != 0 {
		t.Fatalf("nothing may be sent while the cap cannot be recorded: requests=%d %v", requests, err)
	}
}
