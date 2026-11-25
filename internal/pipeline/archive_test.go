package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/hktest"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// archiveRunner scripts herdr and git and runs everything else (sh, bash,
// tar, cat, rm) for real against temp dirs. hk is reached over HTTP
// (hktest); an hk CLI call is recorded and refused.
type archiveRunner struct {
	script              *scriptRunner
	hkCLI               [][]string
	calls               int
	vanishDuringRefresh string
	afterTreeListing    func()
	treeBytes           int
	// failCatPath is a job-relative file the remote reads cannot open. A cat
	// of it exits failCatRC (1 when zero); the batch read's diagnosis command
	// reports that same status for it.
	failCatPath string
	failCatRC   int
	// failTarDir makes the batch read of that dir exit failTarCode (1 when zero).
	failTarDir  string
	failTarCode int
	// failProbeExit makes the diagnosis command itself exit with that status.
	failProbeExit int
	tarProbes     int
	probes        int
}

type archiveBlockRunner struct {
	base  integrations.Runner
	match string
}

type archiveAfterCommandRunner struct {
	base  integrations.Runner
	match string
	after func()
}

type archiveCancelAfterCommandRunner struct {
	base   integrations.Runner
	match  string
	cancel context.CancelFunc
}

func (r archiveCancelAfterCommandRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	out, err := r.base.Run(ctx, name, args, input, dir)
	if err == nil && strings.Contains(name+" "+strings.Join(args, " "), r.match) {
		r.cancel()
	}
	return out, err
}

func (r archiveAfterCommandRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	out, err := r.base.Run(ctx, name, args, input, dir)
	if err == nil && strings.Contains(name+" "+strings.Join(args, " "), r.match) {
		r.after()
	}
	return out, err
}

func (r archiveBlockRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if strings.Contains(name+" "+strings.Join(args, " "), r.match) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.base.Run(ctx, name, args, input, dir)
}

type archivePauseDeleteRunner struct {
	base        integrations.Runner
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	fail        bool
}

func (r *archivePauseDeleteRunner) unblock() { r.releaseOnce.Do(func() { close(r.release) }) }

// archiveLateWriteRunner overwrites file with content right after the first
// manifest publish, so the job changes between publish and delete.
type archiveLateWriteRunner struct {
	base     integrations.Runner
	file     string
	content  string
	once     sync.Once
	writeErr error
}

func (r *archiveLateWriteRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	out, err := r.base.Run(ctx, name, args, input, dir)
	if err == nil && name == "sh" && strings.Contains(strings.Join(args, " "), `mv -f "$1.partial" "$1"`) {
		r.once.Do(func() { r.writeErr = os.WriteFile(r.file, []byte(r.content), 0o644) })
	}
	return out, err
}

func (r *archivePauseDeleteRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if name == "sh" && strings.Contains(strings.Join(args, " "), `rm -rf "$1" || {`) {
		r.once.Do(func() { close(r.entered) })
		<-r.release
		if r.fail {
			return nil, errors.New("injected delete failure")
		}
	}
	return r.base.Run(ctx, name, args, input, dir)
}

// testExitStatus is a typed failure carrying an exit code, the shape the real
// runner's *exec.ExitError has. Its text deliberately omits "exit status N".
type testExitStatus int

func (e testExitStatus) Error() string { return fmt.Sprintf("exited with code %d", int(e)) }
func (e testExitStatus) ExitCode() int { return int(e) }

func orDefault(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func (a *archiveRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	a.calls++
	if name == "cat" && a.failCatPath != "" && strings.HasSuffix(args[len(args)-1], a.failCatPath) {
		return nil, testExitStatus(orDefault(a.failCatRC, 1))
	}
	if name == "sh" && len(args) > 1 && strings.Contains(args[1], `cat -- "$p"`) {
		a.probes++
		if a.failProbeExit != 0 {
			return nil, testExitStatus(a.failProbeExit)
		}
		if a.failCatPath != "" {
			for _, p := range strings.Split(strings.TrimSuffix(input, "\n"), "\n") {
				if p == a.failCatPath {
					return []byte(fmt.Sprintf("%d %s\n", orDefault(a.failCatRC, 1), p)), nil
				}
			}
		}
	}
	if (name == "tar" || name == "env") && a.failTarDir != "" {
		if name == "env" && strings.Contains(strings.Join(args, " "), "tar") {
			a.tarProbes++
		}
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "-C" && args[i+1] == a.failTarDir && (name == "tar" || strings.Contains(strings.Join(args[:i], " "), "tar")) {
				return nil, testExitStatus(orDefault(a.failTarCode, 1))
			}
		}
	}
	if name == "handoffkeep" || len(args) > 0 && (args[0] == "attach" || args[0] == "tasks" || args[0] == "doc") {
		a.hkCLI = append(a.hkCLI, append([]string{name}, args...))
		return nil, errors.New("hk CLI subprocess refused: " + strings.Join(args, " "))
	}
	switch name {
	case "herdr", "git":
		return a.script.Run(ctx, name, args, input, dir)
	}
	if name == "sh" && a.vanishDuringRefresh != "" {
		for i, arg := range args {
			if strings.Contains(arg, "find . ! -type d -exec sh -c") && strings.Contains(arg, "|| continue") {
				old := `for f; do if [ -L "$f" ]; then`
				new := `for f; do if [ "$f" = "` + a.vanishDuringRefresh + `" ]; then rm -f "$f"; fi; if [ -L "$f" ]; then`
				args[i] = strings.Replace(arg, old, new, 1)
				a.vanishDuringRefresh = ""
				break
			}
		}
	}
	c := exec.CommandContext(ctx, name, args...)
	c.Stdin = strings.NewReader(input)
	var out, stderr bytes.Buffer
	c.Stdout, c.Stderr = &out, &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", name, err, stderr.String())
	}
	if name == "sh" && strings.Contains(strings.Join(args, " "), "for d in *; do") {
		a.treeBytes = out.Len()
		if a.afterTreeListing != nil {
			after := a.afterTreeListing
			a.afterTreeListing = nil
			after()
		}
	}
	return out.Bytes(), nil
}

func TestArchiveJobsPreflightCallCountDoesNotScaleWithSkippedJobs(t *testing.T) {
	for _, count := range []int{40, 80} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			f := newArchiveFixture(t, noAgents)
			for i := 0; i < count; i++ {
				name := fmt.Sprintf("too-new-%03d", i)
				jobDir(t, f.jobs, name, time.Hour)
			}
			// Include representative other preflight skips in the same host scan.
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("no-events-%03d", i)
				dir := jobDir(t, f.jobs, name, time.Hour)
				if err := os.RemoveAll(filepath.Join(dir, "events")); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("unfinished-%03d", i)
				dir := jobDir(t, f.jobs, name, 20*24*time.Hour)
				if err := os.Remove(filepath.Join(dir, "events", "00005-job.completed.json")); err != nil {
					t.Fatal(err)
				}
			}
			f.run.calls = 0
			if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
				t.Fatal(err)
			}
			t.Logf("host runner calls = %d", f.run.calls)
			if f.run.calls > 5 {
				t.Fatalf("host calls = %d, want <= 5", f.run.calls)
			}
		})
	}
}

func TestArchiveJobsPaneScanFailureIsUnscannedWithoutFailingRun(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	f.run.script.errs = map[string]error{}
	f.run.script.errs[key("herdr", "pane", "list")] = errors.New("pane response truncated")
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatalf("pane scan failure made run fail: %v", err)
	}
	out := f.out.String()
	if strings.Contains(out, `"action":"archive-job"`) || strings.Contains(out, `"job":"old-job"`) {
		t.Fatalf("job on unscanned host was archived or recorded: %s", out)
	}
	if !strings.Contains(out, `"unscanned_hosts":["local"]`) || !strings.Contains(out, `"unscanned_host_reasons":{"local":"pane scan: pane response truncated"}`) {
		t.Fatalf("summary lacks pane scan failure reason: %s", out)
	}
	if !strings.Contains(out, `"eligible_left":0`) || !strings.Contains(out, "pane scan or jobs listing did not complete") {
		t.Fatalf("pane-scan-failed host should contribute zero and be excluded by scope: %s", out)
	}
}

func TestArchiveJobsBlockingPaneScanStopsAtMaxRunWithSummary(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "50ms"
	f.s.Runner = archiveBlockRunner{base: f.run, match: "herdr agent list"}
	started := time.Now()
	err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true})
	if err != nil {
		t.Fatalf("deadline stopped the run with error: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("run exceeded max_run by too much: %s", elapsed)
	}
	out := f.out.String()
	if !strings.Contains(out, `"stopped_on_budget":true`) || !strings.Contains(out, `"unscanned_hosts":["local"]`) || !strings.Contains(out, `"max_run deadline during pane scan"`) {
		t.Fatalf("deadline run lacks final summary: %s", out)
	}
	if !strings.Contains(out, `"phase":"pane_scan"`) {
		t.Fatalf("deadline run lacks pane scan timing row: %s", out)
	}
}

func TestArchiveJobsBudgetStopDuringHostScanAdvancesRotation(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "50ms"
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	f.s.Runner = archiveBlockRunner{base: f.run, match: "herdr agent list"}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatalf("deadline stopped the run with error: %v", err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"unscanned_hosts":["local","other"]`) {
		t.Fatalf("summary omitted current scan host: %s", out)
	}
	if b, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, archiveNextHostFile)); err != nil || string(b) != "other\n" {
		t.Fatalf("next-host state = %q, %v; want other host", b, err)
	}
}

func TestArchiveJobsApplyFinishesAndRecordsAfterMaxRun(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	dir := jobDir(t, f.jobs, "a-old-job", 20*24*time.Hour)
	jobDir(t, f.jobs, "z-next-job", 20*24*time.Hour)
	runner := &archivePauseDeleteRunner{base: f.run, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(runner.unblock)
	f.s.Runner = runner
	done := make(chan error, 1)
	go func() { done <- f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}) }()
	select {
	case <-runner.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("archive apply never reached source delete")
	}
	time.Sleep(2100 * time.Millisecond)
	runner.unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("apply after max_run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("archive run did not finish after delete was released")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("completed archive left source dir: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row archiveRow
		if json.Unmarshal([]byte(line), &row) == nil && row.Action == "archive-job" && row.Job == "a-old-job" {
			found = row.Result == "archived" && row.Archive != nil && row.Archive.Deleted
		}
	}
	if !found {
		t.Fatalf("durable archive result missing after apply completed: %s", b)
	}
	if !strings.Contains(f.out.String(), `"eligible_left":1`) {
		t.Fatalf("runCtx deadline after apply did not count the remaining listed job: %s", f.out.String())
	}
}

func TestArchiveJobsApplyFailureAfterMaxRunWritesErrorRow(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	runner := &archivePauseDeleteRunner{base: f.run, entered: make(chan struct{}), release: make(chan struct{}), fail: true}
	t.Cleanup(runner.unblock)
	f.s.Runner = runner
	done := make(chan error, 1)
	go func() { done <- f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}) }()
	select {
	case <-runner.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("archive apply never reached source delete")
	}
	time.Sleep(2100 * time.Millisecond)
	runner.unblock()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("apply failure was not returned")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("archive run did not finish after delete failed")
	}
	b, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row archiveRow
		if json.Unmarshal([]byte(line), &row) == nil && row.Action == "archive-job" && row.Job == "old-job" {
			found = row.Result == "error" && row.Archive != nil && row.Archive.Manifest != "" && !row.Archive.Deleted
		}
	}
	if !found {
		t.Fatalf("partial apply result missing from durable row: %s", b)
	}
}

func TestArchiveJobsBusyApplyLockRemainsSkipped(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	h := f.s.Config.Hosts[0]
	p, err := f.s.archiver().Plan(context.Background(), h, "old-job")
	if err != nil {
		t.Fatal(err)
	}
	lockDigest := sha256.Sum256([]byte(p.Dest))
	lockDir := filepath.Join(f.drive, ".locks", hex.EncodeToString(lockDigest[:]))
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(lockDir, "owner"), "another-owner 9999999999\n")
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatalf("routine archive lock skip failed the run: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var skipped, summary bool
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("bad archive row %q: %v", line, err)
		}
		if row["action"] == "archive-job" && row["job"] == "old-job" {
			skipped = row["result"] == "skipped" && row["reason"] == "archive-lock-busy"
		}
		if row["action"] == "archive-jobs" {
			summary = row["errors"] == float64(0)
		}
	}
	if !skipped || !summary {
		t.Fatalf("busy lock was not recorded as a successful skip: %s", b)
	}
}

func TestArchiveJobsPostPublishSkipFailsRunAndRecordsError(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	runner := &archiveLateWriteRunner{base: f.run, file: filepath.Join(dir, "late.txt"), content: "written after publish\n"}
	f.s.Runner = runner
	err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{})
	if runner.writeErr != nil {
		t.Fatalf("late write failed: %v", runner.writeErr)
	}
	if err == nil {
		t.Fatal("post-publish job-changed skip was treated as routine; run did not fail")
	}
	b, rerr := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if rerr != nil {
		t.Fatal(rerr)
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row archiveRow
		if json.Unmarshal([]byte(line), &row) == nil && row.Action == "archive-job" && row.Job == "old-job" {
			found = row.Result == "error" && row.Archive != nil && row.Archive.Manifest != "" && !row.Archive.Deleted
		}
	}
	if !found {
		t.Fatalf("published-but-undeleted job was not recorded as an error row: %s", b)
	}
}

// The next run finds the published manifest, whose file now hashes differently
// (same size, so the manifest still matches the plan). That resume skip must
// also stay visible instead of recording as a routine skip every run.
func TestArchiveJobsResumeAfterPublishedManifestChangedFailsRun(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	runner := &archiveLateWriteRunner{base: f.run, file: filepath.Join(dir, "brief.md"), content: "# BRIEF\n"}
	f.s.Runner = runner
	_ = f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{})
	if runner.writeErr != nil {
		t.Fatalf("rewrite failed: %v", runner.writeErr)
	}
	// Keep the job old so the next run plans it instead of skipping it as too-new.
	old := time.Now().Add(-20 * 24 * time.Hour)
	if err := os.Chtimes(runner.file, old, old); err != nil {
		t.Fatal(err)
	}
	f.s.Runner = f.run
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err == nil {
		t.Fatal("next run skipped the published-but-changed job as routine; run did not fail")
	}
}

func TestArchiveJobsPersistsPhaseAndSummaryRows(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var phase, summary bool
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("bad archive run row %q: %v", line, err)
		}
		switch row["action"] {
		case "archive-phase":
			phase = phase || row["phase"] == "pane_scan"
		case "archive-jobs":
			summary = true
		}
	}
	if !phase || !summary {
		t.Fatalf("archive log lacks phase or summary row: %s", b)
	}
}

func TestArchiveJobsHostPreflightRowsAndEligiblePlan(t *testing.T) {
	f := newArchiveFixture(t, `{"result":{"agents":[{"pane_id":"w2:p1","name":"pane-live","agent_status":"working","cwd":"/x"}]}}`)
	jobDir(t, f.jobs, "eligible", 20*24*time.Hour)
	jobDir(t, f.jobs, "too-new", time.Hour)
	jobDir(t, f.jobs, "unfinished", 20*24*time.Hour)
	if err := os.Remove(filepath.Join(f.jobs, "unfinished", "events", "00005-job.completed.json")); err != nil {
		t.Fatal(err)
	}
	noEvents := jobDir(t, f.jobs, "no-events", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(noEvents, "events")); err != nil {
		t.Fatal(err)
	}
	danglingEvents := jobDir(t, f.jobs, "dangling-events", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(danglingEvents, "events")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(danglingEvents, "missing-events-target"), filepath.Join(danglingEvents, "events")); err != nil {
		t.Fatal(err)
	}
	fileEvents := jobDir(t, f.jobs, "file-events", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(fileEvents, "events")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(fileEvents, "events"), "not a directory")
	jobDir(t, f.jobs, "pane-live", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(f.jobs, "pane-live", "events")); err != nil {
		t.Fatal(err)
	}
	target, err := os.MkdirTemp(filepath.Dir(f.jobs), "symlink-target")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(f.jobs, "symlink-job")); err != nil {
		t.Fatal(err)
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	var got []string
	var plan map[string]any
	for _, line := range strings.Split(f.out.String(), "\n") {
		var row archiveRow
		if json.Unmarshal([]byte(line), &row) == nil && row.Action == "archive-job" {
			got = append(got, row.Job+"|"+row.Result+"|"+row.Reason)
		}
		var v map[string]any
		if json.Unmarshal([]byte(line), &v) == nil && v["action"] == "archive-plan" && v["job"] == "eligible" {
			plan = v
		}
	}
	want := []string{"dangling-events|skipped|not-finished", "eligible|would-archive|", "file-events|skipped|not-finished", "no-events|would-archive|", "pane-live|skipped|pane-live", "symlink-job|skipped|job-dir-symlink", "too-new|skipped|too-new", "unfinished|skipped|not-finished"}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	if plan == nil || plan["job"] != "eligible" || plan["task"] != "77" || plan["files"] != float64(4) {
		t.Fatalf("eligible plan = %v", plan)
	}
}

func TestArchiveJobsDryRunAndShadowRespectMaxJobsPerRun(t *testing.T) {
	for _, mode := range []struct {
		name string
		o    Options
	}{
		{name: "apply"},
		{name: "dry-run", o: Options{DryRun: true}},
		{name: "shadow", o: Options{Shadow: true}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			f := newArchiveFixture(t, noAgents)
			f.s.Config.Reap.Archive.MaxJobsPerRun = 2
			for i := 0; i < 5; i++ {
				dir := jobDir(t, f.jobs, fmt.Sprintf("legacy-%d", i), time.Duration(20+i)*24*time.Hour)
				if err := os.RemoveAll(filepath.Join(dir, "events")); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, mode.o); err != nil {
				t.Fatal(err)
			}
			var selected []string
			for _, line := range strings.Split(f.out.String(), "\n") {
				var row archiveRow
				if json.Unmarshal([]byte(line), &row) == nil && row.Action == "archive-job" && (row.Result == "would-archive" || row.Result == "archived") {
					selected = append(selected, row.Job)
				}
			}
			sort.Strings(selected)
			if strings.Join(selected, ",") != "legacy-3,legacy-4" {
				t.Fatalf("selected = %v, want oldest two", selected)
			}
			if !strings.Contains(f.out.String(), `"eligible_left":3`) {
				t.Fatalf("summary missing eligible remainder: %s", f.out.String())
			}
		})
	}
}

func TestArchiveJobsEventlessDryRunUsesNoTaskDestination(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	old := jobDir(t, f.jobs, "legacy", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(old, "events")); err != nil {
		t.Fatal(err)
	}
	young := jobDir(t, f.jobs, "young", time.Hour)
	if err := os.RemoveAll(filepath.Join(young, "events")); err != nil {
		t.Fatal(err)
	}
	noTaskLink := jobDir(t, f.jobs, "finished-no-task", 20*24*time.Hour)
	if err := os.Remove(filepath.Join(noTaskLink, "events", "00004-job.task_link.json")); err != nil {
		t.Fatal(err)
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.out.String(), `"job":"legacy","result":"would-archive"`) || !strings.Contains(f.out.String(), filepath.Join(f.drive, "no-task", time.Now().Add(-20*24*time.Hour).UTC().Format("2006-01"), "local-legacy")) {
		t.Fatalf("missing no-task plan: %s", f.out.String())
	}
	if !strings.Contains(f.out.String(), `"job":"young","result":"skipped","reason":"too-new"`) {
		t.Fatalf("young eventless job not skipped by age: %s", f.out.String())
	}
	if !strings.Contains(f.out.String(), filepath.Join(f.drive, "no-task", time.Now().Add(-20*24*time.Hour).UTC().Format("2006-01"), "local-finished-no-task")) {
		t.Fatalf("finished taskless job missing from no-task plans: %s", f.out.String())
	}
}

func TestArchiveJobsMaxJobsPerRunArchivesOldestAndCountsRemainder(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxJobsPerRun = 2
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("legacy-%d", i)
		dir := jobDir(t, f.jobs, name, time.Duration(20+i)*24*time.Hour)
		if err := os.RemoveAll(filepath.Join(dir, "events")); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"legacy-4", "legacy-3"} {
		age := 20 + indexFor(name)
		if _, err := os.Stat(filepath.Join(f.drive, "no-task", time.Now().Add(-time.Duration(age)*24*time.Hour).UTC().Format("2006-01"), "local-"+name, "manifest.json")); err != nil {
			t.Fatalf("oldest %s not archived: %v", name, err)
		}
	}
	for _, name := range []string{"legacy-0", "legacy-1", "legacy-2"} {
		if _, err := os.Stat(filepath.Join(f.jobs, name)); err != nil {
			t.Fatalf("capped job %s removed: %v", name, err)
		}
	}
	if !strings.Contains(f.out.String(), `"eligible_left":3`) {
		t.Fatalf("summary missing eligible remainder: %s", f.out.String())
	}
	if len(f.hk.Comments()) != 0 {
		t.Fatalf("taskless archives wrote task comments: %v", f.hk.Comments())
	}
	if _, ok := f.hk.Documents()["archive/no-task/local-legacy-4"]; !ok {
		t.Fatalf("missing taskless hk record; docs=%v", f.hk.Documents())
	}
}

func indexFor(name string) int {
	switch name {
	case "legacy-4":
		return 4
	case "legacy-3":
		return 3
	}
	return 0
}

func TestArchiveJobsListingContinuesWhenEntryDisappearsBeforeStat(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "volatile", 20*24*time.Hour)
	vanishing := filepath.Join(dir, "vanishing.tmp")
	writeFile(t, vanishing, "temporary")
	old := time.Now().Add(-20 * 24 * time.Hour)
	if err := os.Chtimes(vanishing, old, old); err != nil {
		t.Fatal(err)
	}
	f.run.vanishDuringRefresh = "./vanishing.tmp"
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if f.run.vanishDuringRefresh != "" {
		t.Fatalf("refresh race hook did not run; output: %s", f.out.String())
	}
	if strings.Contains(f.out.String(), `"action":"archive-job","host":"local","job":"volatile","result":"error"`) {
		t.Fatalf("entry race failed the job scan: %s", f.out.String())
	}
	if _, err := os.Stat(vanishing); !os.IsNotExist(err) {
		t.Fatalf("refresh race did not remove test entry: %v", err)
	}
}

func TestArchiveJobsUnreadableSubtreeDoesNotSkipOtherJobs(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read mode-000 directories")
	}
	f := newArchiveFixture(t, noAgents)
	bad := jobDir(t, f.jobs, "bad-job", 20*24*time.Hour)
	private := filepath.Join(bad, "private")
	writeFile(t, filepath.Join(private, "contents.txt"), "inaccessible")
	if err := os.Chmod(private, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(private, 0o755) })
	badEvents := jobDir(t, f.jobs, "bad-events", 20*24*time.Hour)
	eventsDir := filepath.Join(badEvents, "events")
	if err := os.Chmod(eventsDir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(eventsDir, 0o755) })
	jobDir(t, f.jobs, "good-job", 20*24*time.Hour)
	err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true})
	if err == nil {
		t.Fatal("unreadable job should have an error row")
	}
	if !strings.Contains(f.out.String(), `"job":"bad-job","result":"error"`) || !strings.Contains(f.out.String(), `"job":"bad-events","result":"error"`) || !strings.Contains(f.out.String(), `"job":"good-job","result":"would-archive"`) {
		t.Fatalf("one unreadable job affected other rows: %s", f.out.String())
	}
}

func TestArchiveJobsUnreadableReadSkipsAndContinues(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	bad := jobDir(t, f.jobs, "bad-job", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(bad, "events")); err != nil {
		t.Fatal(err)
	}
	f.run.failTarDir = bad
	f.run.failCatPath = "brief.md"
	jobDir(t, f.jobs, "good-job", 20*24*time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatalf("unreadable job failed the run: %v", err)
	}
	for _, want := range []string{`"job":"bad-job","result":"skipped","reason":"unreadable: cat brief.md: cat exit status 1"`, `"job":"good-job","result":"would-archive"`} {
		if !strings.Contains(f.out.String(), want) {
			t.Fatalf("missing %s in output: %s", want, f.out.String())
		}
	}
	if f.run.probes != 1 {
		t.Fatalf("diagnosis commands = %d, want exactly 1", f.run.probes)
	}
}

// Only a cat that exits exactly 1 makes a file unreadable; 126, 127 and 137
// (and 255 from a transport) are run errors, whether they come from the batch
// read or from the diagnosis that names the file.
func TestArchiveJobsUnreadableExitCodesOtherThanOneRemainRunErrors(t *testing.T) {
	for _, rc := range []int{126, 127, 137, 255} {
		t.Run(strconv.Itoa(rc), func(t *testing.T) {
			f := newArchiveFixture(t, noAgents)
			bad := jobDir(t, f.jobs, "bad-job", 20*24*time.Hour)
			if err := os.RemoveAll(filepath.Join(bad, "events")); err != nil {
				t.Fatal(err)
			}
			f.run.failTarDir = bad
			f.run.failCatPath = "brief.md"
			f.run.failCatRC = rc
			if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err == nil {
				t.Fatalf("cat status %d must fail the run", rc)
			}
			if !strings.Contains(f.out.String(), `"job":"bad-job","result":"error"`) || strings.Contains(f.out.String(), "unreadable") {
				t.Fatalf("cat status %d was not a run error: %s", rc, f.out.String())
			}
		})
	}
}

func TestArchiveJobsBatchReadTransportExit255IsARunErrorWithoutDiagnosis(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	bad := jobDir(t, f.jobs, "bad-job", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(bad, "events")); err != nil {
		t.Fatal(err)
	}
	f.run.failTarDir = bad
	f.run.failTarCode = 255
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err == nil {
		t.Fatal("batch read exit 255 must fail the run")
	}
	if f.run.probes != 0 {
		t.Fatalf("exit 255 ran the diagnosis %d times, want 0", f.run.probes)
	}
}

func TestArchiveJobsDiagnosisTransportFailureRemainsRunError(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	bad := jobDir(t, f.jobs, "bad-job", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(bad, "events")); err != nil {
		t.Fatal(err)
	}
	f.run.failTarDir = bad
	f.run.failCatPath = "brief.md"
	f.run.failProbeExit = 255
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err == nil {
		t.Fatal("diagnosis exit 255 must fail the run")
	}
	if !strings.Contains(f.out.String(), `"job":"bad-job","result":"error"`) {
		t.Fatalf("transport failure was not recorded as a run error: %s", f.out.String())
	}
}

func TestReapArchiveUnreadableTaskReadRemainsRunError(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "bad-job", 20*24*time.Hour)
	f.run.failCatPath = "00004-job.task_link.json"
	rec := reapReceipt{Host: "local", Job: "bad-job"}
	f.s.archiveJob(context.Background(), f.s.Config.Hosts[0], "bad-job", false, &rec)
	if rec.Result != "error" || !strings.Contains(rec.Reason, "unreadable: cat events/00004-job.task_link.json") {
		t.Fatalf("reap archive row = %+v, want error for the same unreadable task read", rec)
	}
}

// budgetClock is a run clock that is past the budget from the moment the
// first row for job "first" is printed, so the gate's next look sees a spent
// budget and nothing before it moved time.
func budgetClock(f archiveFixture, base time.Time, budget time.Duration) func() time.Time {
	return func() time.Time {
		if strings.Contains(f.out.String(), `"job":"first"`) {
			return base.Add(budget)
		}
		return base
	}
}

func TestArchiveJobsBudgetSpentOnHostALastJobRunsNoCommandOnHostB(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	jobDir(t, otherJobs, "third", 20*24*time.Hour)
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	base := time.Now()
	clock := budgetClock(f, base, time.Hour)
	stopAt := -1
	f.s.Now = func() time.Time {
		// The first look after host A's last row marks the stop point.
		if stopAt < 0 && strings.Contains(f.out.String(), `"job":"first"`) {
			stopAt = f.run.calls
		}
		return clock()
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatalf("budget stop failed the run: %v", err)
	}
	if stopAt < 0 {
		t.Fatal("budget never ran out after host A's last job")
	}
	if f.run.calls != stopAt {
		t.Fatalf("runner calls after the budget ran out: %d more", f.run.calls-stopAt)
	}
	out := f.out.String()
	if !strings.Contains(out, `"job":"first","result":"would-archive"`) || !strings.Contains(out, `"stopped_on_budget":true`) || !strings.Contains(out, `"unscanned_hosts":["other"]`) {
		t.Fatalf("budget summary is wrong: %s", out)
	}
	if strings.Contains(out, `"job":"third"`) {
		t.Fatalf("host B was walked after the budget: %s", out)
	}
	if b, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, archiveNextHostFile)); err != nil || string(b) != "other\n" {
		t.Fatalf("next-host state = %q, %v; want %q", b, err, "other\n")
	}
}

func TestArchiveJobsNextRunStartsAtHostAfterBudgetStop(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	jobDir(t, otherJobs, "third", 20*24*time.Hour)
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	base := time.Now()
	f.s.Now = budgetClock(f, base, time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	f.out.Reset()
	f.s.Config.Reap.Archive.MaxRun = ""
	f.s.Now = func() time.Time { return base }
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	out := f.out.String()
	if i, j := strings.Index(out, `"host":"other"`), strings.Index(out, `"host":"local"`); i < 0 || j < 0 || i > j {
		t.Fatalf("second run did not start at host other: %s", out)
	}
	if _, err := os.Stat(filepath.Join(f.s.Config.StateDir, archiveNextHostFile)); !os.IsNotExist(err) {
		t.Fatalf("a complete run must clear the next-host state, stat err = %v", err)
	}
}

// seedNextHost writes a next-host state naming host and backdates it, so a
// test can tell whether a run touched the file.
func seedNextHost(t *testing.T, f archiveFixture, host string) string {
	t.Helper()
	if err := os.MkdirAll(f.s.Config.StateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(f.s.Config.StateDir, archiveNextHostFile)
	writeFile(t, file, host+"\n")
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(file, old, old); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestArchiveJobsDryRunPreviewsRotatedOrderAndLeavesNextHostState(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	jobDir(t, otherJobs, "third", 20*24*time.Hour)
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	file := seedNextHost(t, f, "other")
	before, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	out := f.out.String()
	if i, j := strings.Index(out, `"host":"other"`), strings.Index(out, `"host":"local"`); i < 0 || j < 0 || i > j {
		t.Fatalf("dry run did not preview the rotated order (host other first): %s", out)
	}
	after, err := os.Stat(file)
	if err != nil {
		t.Fatalf("dry run removed the next-host state: %v", err)
	}
	if b, _ := os.ReadFile(file); string(b) != "other\n" {
		t.Fatalf("dry run changed the next-host state to %q", b)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("dry run touched the next-host state: mtime %v -> %v", before.ModTime(), after.ModTime())
	}
}

func TestArchiveJobsApplyBudgetStopUpdatesNextHostState(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	jobDir(t, otherJobs, "third", 20*24*time.Hour)
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	// Seeded at "local", the walk is local then other. The budget runs out
	// after local's first job, so the next run should start at "other".
	file := seedNextHost(t, f, "local")
	base := time.Now()
	f.s.Now = budgetClock(f, base, time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(file); err != nil || string(b) != "other\n" {
		t.Fatalf("apply budget stop left next-host state %q, %v; want %q", b, err, "other\n")
	}
}

func TestArchiveJobsApplyCrossingBudgetOnLastJobLeavesRotationReset(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	base := time.Now()
	f.s.Now = budgetClock(f, base, time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatal(err)
	}
	// The only job finished, so no work remains: the run must reset rotation
	// instead of recording a budget stop that restarts at this host.
	if _, err := os.Stat(filepath.Join(f.s.Config.StateDir, archiveNextHostFile)); !os.IsNotExist(err) {
		t.Fatalf("apply crossing max_run on the last job left next-host state (stat err %v)", err)
	}
}

func TestArchiveJobsCancelledRunWithSpentBudgetReturnsContextError(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	base := time.Now()
	f.s.Now = func() time.Time { return base.Add(time.Hour) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.run.calls = 0
	err := f.s.ArchiveJobs(ctx, "", 14*24*time.Hour, Options{Shadow: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run returned %v, want context.Canceled", err)
	}
	if f.run.calls != 0 {
		t.Fatalf("cancelled run issued %d commands", f.run.calls)
	}
	if !strings.Contains(f.out.String(), `"action":"archive-jobs"`) {
		t.Fatalf("cancelled run omitted summary row: %s", f.out.String())
	}
}

func TestArchiveJobsCancelledMidHostCountsInterruptedAndRemainingJobs(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	f.run.afterTreeListing = cancel
	err := f.s.ArchiveJobs(ctx, "", 14*24*time.Hour, Options{Shadow: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run returned %v, want context.Canceled", err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"eligible_left":2`) || !strings.Contains(out, `"unscanned_hosts":["local"]`) || !strings.Contains(out, `"unscanned_host_reasons":{"local":"caller cancelled during job scan"}`) {
		t.Fatalf("cancelled mid-host summary omitted eligible work or stop host: %s", out)
	}
}

func TestArchiveJobsCancelledDuringRefreshCountsInterruptedJobs(t *testing.T) {
	for _, tc := range []struct {
		name string
		jobs []string
	}{
		{name: "two jobs", jobs: []string{"first", "second"}},
		{name: "last job", jobs: []string{"first"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveFixture(t, noAgents)
			for _, job := range tc.jobs {
				jobDir(t, f.jobs, job, 20*24*time.Hour)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.s.Runner = archiveCancelAfterCommandRunner{base: f.run, match: "|| continue; printf \"%s %s %s\\0\"", cancel: cancel}
			err := f.s.ArchiveJobs(ctx, "", 14*24*time.Hour, Options{Shadow: true})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled run returned %v, want context.Canceled", err)
			}
			out := f.out.String()
			if !strings.Contains(out, `"eligible_left":`+strconv.Itoa(len(tc.jobs))) || !strings.Contains(out, `"unscanned_hosts":["local"]`) {
				t.Fatalf("refresh cancellation omitted pending work or stop host: %s", out)
			}
		})
	}
}

func TestArchiveJobsCancelledDuringPlanCountsInterruptedJobs(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.s.Runner = archiveCancelAfterCommandRunner{base: f.run, match: "cat " + filepath.Join(f.jobs, "first", "events", "00004-job.task_link.json"), cancel: cancel}
	err := f.s.ArchiveJobs(ctx, "", 14*24*time.Hour, Options{Shadow: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run returned %v, want context.Canceled", err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"eligible_left":2`) || !strings.Contains(out, `"unscanned_hosts":["local"]`) {
		t.Fatalf("plan cancellation omitted pending work or stop host: %s", out)
	}
}

func TestArchiveJobsCancelledDuringApplyRecordsRowAndCountsPending(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.s.Runner = archiveCancelAfterCommandRunner{base: f.run, match: `mv -f "$1.partial" "$1"`, cancel: cancel}
	err := f.s.ArchiveJobs(ctx, "", 14*24*time.Hour, Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run returned %v, want context.Canceled", err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"eligible_left":2`) || !strings.Contains(out, `"unscanned_hosts":["local"]`) {
		t.Fatalf("apply cancellation omitted pending work or stop host: %s", out)
	}
	log, readErr := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(log), `"action":"archive-job","host":"local","job":"first","result":"error","reason":"caller cancelled during archive apply:`) || !strings.Contains(string(log), `"manifest_sha256"`) {
		t.Fatalf("apply cancellation did not persist a cancelled partial archive row: %s", log)
	}
}

func TestArchiveJobsSummaryNamesUnscannedHostsAndEligibleScope(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	jobDir(t, otherJobs, "third", 20*24*time.Hour)
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	base := time.Now()
	f.s.Now = budgetClock(f, base, time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"unscanned_hosts":["local","other"]`) || !strings.Contains(out, `"eligible_left":1`) || !strings.Contains(out, `"eligible_left_scope":"`+archiveEligibleScope+`"`) {
		t.Fatalf("summary does not name unscanned hosts or scope eligible_left: %s", out)
	}
	if strings.Contains(out, `"job":"third"`) {
		t.Fatalf("unscanned host was scanned to count it: %s", out)
	}
}

func TestArchiveJobsSummaryScopeWhenNoHostIsReached(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	otherJobs := filepath.Join(filepath.Dir(f.jobs), "other-jobs")
	if err := os.MkdirAll(otherJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	f.s.Config.Hosts = append(f.s.Config.Hosts, reap.Host{Name: "other", Jobs: otherJobs})
	base := time.Now()
	calls := 0
	f.s.Now = func() time.Time {
		calls++
		if calls > 1 {
			return base.Add(2 * time.Second)
		}
		return base
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"stopped_on_budget":true`) || !strings.Contains(out, `"unscanned_hosts":["local","other"]`) || !strings.Contains(out, `"eligible_left":0`) || !strings.Contains(out, `"eligible_left_scope":"`+archiveEligibleScope+`"`) {
		t.Fatalf("summary has incorrect scope when no host was reached: %s", out)
	}
	if f.run.calls != 0 {
		t.Fatalf("budget stop before first host issued %d commands", f.run.calls)
	}
}

func TestArchiveJobsCompletedSummaryScope(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"stopped_on_budget":false`) || strings.Contains(out, `"unscanned_hosts"`) || !strings.Contains(out, `"eligible_left_scope":"`+archiveEligibleScope+`"`) {
		t.Fatalf("completed run summary has incorrect scope: %s", out)
	}
}

func TestArchiveJobsFailedBatchReadProbesAreBounded(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	bad := jobDir(t, f.jobs, "many-files", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(bad, "events")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		file := filepath.Join(bad, fmt.Sprintf("file-%03d.md", i))
		writeFile(t, file, "body")
		old := time.Now().Add(-20 * 24 * time.Hour)
		if err := os.Chtimes(file, old, old); err != nil {
			t.Fatal(err)
		}
	}
	f.run.failTarDir = bad
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err == nil {
		t.Fatal("generic batch read failure should remain an error")
	}
	// No bisection: one batch read, then one diagnosis command that finds no
	// unreadable file, so the batch error stays a run error.
	if f.run.tarProbes != 1 || f.run.probes != 1 {
		t.Fatalf("failed batch read issued %d tar and %d diagnosis commands, want 1 and 1", f.run.tarProbes, f.run.probes)
	}
}

func TestArchiveJobsRefreshesFinishedStateBeforePlanning(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "restarted", 20*24*time.Hour)
	f.run.afterTreeListing = func() {
		if err := os.Remove(filepath.Join(dir, "events", "00005-job.completed.json")); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, "events", "00006-job.placement.json"), `{}`)
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.out.String(), `"job":"restarted","result":"skipped","reason":"not-finished"`) {
		t.Fatalf("refreshed restart was not skipped: %s", f.out.String())
	}
	if f.run.script.called("git") {
		t.Fatal("refreshed unfinished job entered remaining planning")
	}
}

func TestArchiveJobsHostListingOutputDoesNotScaleWithJobFiles(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "many-files", time.Hour)
	for i := 0; i < 500; i++ {
		writeFile(t, filepath.Join(dir, fmt.Sprintf("file-%04d.txt", i)), "x")
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if f.run.treeBytes > 1024 {
		t.Fatalf("host listing summary bytes = %d, want <= 1024", f.run.treeBytes)
	}
	if !strings.Contains(f.out.String(), `"job":"many-files","result":"skipped","reason":"too-new"`) {
		t.Fatalf("missing too-new row: %s", f.out.String())
	}
}

func writeFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func jobDir(t *testing.T, jobs, name string, age time.Duration) string {
	t.Helper()
	dir := filepath.Join(jobs, name)
	writeFile(t, filepath.Join(dir, "brief.md"), "# brief\n")
	writeFile(t, filepath.Join(dir, "report.md"), "# report\n")
	writeFile(t, filepath.Join(dir, "events", "00004-job.task_link.json"), `{"payload":{"task_id":"77"}}`)
	writeFile(t, filepath.Join(dir, "events", "00005-job.completed.json"), `{}`)
	writeFile(t, filepath.Join(dir, ".env"), "SECRET=1\n")
	old := time.Now().Add(-age)
	_ = filepath.Walk(dir, func(p string, _ os.FileInfo, _ error) error { return os.Chtimes(p, old, old) })
	return dir
}

type archiveFixture struct {
	s     *Service
	run   *archiveRunner
	hk    *hktest.Server
	out   *bytes.Buffer
	jobs  string
	drive string
}

// hkCalls counts the requests the fake hk received.
func (f archiveFixture) hkCalls() int { return len(f.hk.Requests()) }

// comments returns the bodies of the hk comments posted so far.
func (f archiveFixture) comments() []string {
	var out []string
	for _, c := range f.hk.Comments() {
		out = append(out, c.Body)
	}
	return out
}

func newArchiveFixture(t *testing.T, agents string) archiveFixture {
	t.Helper()
	for _, tool := range []string{"bash", "tar", "find", "xargs"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool + " not available")
		}
	}
	root := t.TempDir()
	f := archiveFixture{jobs: filepath.Join(root, "jobs"), drive: filepath.Join(root, "drive"), out: &bytes.Buffer{}, hk: hktest.New(t)}
	f.hk.SetTask(77, `{"id":77,"state":"merged","refs":{}}`)
	panes := `{"result":{"panes":[]}}`
	f.run = &archiveRunner{script: &scriptRunner{outs: map[string]string{
		key("herdr", "agent", "list"): agents,
		key("herdr", "pane", "list"):  panes,
	}}}
	c := repoConfig(filepath.Join(root, "state"))
	c.Hosts = []reap.Host{{Name: "local", Jobs: f.jobs}}
	c.Reap.Archive = config.Archive{Enabled: true, DriveDir: f.drive, TextMaxBytes: 1 << 20, MinAge: "24h"}
	f.s = &Service{Config: c, GH: &reapGH{}, Runner: f.run, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: f.out}
	t.Cleanup(func() {
		if len(f.run.hkCLI) > 0 {
			t.Errorf("hk CLI subprocesses run: %q", f.run.hkCLI)
		}
		for _, r := range f.hk.Requests() {
			if r.Auth != "Bearer "+hktest.Token {
				t.Errorf("%s: Authorization %q", r.Route(), r.Auth)
			}
		}
	})
	return f
}

// withJobs points the fixture's hk sink at the given job states; a job dir
// with no entry reads as a missing record.
func (f archiveFixture) withJobs(t *testing.T, states map[string]string) {
	t.Helper()
	sink, ok := f.s.Sink.(*jobSink)
	if !ok {
		sink = &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
		f.s.Sink = sink
	}
	for job, state := range states {
		rec := sink.recs[job]
		rec.State = state
		sink.recs[job] = rec
	}
}

const noAgents = `{"result":{"agents":[]}}`

// seedArchive writes one archive-job row, standing in for a prior pass, so a
// test can age a sighting without waiting out min_age.
func (f archiveFixture) seedArchive(t *testing.T, job, result, reason string, at time.Time) {
	t.Helper()
	b, _ := json.Marshal(reapReceipt{Time: at, Action: "archive-job", Repo: "example/project", Host: "local", Job: job, Result: result, Reason: reason})
	p := f.s.reapLog("example/project")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(b, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func (f archiveFixture) rows(t *testing.T) []reapReceipt {
	t.Helper()
	all, err := f.s.reapReceipts("example/project")
	if err != nil {
		t.Fatal(err)
	}
	var out []reapReceipt
	for _, r := range all {
		if r.Action == "archive-job" {
			out = append(out, r)
		}
	}
	return out
}

func TestReapArchivesGoneJobOnceAndDeletesIt(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	// The first pass sights the ended job — pane gone, events finished —
	// and records the sighting without touching the dir.
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "skipped" || rows[0].Reason != "awaiting-min-age" {
		t.Fatalf("rows = %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir touched before min_age")
	}
	if f.hkCalls() != 0 {
		t.Fatal("sighting called hk")
	}
	// A repeat pass before min_age writes nothing new.
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != 1 {
		t.Fatalf("sighting rewritten: %+v", f.rows(t))
	}
	// Once the sighting ages out, the job archives and deletes.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows = f.rows(t)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v", rows)
	}
	got := rows[1]
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("job dir kept: %v", err)
	}
	if got.Result != "archived" || got.Archive == nil || !got.Archive.Deleted {
		t.Fatalf("rows = %+v", rows)
	}
	if ex := got.Archive.Excluded; len(ex) != 1 || ex[0] != ".env" {
		t.Fatalf("excluded = %v", ex)
	}
	if _, err := os.Stat(filepath.Join(got.Archive.Dest, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(got.Archive.Dest, ".env")); !os.IsNotExist(err) {
		t.Fatal("credential file reached drive")
	}
	if c := f.comments(); len(c) != 1 || !strings.Contains(c[0], "archive/77/wk-7") {
		t.Fatalf("hk comments = %q", c)
	}
	calls := f.hkCalls()
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.hkCalls() != calls || len(f.rows(t)) != 2 {
		t.Fatal("an archived job was processed again")
	}
}

func TestReapArchiveShadowOnlyPlans(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	f.seedArchive(t, "wk-7", "skipped", "awaiting-min-age", time.Now().Add(-48*time.Hour))
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("shadow reap touched the job dir")
	}
	if f.hkCalls() != 0 {
		t.Fatal("shadow reap called hk")
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[1].Result != "would-archive" {
		t.Fatalf("rows = %+v", rows)
	}
	// A second shadow pass plans nothing new: one shadow plan per job.
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != 2 || f.hkCalls() != 0 {
		t.Fatalf("shadow replanned: %+v", f.rows(t))
	}
	var plan map[string]any
	for _, line := range strings.Split(f.out.String(), "\n") {
		if strings.Contains(line, `"archive-plan"`) {
			_ = json.Unmarshal([]byte(line), &plan)
		}
	}
	if plan == nil || plan["delete"] != dir || !strings.Contains(f.out.String(), `"excluded":[".env"]`) {
		t.Fatalf("dry-run plan missing upload/excluded/delete listing:\n%s", f.out.String())
	}
}

func TestReapArchiveKeepsLiveActiveAndUnfinished(t *testing.T) {
	for name, tc := range map[string]struct {
		agents string
		active bool
		inDir  bool
		setup  func(t *testing.T, f archiveFixture)
		want   string
	}{
		"live-pane":   {`{"result":{"agents":[{"pane_id":"w2:p1","name":"wk-7","agent_status":"working","cwd":"/elsewhere"}]}}`, false, false, nil, "skipped|pane-live"},
		"pane-in-dir": {`{"result":{"agents":[{"pane_id":"w2:p1","name":"other","agent_status":"working","cwd":"/x"}]}}`, false, true, nil, "skipped|pane-live"},
		"job-active":  {noAgents, true, false, nil, "skipped|job-active"},
		"unfinished": {noAgents, false, false, func(t *testing.T, f archiveFixture) {
			if err := os.Remove(filepath.Join(f.jobs, "wk-7", "events", "00005-job.completed.json")); err != nil {
				t.Fatal(err)
			}
		}, "skipped|not-finished"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newArchiveFixture(t, tc.agents)
			dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
			f.withJobs(t, map[string]string{"wk-7": "completed"})
			if tc.inDir {
				f.run.script = &scriptRunner{outs: map[string]string{
					key("herdr", "agent", "list"): tc.agents,
					key("herdr", "pane", "list"):  `{"result":{"panes":[{"pane_id":"w2:p1","cwd":"` + dir + `"}]}}`,
				}}
			}
			if tc.setup != nil {
				tc.setup(t, f)
			}
			if tc.active {
				f.s.Sink = &jobSink{recs: map[string]integrations.JobRecord{"wk-7": {State: "spawned", Role: "worker"}}}
			}
			_ = f.s.Reap(context.Background(), testRepo, 0, Options{})
			if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
				t.Fatal("job dir lost")
			}
			rows := f.rows(t)
			got := ""
			if len(rows) > 0 {
				got = rows[len(rows)-1].Result + "|" + rows[len(rows)-1].Reason
			}
			if got != tc.want || f.hkCalls() != 0 {
				t.Fatalf("row = %q, hk calls = %d", got, f.hkCalls())
			}
		})
	}
}

// A job dir no hk record names waits as job-unknown, however finished its
// events look: without hk proof of a terminal job the dir is not archived.
func TestReapArchiveSkipsJobWithoutRecord(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "skipped" || rows[0].Reason != "job-unknown" {
		t.Fatalf("rows = %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir touched without hk proof")
	}
	// Once the sighting ages out the job still waits: the row is not rewritten.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.rows(t)) != 1 || f.hkCalls() != 0 {
		t.Fatalf("rows = %+v hk calls = %d", f.rows(t), f.hkCalls())
	}
}

func TestArchiveJobsBacklog(t *testing.T) {
	f := newArchiveFixture(t, `{"result":{"agents":[{"pane_id":"w2:p1","name":"live-1","agent_status":"working","cwd":"/x"}]}}`)
	old := jobDir(t, f.jobs, "old-1", 20*24*time.Hour)
	fresh := jobDir(t, f.jobs, "new-1", time.Hour)
	live := jobDir(t, f.jobs, "live-1", 20*24*time.Hour)
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if f.hkCalls() != 0 {
		t.Fatal("plan-only run called hk")
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old job kept")
	}
	for _, d := range []string{fresh, live} {
		if _, err := os.Stat(filepath.Join(d, "report.md")); err != nil {
			t.Fatalf("%s touched", d)
		}
	}
	log, _ := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	for _, want := range []string{`"job":"new-1","result":"skipped","reason":"too-new"`, `"job":"live-1","result":"skipped","reason":"pane-live"`, `"job":"old-1","result":"would-archive"`, `"job":"old-1","result":"archived"`} {
		if !strings.Contains(string(log), want) {
			t.Errorf("archive-jobs.jsonl lacks %s:\n%s", want, log)
		}
	}
	f.s.Config.Reap.Archive.Enabled = false
	if err := f.s.ArchiveJobs(context.Background(), "", time.Hour, Options{}); err == nil {
		t.Fatal("archive-jobs ran with archive disabled")
	}
}

func TestArchiveJobsRecordsUnsafeCheckoutAsRetained(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "unsafe-checkout", 20*24*time.Hour)
	if err := os.RemoveAll(filepath.Join(dir, "events")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "wt", ".git"), "gitdir: missing\n")
	old := time.Now().Add(-20 * 24 * time.Hour)
	_ = filepath.Walk(dir, func(p string, _ os.FileInfo, _ error) error { return os.Chtimes(p, old, old) })
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("unsafe source was removed: %v", err)
	}
	log, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(log), `"result":"archived-retained"`) || !strings.Contains(string(log), `retained: checkout-unsafe wt git-error`) {
		t.Fatalf("retention row missing: %s", log)
	}
}

func TestArchiveExcludesAndFinishedJobs(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.Exclude = []string{"pw-rollout-*", "pw-hk*", "events-lane"}
	for _, name := range []string{"pw-rollout-x", "pw-hk1", "events-lane", "done-job", "open-job"} {
		jobDir(t, f.jobs, name, 20*24*time.Hour)
	}
	writeFile(t, filepath.Join(f.jobs, "done-job", "events", "00005-job.completed.json"), `{}`)
	if err := os.Remove(filepath.Join(f.jobs, "open-job", "events", "00005-job.completed.json")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(f.jobs, "open-job", "events", "00005-job.placement.json"), `{}`)
	f.withJobs(t, map[string]string{"done-job": "completed", "open-job": "completed"})
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	got := map[string]string{}
	for _, row := range rows {
		if row.Action == "archive-job" {
			got[row.Job] = row.Result + "|" + row.Reason
		}
	}
	if got["pw-hk1"] != "skipped|excluded-name" || got["done-job"] != "skipped|awaiting-min-age" || got["open-job"] != "skipped|not-finished" {
		t.Fatalf("reap archive outcomes = %v", got)
	}
	// Once the sighting ages out, the finished job archives; the unfinished
	// one keeps its row without writing a new one.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows = f.rows(t)
	got = map[string]string{}
	for _, row := range rows {
		if row.Action == "archive-job" {
			got[row.Job] = row.Result + "|" + row.Reason
		}
	}
	if got["done-job"] != "archived|" || got["open-job"] != "skipped|not-finished" {
		t.Fatalf("reap archive outcomes after min_age = %v", got)
	}
	if _, err := os.Stat(filepath.Join(f.jobs, "done-job")); !os.IsNotExist(err) {
		t.Fatal("finished job not archived after its own min_age")
	}
}

func TestArchiveJobsSkipsExcludedAndUnfinished(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.Exclude = config.DefaultArchiveExclude
	for _, name := range []string{"pw-rollout-x", "pw-hk1", "events-lane", "unfinished"} {
		jobDir(t, f.jobs, name, 20*24*time.Hour)
	}
	if err := os.Remove(filepath.Join(f.jobs, "unfinished", "events", "00005-job.completed.json")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(f.jobs, "unfinished", "events", "00005-job.placement.json"), `{}`)
	if err := f.s.ArchiveJobs(context.Background(), "", time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(filepath.Join(f.s.Config.StateDir, "archive-jobs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pw-rollout-x", "pw-hk1", "events-lane"} {
		if !strings.Contains(string(log), `"job":"`+name+`","result":"skipped","reason":"excluded-name"`) {
			t.Errorf("missing exclude row for %s: %s", name, log)
		}
	}
	if !strings.Contains(string(log), `"job":"unfinished","result":"skipped","reason":"not-finished"`) {
		t.Errorf("missing not-finished row: %s", log)
	}
}

// A reused job name restarts every clock: an earlier job's archive row is
// final for that job only, and a new dir under the same name is sighted and
// aged on its own.
func TestReapArchiveReusedNameArchivesAgain(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "wk-7", 30*24*time.Hour)
	f.seedArchive(t, "wk-7", "archived", "", time.Now().Add(-29*24*time.Hour))
	// The new incarnation's files are newer than the earlier archive row.
	dir := jobDir(t, f.jobs, "wk-7", time.Hour)
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[1].Result != "skipped" || rows[1].Reason != "awaiting-min-age" {
		t.Fatalf("rows = %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil || f.hkCalls() != 0 {
		t.Fatalf("job dir archived before min_age of its own sighting: %v, hk=%d", err, f.hkCalls())
	}
	// Once the new sighting ages out, the earlier job's row does not block it.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("reused job not archived after its own min_age")
	}
}

// A new incarnation after a non-terminal sighting gets its own sighting: the
// old awaiting row does not release the new dir on its own clock.
func TestReapArchiveReusedNameRestartsStaleSighting(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	f.seedArchive(t, "wk-7", "skipped", "awaiting-min-age", time.Now().Add(-48*time.Hour))
	// The job runs again under the same name after that sighting: its files
	// are newer than the old row.
	dir := jobDir(t, f.jobs, "wk-7", time.Hour)
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[1].Result != "skipped" || rows[1].Reason != "awaiting-min-age" {
		t.Fatalf("rows = %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("reused job archived on the old sighting")
	}
	// Once the new sighting ages out, the new incarnation archives.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("reused job not archived after its own min_age")
	}
}

func TestReapArchiveErrorCountedOnce(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.hk.Fail("POST /v1/tasks/{id}/comments", 500, "internal")
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	f.seedArchive(t, "wk-7", "skipped", "awaiting-min-age", time.Now().Add(-48*time.Hour))
	err := f.s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "hk comment") {
		t.Fatalf("err = %v", err)
	}
	if n := summaryCount(t, f.out, "errors"); n != 1 {
		t.Fatalf("summary counts %d errors, want 1:\n%s", n, f.out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir lost")
	}
	if rows := f.rows(t); len(rows) != 2 || rows[1].Result != "error" {
		t.Fatalf("rows = %+v", rows)
	}
	// The retry waits out the hour: no new row and no new error.
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatalf("throttled retry failed: %v", err)
	}
	if len(f.rows(t)) != 2 {
		t.Fatalf("throttled retry wrote a row: %+v", f.rows(t))
	}
	// Past the hour the retry attempts the archive again at once, instead of
	// starting a fresh min_age sighting.
	f.s.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err == nil {
		t.Fatal("unthrottled retry did not fail")
	}
	if rows := f.rows(t); len(rows) != 3 || rows[2].Result != "error" {
		t.Fatalf("retry did not re-attempt the archive: %+v", rows)
	}
}

// A retained archive is re-attempted after the hour, and each attempt is
// recorded: the retry row is appended even though the outcome repeats.
func TestReapArchiveRetainedRetryAppendsRow(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	if err := os.RemoveAll(filepath.Join(dir, "events")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "wt", ".git"), "gitdir: missing\n")
	old := time.Now().Add(-48 * time.Hour)
	_ = filepath.Walk(dir, func(p string, _ os.FileInfo, _ error) error { return os.Chtimes(p, old, old) })
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Reason != "awaiting-min-age" {
		t.Fatalf("rows = %+v", rows)
	}
	// Once the sighting ages out the archive runs and is retained.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(rows) != 2 || rows[1].Result != "archived-retained" {
		t.Fatalf("rows = %+v", rows)
	}
	// Past the retry hour the attempt is recorded again, not deduped away.
	f.s.Now = func() time.Time { return time.Now().Add(50 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(rows) != 3 || rows[2].Result != "archived-retained" {
		t.Fatalf("retry did not re-attempt the archive: %+v", rows)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal("retained source removed")
	}
}

// An hk read failure is reported but never counts as an archive attempt: once
// the hour passes the job is sighted fresh instead of archived sight-unseen.
func TestReapArchiveHkErrorLeadsToSighting(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.s.Sink = failingJobSink{&jobSink{}}
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err == nil {
		t.Fatal("unreadable hk record wrote no error")
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Result != "error" {
		t.Fatalf("rows = %+v", rows)
	}
	// Once hk answers again, the stale lookup error must not trigger a
	// direct archive: the job is sighted fresh instead.
	f.s.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	f.s.Sink = &jobSink{recs: map[string]integrations.JobRecord{"wk-7": {State: "completed"}}}
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(rows) != 2 || rows[1].Result != "skipped" || rows[1].Reason != "awaiting-min-age" {
		t.Fatalf("recovered hk did not sight fresh: %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir archived without a sighting")
	}
	// Once the fresh sighting ages out, the ended job archives.
	f.s.Now = func() time.Time { return time.Now().Add(50 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("ended job not archived after hk recovered")
	}
}

// An uncertain host listing is refreshed before any decision: without a
// trustworthy mtime the job is not archived, and the refresh problem is
// recorded instead of a sighting.
func TestReapArchiveUncertainListingRefreshesBeforeDeciding(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read mode-000 directories")
	}
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	if err := os.Chmod(filepath.Join(dir, "events"), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "events"), 0o755) })
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	err := f.s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "Permission denied") {
		t.Fatalf("unreadable listing wrote no error: %v", err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "error" {
		t.Fatalf("uncertain listing not refreshed into an error: %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir touched")
	}
}

// A failed listing refresh never counts as an archive attempt: once the
// listing reads cleanly the job gets its first sighting, not a direct
// archive, even past the retry hour.
func TestReapArchiveRefreshErrorSightsBeforeArchiving(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read mode-000 directories")
	}
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	if err := os.Chmod(filepath.Join(dir, "events"), 0); err != nil {
		t.Fatal(err)
	}
	f.withJobs(t, map[string]string{"wk-7": "completed"})
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err == nil {
		t.Fatal("unreadable listing wrote no error")
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "error" || !strings.HasPrefix(rows[0].Reason, "refresh: ") {
		t.Fatalf("refresh failure not marked: %+v", rows)
	}
	// The listing reads cleanly now, past the retry hour: the job still
	// gets its first sighting instead of a direct archive.
	if err := os.Chmod(filepath.Join(dir, "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.s.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	rows = f.rows(t)
	if len(rows) != 2 || rows[1].Result != "skipped" || rows[1].Reason != "awaiting-min-age" {
		t.Fatalf("refresh error archived without a sighting: %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir archived without a sighting")
	}
}

// Under reap.transport panewire a remote host is reached through the hub, so
// archive, which moves files over ssh, must leave it alone even when it also
// has an ssh destination.
func TestArchiveJobsSkipsHubHost(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "old-1", 20*24*time.Hour)
	f.s.Config.Reap.Transport = config.ReapTransportPanewire
	f.s.Config.Hosts[0].SSH, f.s.Config.Hosts[0].Machine = "ops@node.example", "node-a"
	err := f.s.ArchiveJobs(context.Background(), "", time.Hour, Options{})
	if err == nil || !strings.Contains(err.Error(), "panewire") {
		t.Fatalf("hub host archived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir touched")
	}
}

// archiveFailRunner fails every command of one executable with err while fail
// reports true; other commands run on the wrapped runner.
type archiveFailRunner struct {
	base integrations.Runner
	name string
	err  error
	fail func() bool
}

func (r archiveFailRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if name == r.name && r.fail() {
		return nil, r.err
	}
	return r.base.Run(ctx, name, args, input, dir)
}

func TestArchiveJobsNonDeadlineErrorAfterMaxRunFailsRun(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "old-job", 20*24*time.Hour)
	base := time.Now()
	// The wall clock reads as spent from the first command on, while the run's
	// own timer never fires: the listing failure is a real error, not a stop.
	f.s.Now = func() time.Time {
		if f.run.calls > 0 {
			return base.Add(time.Hour)
		}
		return base
	}
	f.s.Runner = archiveFailRunner{base: f.run, name: "sh", err: errors.New("permission denied"), fail: func() bool { return f.run.calls > 0 }}
	err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true})
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("listing failure after max_run was not returned as a run error: %v", err)
	}
	if out := f.out.String(); strings.Contains(out, `"stopped_on_budget":true`) {
		t.Fatalf("real failure was classified as a budget stop: %s", out)
	}
}

func TestArchiveJobsBudgetGateBetweenJobsNamesCurrentHost(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "a-job", 20*24*time.Hour)
	jobDir(t, f.jobs, "b-job", 20*24*time.Hour)
	base := time.Now()
	// The wall clock passes max_run after the first job's row; the run's own
	// timer has not fired, so the between-jobs gate is what stops the run.
	f.s.Now = func() time.Time {
		if strings.Contains(f.out.String(), `"action":"archive-job"`) {
			return base.Add(time.Hour)
		}
		return base
	}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatalf("budget stop failed the run: %v", err)
	}
	out := f.out.String()
	if !strings.Contains(out, `"stopped_on_budget":true`) || !strings.Contains(out, `"unscanned_hosts":["local"]`) || !strings.Contains(out, `"max_run deadline during job scan"`) {
		t.Fatalf("summary omits the host stopped between jobs: %s", out)
	}
	if !strings.Contains(out, `"eligible_left":1`) {
		t.Fatalf("between-jobs stop did not count remaining listed eligible job: %s", out)
	}
}

func TestArchiveJobsDeadlineDuringJobReadsCountsInterruptedAndRemainingJobs(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "500ms"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	f.s.Runner = archiveBlockRunner{base: f.run, match: "|| continue; printf \"%s %s %s\\0\""}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if out := f.out.String(); !strings.Contains(out, `"max_run deadline during job reads"`) || !strings.Contains(out, `"eligible_left":2`) {
		t.Fatalf("job-read deadline did not count interrupted and remaining jobs: %s", out)
	}
}

func TestArchiveJobsDeadlineDuringHKReadsCountsInterruptedAndRemainingJobs(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "500ms"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	f.s.Runner = archiveBlockRunner{base: f.run, match: "cat " + filepath.Join(f.jobs, "first", "events", "00004-job.task_link.json")}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if out := f.out.String(); !strings.Contains(out, `"max_run deadline during hk reads"`) || !strings.Contains(out, `"eligible_left":2`) {
		t.Fatalf("hk-read deadline did not count interrupted and remaining jobs: %s", out)
	}
}

func TestArchiveJobsDeadlineBeforeApplyCountsInterruptedAndRemainingJobs(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.s.Config.Reap.Archive.MaxRun = "2s"
	jobDir(t, f.jobs, "first", 20*24*time.Hour)
	jobDir(t, f.jobs, "second", 20*24*time.Hour)
	base := time.Now()
	f.s.Now = func() time.Time { return base }
	f.s.Runner = archiveAfterCommandRunner{base: f.run, match: "cat " + filepath.Join(f.jobs, "first", "events", "00004-job.task_link.json"), after: func() {
		f.s.Now = func() time.Time { return base.Add(time.Hour) }
	}}
	if err := f.s.ArchiveJobs(context.Background(), "", 14*24*time.Hour, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if out := f.out.String(); !strings.Contains(out, `"max_run deadline before archive apply"`) || !strings.Contains(out, `"eligible_left":2`) {
		t.Fatalf("pre-apply deadline did not count interrupted and remaining jobs: %s", out)
	}
}
