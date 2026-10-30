package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/reap"
)

// archiveRunner scripts herdr and git, answers handoffkeep in-process and runs
// everything else (sh, bash, tar, cat, rm) for real against temp dirs.
type archiveRunner struct {
	script   *scriptRunner
	hkCalls  int
	comments []string
}

func (a *archiveRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	switch name {
	case "herdr", "git":
		return a.script.Run(ctx, name, args, input, dir)
	case "handoffkeep":
		a.hkCalls++
		if len(args) == 5 && args[0] == "attach" && args[1] == "put" {
			b, err := os.ReadFile(args[2])
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(b)
			return json.Marshal(map[string]string{"sha256": hex.EncodeToString(sum[:])})
		}
		if len(args) == 5 && args[0] == "tasks" && args[1] == "comment" {
			a.comments = append(a.comments, input)
			return []byte("{}"), nil
		}
		return nil, errors.New("unexpected handoffkeep " + strings.Join(args, " "))
	}
	c := exec.CommandContext(ctx, name, args...)
	c.Stdin = strings.NewReader(input)
	var out, stderr bytes.Buffer
	c.Stdout, c.Stderr = &out, &stderr
	if err := c.Run(); err != nil {
		return nil, errors.New(name + ": " + err.Error() + ": " + stderr.String())
	}
	return out.Bytes(), nil
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
	writeFile(t, filepath.Join(dir, ".env"), "SECRET=1\n")
	old := time.Now().Add(-age)
	_ = filepath.Walk(dir, func(p string, _ os.FileInfo, _ error) error { return os.Chtimes(p, old, old) })
	return dir
}

type archiveFixture struct {
	s     *Service
	run   *archiveRunner
	out   *bytes.Buffer
	jobs  string
	drive string
}

func newArchiveFixture(t *testing.T, agents string) archiveFixture {
	t.Helper()
	for _, tool := range []string{"bash", "tar", "find", "xargs"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool + " not available")
		}
	}
	root := t.TempDir()
	f := archiveFixture{jobs: filepath.Join(root, "jobs"), drive: filepath.Join(root, "drive"), out: &bytes.Buffer{}}
	panes := `{"result":{"panes":[]}}`
	f.run = &archiveRunner{script: &scriptRunner{outs: map[string]string{
		key("herdr", "agent", "list"): agents,
		key("herdr", "pane", "list"):  panes,
	}}}
	c := repoConfig(filepath.Join(root, "state"))
	c.Hosts = []reap.Host{{Name: "local", Jobs: f.jobs}}
	c.Reap.Archive = config.Archive{Enabled: true, DriveDir: f.drive, TextMaxBytes: 1 << 20, MinAge: "24h"}
	f.s = &Service{Config: c, GH: &reapGH{}, Runner: f.run, Sink: &fakeSink{}, Notifier: &fakeNotifier{}, Out: f.out}
	return f
}

const noAgents = `{"result":{"agents":[]}}`

func (f archiveFixture) seedClose(t *testing.T, job, result string, at time.Time) {
	t.Helper()
	b, _ := json.Marshal(reapReceipt{Time: at, Action: "close-pane", Repo: "example/project", PR: 12, Host: "local",
		Pane: "w1:p1", Cwd: "/repo.wt", Worktree: "/repo.wt", Branch: "wk/x", Saved: filepath.Join(f.jobs, job, "report.md"), Job: job, Result: result})
	writeFile(t, f.s.reapLog("example/project"), string(b)+"\n")
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

func TestReapArchivesClosedJobOnceAndDeletesIt(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.seedClose(t, "wk-7", "closed", time.Now().Add(-48*time.Hour))
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("job dir kept: %v", err)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "archived" || rows[0].Archive == nil || !rows[0].Archive.Deleted {
		t.Fatalf("rows = %+v", rows)
	}
	if got := rows[0].Archive.Excluded; len(got) != 1 || got[0] != ".env" {
		t.Fatalf("excluded = %v", got)
	}
	if _, err := os.Stat(filepath.Join(rows[0].Archive.Dest, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(rows[0].Archive.Dest, ".env")); !os.IsNotExist(err) {
		t.Fatal("credential file reached drive")
	}
	if len(f.run.comments) != 1 || !strings.Contains(f.run.comments[0], ".env") {
		t.Fatalf("hk comments = %q", f.run.comments)
	}
	calls := f.run.hkCalls
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.run.hkCalls != calls || len(f.rows(t)) != 1 {
		t.Fatal("an archived job was processed again")
	}
}

func TestReapArchiveShadowOnlyPlans(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.seedClose(t, "wk-7", "closed", time.Now().Add(-48*time.Hour))
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("shadow reap touched the job dir")
	}
	if f.run.hkCalls != 0 {
		t.Fatal("shadow reap called hk")
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Result != "would-archive" {
		t.Fatalf("rows = %+v", rows)
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

func TestReapArchiveWaitsAndKeeps(t *testing.T) {
	for name, tc := range map[string]struct {
		agents string
		result string
		closed time.Duration
		want   string
	}{
		"min-age":      {noAgents, "closed", time.Hour, ""},
		"never-closed": {noAgents, "would-close", 48 * time.Hour, ""},
		"live-pane":    {`{"result":{"agents":[{"pane_id":"w2:p1","name":"wk-7","agent_status":"working","cwd":"/elsewhere"}]}}`, "closed", 48 * time.Hour, "skipped|pane-live"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newArchiveFixture(t, tc.agents)
			f.run.script.fn = func(string) (string, bool) { return "", false }
			dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
			f.seedClose(t, "wk-7", tc.result, time.Now().Add(-tc.closed))
			_ = f.s.Reap(context.Background(), testRepo, 0, Options{})
			if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
				t.Fatal("job dir lost")
			}
			rows := f.rows(t)
			got := ""
			if len(rows) > 0 {
				got = rows[0].Result + "|" + rows[0].Reason
			}
			if got != tc.want || f.run.hkCalls != 0 {
				t.Fatalf("row = %q, hk calls = %d", got, f.run.hkCalls)
			}
		})
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
	if f.run.hkCalls != 0 {
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

// A reused job name closed again restarts min_age: the stale first close and
// the earlier job's archive row must not release the new job dir.
func TestReapArchiveUsesNewestClose(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	dir := jobDir(t, f.jobs, "wk-7", time.Hour)
	f.seedClose(t, "wk-7", "closed", time.Now().Add(-30*24*time.Hour))
	old, _ := json.Marshal(reapReceipt{Time: time.Now().Add(-29 * 24 * time.Hour), Action: "archive-job", Repo: "example/project", PR: 12, Host: "local", Job: "wk-7", Result: "archived"})
	recent, _ := json.Marshal(reapReceipt{Time: time.Now().Add(-time.Hour), Action: "close-pane", Repo: "example/project", PR: 12, Host: "local", Pane: "w1:p9", Job: "wk-7", Result: "closed"})
	b, _ := os.ReadFile(f.s.reapLog("example/project"))
	writeFile(t, f.s.reapLog("example/project"), string(b)+string(old)+"\n"+string(recent)+"\n")
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil || f.run.hkCalls != 0 {
		t.Fatalf("job dir archived before min_age of its newest close: %v, hk=%d", err, f.run.hkCalls)
	}
	// Once the newest close ages out, the earlier job's row does not block it.
	f.s.Now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("reused job not archived after its own min_age")
	}
}

func TestArchiveJobsReportsUnreadableJobsDir(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	err := f.s.ArchiveJobs(context.Background(), "", time.Hour, Options{Shadow: true})
	if err == nil || !strings.Contains(err.Error(), "list jobs") {
		t.Fatalf("missing jobs dir reported as %v", err)
	}
}
