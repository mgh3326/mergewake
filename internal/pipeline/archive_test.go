package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/hktest"
	"github.com/mgh3326/mergewake/internal/reap"
)

// archiveRunner scripts herdr and git and runs everything else (sh, bash,
// tar, cat, rm) for real against temp dirs. hk is reached over HTTP
// (hktest); an hk CLI call is recorded and refused.
type archiveRunner struct {
	script *scriptRunner
	hkCLI  [][]string
}

func (a *archiveRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if name == "handoffkeep" || len(args) > 0 && (args[0] == "attach" || args[0] == "tasks" || args[0] == "doc") {
		a.hkCLI = append(a.hkCLI, append([]string{name}, args...))
		return nil, errors.New("hk CLI subprocess refused: " + strings.Join(args, " "))
	}
	switch name {
	case "herdr", "git":
		return a.script.Run(ctx, name, args, input, dir)
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

const noAgents = `{"result":{"agents":[]}}`

func (f archiveFixture) seedClose(t *testing.T, job, result string, at time.Time) {
	t.Helper()
	b, _ := json.Marshal(reapReceipt{Time: at, Action: "close-pane", Repo: "example/project", PR: 12, Host: "local",
		Pane: "w1:p1", Cwd: "/repo.wt", Worktree: "/repo.wt", Branch: "wk/x", Saved: filepath.Join(f.jobs, job, "report.md"), Job: job, Result: result})
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
	if c := f.comments(); len(c) != 1 || !strings.Contains(c[0], "archive/77/wk-7") {
		t.Fatalf("hk comments = %q", c)
	}
	calls := f.hkCalls()
	if err := f.s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.hkCalls() != calls || len(f.rows(t)) != 1 {
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
	if f.hkCalls() != 0 {
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
			if got != tc.want || f.hkCalls() != 0 {
				t.Fatalf("row = %q, hk calls = %d", got, f.hkCalls())
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
	// The reap-stage path must apply the same name and event checks.
	f.seedClose(t, "pw-hk1", "closed", time.Now().Add(-48*time.Hour))
	f.seedClose(t, "done-job", "closed", time.Now().Add(-48*time.Hour))
	f.seedClose(t, "open-job", "closed", time.Now().Add(-48*time.Hour))
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
	if got["pw-hk1"] != "skipped|excluded-name" || got["done-job"] != "archived|" || got["open-job"] != "skipped|not-finished" {
		t.Fatalf("reap archive outcomes = %v", got)
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
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil || f.hkCalls() != 0 {
		t.Fatalf("job dir archived before min_age of its newest close: %v, hk=%d", err, f.hkCalls())
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

// One failed archive is one error in the reap summary, kept for retry.
func TestReapArchiveErrorCountedOnce(t *testing.T) {
	f := newArchiveFixture(t, noAgents)
	f.hk.Fail("POST /v1/tasks/{id}/comments", 500, "internal")
	dir := jobDir(t, f.jobs, "wk-7", 48*time.Hour)
	f.seedClose(t, "wk-7", "closed", time.Now().Add(-48*time.Hour))
	err := f.s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "hk comment") {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(f.out.String(), `"action":"reap"`) || !strings.Contains(f.out.String(), `"errors":1}`) {
		t.Fatalf("summary does not count the archive error once:\n%s", f.out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatal("job dir lost")
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Result != "error" {
		t.Fatalf("rows = %+v", rows)
	}
}

// Every error row is counted once in the reap summary, archive or not.
func TestReapSummaryCountsEveryErrorRow(t *testing.T) {
	r := &scriptRunner{outs: worktreeOuts("/repo"), errs: map[string]error{
		key("herdr", "pane", "close", "w1:p1"): errors.New("close failed"),
	}}
	s := svc(t, &reapGH{}, r)
	var out bytes.Buffer
	s.Out = &out
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "close-pane pane w1:p1: close failed") {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(out.String(), `"errors":1}`) {
		t.Fatalf("summary does not count the error row once:\n%s", out.String())
	}
}

func TestJobFromSaved(t *testing.T) {
	for _, tc := range []struct{ jobs, saved, want string }{
		{"/jobs", "/jobs/wk-7/report.md", "wk-7"},
		{"/jobs", "/jobs/wk-7/pane-screen.txt", "wk-7"},
		{"~/jobs", "~/jobs/wk-7/report.md", "wk-7"},
		{"~/jobs", "/home/ops/jobs/wk-7/report.md", "wk-7"},
		{"~/jobs", "/home/ops/other/wk-7/report.md", ""},
		{"~/jobs", "/home/ops/myjobs/wk-7/report.md", ""},
		{"/jobs", "/jobs/a/wk-7/report.md", ""},
		{"/jobs", "/jobs/.hidden/report.md", ""},
		{"", "/jobs/wk-7/report.md", ""},
	} {
		hs := &reapHost{host: reap.Host{Name: "h", Jobs: tc.jobs}}
		if got := jobFromSaved(hs, tc.saved); got != tc.want {
			t.Errorf("jobFromSaved(%q, %q) = %q, want %q", tc.jobs, tc.saved, got, tc.want)
		}
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
