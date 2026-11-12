package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
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
	script              *scriptRunner
	hkCLI               [][]string
	calls               int
	vanishDuringRefresh string
	afterTreeListing    func()
	treeBytes           int
}

func (a *archiveRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	a.calls++
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
		return nil, errors.New(name + ": " + err.Error() + ": " + stderr.String())
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
