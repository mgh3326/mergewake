package archive

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
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/hktest"
	"github.com/mgh3326/mergewake/internal/reap"
)

// fakeRunner executes real commands against temp dirs; ssh resolves to the
// fake script on PATH. hk is reached over HTTP (hktest), never as a
// subprocess: an hk CLI call is recorded and refused.
type fakeRunner struct {
	calls  int
	failRm bool
	rms    [][]string // delete argv, recorded
	hkCLI  [][]string // hk CLI calls, which must not happen
}

type mutateBeforeSnapshotWrite struct {
	reap.Runner
	source      string
	replacement []byte
	mutated     bool
}

func (r *mutateBeforeSnapshotWrite) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if !r.mutated && name == "sh" && len(args) > 1 && strings.Contains(args[1], `cat > "$1.partial"`) {
		if err := os.WriteFile(r.source, r.replacement, 0o644); err != nil {
			return nil, err
		}
		r.mutated = true
	}
	return r.Runner.Run(ctx, name, args, input, dir)
}

func (f *fakeRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	f.calls++
	if name == "handoffkeep" || len(args) > 0 && (args[0] == "attach" || args[0] == "tasks" || args[0] == "doc") {
		f.hkCLI = append(f.hkCLI, append([]string{name}, args...))
		return nil, errors.New("hk CLI subprocess refused: " + strings.Join(args, " "))
	}
	if name == "sh" && len(args) > 1 && strings.HasPrefix(args[1], "rm ") {
		f.rms = append(f.rms, args)
		if f.failRm {
			return nil, errors.New("rm failed")
		}
	}
	c := exec.CommandContext(ctx, name, args...)
	c.Stdin = strings.NewReader(input)
	c.Dir = dir
	var out, stderr bytes.Buffer
	c.Stdout, c.Stderr = &out, &stderr
	if err := c.Run(); err != nil {
		return nil, errors.New(name + ": " + err.Error() + ": " + stderr.String())
	}
	return out.Bytes(), nil
}

// fakeSSH drops ssh options and the destination, logs the destination and
// runs the remote command line locally, so "remote" hosts share the test FS.
const fakeSSH = `#!/bin/sh
while [ $# -gt 0 ]; do case "$1" in -o) shift 2;; -*) shift;; *) break;; esac; done
echo "$1" >> "$FAKE_SSH_LOG"
shift
exec sh -c "$*"
`

type env struct {
	jobs, drive, sshLog string
	run                 *fakeRunner
	hk                  *hktest.Server
}

func setup(t *testing.T) env {
	t.Helper()
	for _, tool := range []string{"bash", "tar", "find", "xargs"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool + " not available")
		}
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(fakeSSH), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	e := env{jobs: filepath.Join(root, "jobs"), drive: filepath.Join(root, "drive"), sshLog: filepath.Join(root, "ssh.log"),
		run: &fakeRunner{}, hk: hktest.New(t)}
	for _, id := range []int64{2402, 9, 11} {
		e.hk.SetTask(id, fmt.Sprintf(`{"id":%d,"state":"merged","refs":{}}`, id))
	}
	t.Setenv("FAKE_SSH_LOG", e.sshLog)
	t.Cleanup(func() {
		if len(e.run.hkCLI) > 0 {
			t.Errorf("hk CLI subprocesses run: %q", e.run.hkCLI)
		}
		for _, r := range e.hk.Requests() {
			if r.Auth != "Bearer "+hktest.Token {
				t.Errorf("%s: Authorization %q", r.Route(), r.Auth)
			}
		}
	})
	return e
}

// uploads maps each uploaded attachment's name to its bytes.
func (e env) uploads() map[string][]byte {
	out := map[string][]byte{}
	for _, a := range e.hk.Attachments() {
		out[a.Name] = a.Body
	}
	return out
}

// comment returns the hk comment bodies posted to task, joined.
func (e env) comment(task int64) string {
	var b strings.Builder
	for _, c := range e.hk.Comments() {
		if c.Task == task {
			b.WriteString(c.Body)
		}
	}
	return b.String()
}

// routes counts the hk requests by method and escaped path.
func (e env) routes() map[string]int {
	out := map[string]int{}
	for _, r := range e.hk.Requests() {
		out[r.Route()]++
	}
	return out
}

func write(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// job lays out a panewire-shaped job dir with credential-like files mixed in.
func job(t *testing.T, jobs, name string) string {
	t.Helper()
	dir := filepath.Join(jobs, name)
	write(t, filepath.Join(dir, "brief.md"), "# brief\n")
	write(t, filepath.Join(dir, "report.md"), "# report\n")
	write(t, filepath.Join(dir, "events", "00001-job.claim.json"), `{"payload":{"task_id":11}}`)
	write(t, filepath.Join(dir, "events", "00004-job.task_link.json"), `{"kind":"job.task_link","payload":{"task_id":"2402"}}`)
	write(t, filepath.Join(dir, "events", "00005-job.completed.json"), `{}`)
	write(t, filepath.Join(dir, "build", "game.bin"), strings.Repeat("x", 4096))
	write(t, filepath.Join(dir, "notes with space.md"), "spaced\n")
	write(t, filepath.Join(dir, ".env.local"), "SECRET=1\n")
	write(t, filepath.Join(dir, "gh_token.txt"), "t\n")
	write(t, filepath.Join(dir, "certs", "server.pem"), "pem\n")
	write(t, filepath.Join(dir, "API_KEY.json"), `{"k":1}`)
	write(t, filepath.Join(dir, "keys", "notes.md"), "below a key dir\n")
	if err := os.Symlink("/etc/hosts", filepath.Join(dir, "link.md")); err != nil {
		t.Fatal(err)
	}
	return dir
}

func finishJob(t *testing.T, dir string) {
	t.Helper()
	write(t, filepath.Join(dir, "events", "00005-job.completed.json"), `{}`)
}

func archiver(e env, ssh string) *Archiver {
	return &Archiver{Runner: e.run, DriveDir: e.drive, SSH: ssh, TextMax: 1024}
}

func TestPlanDriveMaxFileBytesMovesSmallAndListsLarge(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-oversized")
	write(t, filepath.Join(dir, "small.bin"), strings.Repeat("s", 50))
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
	write(t, filepath.Join(dir, "report.md"), strings.Repeat("r", 150))
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	a.DriveMaxFileBytes = 100
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-oversized")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.Move, ",") != "events/00001-job.task_link.json,events/00005-job.completed.json,small.bin" || p.Files != 3 || p.Bytes != 80 {
		t.Fatalf("moves = %v, files=%d bytes=%d", p.Move, p.Files, p.Bytes)
	}
	if len(p.Oversized) != 2 || p.Oversized[0].Path != "large.bin" || p.Oversized[0].Size != 150 || p.Oversized[1].Path != "report.md" || p.Oversized[1].Size != 150 || p.OversizedBytes != 300 {
		t.Fatalf("oversized = %+v, bytes=%d", p.Oversized, p.OversizedBytes)
	}
	if strings.Contains(strings.Join(p.Text, ","), "report.md") {
		t.Fatalf("oversized text file included in hk list: %v", p.Text)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var printed map[string]any
	if err := json.Unmarshal(b, &printed); err != nil {
		t.Fatal(err)
	}
	if printed["oversized_bytes"] != float64(300) {
		t.Fatalf("plan JSON oversized_bytes = %v", printed["oversized_bytes"])
	}
}

func TestArchiveTaskLayoutSkipsGitCheckoutAndSecretText(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-layout-repro")
	write(t, filepath.Join(dir, "wt", ".git"), "gitdir: ../.git/worktrees/wt\n")
	write(t, filepath.Join(dir, "wt", "docs", "a.md"), "tracked copy\n")
	write(t, filepath.Join(dir, "wt-dir", ".git", "config"), "[core]\n")
	write(t, filepath.Join(dir, "wt-dir", "docs", "b.md"), "tracked directory copy\n")
	write(t, filepath.Join(dir, "notes.md"), "TO"+"KEN="+"abcdefghijklmnopqrstuvwxyz\n")
	newest := time.Unix(1800000000, 0)
	if err := os.Chtimes(filepath.Join(dir, "notes.md"), newest, newest); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "report.md"), "ordinary report\n")
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-layout-repro")
	if err != nil {
		t.Fatal(err)
	}
	if p.Dest != filepath.Join(e.drive, "tasks", "2402", "wk-layout-repro") {
		t.Fatalf("dest=%s", p.Dest)
	}
	if strings.Contains(strings.Join(p.Move, ","), "wt/") || strings.Contains(strings.Join(p.Move, ","), "wt-dir/") || strings.Contains(strings.Join(p.Move, ","), "notes.md") {
		t.Fatalf("unsafe move list: %v", p.Move)
	}
	if p.Newest != newest.Unix() {
		t.Fatalf("newest=%d, want excluded file time %d", p.Newest, newest.Unix())
	}
	e.run.failRm = true
	if _, err := a.Apply(context.Background(), p, time.Now()); err == nil {
		t.Fatal("expected delete failure fixture")
	}
	e.run.failRm = false
	for _, rel := range []string{"report.md", "events/00001-job.task_link.json", "events/00005-job.completed.json"} {
		if _, err := os.Stat(filepath.Join(p.Dest, rel)); err != nil {
			t.Fatalf("missing copied %s: %v", rel, err)
		}
	}
	if _, err := os.Stat(filepath.Join(p.Dest, "notes.md")); !os.IsNotExist(err) {
		t.Fatalf("secret copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(p.Dest, "wt")); !os.IsNotExist(err) {
		t.Fatalf("checkout copied: %v", err)
	}
	var m manifest
	raw, err := os.ReadFile(filepath.Join(p.Dest, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	foundGit, foundGitDir, foundSecret := false, false, false
	for _, x := range m.ExcludedEntries {
		if x.Path == "wt" && x.Reason == "git-tracked" && x.Files == 2 {
			foundGit = true
		}
		if x.Path == "wt-dir" && x.Reason == "git-tracked" && x.Files == 2 {
			foundGitDir = true
		}
		if x.Path == "notes.md" && x.Reason == "secret-like:token_assignment" {
			foundSecret = true
		}
	}
	if !foundGit || !foundGitDir || !foundSecret {
		t.Fatalf("excluded entries: %+v", m.ExcludedEntries)
	}
	if len(e.uploads()) != 0 {
		t.Fatalf("attachments uploaded: %v", e.uploads())
	}
	doc := e.hk.Documents()["archive/2402/wk-layout-repro"]
	if doc.Kind != "note" || !strings.Contains(doc.Body, "secret-like:token_assignment") || strings.Contains(doc.Body, "abcdefghijklmnopqrstuvwxyz") {
		t.Fatalf("document=%+v", doc)
	}
	if len(e.hk.Comments()) != 1 {
		t.Fatalf("comments=%d", len(e.hk.Comments()))
	}
	plan, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-layout-repro")
	if err != nil || !plan.Resume {
		t.Fatalf("resume plan=%+v err=%v", plan, err)
	}
	if _, err := a.Apply(context.Background(), plan, time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(e.hk.Comments()) != 1 || len(e.hk.Documents()) != 1 {
		t.Fatal("resume changed hk record")
	}
}

func TestArchiveKeepsRootGitCheckout(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-root-checkout")
	write(t, filepath.Join(dir, ".git"), "gitdir: .git/worktrees/x\n")
	write(t, filepath.Join(dir, "report.md"), "retain this source\n")
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	_, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-root-checkout")
	var skip *Skip
	if !errors.As(err, &skip) || skip.Reason != "job-root-git-checkout" {
		t.Fatalf("plan error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatalf("source removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(e.drive, "tasks", "2402", "wk-root-checkout", "manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("manifest published: %v", err)
	}
}

func TestSecretPatternsMatchPinnedGuard(t *testing.T) {
	cases := map[string][2]string{
		"openai_key":                {"sk-" + "proj-" + strings.Repeat("a", 26), "SK-PROJ-ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
		"anthropic_key":             {"sk-" + "ant-api03-" + strings.Repeat("a", 26), "SK-ANT-API03-ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
		"github_token":              {"ghp_" + strings.Repeat("a", 36), "ghp_short"},
		"slack_token":               {"xoxb-" + strings.Repeat("1", 10) + "-" + strings.Repeat("2", 10) + "-" + strings.Repeat("a", 20), "xoxb-1-2-short"},
		"aws_access_key":            {"AKIA" + strings.Repeat("A", 16), "AKIA123"},
		"private_key":               {"-----" + "BEGIN" + " PRIVATE KEY-----", "BEGIN PUBLIC KEY"},
		"bearer_token":              {"Be" + "arer " + strings.Repeat("a", 26), "Bearer short"},
		"token_assignment":          {"TO" + "KEN=" + strings.Repeat("a", 12), "TOKEN=short"},
		"github_fine_grained_token": {"github_" + "pat_" + strings.Repeat("a", 82), "github_pat_short"},
	}
	if len(cases) != len(secretPatterns) {
		t.Fatalf("cases=%d patterns=%d", len(cases), len(secretPatterns))
	}
	for name, pair := range cases {
		if got := secretPattern([]byte(pair[0])); got != name {
			t.Errorf("%s positive=%q", name, got)
		}
		if got := secretPattern([]byte(pair[1])); got != "" {
			t.Errorf("%s negative=%q", name, got)
		}
	}
}

func TestApplyRecordsAndDeletesOversizedFiles(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-oversized")
	write(t, filepath.Join(dir, "small.bin"), strings.Repeat("s", 50))
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
	write(t, filepath.Join(dir, "report.md"), strings.Repeat("r", 150))
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	a.DriveMaxFileBytes = 100
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-oversized")
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Deleted || res.Files != 3 || res.Bytes != 80 {
		t.Fatalf("result = %+v", res)
	}
	if _, err := os.Stat(filepath.Join(dir, "large.bin")); !os.IsNotExist(err) {
		t.Fatalf("job dir retained or oversized file remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(res.Dest, "large.bin")); !os.IsNotExist(err) {
		t.Fatalf("oversized file copied to Drive: %v", err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(res.Dest, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Oversized) != 2 || m.Oversized[0].Path != "large.bin" || m.Oversized[1].Path != "report.md" {
		t.Fatalf("manifest oversized = %+v", m.Oversized)
	}
	note := e.comment(2402)
	if strings.Contains(note, "large.bin") || strings.Contains(note, "report.md") {
		t.Fatalf("comment should stay concise: %s", note)
	}
	doc := e.hk.Documents()["archive/2402/wk-oversized"]
	if doc.Kind != "note" || !strings.Contains(doc.Body, "large.bin oversized") || !strings.Contains(doc.Body, "report.md oversized") {
		t.Fatalf("document=%+v", doc)
	}
	if len(e.uploads()) != 0 {
		t.Fatalf("attachments uploaded: %v", e.uploads())
	}
}

func TestApplyKeepsChangedOversizedFile(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-oversized")
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	a.DriveMaxFileBytes = 100
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-oversized")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("m", 151))
	res, err := a.Apply(context.Background(), p, time.Now())
	var s *Skip
	if !errors.As(err, &s) || s.Reason != "job-changed" || res == nil || res.Deleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "large.bin")); err != nil {
		t.Fatal("changed oversized file's job dir was deleted")
	}
}

func TestApplyKeepsOversizedContentChangedWithoutMetadataChange(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-oversized-same-metadata")
	large := filepath.Join(dir, "large.bin")
	write(t, large, strings.Repeat("l", 150))
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	a.DriveMaxFileBytes = 100
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-oversized-same-metadata")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(large)
	if err != nil {
		t.Fatal(err)
	}
	write(t, large, strings.Repeat("m", 150))
	if err := os.Chtimes(large, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	var s *Skip
	if !errors.As(err, &s) || s.Reason != "job-changed" || res == nil || res.Deleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(large); err != nil {
		t.Fatal("changed oversized file's job dir was deleted")
	}
}

func TestCommentBoundsBytesAndSignalsTruncation(t *testing.T) {
	p := &Plan{Job: "wk-large", Task: "2402", Host: "h1", Dest: "/drive/archive", Dir: "/jobs/wk-large"}
	note := comment(p, "manifest-sha")
	if !strings.Contains(note, "archive/2402/wk-large") || !strings.Contains(note, "manifest-sha") {
		t.Fatalf("comment=%s", note)
	}
}

func TestApplyOnlyOversizedFiles(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-oversized")
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
	a := archiver(e, "")
	a.DriveMaxFileBytes = 1
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402,"padding":"`+strings.Repeat("x", 100)+`"}}`)
	finishJob(t, dir)
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-oversized")
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Move) != 0 || len(p.Oversized) != 3 || !res.Deleted {
		t.Fatalf("plan=%+v result=%+v", p, res)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("oversized-only job dir retained: %v", err)
	}
	var m manifest
	b, err := os.ReadFile(filepath.Join(res.Dest, "manifest.json"))
	if err != nil || json.Unmarshal(b, &m) != nil || len(m.Oversized) != 3 {
		t.Fatalf("manifest oversized=%+v err=%v", m.Oversized, err)
	}
}

func TestResumeIgnoresRecordedOversizedFilesForArchiveMatch(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-resume-oversized")
	write(t, filepath.Join(dir, "small.bin"), "small")
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":9}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	a.DriveMaxFileBytes = 100
	h := reap.Host{Name: "h1", Jobs: e.jobs}
	p, err := a.Plan(context.Background(), h, "wk-resume-oversized")
	if err != nil {
		t.Fatal(err)
	}
	e.run.failRm = true
	if _, err := a.Apply(context.Background(), p, time.Now()); err == nil {
		t.Fatal("expected simulated delete failure")
	}
	e.run.failRm = false
	p, err = a.Plan(context.Background(), h, "wk-resume-oversized")
	if err != nil || !p.Resume {
		t.Fatalf("oversized entry made finished archive differ: plan=%+v err=%v", p, err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil || res == nil || !res.Deleted {
		t.Fatalf("resume result=%+v err=%v", res, err)
	}
}

func TestResumeRejectsChangedOversizedManifestEntries(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*testing.T, string)
	}{
		{"size changed", func(t *testing.T, dir string) {
			write(t, filepath.Join(dir, "large.bin"), strings.Repeat("g", 151))
		}},
		{"new oversized file", func(t *testing.T, dir string) {
			write(t, filepath.Join(dir, "new-large.bin"), strings.Repeat("n", 175))
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			e := setup(t)
			dir := filepath.Join(e.jobs, "wk-resume-oversized-change")
			write(t, filepath.Join(dir, "small.bin"), "small")
			write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
			write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":9}}`)
			finishJob(t, dir)
			a := archiver(e, "")
			a.DriveMaxFileBytes = 100
			h := reap.Host{Name: "h1", Jobs: e.jobs}
			p, err := a.Plan(context.Background(), h, "wk-resume-oversized-change")
			if err != nil {
				t.Fatal(err)
			}
			e.run.failRm = true
			if _, err := a.Apply(context.Background(), p, time.Now()); err == nil {
				t.Fatal("expected simulated delete failure")
			}
			e.run.failRm = false
			change.edit(t, dir)
			p, err = a.Plan(context.Background(), h, "wk-resume-oversized-change")
			var s *Skip
			if !errors.As(err, &s) || s.Reason != "drive-dir-exists" || (p != nil && p.Resume) {
				t.Fatalf("resume plan=%+v err=%v", p, err)
			}
			if _, err := os.Stat(dir); err != nil {
				t.Fatalf("job dir was deleted or lost: %v", err)
			}
		})
	}
}

func TestDriveMaxFileBytesZeroPreservesExistingBehavior(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-oversized")
	write(t, filepath.Join(dir, "large.bin"), strings.Repeat("l", 150))
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":9}}`)
	finishJob(t, dir)
	p, err := archiver(e, "").Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-oversized")
	if err != nil || len(p.Move) != 3 || len(p.Oversized) != 0 || p.Files != 3 {
		t.Fatalf("plan=%+v err=%v", p, err)
	}
}

func TestCredential(t *testing.T) {
	for p, want := range map[string]bool{
		".env": true, ".env.local": true, "sub/.envrc": true, "gh_token.txt": true, "TOKENS/a": true,
		"server.PEM": true, "API_KEY.json": true, "keys/notes.md": true, "monkey.md": true,
		"report.md": false, "events/00001-job.claim.json": false, "build/game.bin": false, "env.md": false,
	} {
		if got := Credential(p); got != want {
			t.Errorf("Credential(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestPlanExcludesCredentialsAndListsThem(t *testing.T) {
	e := setup(t)
	job(t, e.jobs, "wk-7")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-7")
	if err != nil {
		t.Fatal(err)
	}
	wantExcluded := []string{".env.local", "API_KEY.json", "certs/server.pem", "gh_token.txt", "keys/notes.md"}
	if strings.Join(p.Excluded, ",") != strings.Join(wantExcluded, ",") {
		t.Fatalf("excluded = %v", p.Excluded)
	}
	for _, m := range append(p.Move, p.Text...) {
		if Credential(m) {
			t.Fatalf("credential-like %s planned for archive", m)
		}
	}
	if strings.Join(p.Skipped, ",") != "link.md" {
		t.Fatalf("skipped = %v", p.Skipped)
	}
	// build/game.bin is over the 1 KiB text cap and not markdown/JSON: Drive only.
	if strings.Join(p.Text, ",") != "brief.md,events/00001-job.claim.json,events/00004-job.task_link.json,events/00005-job.completed.json,notes with space.md,report.md" {
		t.Fatalf("text = %v", p.Text)
	}
	if p.Task != "2402" || p.Files != 7 || p.Delete != filepath.Join(e.jobs, "wk-7") {
		t.Fatalf("plan = %+v", p)
	}
	if p.Dest != filepath.Join(e.drive, "tasks", "2402", "wk-7") {
		t.Fatalf("dest = %s", p.Dest)
	}
}

func TestPlanSkips(t *testing.T) {
	e := setup(t)
	a := archiver(e, "")
	h := reap.Host{Jobs: e.jobs}
	write(t, filepath.Join(e.jobs, "notask", "report.md"), "r\n")
	write(t, filepath.Join(e.jobs, "notask", "events", "00005-job.completed.json"), `{}`)
	write(t, filepath.Join(e.jobs, "own-manifest", "manifest.json"), "{}")
	write(t, filepath.Join(e.jobs, "own-manifest", "events", "00004-job.task_link.json"), `{"payload":{"task_id":5}}`)
	write(t, filepath.Join(e.jobs, "own-manifest", "events", "00005-job.completed.json"), `{}`)
	for job, reason := range map[string]string{"gone": "job-gone", "notask": "task-unknown", "own-manifest": "manifest-collision", "../x": "job-name-invalid"} {
		_, err := a.Plan(context.Background(), h, job)
		var s *Skip
		if !errors.As(err, &s) || s.Reason != reason {
			t.Errorf("%s: err = %v, want %s", job, err, reason)
		}
	}
	if _, err := a.Plan(context.Background(), reap.Host{Name: "h1"}, "x"); err == nil || err.Error() != "skipped: jobs-dir-unset" {
		t.Errorf("jobs unset: %v", err)
	}
}

func TestPlanRequiresFinishedLifecycle(t *testing.T) {
	e := setup(t)
	a := archiver(e, "")
	h := reap.Host{Jobs: e.jobs}
	writeEvents := func(job string, events ...string) {
		for i, event := range events {
			write(t, filepath.Join(e.jobs, job, "events", fmt.Sprintf("%05d-%s.json", i+1, event)), `{}`)
		}
		write(t, filepath.Join(e.jobs, job, "events", "00010-job.task_link.json"), `{"payload":{"task_id":"2402"}}`)
	}
	for i, tc := range []struct {
		name   string
		events []string
		want   string
	}{
		{"completed then bookkeeping", []string{"job.claim", "job.spawned", "job.completed", "quota_pool.release", "job.reaped"}, "planned"},
		{"lost", []string{"job.claim", "job.lost"}, "planned"},
		{"placement unfinished", []string{"job.placement"}, "not-finished"},
		{"joined unfinished", []string{"job.joined"}, "not-finished"},
		{"restarted after completion", []string{"job.claim", "job.completed", "job.spawned"}, "not-finished"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobName := fmt.Sprintf("lifecycle-%d", i)
			writeEvents(jobName, tc.events...)
			_, err := a.Plan(context.Background(), h, jobName)
			if tc.want == "planned" && err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if tc.want != "planned" {
				var skip *Skip
				if !errors.As(err, &skip) || skip.Reason != tc.want {
					t.Fatalf("Plan() error = %v, want skipped %s", err, tc.want)
				}
			}
		})
	}
}

func TestPlanSkipsJobWithoutEvents(t *testing.T) {
	e := setup(t)
	job(t, e.jobs, "no-events")
	if err := os.RemoveAll(filepath.Join(e.jobs, "no-events", "events")); err != nil {
		t.Fatal(err)
	}
	_, err := archiver(e, "").Plan(context.Background(), reap.Host{Jobs: e.jobs}, "no-events")
	var skip *Skip
	if !errors.As(err, &skip) || skip.Reason != "no-events" {
		t.Fatalf("Plan() error = %v, want skipped no-events", err)
	}
}

func TestPlanEmptyEventsDirectoryIsNotFinished(t *testing.T) {
	e := setup(t)
	if err := os.MkdirAll(filepath.Join(e.jobs, "empty-events", "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := archiver(e, "").Plan(context.Background(), reap.Host{Jobs: e.jobs}, "empty-events")
	var skip *Skip
	if !errors.As(err, &skip) || skip.Reason != "not-finished" {
		t.Fatalf("Plan() error = %v, want skipped not-finished", err)
	}
}

func TestPlanExcludesNamesBeforeHostRead(t *testing.T) {
	e := setup(t)
	a := archiver(e, "")
	a.Exclude = []string{"pw-rollout-*", "pw-hk*", "events-lane"}
	for _, name := range []string{"pw-rollout-x", "pw-hk1", "events-lane"} {
		_, err := a.Plan(context.Background(), reap.Host{Jobs: e.jobs}, name)
		var skip *Skip
		if !errors.As(err, &skip) || skip.Reason != "excluded-name" {
			t.Errorf("%s: error = %v", name, err)
		}
	}
	if e.run.calls != 0 {
		t.Fatalf("excluded names caused %d host commands", e.run.calls)
	}
	a.Exclude = []string{}
	if ExcludedName(a.Exclude, "pw-rollout-x") {
		t.Fatal("explicit empty exclude matched a name")
	}
}

func checkArchived(t *testing.T, e env, res *Result, dir string) {
	t.Helper()
	if !res.Deleted {
		t.Fatal("job dir not deleted")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("job dir still present: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(res.Dest, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Files []File `json:"files"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 7 || res.Files != 7 {
		t.Fatalf("manifest files = %d", len(m.Files))
	}
	for _, f := range m.Files {
		got, err := os.ReadFile(filepath.Join(res.Dest, f.Path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(got)
		if hex.EncodeToString(sum[:]) != f.SHA256 {
			t.Fatalf("%s: drive copy does not match manifest", f.Path)
		}
	}
	// Nothing credential-like reached Drive or hk.
	_ = filepath.Walk(res.Dest, func(p string, _ os.FileInfo, _ error) error {
		rel, _ := filepath.Rel(res.Dest, p)
		if rel != "." && Credential(filepath.ToSlash(rel)) {
			t.Errorf("credential-like %s copied to drive", rel)
		}
		if filepath.Base(p) == "link.md" {
			t.Errorf("symlink copied to drive")
		}
		return nil
	})
	uploads := e.uploads()
	for name, body := range uploads {
		if Credential(name) || strings.Contains(string(body), "SECRET") {
			t.Errorf("credential-like upload %s", name)
		}
	}
	note := e.comment(2402)
	jobName := filepath.Base(res.Dest)
	for _, want := range []string{res.Dest, "archive/2402/" + jobName, "manifest sha256: " + res.Manifest} {
		if !strings.Contains(note, want) {
			t.Errorf("hk comment lacks %q:\n%s", want, note)
		}
	}
	if len(uploads) != 0 || len(res.Attachments) != 0 {
		t.Fatalf("hk received file bytes: %v", uploads)
	}
	if got := e.routes(); got["PUT /v1/documents/archive%2F2402%2F"+jobName] != 1 || got["POST /v1/tasks/2402/comments"] != 1 || len(got) != 2 {
		t.Fatalf("hk routes = %v", got)
	}
}

func TestApplyLocalMovesVerifiesAndDeletes(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-7")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-7")
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	checkArchived(t, e, res, dir)
	if _, err := os.Stat(e.sshLog); !os.IsNotExist(err) {
		t.Fatal("local archive used ssh")
	}
	// The delete takes the absolute job dir as a quoted argument, no "--".
	if len(e.run.rms) != 1 || strings.Join(e.run.rms[0], "|") != `-c|rm -rf "$1"|sh|`+dir {
		t.Fatalf("delete argv = %q", e.run.rms)
	}
	// A byte-identical job dir under the same name is already archived: it
	// resumes to a delete-only plan rather than copying over the archive.
	job(t, e.jobs, "wk-7")
	if p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-7"); err != nil || !p.Resume {
		t.Fatalf("rerun: %+v %v", p, err)
	}
}

func TestApplyRemoteHostsStreamOverSSH(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-8")
	a := archiver(e, "archive-mac")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h2", SSH: "node-b", Jobs: e.jobs}, "wk-8")
	if err != nil {
		t.Fatal(err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	checkArchived(t, e, res, dir)
	log, _ := os.ReadFile(e.sshLog)
	if !strings.Contains(string(log), "node-b") || !strings.Contains(string(log), "archive-mac") {
		t.Fatalf("ssh destinations = %q", log)
	}
}

func TestArchiveRejectsStalePlanFromAnotherHost(t *testing.T) {
	e := setup(t)
	jobs2 := filepath.Join(filepath.Dir(e.jobs), "jobs-second-host")
	dir1 := job(t, e.jobs, "wk-collision")
	dir2 := job(t, jobs2, "wk-collision")
	write(t, filepath.Join(dir1, "payload.bin"), "source-one")
	write(t, filepath.Join(dir2, "payload.bin"), "source-two")
	a := archiver(e, "")
	p1, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-collision")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := a.Plan(context.Background(), reap.Host{Name: "h2", Jobs: jobs2}, "wk-collision")
	if err != nil {
		t.Fatal(err)
	}
	if p1.Dest != p2.Dest {
		t.Fatalf("layout changed: %s != %s", p1.Dest, p2.Dest)
	}
	if _, err := a.Apply(context.Background(), p1, time.Now()); err != nil {
		t.Fatal(err)
	}
	res, err := a.Apply(context.Background(), p2, time.Now())
	var skip *Skip
	if !errors.As(err, &skip) || skip.Reason != "drive-dir-exists" || (res != nil && res.Deleted) {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(dir2, "payload.bin")); err != nil {
		t.Fatalf("second source lost: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(p1.Dest, "payload.bin"))
	if err != nil || string(got) != "source-one" {
		t.Fatalf("archive overwritten: %q %v", got, err)
	}
}

func TestArchiveClaimUsesExternalExpiringLocks(t *testing.T) {
	e := setup(t)
	a := archiver(e, "")
	dest := filepath.Join(e.drive, "tasks", "2402", "X")
	release, err := a.claim(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.claim(context.Background(), dest); err == nil || err.Error() != "skipped: archive-lock-busy" {
		t.Fatalf("busy claim err=%v", err)
	}
	lock := filepath.Join(e.drive, ".locks", fmt.Sprintf("%x", sha256.Sum256([]byte(dest))))
	if _, err := os.Stat(lock); err != nil {
		t.Fatalf("lock not outside destination namespace: %v", err)
	}
	other, err := a.claim(context.Background(), dest+".lock")
	if err != nil {
		t.Fatalf("job ending in .lock collided with another lock: %v", err)
	}
	other()
	release()
	if err := os.MkdirAll(lock, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lock, "owner"), []byte("dead 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	release, err = a.claim(context.Background(), dest)
	if err != nil {
		t.Fatalf("stale lock was not reclaimed: %v", err)
	}
	release()
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatalf("released lock remains: %v", err)
	}
}

func TestArchiveCopiesOnlyInspectedTextSnapshot(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-snapshot")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "report.md"), "TO"+"KEN="+strings.Repeat("z", 24)+"\n")
	res, err := a.Apply(context.Background(), p, time.Now())
	var skip *Skip
	if !errors.As(err, &skip) || skip.Reason != "job-changed" || (res != nil && res.Deleted) {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatalf("source lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(p.Dest, "report.md")); !os.IsNotExist(err) {
		t.Fatalf("changed report copied: %v", err)
	}
}

func TestArchiveSnapshotWriteDoesNotCopyConcurrentSecretChange(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-snapshot-race")
	report := filepath.Join(dir, "report.md")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-snapshot-race")
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("TO" + "KEN=" + strings.Repeat("q", 24) + "\n")
	mutator := &mutateBeforeSnapshotWrite{Runner: e.run, source: report, replacement: secret}
	a.Runner = mutator
	res, err := a.Apply(context.Background(), p, time.Now())
	var skip *Skip
	if !errors.As(err, &skip) || skip.Reason != "job-changed" || res == nil || res.Deleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if !mutator.mutated {
		t.Fatal("fixture did not mutate source before snapshot write")
	}
	got, err := os.ReadFile(filepath.Join(p.Dest, "report.md"))
	if err != nil || string(got) != "# report\n" {
		t.Fatalf("Drive copy=%q err=%v", got, err)
	}
	if strings.Contains(string(got), string(secret)) {
		t.Fatal("concurrent secret reached Drive")
	}
	if _, err := os.Stat(report); err != nil {
		t.Fatalf("changed source was deleted: %v", err)
	}
}

func TestApplyFailuresKeepJobDir(t *testing.T) {
	for name, mut := range map[string]func(*testing.T, env, *Archiver){
		"hk-put": func(_ *testing.T, e env, _ *Archiver) {
			e.hk.Fail("PUT /v1/documents/{key}", 503, "document_unavailable")
		},
		"hk-too-big": func(_ *testing.T, e env, _ *Archiver) {
			e.hk.Fail("PUT /v1/documents/{key}", 413, "document_too_large")
		},
		"hk-comment": func(_ *testing.T, e env, _ *Archiver) { e.hk.Fail("POST /v1/tasks/{id}/comments", 500, "internal") },
		"hk-unset":   func(t *testing.T, _ env, _ *Archiver) { t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "") },
		"drive":      func(_ *testing.T, _ env, a *Archiver) { a.DriveDir = "/dev/null/drive" },
	} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			dir := job(t, e.jobs, "wk-9")
			a := archiver(e, "")
			p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-9")
			if err != nil {
				t.Fatal(err)
			}
			mut(t, e, a)
			p.Dest = filepath.Join(a.DriveDir, "tasks", "2402", "wk-9")
			res, err := a.Apply(context.Background(), p, time.Now())
			if err == nil || (res != nil && res.Deleted) {
				t.Fatalf("res=%+v err=%v", res, err)
			}
			// An hk failure is an error, never a skip: the reap stage retries
			// an error row after archiveRetry and leaves a skip for a person.
			if result, reason := Reason(err); result != "error" {
				t.Fatalf("%s failure recorded as %s %s", name, result, reason)
			}
			if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
				t.Fatalf("job dir lost after %s failure: %v", name, err)
			}
			// No manifest may mark the archive done, or the retry would be
			// refused as drive-dir-exists and the hk record never written.
			if _, err := os.Stat(filepath.Join(p.Dest, "manifest.json")); err == nil {
				t.Fatalf("manifest.json published after %s failure", name)
			}
		})
	}
}

func TestApplyKeepsJobChangedAfterPlan(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-10")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-10")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "late.log"), "written after the plan\n")
	res, err := a.Apply(context.Background(), p, time.Now())
	var s *Skip
	if !errors.As(err, &s) || s.Reason != "job-changed" || res == nil || res.Deleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "late.log")); err != nil {
		t.Fatal("changed job dir was deleted")
	}
}

// A delete that failed after the archive and hk record completed resumes to
// a delete-only pass instead of being stuck as drive-dir-exists.
func TestArchiveResumesFailedDelete(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-11")
	a := archiver(e, "")
	h := reap.Host{Name: "h1", Jobs: e.jobs}
	p, err := a.Plan(context.Background(), h, "wk-11")
	if err != nil {
		t.Fatal(err)
	}
	e.run.failRm = true
	if _, err := a.Apply(context.Background(), p, time.Now()); err == nil {
		t.Fatal("delete failure not reported")
	}
	e.run.failRm = false
	puts := e.routes()["PUT /v1/documents/archive%2F2402%2Fwk-11"]
	p, err = a.Plan(context.Background(), h, "wk-11")
	if err != nil || !p.Resume {
		t.Fatalf("resume plan: %+v %v", p, err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil || !res.Deleted {
		t.Fatalf("resume: %+v %v", res, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("job dir kept after resume")
	}
	if e.routes()["PUT /v1/documents/archive%2F2402%2Fwk-11"] != puts || len(e.hk.Comments()) != 1 {
		t.Fatal("resume uploaded or commented again")
	}
	// A different job under the same name and month never resumes.
	job(t, e.jobs, "wk-11")
	write(t, filepath.Join(e.jobs, "wk-11", "report.md"), "# a different, longer report\n")
	if _, err := a.Plan(context.Background(), h, "wk-11"); err == nil || err.Error() != "skipped: drive-dir-exists" {
		t.Fatalf("different job: %v", err)
	}
}

// A job whose every file is excluded still records and deletes without a copy.
func TestApplyNothingToMove(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-12")
	write(t, filepath.Join(dir, "events", "00004-token-job.task_link.json"), `{"payload":{"task_id":9}}`)
	finishJob(t, dir)
	write(t, filepath.Join(dir, ".env"), "SECRET=1\n")
	a := archiver(e, "")
	a.DriveMaxFileBytes = 1
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-12")
	if err != nil || len(p.Move) != 0 || p.Task != "9" {
		t.Fatalf("plan: %+v %v", p, err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil || !res.Deleted || res.Files != 0 {
		t.Fatalf("apply: %+v %v", res, err)
	}
	if _, err := os.Stat(filepath.Join(res.Dest, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(e.comment(9), ".env") {
		t.Fatal("comment should not repeat excluded list")
	}
	doc := e.hk.Documents()["archive/9/wk-12"]
	if !strings.Contains(doc.Body, ".env credential-path") || !strings.Contains(doc.Body, "events/00005-job.completed.json oversized") {
		t.Fatalf("document=%+v", doc)
	}
	if len(e.uploads()) != 0 {
		t.Fatalf("attachments uploaded: %v", e.uploads())
	}
}

// A job dir that is itself a symlink is refused: archiving would follow it
// out of the jobs tree.
func TestPlanRefusesSymlinkedJobDir(t *testing.T) {
	e := setup(t)
	outside := job(t, filepath.Join(filepath.Dir(e.jobs), "elsewhere"), "real")
	if err := os.MkdirAll(e.jobs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(e.jobs, "wk-13")); err != nil {
		t.Fatal(err)
	}
	_, err := archiver(e, "").Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-13")
	var s *Skip
	if !errors.As(err, &s) || s.Reason != "job-dir-symlink" {
		t.Fatalf("err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "report.md")); err != nil {
		t.Fatal("symlink target touched")
	}
}

type stubRunner string

func (s stubRunner) Run(context.Context, string, []string, string, string) ([]byte, error) {
	return []byte(s), nil
}

// hashes fails closed unless every requested file has exactly one record.
func TestHashesFailClosed(t *testing.T) {
	sum := strings.Repeat("a", 64)
	for name, tc := range map[string]struct {
		out string
		ok  bool
	}{
		"complete":   {sum + " ./a\x00" + sum + " ./b\x00", true},
		"missing":    {sum + " ./a\x00", false},
		"duplicate":  {sum + " ./a\x00" + sum + " ./a\x00", false},
		"extra":      {sum + " ./a\x00" + sum + " ./b\x00" + sum + " ./c\x00", false},
		"swapped":    {sum + " ./a\x00" + sum + " ./c\x00", false},
		"malformed":  {"abc ./a\x00" + sum + " ./b\x00", false},
		"unhashable": {"!./b\x00", false},
	} {
		a := &Archiver{Runner: stubRunner(tc.out)}
		got, err := a.hashes(context.Background(), "", "/x", []string{"a", "b"})
		if (err == nil) != tc.ok || tc.ok && len(got) != 2 {
			t.Errorf("%s: %v %v", name, got, err)
		}
	}
}

// A ~/ jobs dir is expanded with the job host's own $HOME before any quoted
// path reaches its shell; a relative one is refused.
func TestTildeJobsExpandOnJobHost(t *testing.T) {
	e := setup(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := job(t, filepath.Join(home, "jobs"), "wk-14")
	a := archiver(e, "archive-mac")
	h := reap.Host{Name: "h2", SSH: "node-b", Jobs: "~/jobs"}
	if got, err := a.Jobs(context.Background(), h); err != nil || got != filepath.Join(home, "jobs") {
		t.Fatalf("Jobs = %q, %v", got, err)
	}
	p, err := a.Plan(context.Background(), h, "wk-14")
	if err != nil || p.Dir != dir {
		t.Fatalf("plan dir = %+v, %v", p, err)
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil || !res.Deleted {
		t.Fatalf("apply: %+v %v", res, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("job dir kept")
	}
	if _, err := a.Plan(context.Background(), reap.Host{Name: "h3", Jobs: "jobs"}, "wk-14"); err == nil || err.Error() != "skipped: jobs-dir-not-absolute" {
		t.Fatalf("relative jobs: %v", err)
	}
}
