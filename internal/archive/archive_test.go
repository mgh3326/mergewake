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
	failRm bool
	rms    [][]string // delete argv, recorded
	hkCLI  [][]string // hk CLI calls, which must not happen
}

func (f *fakeRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
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

func archiver(e env, ssh string) *Archiver {
	return &Archiver{Runner: e.run, DriveDir: e.drive, SSH: ssh, TextMax: 1024}
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
	if strings.Join(p.Text, ",") != "brief.md,events/00001-job.claim.json,events/00004-job.task_link.json,notes with space.md,report.md" {
		t.Fatalf("text = %v", p.Text)
	}
	if p.Task != "2402" || p.Files != 6 || p.Delete != filepath.Join(e.jobs, "wk-7") {
		t.Fatalf("plan = %+v", p)
	}
	if !strings.HasPrefix(p.Dest, filepath.Join(e.drive, "h1")+"/") || filepath.Base(p.Dest) != "wk-7" {
		t.Fatalf("dest = %s", p.Dest)
	}
}

func TestPlanSkips(t *testing.T) {
	e := setup(t)
	a := archiver(e, "")
	h := reap.Host{Name: "h1", Jobs: e.jobs}
	write(t, filepath.Join(e.jobs, "notask", "report.md"), "r\n")
	write(t, filepath.Join(e.jobs, "own-manifest", "manifest.json"), "{}")
	write(t, filepath.Join(e.jobs, "own-manifest", "events", "00004-job.task_link.json"), `{"payload":{"task_id":5}}`)
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
	if len(m.Files) != 6 || res.Files != 6 {
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
	for _, want := range []string{res.Dest, "files: 6", "manifest: ", res.Manifest, ".env.local", "gh_token.txt", "API_KEY.json", "build/game.bin", "deleted after this record"} {
		if !strings.Contains(note, want) {
			t.Errorf("hk comment lacks %q:\n%s", want, note)
		}
	}
	if len(res.Attachments) != 5 || uploads["manifest.json"] == nil {
		t.Fatalf("attachments = %+v", res.Attachments)
	}
	// Five job text files and the manifest go up as attachments, one comment
	// records them; each goes to hk over HTTP with the svc token (setup).
	if got := e.routes(); got["PUT /v1/attachments"] != 6 || got["POST /v1/tasks/2402/comments"] != 1 || len(got) != 2 {
		t.Fatalf("hk routes = %v", got)
	}
	if a := e.hk.Attachments()[res.Manifest]; a.Name != "manifest.json" || a.MIME != "application/json" {
		t.Fatalf("manifest attachment = %q %q", a.Name, a.MIME)
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

func TestApplyFailuresKeepJobDir(t *testing.T) {
	for name, mut := range map[string]func(*testing.T, env, *Archiver){
		"hk-put": func(_ *testing.T, e env, _ *Archiver) {
			e.hk.Fail("PUT /v1/attachments", 503, "attachment_r2_unavailable")
		},
		"hk-too-big": func(_ *testing.T, e env, _ *Archiver) { e.hk.MaxAttachmentBytes = 4 },
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
			p.Dest = filepath.Join(a.DriveDir, "h1", "m", "wk-9")
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
func TestApplyResumesFailedDelete(t *testing.T) {
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
	puts := e.routes()["PUT /v1/attachments"]
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
	if e.routes()["PUT /v1/attachments"] != puts || len(e.hk.Comments()) != 1 {
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
	write(t, filepath.Join(dir, ".env"), "SECRET=1\n")
	a := archiver(e, "")
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
	if !strings.Contains(e.comment(9), ".env") {
		t.Fatal("excluded files not recorded")
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
		"complete":  {sum + " ./a\x00" + sum + " ./b\x00", true},
		"missing":   {sum + " ./a\x00", false},
		"duplicate": {sum + " ./a\x00" + sum + " ./a\x00", false},
		"extra":     {sum + " ./a\x00" + sum + " ./b\x00" + sum + " ./c\x00", false},
		"swapped":   {sum + " ./a\x00" + sum + " ./c\x00", false},
		"malformed": {"abc ./a\x00" + sum + " ./b\x00", false},
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
