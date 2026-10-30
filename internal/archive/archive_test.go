package archive

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

	"github.com/mgh3326/mergewake/internal/reap"
)

// fakeRunner executes real commands against temp dirs except handoffkeep,
// which it answers in-process; ssh resolves to the fake script on PATH.
type fakeRunner struct {
	puts     map[string][]byte // --name -> bytes
	comments map[string]string // task -> body
	failPut  string            // --name whose put fails
	failNote bool
}

func (f *fakeRunner) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if name == "handoffkeep" {
		switch {
		case len(args) == 5 && args[0] == "attach" && args[1] == "put" && args[3] == "--name":
			if args[4] == f.failPut {
				return nil, errors.New("hk put failed")
			}
			b, err := os.ReadFile(args[2])
			if err != nil {
				return nil, err
			}
			f.puts[args[4]] = b
			sum := sha256.Sum256(b)
			return json.Marshal(map[string]any{"sha256": hex.EncodeToString(sum[:]), "size_bytes": len(b), "created": true})
		case len(args) == 5 && args[0] == "tasks" && args[1] == "comment" && args[3] == "--file" && args[4] == "-":
			if f.failNote {
				return nil, errors.New("hk comment failed")
			}
			f.comments[args[2]] = input
			return []byte("{}"), nil
		}
		return nil, errors.New("unexpected handoffkeep call: " + strings.Join(args, " "))
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
		run: &fakeRunner{puts: map[string][]byte{}, comments: map[string]string{}}}
	t.Setenv("FAKE_SSH_LOG", e.sshLog)
	return e
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

// job lays out a runtug-shaped job dir with credential-like files mixed in.
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
	return &Archiver{Runner: e.run, HK: "handoffkeep", DriveDir: e.drive, SSH: ssh, TextMax: 1024, TempDir: ""}
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
	for name, body := range e.run.puts {
		if Credential(name) || strings.Contains(string(body), "SECRET") {
			t.Errorf("credential-like upload %s", name)
		}
	}
	note := e.run.comments["2402"]
	for _, want := range []string{res.Dest, "files: 6", "manifest: ", res.Manifest, ".env.local", "gh_token.txt", "API_KEY.json", "build/game.bin", "deleted after this record"} {
		if !strings.Contains(note, want) {
			t.Errorf("hk comment lacks %q:\n%s", want, note)
		}
	}
	if len(res.Attachments) != 5 || e.run.puts["manifest.json"] == nil {
		t.Fatalf("attachments = %+v", res.Attachments)
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
	// A rerun of the same job name in the same month never overwrites it.
	job(t, e.jobs, "wk-7")
	if _, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-7"); err == nil || err.Error() != "skipped: drive-dir-exists" {
		t.Fatalf("rerun: %v", err)
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
	for name, mut := range map[string]func(*fakeRunner, *Archiver){
		"hk-put":     func(r *fakeRunner, _ *Archiver) { r.failPut = "report.md" },
		"hk-comment": func(r *fakeRunner, _ *Archiver) { r.failNote = true },
		"drive":      func(_ *fakeRunner, a *Archiver) { a.DriveDir = "/dev/null/drive" },
	} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			dir := job(t, e.jobs, "wk-9")
			a := archiver(e, "")
			p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-9")
			if err != nil {
				t.Fatal(err)
			}
			mut(e.run, a)
			p.Dest = filepath.Join(a.DriveDir, "h1", "m", "wk-9")
			res, err := a.Apply(context.Background(), p, time.Now())
			if err == nil || (res != nil && res.Deleted) {
				t.Fatalf("res=%+v err=%v", res, err)
			}
			if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
				t.Fatalf("job dir lost after %s failure: %v", name, err)
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
