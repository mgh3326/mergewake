package archive

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// dropOnDriveHash removes one file from the Drive copy just before the
// Drive-side hash runs. That is the b1950 failure: the copy held fewer files
// than the plan moved, so the hash found a path missing.
type dropOnDriveHash struct {
	reap.Runner
	dest, name string
}

func (d dropOnDriveHash) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if name == "sh" && len(args) == 4 && args[1] == hashScript && args[3] == d.dest {
		_ = os.Remove(filepath.Join(d.dest, d.name))
	}
	return d.Runner.Run(ctx, name, args, input, dir)
}

// AppleDouble ._ files are macOS metadata. They are never copied to Drive or
// uploaded to hk, they are named in the manifest, and the job dir still goes.
func TestApplyArchivesAppleDoubleWithoutCopying(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-ad")
	write(t, filepath.Join(dir, "refs", "x.png"), "png\n")
	write(t, filepath.Join(dir, "refs", "._x.png"), "meta\n")
	write(t, filepath.Join(dir, "._notes.md"), "# meta\n")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-ad")
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(p.Move, "refs/._x.png") || slices.Contains(p.Move, "._notes.md") {
		t.Fatalf("AppleDouble planned for copy: %v", p.Move)
	}
	if got := strings.Join(p.AppleDouble, ","); got != "._notes.md,refs/._x.png" {
		t.Fatalf("plan AppleDouble = %q", got)
	}
	a.Runner = dropOnDriveHash{Runner: e.run, dest: p.Dest, name: "refs/._x.png"}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !res.Deleted {
		t.Fatal("job dir not deleted")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("job dir still present: %v", err)
	}
	if _, err := os.Stat(filepath.Join(res.Dest, "refs", "._x.png")); !os.IsNotExist(err) {
		t.Fatalf("AppleDouble copied to drive: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(res.Dest, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Files       []File   `json:"files"`
		AppleDouble []string `json:"appledouble"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range m.Files {
		if strings.HasPrefix(filepath.Base(f.Path), "._") {
			t.Fatalf("AppleDouble %s in manifest files", f.Path)
		}
	}
	if got := strings.Join(m.AppleDouble, ","); got != "._notes.md,refs/._x.png" {
		t.Fatalf("manifest appledouble = %q", got)
	}
	if len(m.Files) != 8 || res.Files != 8 {
		t.Fatalf("archived files = %d (result %d), want 8", len(m.Files), res.Files)
	}
	uploads := e.uploads()
	if _, ok := uploads["._notes.md"]; ok {
		t.Fatal("AppleDouble text file uploaded to hk")
	}
	note := e.comment(2402)
	if !strings.Contains(note, "archive/2402/wk-ad") {
		t.Fatalf("hk comment lacks archive record:\n%s", note)
	}
}

// A Drive-side hash failure names the first path it could not hash, and the
// job dir stays.
func TestApplyDriveHashFailureNamesPath(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-hash")
	write(t, filepath.Join(dir, "refs", "x.png"), "png\n")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-hash")
	if err != nil {
		t.Fatal(err)
	}
	a.Runner = dropOnDriveHash{Runner: e.run, dest: p.Dest, name: "refs/x.png"}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err == nil || (res != nil && res.Deleted) {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if !strings.Contains(err.Error(), "cannot hash refs/x.png") {
		t.Fatalf("error does not name the path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.md")); err != nil {
		t.Fatalf("job dir lost after hash failure: %v", err)
	}
	if _, err := os.Stat(filepath.Join(p.Dest, "manifest.json")); err == nil {
		t.Fatal("manifest.json published after hash failure")
	}
}

// A destination holding a partial copy and no manifest.json, as b1950 left
// it, is copied over and archived normally.
func TestApplyArchivesOverPartialDriveCopyWithoutManifest(t *testing.T) {
	e := setup(t)
	dir := job(t, e.jobs, "wk-partial")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-partial")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(p.Dest, "brief.md"), "partial\n")
	write(t, filepath.Join(p.Dest, "build", "game.bin"), "x")
	if _, err := os.Stat(filepath.Join(p.Dest, "manifest.json")); err == nil {
		t.Fatal("fixture already has a manifest")
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatalf("apply over partial copy: %v", err)
	}
	checkArchived(t, e, res, dir)
}
