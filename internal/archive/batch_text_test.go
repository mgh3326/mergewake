package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/reap"
)

// batchJob lays out a finished job with n small markdown files and one
// non-text payload, so only the text-file count varies between cases.
func batchJob(t *testing.T, jobs, name string, n int) string {
	t.Helper()
	dir := filepath.Join(jobs, name)
	for i := 0; i < n; i++ {
		write(t, filepath.Join(dir, fmt.Sprintf("note-%04d.md", i)), fmt.Sprintf("# note %d\nbody %d\n", i, i))
	}
	write(t, filepath.Join(dir, "payload.bin"), "binary payload\n")
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	return dir
}

// planHostCalls plans one job and returns the plan with the number of host
// commands planning took.
func planHostCalls(t *testing.T, e env, name string) (*Plan, int) {
	t.Helper()
	p, err := archiver(e, "").Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, name)
	if err != nil {
		t.Fatal(err)
	}
	return p, e.run.calls
}

// (a) Planning reads a job's text files with a host-command count that does
// not depend on how many there are.
func TestArchiveTextReadUsesConstantHostCommands(t *testing.T) {
	calls := map[int]int{}
	for _, n := range []int{5, 500} {
		e := setup(t)
		name := fmt.Sprintf("wk-read-%d", n)
		batchJob(t, e.jobs, name, n)
		p, count := planHostCalls(t, e, name)
		if len(p.Text) != n+2 {
			t.Fatalf("%d files: text=%d, want %d", n, len(p.Text), n+2)
		}
		calls[n] = count
		t.Logf("%d text files: %d plan host commands", n, count)
	}
	if calls[5] != calls[500] {
		t.Fatalf("plan host commands: 5 files=%d, 500 files=%d", calls[5], calls[500])
	}
}

// (b) Applying a job writes every snapshot with a constant number of host
// commands, and the archived files equal the sources byte for byte.
func TestArchiveTextWriteUsesConstantHostCommands(t *testing.T) {
	type applied struct {
		calls int
		dest  string
	}
	got := map[int]applied{}
	for _, n := range []int{5, 500} {
		e := setup(t)
		name := fmt.Sprintf("wk-write-%d", n)
		dir := batchJob(t, e.jobs, name, n)
		sources := map[string][]byte{}
		for i := 0; i < n; i++ {
			rel := fmt.Sprintf("note-%04d.md", i)
			body, err := os.ReadFile(filepath.Join(dir, rel))
			if err != nil {
				t.Fatal(err)
			}
			sources[rel] = body
		}
		sources["payload.bin"] = []byte("binary payload\n")
		a := archiver(e, "")
		p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, name)
		if err != nil {
			t.Fatal(err)
		}
		e.run.calls = 0
		res, err := a.Apply(context.Background(), p, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if !res.Deleted {
			t.Fatal("job dir kept after a complete archive")
		}
		got[n] = applied{e.run.calls, res.Dest}
		t.Logf("%d text files: %d apply host commands", n, e.run.calls)
		for rel, want := range sources {
			body, err := os.ReadFile(filepath.Join(res.Dest, rel))
			if err != nil {
				t.Fatalf("%d files: %s: %v", n, rel, err)
			}
			if string(body) != string(want) {
				t.Fatalf("%d files: %s drive copy=%q, source=%q", n, rel, body, want)
			}
		}
	}
	if got[5].calls != got[500].calls {
		t.Fatalf("apply host commands: 5 files=%d, 500 files=%d", got[5].calls, got[500].calls)
	}
}

func TestArchiveTextStagingDoesNotCollideWithPartialPaths(t *testing.T) {
	e := setup(t)
	dir := batchJob(t, e.jobs, "wk-partial-paths", 0)
	write(t, filepath.Join(dir, ".partial", "data.bin"), "binary in archived .partial\n")
	write(t, filepath.Join(dir, ".partial", "note.md"), "# text in archived .partial\n")
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-partial-paths")
	if err != nil {
		t.Fatal(err)
	}
	staleStage := filepath.Join(filepath.Dir(p.Dest), "."+filepath.Base(p.Dest)+".text-stage.stale")
	write(t, filepath.Join(staleStage, "note.md"), "stale staging content")
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staleStage); !os.IsNotExist(err) {
		t.Fatalf("stale text staging dir remains: %v", err)
	}
	for rel, want := range map[string]string{
		".partial/data.bin": "binary in archived .partial\n",
		".partial/note.md":  "# text in archived .partial\n",
	} {
		got, err := os.ReadFile(filepath.Join(res.Dest, rel))
		if err != nil || string(got) != want {
			t.Errorf("archived %s = %q, err=%v; want %q", rel, got, err, want)
		}
	}
}

// changeBeforeTextRead rewrites one source just before the job's text read
// runs, so the listing still holds the old size.
type changeBeforeTextRead struct {
	reap.Runner
	source string
	body   []byte
	fired  bool
}

func (r *changeBeforeTextRead) Run(ctx context.Context, name string, args []string, input, dir string) ([]byte, error) {
	if !r.fired && readsJobText(name, args) {
		r.fired = true
		if err := os.WriteFile(r.source, r.body, 0o644); err != nil {
			return nil, err
		}
	}
	return r.Runner.Run(ctx, name, args, input, dir)
}

// readsJobText reports the commands that read a job's text content: the batch
// tar stream, or the single-file cat it replaced.
func readsJobText(name string, args []string) bool {
	if name == "cat" {
		return true
	}
	for _, arg := range args {
		if arg == "-cf" {
			return true
		}
	}
	return false
}

// (c) A file whose size changed between the listing and the read keeps the job
// as job-changed, exactly as the single-file read did.
func TestArchiveTextReadRejectsFileChangedSinceListing(t *testing.T) {
	e := setup(t)
	dir := batchJob(t, e.jobs, "wk-changed", 3)
	shim := &changeBeforeTextRead{Runner: e.run, source: filepath.Join(dir, "note-0001.md"), body: []byte("# note 1\nbody 1 grew\n")}
	a := archiver(e, "")
	a.Runner = shim
	_, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-changed")
	var s *Skip
	if !errors.As(err, &s) || s.Reason != "job-changed" {
		t.Fatalf("plan err=%v, want skipped job-changed", err)
	}
	if !shim.fired {
		t.Fatal("fixture never changed the source before the read")
	}
}

// (d) A secret-like body is excluded with today's reason and the other text
// files of the same job are still archived.
func TestArchiveTextReadExcludesSecretAndArchivesTheRest(t *testing.T) {
	e := setup(t)
	dir := batchJob(t, e.jobs, "wk-secret", 3)
	secret := "TO" + "KEN=" + strings.Repeat("s", 24) + "\n"
	write(t, filepath.Join(dir, "note-0001.md"), secret)
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-secret")
	if err != nil {
		t.Fatal(err)
	}
	want := "events/00001-job.task_link.json,events/00005-job.completed.json,note-0000.md,note-0002.md"
	if strings.Join(p.Text, ",") != want {
		t.Fatalf("text = %v, want %s", p.Text, want)
	}
	found := false
	for _, x := range p.ExcludedEntries {
		if x.Path == "note-0001.md" {
			found = x.Reason == "secret-like:token_assignment"
		}
	}
	if !found {
		t.Fatalf("secret exclusion missing: %+v", p.ExcludedEntries)
	}
	if _, ok := p.textSnapshots["note-0001.md"]; ok {
		t.Fatal("secret body kept as a snapshot")
	}
	for _, name := range p.Move {
		if name == "note-0001.md" {
			t.Fatal("secret file planned for Drive")
		}
	}
	sources := map[string][]byte{}
	for _, rel := range []string{"note-0000.md", "note-0002.md"} {
		body, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		sources[rel] = body
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(res.Dest, "note-0001.md")); !os.IsNotExist(err) {
		t.Fatalf("secret copied to drive: %v", err)
	}
	for rel, src := range sources {
		body, err := os.ReadFile(filepath.Join(res.Dest, rel))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if string(body) != string(src) {
			t.Fatalf("%s drive copy=%q, source=%q", rel, body, src)
		}
	}
}

// (e) Names with spaces, unicode and leading dashes round-trip through both
// the batch read and the batch write.
func TestArchiveTextReadRoundTripsAwkwardNames(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-names")
	files := map[string]string{
		"notes with space.md":    "# space\n",
		"보고서.md":                 "# unicode\n",
		"-leading-dash.md":       "# dash\n",
		"--two-dashes.md":        "# two\n",
		"dir with space/파일.json": `{"k":"v"}`,
		"sub/-dash.json":         `{"d":1}`,
	}
	for name, body := range files {
		write(t, filepath.Join(dir, name), body)
	}
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-names")
	if err != nil {
		t.Fatal(err)
	}
	text := map[string]bool{}
	for _, name := range p.Text {
		text[name] = true
	}
	for name := range files {
		if !text[name] {
			t.Fatalf("%q not read as text: %v", name, p.Text)
		}
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range files {
		body, err := os.ReadFile(filepath.Join(res.Dest, name))
		if err != nil || string(body) != want {
			t.Fatalf("%q drive copy=%q err=%v, want %q", name, body, err, want)
		}
	}
}

// (f) An empty text file and a file of exactly TextMax bytes are archived as
// today; one byte over the cap is copied to Drive without being read.
func TestArchiveTextReadKeepsEmptyAndCappedFiles(t *testing.T) {
	e := setup(t)
	dir := filepath.Join(e.jobs, "wk-capped")
	exact := strings.Repeat("e", 1024)
	write(t, filepath.Join(dir, "empty.md"), "")
	write(t, filepath.Join(dir, "exact.md"), exact)
	write(t, filepath.Join(dir, "over.md"), exact+"x")
	write(t, filepath.Join(dir, "events", "00001-job.task_link.json"), `{"payload":{"task_id":2402}}`)
	finishJob(t, dir)
	a := archiver(e, "")
	p, err := a.Plan(context.Background(), reap.Host{Name: "h1", Jobs: e.jobs}, "wk-capped")
	if err != nil {
		t.Fatal(err)
	}
	text := map[string]bool{}
	for _, name := range p.Text {
		text[name] = true
	}
	if !text["empty.md"] || !text["exact.md"] || text["over.md"] {
		t.Fatalf("text = %v", p.Text)
	}
	if body, ok := p.textSnapshots["empty.md"]; !ok || len(body) != 0 {
		t.Fatalf("empty snapshot=%q present=%v", body, ok)
	}
	if body := p.textSnapshots["exact.md"]; string(body) != exact {
		t.Fatalf("capped snapshot is %d bytes", len(body))
	}
	res, err := a.Apply(context.Background(), p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"empty.md": "", "exact.md": exact, "over.md": exact + "x"} {
		body, err := os.ReadFile(filepath.Join(res.Dest, name))
		if err != nil || string(body) != want {
			t.Fatalf("%q drive copy=%d bytes err=%v, want %d", name, len(body), err, len(want))
		}
	}
}
