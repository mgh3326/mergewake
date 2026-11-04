package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

const repo = "mgh3326/brewdial"

func sha(name string) string {
	s := sha256.Sum256([]byte(name))
	return hex.EncodeToString(s[:20])
}

func digestOf(name string) string {
	s := sha256.Sum256([]byte("image:" + name))
	return "sha256:" + hex.EncodeToString(s[:])
}

func testSuite(t *testing.T) Suite {
	t.Helper()
	all, e := Suites()
	if e != nil {
		t.Fatal(e)
	}
	return all[repo]
}

// world is a fake GitHub + GHCR for one repo. Every write is logged in order.
type world struct {
	mu       sync.Mutex
	parents  map[string]string // first parent
	branches map[string]string
	tags     map[string]Tag // name -> object
	runs     map[string][]Run
	jobs     map[int64][]Job
	images   map[string]string // registry tag -> digest
	config   map[string]Image  // digest -> image
	log      []string
	down     bool // GitHub unreachable
	failNext string
	nextRun  int64
}

func newWorld() *world {
	return &world{parents: map[string]string{}, branches: map[string]string{}, tags: map[string]Tag{}, runs: map[string][]Run{}, jobs: map[int64][]Job{}, images: map[string]string{}, config: map[string]Image{}}
}

var errDown = errors.New("connection refused")

// merge appends commit name to main's first-parent history and publishes its image.
func (w *world) merge(name string) string {
	s := sha(name)
	w.parents[s] = w.branches["main"]
	w.branches["main"] = s
	w.images["sha-"+s[:7]] = digestOf(name)
	w.config[digestOf(name)] = Image{OS: "linux", Arch: "arm64", Labels: map[string]string{"org.opencontainers.image.revision": s}}
	// Mutable tags a consumer must never follow.
	w.images["latest"] = digestOf(name)
	w.images["production"] = digestOf(name)
	return s
}

// ci records the suite outcome for a commit: "pending", "green", "red", or
// "partial" (CI Gate green, Kotlin API CI absent).
func (w *world) ci(name, state string) {
	s := sha(name)
	w.runs[s] = nil
	for _, wf := range []Workflow{{Path: ".github/workflows/ci.yml", Jobs: []string{"Typecheck, Lint & Test"}}, {Path: ".github/workflows/ci-kt.yml", Jobs: []string{"API paths changed", "API tests (PostgreSQL 17)", "API tests (MySQL 8.4)", "Data copy (PostgreSQL <-> MySQL)", "API tests (PostgreSQL + MySQL)"}}} {
		if state == "partial" && wf.Path == ".github/workflows/ci-kt.yml" {
			continue
		}
		w.nextRun++
		status, concl := "completed", "success"
		switch state {
		case "pending":
			status, concl = "in_progress", ""
		case "red":
			concl = "failure"
		}
		w.runs[s] = append(w.runs[s], Run{ID: w.nextRun, WorkflowID: 100 + int64(len(w.runs[s])), Path: wf.Path, Event: "push", HeadBranch: "main", HeadSHA: s, Attempt: 1, Status: status, Conclusion: concl})
		var js []Job
		for _, j := range wf.Jobs {
			js = append(js, Job{Name: j, HeadSHA: s, RunAttempt: 1, Status: status, Conclusion: concl})
		}
		w.jobs[w.nextRun] = js
	}
}

func (w *world) check(op string) error {
	if w.down {
		return errDown
	}
	if w.failNext == op {
		w.failNext = ""
		return fmt.Errorf("%s failed", op)
	}
	return nil
}

func (w *world) Branch(_ context.Context, _, name string) (string, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.check("branch"); e != nil {
		return "", false, e
	}
	s, ok := w.branches[name]
	return s, ok, nil
}

func (w *world) PromotionTags(context.Context, string) ([]TagRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.check("tags"); e != nil {
		return nil, e
	}
	var out []TagRef
	for name, t := range w.tags {
		out = append(out, TagRef{Name: name, Object: sha("tagobj:" + name), Target: t.Target})
	}
	return out, nil
}

func (w *world) TagObject(_ context.Context, _ string, ref TagRef) (Tag, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.check("tagobject"); e != nil {
		return Tag{}, e
	}
	t, ok := w.tags[ref.Name]
	if !ok {
		return Tag{}, errors.New("404")
	}
	return t, nil
}

func (w *world) Parents(_ context.Context, _, s string) ([]string, error) {
	if e := w.check("parents"); e != nil {
		return nil, e
	}
	if p := w.parents[s]; p != "" {
		return []string{p}, nil
	}
	return nil, nil
}

func (w *world) PushRuns(_ context.Context, _, _, s string) ([]Run, error) {
	if e := w.check("runs"); e != nil {
		return nil, e
	}
	return w.runs[s], nil
}

func (w *world) Jobs(_ context.Context, _ string, id int64, _ int) ([]Job, error) {
	if e := w.check("jobs"); e != nil {
		return nil, e
	}
	return w.jobs[id], nil
}

func (w *world) CreateRecord(_ context.Context, _, name, target, msg string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.check("record"); e != nil {
		return e
	}
	if _, ok := w.tags[name]; ok {
		return errors.New("422 reference already exists")
	}
	w.tags[name] = Tag{Name: name, Target: target, TargetType: "commit", Message: msg}
	w.log = append(w.log, "record "+name+" "+target)
	return nil
}

func (w *world) CreateBranch(_ context.Context, _, name, s string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.check("pointer"); e != nil {
		return e
	}
	if _, ok := w.branches[name]; ok {
		return errors.New("422 exists")
	}
	w.branches[name] = s
	w.log = append(w.log, "pointer "+s)
	return nil
}

func (w *world) AdvanceBranch(_ context.Context, _, name, s string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.check("pointer"); e != nil {
		return e
	}
	// GitHub's force=false: the new commit must descend from the old one.
	old := w.branches[name]
	for c := s; ; c = w.parents[c] {
		if c == old {
			break
		}
		if c == "" {
			return errors.New("422 not a fast forward")
		}
	}
	w.branches[name] = s
	w.log = append(w.log, "pointer "+s)
	return nil
}

func (w *world) Resolve(_ context.Context, _, tag string) (string, error) {
	if w.down {
		return "", errDown
	}
	d, ok := w.images[tag]
	if !ok {
		return "", errors.New("manifest unknown")
	}
	return d, nil
}

func (w *world) Inspect(_ context.Context, _, d, _ string) (Image, error) {
	img, ok := w.config[d]
	if !ok {
		return Image{}, errors.New("manifest unknown")
	}
	return img, nil
}

type noLock struct{}

func (noLock) Lock(string) (func(), error) { return func() {}, nil }

func promoter(w *world) *Promoter {
	return &Promoter{GH: w, Registry: w, Lock: noLock{}, Now: func() time.Time { return time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC) }, Identity: "mergewake-promotion", Generation: 1}
}

func promote(t *testing.T, w *world, name string, seed bool) (Record, error) {
	t.Helper()
	return promoter(w).Promote(context.Background(), repo, testSuite(t), Options{SHA: sha(name), Seed: seed})
}

func admitted(t *testing.T, w *world) (Admission, error) {
	t.Helper()
	return Admit(context.Background(), w, repo, testSuite(t))
}

func mustAdmit(t *testing.T, w *world, name string) {
	t.Helper()
	a, e := admitted(t, w)
	if e != nil {
		t.Fatalf("admit: %v", e)
	}
	if a.SHA != sha(name) || a.Digest != digestOf(name) {
		t.Fatalf("admitted %s %s, want %s's commit and digest", a.SHA[:7], a.Digest, name)
	}
}

func mustRefuse(t *testing.T, e error, want string) {
	t.Helper()
	if e == nil {
		t.Fatalf("want refusal containing %q, got success", want)
	}
	if !strings.Contains(e.Error(), want) {
		t.Fatalf("refusal %q does not mention %q", e, want)
	}
}
