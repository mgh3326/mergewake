package rollout

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
)

var oldDigest = "sha256:" + strings.Repeat("a", 64)
var newDigest = "sha256:" + strings.Repeat("b", 64)
var olderDigest = "sha256:" + strings.Repeat("c", 64)
var staleDigest = "sha256:" + strings.Repeat("d", 64)

func containerSpec() config.Install {
	keep := 1
	return config.Install{
		Kind:    "container",
		Target:  "api",
		Image:   "registry.example/api",
		Name:    "fleet-api",
		Ports:   []string{"127.0.0.1:8080:8080"},
		Restart: "unless-stopped",
		Health:  &config.Health{URL: "http://127.0.0.1:8080/healthz", Status: 200, Timeout: "1s", Retries: 2},
		Keep:    &keep,
		Runtime: "docker",
	}
}
func containerManifest(t *testing.T, commit, digest string) manifest.Manifest {
	t.Helper()
	spec := containerSpec()
	spec.Digest = digest
	m, e := manifest.Build("example/project", commit, []config.Install{spec}, nil)
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func runtimeFor(rt Runtime) RuntimeFor {
	return func(config.Install) Runtime { return rt }
}
func healthy(context.Context, config.Health) error { return nil }

type fakeContainer struct {
	digest  string
	running bool
	id      string
}
type fakeRuntime struct {
	containers map[string]*fakeContainer
	images     []Image
	pulls      []string
	runs       []RunSpec
	removals   []string
	failPull   error
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{containers: map[string]*fakeContainer{}}
}
func (f *fakeRuntime) Pull(_ context.Context, ref string) error {
	if f.failPull != nil {
		return f.failPull
	}
	f.pulls = append(f.pulls, ref)
	d := ref[strings.Index(ref, "@")+1:]
	for _, img := range f.images {
		if img.Digest == d {
			return nil
		}
	}
	f.images = append(f.images, Image{ID: "img-" + d, Digest: d, Ref: ref})
	return nil
}
func (f *fakeRuntime) Inspect(_ context.Context, name, repo string) (ContainerState, error) {
	c, ok := f.containers[name]
	if !ok {
		return ContainerState{}, nil
	}
	return ContainerState{Exists: true, Running: c.running, Ref: repo + "@" + c.digest, ID: c.id, Digest: c.digest}, nil
}
func (f *fakeRuntime) Stop(_ context.Context, name string) error {
	c, ok := f.containers[name]
	if !ok {
		return errors.New("no such container")
	}
	c.running = false
	return nil
}
func (f *fakeRuntime) Start(_ context.Context, name string) error {
	c, ok := f.containers[name]
	if !ok {
		return errors.New("no such container")
	}
	c.running = true
	return nil
}
func (f *fakeRuntime) Rename(_ context.Context, from, to string) error {
	c, ok := f.containers[from]
	if !ok {
		return errors.New("no such container")
	}
	if _, taken := f.containers[to]; taken {
		return errors.New("container name already in use")
	}
	delete(f.containers, from)
	f.containers[to] = c
	return nil
}
func (f *fakeRuntime) Run(_ context.Context, spec RunSpec) error {
	if _, taken := f.containers[spec.Name]; taken {
		return errors.New("container name already in use")
	}
	d := spec.Image[strings.Index(spec.Image, "@")+1:]
	f.runs = append(f.runs, spec)
	f.containers[spec.Name] = &fakeContainer{digest: d, running: true, id: "img-" + d}
	return nil
}
func (f *fakeRuntime) Remove(_ context.Context, name string) error {
	delete(f.containers, name)
	return nil
}
func (f *fakeRuntime) Images(_ context.Context, repo string) ([]Image, error) {
	var out []Image
	for _, img := range f.images {
		if strings.HasPrefix(img.Ref, repo+"@") {
			out = append(out, img)
		}
	}
	return out, nil
}
func (f *fakeRuntime) InUse(context.Context) ([]string, error) {
	var ids []string
	for _, c := range f.containers {
		ids = append(ids, c.id)
	}
	return ids, nil
}
func (f *fakeRuntime) RemoveImage(_ context.Context, ref string) error {
	f.removals = append(f.removals, ref)
	for i, img := range f.images {
		if img.Ref == ref {
			f.images = append(f.images[:i], f.images[i+1:]...)
			break
		}
	}
	return nil
}

func seedPrior(t *testing.T, state string, commit string) []byte {
	t.Helper()
	spec := containerSpec()
	prior := ContainerReceipt{Host: "host-a", Repo: "example/project", Commit: commit, Targets: map[string]ContainerTargetState{
		spec.Target: {Image: spec.Image, Digest: oldDigest, Previous: []string{olderDigest}},
	}}
	if e := writeJSON(filepath.Join(state, ContainerReceiptName), prior); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(state, ContainerReceiptName))
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestContainerDeploySuccessAndGarbageCollection(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	rt.images = []Image{
		{ID: "img-" + oldDigest, Digest: oldDigest, Ref: spec.Image + "@" + oldDigest},
		{ID: "img-" + olderDigest, Digest: olderDigest, Ref: spec.Image + "@" + olderDigest},
		{ID: "img-" + staleDigest, Digest: staleDigest, Ref: spec.Image + "@" + staleDigest},
	}
	seedPrior(t, state, strings.Repeat("d", 40))
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	res, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e != nil || res.Status != "success" {
		t.Fatalf("%+v %v", res, e)
	}
	receipt, _, e := LoadContainerReceipt(state, "host-a", "example/project")
	if e != nil {
		t.Fatal(e)
	}
	rec := receipt.Targets[spec.Target]
	if rec.Digest != newDigest || !reflect.DeepEqual(rec.Previous, []string{oldDigest}) || receipt.Commit != m.Commit {
		t.Fatalf("receipt %+v", receipt)
	}
	c := rt.containers[spec.Name]
	if c == nil || c.digest != newDigest || !c.running {
		t.Fatalf("running container %+v", c)
	}
	if _, ok := rt.containers[spec.Name+containerPreservedSuffix]; ok {
		t.Fatal("preserved container left behind")
	}
	want := []string{spec.Image + "@" + olderDigest, spec.Image + "@" + staleDigest}
	if !reflect.DeepEqual(rt.removals, want) {
		t.Fatalf("gc removals %v", rt.removals)
	}
	if !reflect.DeepEqual(res.Removed[spec.Target], []string{olderDigest, staleDigest}) {
		t.Fatalf("reported removals %+v", res.Removed)
	}
	if !reflect.DeepEqual(res.Digests, map[string]string{spec.Target: spec.Image + "@" + newDigest}) {
		t.Fatalf("digests %+v", res.Digests)
	}
	pulls := len(rt.pulls)
	res, e = ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e != nil || res.Status != "current" || len(rt.pulls) != pulls {
		t.Fatalf("current poll %+v %v pulls=%d", res, e, len(rt.pulls))
	}
}

func TestContainerGarbageCollectionKeepsUnselectedTargetDigests(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	rt.images = []Image{
		{ID: "img-" + oldDigest, Digest: oldDigest, Ref: spec.Image + "@" + oldDigest},
		{ID: "img-" + olderDigest, Digest: olderDigest, Ref: spec.Image + "@" + olderDigest},
		{ID: "img-" + staleDigest, Digest: staleDigest, Ref: spec.Image + "@" + staleDigest},
	}
	// A second target shares the repository but is not part of this deploy and
	// has no container now, so only its receipt protects its digest.
	prior := ContainerReceipt{Host: "host-a", Repo: "example/project", Commit: strings.Repeat("d", 40), Targets: map[string]ContainerTargetState{
		spec.Target: {Image: spec.Image, Digest: oldDigest, Previous: []string{olderDigest}},
		"worker":    {Image: spec.Image, Digest: staleDigest},
	}}
	if e := writeJSON(filepath.Join(state, ContainerReceiptName), prior); e != nil {
		t.Fatal(e)
	}
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	res, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e != nil || res.Status != "success" {
		t.Fatalf("%+v %v", res, e)
	}
	want := []string{spec.Image + "@" + olderDigest}
	if !reflect.DeepEqual(rt.removals, want) {
		t.Fatalf("gc removed another target's receipt digest: %v", rt.removals)
	}
}

func TestContainerHealthFailureRollsBack(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	rt.images = []Image{{ID: "img-" + oldDigest, Digest: oldDigest, Ref: spec.Image + "@" + oldDigest}}
	priorBytes := seedPrior(t, state, strings.Repeat("d", 40))
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	calls := 0
	health := func(context.Context, config.Health) error {
		calls++
		if calls == 1 {
			return errors.New("unhealthy")
		}
		return nil
	}
	res, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), health)
	if e == nil || res.Status != "rolled-back" {
		t.Fatalf("%+v %v", res, e)
	}
	c := rt.containers[spec.Name]
	if c == nil || c.digest != oldDigest || !c.running {
		t.Fatalf("restored container %+v", c)
	}
	if _, ok := rt.containers[spec.Name+containerPreservedSuffix]; ok {
		t.Fatal("preserved container left behind after rollback")
	}
	restored, e := os.ReadFile(filepath.Join(state, ContainerReceiptName))
	if e != nil || string(restored) != string(priorBytes) {
		t.Fatalf("receipt not restored: %s %v", restored, e)
	}
	if _, e = os.Stat(filepath.Join(state, containerRejectName)); e != nil {
		t.Fatal("missing rejection receipt", e)
	}
	if _, e = os.Stat(filepath.Join(state, containerJournalName)); !os.IsNotExist(e) {
		t.Fatal("journal not removed after successful rollback")
	}
	res, e = ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e == nil || res.Status != "rejected" {
		t.Fatalf("rejected digest retried: %+v %v", res, e)
	}
}

func TestContainerDigestFlagRequiresSingleTarget(t *testing.T) {
	api, worker := containerSpec(), containerSpec()
	worker.Target, worker.Name = "worker", "fleet-worker"
	m, e := manifest.Build("example/project", strings.Repeat("e", 40), []config.Install{api, worker}, nil)
	if e != nil {
		t.Fatal(e)
	}
	rt := newFakeRuntime()
	state := t.TempDir()
	_, e = ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{api, worker}, Digest: api.Image + "@" + newDigest}, "host-a", runtimeFor(rt), healthy)
	if e == nil || !strings.Contains(e.Error(), "exactly one") || len(rt.pulls) != 0 {
		t.Fatalf("ambiguous digest flag accepted: %v pulls=%v", e, rt.pulls)
	}
}

func TestContainerAdoptRefusesPendingJournal(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	j := containerJournal{Repo: "example/project", Host: "host-a", Commit: strings.Repeat("e", 40), Order: []string{spec.Target}, Targets: map[string]containerMutation{
		spec.Target: {Target: spec.Target, Name: spec.Name, PrevName: spec.Name + containerPreservedSuffix, Recipe: containerRecipe(spec), NewDigest: newDigest, PreviousDigest: oldDigest},
	}}
	if e := writeJSON(filepath.Join(state, containerJournalName), j); e != nil {
		t.Fatal(e)
	}
	if _, e := AdoptContainer(context.Background(), spec, "host-a", "example/project", Options{StateDir: state}, rt); e == nil || !strings.Contains(e.Error(), "journal") {
		t.Fatalf("adopt accepted a pending journal: %v", e)
	}
	if _, e := os.Stat(filepath.Join(state, ContainerReceiptName)); !os.IsNotExist(e) {
		t.Fatal("refused adopt wrote a receipt")
	}
}

func TestContainerAdopt(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	rt.images = []Image{{ID: "img-" + oldDigest, Digest: oldDigest, Ref: spec.Image + "@" + oldDigest}}
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	if _, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy); e == nil || !strings.Contains(e.Error(), "unmanaged") {
		t.Fatalf("unmanaged container was replaced: %v", e)
	}
	res, e := AdoptContainer(context.Background(), spec, "host-a", "example/project", Options{StateDir: state}, rt)
	if e != nil || res.Status != "adopted" {
		t.Fatalf("%+v %v", res, e)
	}
	receipt, _, e := LoadContainerReceipt(state, "host-a", "example/project")
	if e != nil || receipt.Targets[spec.Target].Digest != oldDigest || !receipt.Targets[spec.Target].Adopted {
		t.Fatalf("adopted receipt %+v %v", receipt, e)
	}
	res, e = ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e != nil || res.Status != "success" {
		t.Fatalf("%+v %v", res, e)
	}
	receipt, _, _ = LoadContainerReceipt(state, "host-a", "example/project")
	if !reflect.DeepEqual(receipt.Targets[spec.Target].Previous, []string{oldDigest}) {
		t.Fatalf("adopted digest was not the rollback source: %+v", receipt.Targets[spec.Target])
	}
}
func TestContainerAdoptDryRunAndDrift(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	res, e := AdoptContainer(context.Background(), spec, "host-a", "example/project", Options{StateDir: state, DryRun: true}, rt)
	if e != nil || res.Status != "dry-run" {
		t.Fatalf("%+v %v", res, e)
	}
	if _, e = os.Stat(filepath.Join(state, ContainerReceiptName)); !os.IsNotExist(e) {
		t.Fatal("dry-run adopt wrote a receipt")
	}
	seedPrior(t, state, strings.Repeat("d", 40))
	rt.containers[spec.Name].digest = newDigest
	m := containerManifest(t, strings.Repeat("e", 40), olderDigest)
	_, e = ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e == nil || !strings.Contains(e.Error(), "drift") || len(rt.pulls) != 0 || len(rt.runs) != 0 {
		t.Fatalf("drift not refused: %v pulls=%v runs=%v", e, rt.pulls, rt.runs)
	}
	rt.containers[spec.Name].digest = oldDigest
	rt.containers[spec.Name].running = false
	if _, e = ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy); e == nil || !strings.Contains(e.Error(), "not running") {
		t.Fatalf("stopped container not refused: %v", e)
	}
	rt.containers[spec.Name].running = true
	seedPrior(t, state, strings.Repeat("d", 40))
	if e = os.Remove(filepath.Join(state, containerRejectName)); e != nil && !os.IsNotExist(e) {
		t.Fatal(e)
	}
	res, e = AdoptContainer(context.Background(), spec, "host-a", "example/project", Options{StateDir: state}, rt)
	if e != nil || res.Status != "current" {
		t.Fatalf("adopt of the recorded digest: %+v %v", res, e)
	}
}

func TestContainerDenyAndDryRun(t *testing.T) {
	spec := containerSpec()
	state := filepath.Join(t.TempDir(), "state")
	rt := newFakeRuntime()
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	res, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}, Deny: []string{"api"}}, "host-a", runtimeFor(rt), healthy)
	if e == nil || !reflect.DeepEqual(res.Denied, []string{"api"}) || len(rt.pulls) != 0 {
		t.Fatalf("denied target was fetched: %+v %v", res, e)
	}
	nodigest := containerManifest(t, strings.Repeat("e", 40), "")
	res, e = ApplyContainer(context.Background(), nodigest, Options{StateDir: state, Allowed: []config.Install{spec}, DryRun: true, Digest: spec.Image + "@" + newDigest}, "host-a", runtimeFor(rt), healthy)
	if e != nil || res.Status != "dry-run" || res.Digests[spec.Target] != spec.Image+"@"+newDigest {
		t.Fatalf("%+v %v", res, e)
	}
	if len(rt.pulls) != 0 || len(rt.runs) != 0 {
		t.Fatal("dry-run pulled or started a container")
	}
	if _, e = os.Stat(state); !os.IsNotExist(e) {
		t.Fatal("dry-run wrote state")
	}
	if _, e = ApplyContainer(context.Background(), nodigest, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy); e == nil || !strings.Contains(e.Error(), "digest") {
		t.Fatal("mutable tag was accepted")
	}
}

func TestContainerCrashRecovery(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: newDigest, running: true, id: "img-" + newDigest}
	rt.containers[spec.Name+containerPreservedSuffix] = &fakeContainer{digest: oldDigest, running: false, id: "img-" + oldDigest}
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	j := containerJournal{Repo: m.Repo, Host: "host-a", Commit: m.Commit, Order: []string{spec.Target}, Targets: map[string]containerMutation{
		spec.Target: {Target: spec.Target, Name: spec.Name, PrevName: spec.Name + containerPreservedSuffix, Recipe: containerRecipe(spec), NewDigest: newDigest, PreviousDigest: oldDigest},
	}}
	if e := writeJSON(filepath.Join(state, containerJournalName), j); e != nil {
		t.Fatal(e)
	}
	res, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e == nil || res.Status != "rolled-back" {
		t.Fatalf("%+v %v", res, e)
	}
	c := rt.containers[spec.Name]
	if c == nil || c.digest != oldDigest || !c.running {
		t.Fatalf("recovered container %+v", c)
	}
	if _, e = os.Stat(filepath.Join(state, containerJournalName)); !os.IsNotExist(e) {
		t.Fatal("journal not cleared after recovery")
	}
}

func TestContainerCompletedJournalIsCleanedNotRolledBack(t *testing.T) {
	spec := containerSpec()
	state := t.TempDir()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: newDigest, running: true, id: "img-" + newDigest}
	rt.containers[spec.Name+containerPreservedSuffix] = &fakeContainer{digest: oldDigest, running: false, id: "img-" + oldDigest}
	m := containerManifest(t, strings.Repeat("e", 40), newDigest)
	receipt := ContainerReceipt{Host: "host-a", Repo: m.Repo, Commit: m.Commit, Targets: map[string]ContainerTargetState{
		spec.Target: {Image: spec.Image, Digest: newDigest, Previous: []string{oldDigest}},
	}}
	if e := writeJSON(filepath.Join(state, ContainerReceiptName), receipt); e != nil {
		t.Fatal(e)
	}
	j := containerJournal{Repo: m.Repo, Host: "host-a", Commit: m.Commit, ReceiptExisted: true, Order: []string{spec.Target}, Targets: map[string]containerMutation{
		spec.Target: {Target: spec.Target, Name: spec.Name, PrevName: spec.Name + containerPreservedSuffix, Recipe: containerRecipe(spec), NewDigest: newDigest, PreviousDigest: oldDigest},
	}}
	if e := writeJSON(filepath.Join(state, containerJournalName), j); e != nil {
		t.Fatal(e)
	}
	res, e := ApplyContainer(context.Background(), m, Options{StateDir: state, Allowed: []config.Install{spec}}, "host-a", runtimeFor(rt), healthy)
	if e != nil || res.Status != "current" {
		t.Fatalf("%+v %v", res, e)
	}
	if _, ok := rt.containers[spec.Name+containerPreservedSuffix]; ok {
		t.Fatal("completed deploy removed the running container instead of the preserved one")
	}
	if rt.containers[spec.Name].digest != newDigest {
		t.Fatal("completed deploy was rolled back")
	}
}

func TestCheckHealth(t *testing.T) {
	old := healthInterval
	healthInterval = time.Millisecond
	defer func() { healthInterval = old }()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if e := CheckHealth(context.Background(), config.Health{URL: srv.URL, Status: 204, Timeout: "1s", Retries: 3}); e != nil {
		t.Fatal(e)
	}
	if e := CheckHealth(context.Background(), config.Health{URL: srv.URL, Status: 200, Timeout: "1s", Retries: 2}); e == nil || !strings.Contains(e.Error(), "2 attempts") {
		t.Fatalf("unexpected status accepted: %v", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := CheckHealth(ctx, config.Health{URL: srv.URL, Status: 204, Timeout: "1s", Retries: 3}); e == nil {
		t.Fatal("cancelled health check passed")
	}
}

func TestContainerStatus(t *testing.T) {
	spec := containerSpec()
	rt := newFakeRuntime()
	rt.containers[spec.Name] = &fakeContainer{digest: oldDigest, running: true, id: "img-" + oldDigest}
	rt.images = []Image{
		{ID: "img-" + oldDigest, Digest: oldDigest, Ref: spec.Image + "@" + oldDigest},
		{ID: "img-" + olderDigest, Digest: olderDigest, Ref: spec.Image + "@" + olderDigest},
		{ID: "img-" + staleDigest, Digest: staleDigest, Ref: spec.Image + "@" + staleDigest},
	}
	rec := ContainerTargetState{Image: spec.Image, Digest: oldDigest, Previous: []string{olderDigest, staleDigest}}
	st, e := InspectContainerStatus(context.Background(), spec, rec, rt, healthy)
	if e != nil || st.Status != "healthy" || !st.Running || st.Health != "healthy" {
		t.Fatalf("%+v %v", st, e)
	}
	if !reflect.DeepEqual(st.ImagesKept, []string{oldDigest, olderDigest}) {
		t.Fatalf("kept images %v", st.ImagesKept)
	}
	rt.containers[spec.Name].digest = newDigest
	st, e = InspectContainerStatus(context.Background(), spec, rec, rt, healthy)
	if e == nil || st.Status != "drift" {
		t.Fatalf("drift not reported: %+v %v", st, e)
	}
	rt.containers[spec.Name].digest = oldDigest
	st, e = InspectContainerStatus(context.Background(), spec, rec, rt, func(context.Context, config.Health) error { return errors.New("down") })
	if e == nil || st.Status != "unhealthy" {
		t.Fatalf("unhealthy not reported: %+v %v", st, e)
	}
}

func TestResolveContainerDigest(t *testing.T) {
	image := "registry.example/api"
	for _, tt := range []struct {
		flag, pinned, want string
		fail               bool
	}{
		{"", "", "", true},
		{"", newDigest, newDigest, false},
		{image + "@" + newDigest, "", newDigest, false},
		{newDigest, "", newDigest, false},
		{"other.example/api@" + newDigest, "", "", true},
		{"main", "", "", true},
		{image + ":v1", "", "", true},
	} {
		got, e := ResolveContainerDigest(image, tt.flag, tt.pinned)
		if (e != nil) != tt.fail || got != tt.want {
			t.Fatalf("%q %q: %q %v", tt.flag, tt.pinned, got, e)
		}
	}
}

type recordRunner struct {
	calls [][]string
	out   map[string]string
}

func (r *recordRunner) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{command}, args...))
	key := strings.Join(args, " ")
	for prefix, out := range r.out {
		if strings.HasPrefix(key, prefix) {
			return []byte(out), nil
		}
	}
	return nil, nil
}

func TestRuntimeCLIArguments(t *testing.T) {
	spec := containerSpec()
	spec.EnvFile = "/etc/fleet/api.env"
	spec.Volumes = []string{"/srv/api:/data:ro"}
	spec.User = "1000:1000"
	r := &recordRunner{out: map[string]string{
		"inspect --format {{.State.Running}}": "true\tregistry.example/api@sha256:" + strings.Repeat("a", 64) + "\timg-a\n",
		"images --all":                        "img-a\n",
		"image inspect --format":              "img-a\t2026-01-01T00:00:00Z\tregistry.example/api@sha256:" + strings.Repeat("a", 64) + ",\n",
		"ps --all":                            "abc\n",
		"inspect --format {{.Image}}":         "img-a\n",
	}}
	c := RuntimeCLI{Runner: r, Runtime: "podman"}
	if e := c.Pull(context.Background(), spec.Image+"@"+oldDigest); e != nil {
		t.Fatal(e)
	}
	if e := c.Run(context.Background(), runSpec(spec, newDigest)); e != nil {
		t.Fatal(e)
	}
	joined := strings.Join(r.calls[len(r.calls)-1], " ")
	for _, want := range []string{"podman", "run --detach --name fleet-api", "--restart unless-stopped", "--publish 127.0.0.1:8080:8080", "--env-file /etc/fleet/api.env", "--volume /srv/api:/data:ro", "--user 1000:1000", spec.Image + "@" + newDigest} {
		if !strings.Contains(joined, want) {
			t.Fatalf("run args %q missing %q", joined, want)
		}
	}
	st, e := c.Inspect(context.Background(), spec.Name, spec.Image)
	if e != nil || !st.Exists || !st.Running || st.Digest != oldDigest {
		t.Fatalf("%+v %v", st, e)
	}
	images, e := c.Images(context.Background(), spec.Image)
	if e != nil || len(images) != 1 || images[0].Digest != oldDigest {
		t.Fatalf("%+v %v", images, e)
	}
	used, e := c.InUse(context.Background())
	if e != nil || len(used) != 1 || used[0] != "img-a" {
		t.Fatalf("%v %v", used, e)
	}
}
