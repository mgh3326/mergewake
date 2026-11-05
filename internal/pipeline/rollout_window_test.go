package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/manifest"
)

func TestRolloutWindowSerializesServices(t *testing.T) {
	state := t.TempDir()
	hk, unlock, e := lockRolloutWindow(state, "control-plane", "example/task-service")
	if e != nil {
		t.Fatal(e)
	}
	if e = hk.begin(shaA); e != nil {
		t.Fatal(e)
	}
	if _, _, e = lockRolloutWindow(state, "control-plane", "example/hub"); e == nil {
		t.Fatal("concurrent hub restart admitted")
	}
	// A returned failure releases the lock but retains the durable owner.
	unlock()
	if _, _, e = lockRolloutWindow(state, "control-plane", "example/hub"); e == nil {
		t.Fatal("unhealthy predecessor admitted hub")
	}
	recovered, unlock, e := lockRolloutWindow(state, "control-plane", "example/task-service")
	if e != nil {
		t.Fatal(e)
	}
	if e = recovered.healthy(); e != nil {
		t.Fatal(e)
	}
	unlock()
	hub, unlock, e := lockRolloutWindow(state, "control-plane", "example/hub")
	if e != nil {
		t.Fatal(e)
	}
	defer unlock()
	if e = hub.begin(shaB); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(state, "rollout-windows/control-plane/active.json")); e != nil {
		t.Fatal(e)
	}
}

type windowBlockingGH struct {
	*fakeGH
	entered, proceed chan struct{}
}

func (g windowBlockingGH) Latest(context.Context, string) (manifest.Manifest, error) {
	close(g.entered)
	<-g.proceed
	return g.m, nil
}
func TestRolloutWindowProductionEntry(t *testing.T) {
	s, g, _, _, r := setup(t)
	r.RolloutGroup = "control-plane"
	r.Root = t.TempDir()
	r.Smoke.Command = "smoke"
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	s.Runner = smokeRunner{}
	block := windowBlockingGH{g, make(chan struct{}), make(chan struct{})}
	s.GH = block
	other := *s
	otherRepo := r
	otherRepo.Name = "example/hub"
	otherRepo.Root = t.TempDir()
	otherGH := *g
	otherGH.m = m
	otherGH.m.Repo = otherRepo.Name
	other.GH = &otherGH
	done := make(chan error, 1)
	go func() { done <- s.Rollout(context.Background(), r, Options{}) }()
	<-block.entered
	concurrent := other.Rollout(context.Background(), otherRepo, Options{})
	close(block.proceed)
	first := <-done
	if concurrent == nil || first != nil {
		t.Fatalf("first=%v concurrent=%v", first, concurrent)
	}
	if e = other.Rollout(context.Background(), otherRepo, Options{}); e != nil {
		t.Fatalf("healthy predecessor did not release window: %v", e)
	}
	if (config.Repo{Name: "mgh3326/handoffkeep"}).RolloutWindowGroup() != (config.Repo{Name: "mgh3326/panewire"}).RolloutWindowGroup() {
		t.Fatal("control-plane peers can overlap")
	}
}

type windowUnhealthyRunner struct{}

func (windowUnhealthyRunner) Run(context.Context, string, []string, string, string) ([]byte, error) {
	return nil, errors.New("service unhealthy")
}
func TestRolloutWindowCurrentMustSmoke(t *testing.T) {
	s, g, _, _, r := setup(t)
	r.RolloutGroup = "control-plane"
	r.Root = t.TempDir()
	r.Smoke.Command = "readiness"
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	s.Runner = smokeRunner{}
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	active := filepath.Join(s.Config.StateDir, "rollout-windows/control-plane/active.json")
	if e = fsutil.WriteJSON(active, windowReceipt{r.Name, shaB}); e != nil {
		t.Fatal(e)
	}
	s.Runner = windowUnhealthyRunner{}
	if e = s.Rollout(context.Background(), r, Options{}); e == nil {
		t.Fatal("unhealthy current service cleared window")
	}
	if _, e = os.Stat(active); e != nil {
		t.Fatal("uncertain window owner removed")
	}
	if _, _, e = lockRolloutWindow(s.Config.StateDir, "control-plane", "example/hub"); e == nil {
		t.Fatal("peer admitted after unhealthy current service")
	}
}

func TestRolloutWindowCaseInsensitivePeers(t *testing.T) {
	for _, repo := range []string{"mgh3326/handoffkeep", "mgh3326/Handoffkeep", "MGH3326/panewire", "mgh3326/PANEWIRE"} {
		if got := (config.Repo{Name: repo, RolloutGroup: "other"}).RolloutWindowGroup(); got != "control-plane" {
			t.Fatalf("%s bypassed reserved group: %q", repo, got)
		}
	}
}

func TestAdoptCurrentContainerEstablishesReviewedInventory(t *testing.T) {
	s, g, _, _, r := setup(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer server.Close()
	digest := "sha256:" + strings.Repeat("b", 64)
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api", Runtime: "docker", Health: &config.Health{URL: server.URL, Status: 200, Timeout: "1s", Retries: 1}}
	r.Install = []config.Install{spec}
	r.Root = ""
	r.Smoke = config.Command{}
	r.RolloutGroup = "control-plane"
	pinned := spec
	pinned.Digest = digest
	m, e := manifest.Build(r.Name, shaB, []config.Install{pinned}, nil)
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	runtime := newDockerCLI()
	ref := spec.Image + "@" + digest
	runtime.containers[spec.Name] = ref
	runtime.running[spec.Name] = true
	runtime.add(ref)
	s.Runner = runtime
	if e = s.Rollout(context.Background(), r, Options{Adopt: "api"}); e != nil {
		t.Fatal(e)
	}
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatalf("approved already-current adoption could not bootstrap: %v", e)
	}
	if _, e = os.Stat(filepath.Join(s.Config.StateDir, "rollout-windows/control-plane/active.json")); !os.IsNotExist(e) {
		t.Fatal("healthy adoption retained availability window")
	}
}
