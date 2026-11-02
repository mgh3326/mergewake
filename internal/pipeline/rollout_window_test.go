package pipeline

import (
	"context"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
	"os"
	"path/filepath"
	"testing"
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
