package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

type deployGH struct {
	*fakeGH
	paths    []policy.File
	rangeErr error
}

func (g deployGH) ChangedPaths(context.Context, string, string, string) ([]policy.File, error) {
	return g.paths, g.rangeErr
}

type deploySink struct {
	fakeSink
	approved  bool
	questions []string
}

func (d *deploySink) DeployDecision(_ context.Context, _ string, q string) (bool, error) {
	d.questions = append(d.questions, q)
	return d.approved, nil
}
func TestRolloutDeployApproval(t *testing.T) {
	for _, tc := range []struct {
		name, path, previous                           string
		approved, rangeError, wantDeploy, wantQuestion bool
	}{
		{"auto", "observability/log.go", shaA, false, false, true, false},
		{"explicit empty", "observability/log.go", shaA, false, false, true, false},
		{"hold", "orders/live.go", shaA, false, false, false, true},
		{"answered", "orders/live.go", shaA, true, false, true, true},
		{"unknown receipt", "observability/log.go", "", false, false, false, true},
		{"unknown range", "observability/log.go", shaA, false, true, false, true},
		{"env", "deploy/.env.production", shaA, false, false, false, true},
		{"secret", "deploy/secrets/key", shaA, false, false, false, true},
		{"secret file", "config/secrets.yaml", shaA, false, false, false, true},
		{"env file", "config/env.yaml", shaA, false, false, false, true},
		{"policy", ".mergewake/policy.yaml", shaA, false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.policyExtra = "deploy_approval:\n  - paths: ['orders/**']\n    reason: Live orders\n"
			if tc.name == "explicit empty" {
				g.policyExtra = "deploy_approval: []\n"
			}
			s.GH = deployGH{fakeGH: g, paths: []policy.File{{Path: tc.path, Kind: "modified"}}}
			if tc.rangeError {
				s.GH = deployGH{fakeGH: g, rangeErr: errors.New("unknown")}
			}
			sink := &deploySink{approved: tc.approved}
			s.Sink = sink
			r.Root = t.TempDir()
			r.Smoke.Command = "smoke"
			r.DeployTask = "1"
			r.Install = nil
			// Keep the real file installer and verify no target or receipt appears on hold.
			r.Install = append(r.Install, config.Install{Source: "tool", Target: "bin/tool", Mode: "0755"})
			m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
			if e != nil {
				t.Fatal(e)
			}
			g.m = m
			s.Runner = smokeRunner{}
			state := filepath.Join(s.state(r.Name), "rollout")

			if tc.previous != "" {
				if e := fsutil.WriteJSON(filepath.Join(state, "approved-inventory.json"), s.deploymentIdentity(r, m, Options{})); e != nil {
					t.Fatal(e)
				}
			}
			if tc.previous != "" {
				if e := fsutil.WriteJSON(filepath.Join(state, "installed.json"), map[string]any{"manifest": manifest.Manifest{Repo: r.Name, Commit: tc.previous}}); e != nil {
					t.Fatal(e)
				}
			}
			if e = s.Rollout(context.Background(), r, Options{}); e != nil {
				t.Fatal(e)
			}
			_, e = os.Stat(filepath.Join(r.Root, "bin/tool"))
			if (e == nil) != tc.wantDeploy {
				t.Fatalf("installed=%v want=%v", e == nil, tc.wantDeploy)
			}
			if (len(sink.questions) > 0) != tc.wantQuestion {
				t.Fatalf("questions %v", sink.questions)
			}
			if tc.wantQuestion && (!strings.Contains(sink.questions[0], shaB) || !strings.Contains(sink.questions[0], r.Name)) {
				t.Fatal("unbound question")
			}
		})
	}
}

func TestSameCommitDigestAndEnvDriftNeedsApproval(t *testing.T) {
	s, g, _, _, r := setup(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	old := "sha256:" + strings.Repeat("a", 64)
	next := "sha256:" + strings.Repeat("b", 64)
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api", Runtime: "docker", EnvFile: filepath.Join(t.TempDir(), "old.env"), Health: &config.Health{URL: srv.URL, Status: 200, Timeout: "1s", Retries: 1}}
	if e := os.WriteFile(spec.EnvFile, []byte("SETTING=one\n"), 0600); e != nil {
		t.Fatal(e)
	}
	r.Install = []config.Install{spec}
	r.Root = ""
	r.Smoke = config.Command{}
	r.DeployTask = "1"
	pinned := spec
	pinned.Digest = old
	m, e := manifest.Build(r.Name, shaB, []config.Install{pinned}, nil)
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	d := newDockerCLI()
	s.Runner = d
	sink := &deploySink{approved: true}
	s.Sink = sink
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	sink.approved = false
	sink.questions = nil
	r.Install[0].EnvFile = filepath.Join(t.TempDir(), "changed.env")
	if e := os.WriteFile(r.Install[0].EnvFile, []byte("SETTING=two\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = s.Rollout(context.Background(), r, Options{Digest: spec.Image + "@" + next}); e != nil {
		t.Fatal(e)
	}
	if d.containers[spec.Name] != spec.Image+"@"+old || len(sink.questions) == 0 {
		t.Fatalf("unapproved replacement=%q questions=%v", d.containers[spec.Name], sink.questions)
	}
}

type deployCheckGH struct {
	*fakeGH
	names []string
}

func (g *deployCheckGH) Check(_ context.Context, _, _ string, names []string) (github.CI, error) {
	g.names = names
	return github.Red, nil
}
func TestRolloutUsesMainChecks(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.policyExtra = "watch_checks: [main-full-suite]\n"
	adapter := &deployCheckGH{fakeGH: g}
	s.GH = adapter
	r.Root = t.TempDir()
	r.Smoke.Command = "smoke"
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	if e = s.Rollout(context.Background(), r, Options{}); e == nil {
		t.Fatal("red release CI admitted")
	}
	if len(adapter.names) != 1 || adapter.names[0] != "main-full-suite" {
		t.Fatalf("checks=%v", adapter.names)
	}
	if _, e = os.Stat(filepath.Join(r.Root, "bin/tool")); !os.IsNotExist(e) {
		t.Fatal("red CI mutated target")
	}
}

type approvalLockGH struct {
	*fakeGH
	entered, proceed chan struct{}
}

func (g approvalLockGH) Latest(context.Context, string) (manifest.Manifest, error) {
	close(g.entered)
	<-g.proceed
	return g.m, nil
}
func TestRolloutSerializesApprovalRange(t *testing.T) {
	s, g, _, _, r := setup(t)
	r.Root = t.TempDir()
	r.Smoke.Command = "smoke"
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	s.Runner = smokeRunner{}
	adapter := approvalLockGH{g, make(chan struct{}), make(chan struct{})}
	s.GH = adapter
	done := make(chan error, 1)
	go func() { done <- s.Rollout(context.Background(), r, Options{}) }()
	<-adapter.entered
	second := s.Rollout(context.Background(), r, Options{})
	close(adapter.proceed)
	first := <-done
	if second == nil || first != nil {
		t.Fatalf("first=%v concurrent=%v", first, second)
	}
}
func TestEnvContentDriftNeedsApproval(t *testing.T) {
	s, g, _, _, r := setup(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	old := "sha256:" + strings.Repeat("a", 64)
	envPath := filepath.Join(t.TempDir(), "service.env")
	if e := os.WriteFile(envPath, []byte("SETTING=one\n"), 0600); e != nil {
		t.Fatal(e)
	}
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api", Runtime: "docker", EnvFile: envPath, Health: &config.Health{URL: srv.URL, Status: 200, Timeout: "1s", Retries: 1}}
	r.Install = []config.Install{spec}
	r.Root = ""
	r.Smoke = config.Command{}
	r.DeployTask = "1"
	pinned := spec
	pinned.Digest = old
	m, e := manifest.Build(r.Name, shaB, []config.Install{pinned}, nil)
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	d := newDockerCLI()
	s.Runner = d
	sink := &deploySink{approved: true}
	s.Sink = sink
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	sink.approved = false
	sink.questions = nil
	if e := os.WriteFile(envPath, []byte("SETTING=two\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if d.containers[spec.Name] != spec.Image+"@"+old || len(sink.questions) == 0 {
		t.Fatalf("unapproved replacement=%q questions=%v", d.containers[spec.Name], sink.questions)
	}
}

func TestNoEnvInventoryIdentityCompatibility(t *testing.T) {
	s, _, _, _, r := setup(t)
	b, e := json.Marshal(struct {
		Host, Repo, Policy, Root string
		Deny                     []string
		Install                  []config.Install
		Smoke                    config.Command
		Timeout                  string
	}{s.Config.Host, r.Name, r.Policy, r.Root, r.Deny, r.Install, r.Smoke, r.SmokeTimeout})
	if e != nil {
		t.Fatal(e)
	}
	got := s.deploymentIdentity(r, manifest.Manifest{}, Options{})
	if got.Config != manifest.Sum(b) || !got.EnvironmentKnown {
		t.Fatal("no-env upgrade changed deployed inventory identity")
	}
}

func TestUnknownEnvIdentityCannotUseAnsweredApproval(t *testing.T) {
	for _, kind := range []string{"missing", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _, _, r := setup(t)
			envPath := filepath.Join(t.TempDir(), "service.env")
			if kind == "oversized" {
				if err := os.WriteFile(envPath, make([]byte, (1<<20)+1), 0600); err != nil {
					t.Fatal(err)
				}
			}
			r.Install = []config.Install{{EnvFile: envPath}}
			sink := &deploySink{approved: true}
			s.Sink = sink
			for i := 0; i < 2; i++ {
				allowed, err := s.approveDeploy(context.Background(), r, manifest.Manifest{Commit: shaB}, t.TempDir(), false, Options{})
				if err != nil || allowed {
					t.Fatalf("unknown env admitted=%v err=%v", allowed, err)
				}
			}
			if len(sink.questions) != 2 {
				t.Fatal("missing hold decision request")
			}
		})
	}
}

func TestEnvDriftDuringHealthRestoresPriorContainer(t *testing.T) {
	s, g, _, _, r := setup(t)
	envPath := filepath.Join(t.TempDir(), "service.env")
	if err := os.WriteFile(envPath, []byte("SETTING=one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	drift := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if drift {
			if err := os.WriteFile(envPath, []byte("SETTING=two\n"), 0600); err != nil {
				t.Error(err)
			}
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	old := "sha256:" + strings.Repeat("a", 64)
	next := "sha256:" + strings.Repeat("b", 64)
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api", Runtime: "docker", EnvFile: envPath, Health: &config.Health{URL: srv.URL, Status: 200, Timeout: "1s", Retries: 1}}
	r.Install = []config.Install{spec}
	r.Root = ""
	r.Smoke = config.Command{}
	r.DeployTask = "1"
	pinned := spec
	pinned.Digest = old
	var err error
	g.m, err = manifest.Build(r.Name, shaA, []config.Install{pinned}, nil)
	if err != nil {
		t.Fatal(err)
	}
	d := newDockerCLI()
	s.Runner = d
	s.Sink = &deploySink{approved: true}
	if err = s.Rollout(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	drift = true
	pinned.Digest = next
	g.m, err = manifest.Build(r.Name, shaB, []config.Install{pinned}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Rollout(context.Background(), r, Options{}); err == nil {
		t.Fatal("environment drift accepted")
	}
	if d.containers[spec.Name] != spec.Image+"@"+old {
		t.Fatalf("prior container not restored: %v", d.containers)
	}
	state := filepath.Join(s.state(r.Name), "rollout")
	if _, err = os.Stat(filepath.Join(state, "container-journal.json")); !os.IsNotExist(err) {
		t.Fatalf("rollback journal remains: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(state, "container-installed.json"))
	if err != nil || !strings.Contains(string(b), shaA) {
		t.Fatalf("prior receipt not restored: %s %v", b, err)
	}
}
