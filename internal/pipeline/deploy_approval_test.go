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
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api", Runtime: "docker", EnvFile: "/operator/old.env", Health: &config.Health{URL: srv.URL, Status: 200, Timeout: "1s", Retries: 1}}
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
	r.Install[0].EnvFile = "/operator/changed.env"
	if e = s.Rollout(context.Background(), r, Options{Digest: spec.Image + "@" + next}); e != nil {
		t.Fatal(e)
	}
	if d.containers[spec.Name] != spec.Image+"@"+old || len(sink.questions) == 0 {
		t.Fatalf("unapproved replacement=%q questions=%v", d.containers[spec.Name], sink.questions)
	}
}
