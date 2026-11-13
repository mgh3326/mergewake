package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/manifest"
)

var shaF = strings.Repeat("f", 40)

// deployChecksGH answers every check read from an explicit per-name map: a
// name it does not list is Absent, unlike fakeGH's blanket fallback. It records
// the names each read asked for and the manifests Release published.
type deployChecksGH struct {
	*fakeGH
	names     map[string]map[string]github.CI
	reads     []string
	published []manifest.Manifest
}

func (g *deployChecksGH) Check(_ context.Context, _, sha string, required []string) (github.CI, error) {
	g.reads = append(g.reads, strings.Join(required, ","))
	for _, n := range required {
		if g.names[sha][n] != github.Green {
			return github.Red, nil
		}
	}
	return github.Green, nil
}

func (g *deployChecksGH) CheckStates(_ context.Context, _, sha string, required []string) (map[string]github.CI, error) {
	out := make(map[string]github.CI, len(required))
	for _, n := range required {
		v, ok := g.names[sha][n]
		if !ok {
			v = github.Absent
		}
		out[n] = v
	}
	return out, nil
}

func (g *deployChecksGH) CheckEvidence(ctx context.Context, repo, sha string, required []string) ([]github.CheckEvidence, error) {
	states, e := g.CheckStates(ctx, repo, sha, required)
	if e != nil {
		return nil, e
	}
	out := make([]github.CheckEvidence, 0, len(required))
	for _, n := range required {
		out = append(out, github.CheckEvidence{Name: n, State: states[n], URL: "https://checks.example/" + n})
	}
	return out, nil
}

func (g *deployChecksGH) Publish(ctx context.Context, m manifest.Manifest) error {
	g.published = append(g.published, m)
	return g.fakeGH.Publish(ctx, m)
}

func buildManifest(t *testing.T, repo, sha string) manifest.Manifest {
	t.Helper()
	m, e := manifest.Build(repo, sha, []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	return m
}

// TestReleaseRequiresDeployOK is (b): with deploy_checks set, ci-ok alone never
// publishes a release, and a published manifest carries the deploy evidence.
func TestReleaseRequiresDeployOK(t *testing.T) {
	s, g, _, _, r := setup(t)
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	g.required = []string{"ci-ok"}
	g.policyExtra = "deploy_checks: [deploy-ok]\n"
	g.head = shaB
	adapter := &deployChecksGH{fakeGH: g, names: map[string]map[string]github.CI{
		shaB: {"ci-ok": github.Green, "deploy-ok": github.Red},
	}}
	s.GH = adapter

	if e := s.Release(context.Background(), r, "", Options{}); e == nil || !strings.Contains(e.Error(), "deploy") {
		t.Fatalf("red deploy-ok released: %v", e)
	}
	delete(adapter.names[shaB], "deploy-ok")
	if e := s.Release(context.Background(), r, "", Options{}); e == nil || !strings.Contains(e.Error(), "deploy") {
		t.Fatalf("missing deploy-ok released on ci-ok: %v", e)
	}
	if len(adapter.published) != 0 {
		t.Fatal("refused release reached publication")
	}

	adapter.names[shaB]["deploy-ok"] = github.Green
	if e := s.Release(context.Background(), r, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(adapter.published) != 1 {
		t.Fatalf("published %d manifests", len(adapter.published))
	}
	m := adapter.published[0]
	if m.Commit != shaB || m.Deploy == nil {
		t.Fatalf("release evidence: %+v", m.Deploy)
	}
	if m.Deploy.SHA != shaB || len(m.Deploy.PolicyRevision) != 40 {
		t.Fatalf("evidence identity: %+v", m.Deploy)
	}
	if len(m.Deploy.Checks) != 1 || m.Deploy.Checks[0].Name != "deploy-ok" || m.Deploy.Checks[0].State != string(github.Green) || m.Deploy.Checks[0].URL != "https://checks.example/deploy-ok" {
		t.Fatalf("evidence checks: %+v", m.Deploy.Checks)
	}
}

// TestReleaseAndRolloutWithoutDeployStage is (c): without deploy_checks the
// manifest is unchanged and main checks still admit the release.
func TestReleaseAndRolloutWithoutDeployStage(t *testing.T) {
	s, g, sink, _, r := setup(t)
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	r.Root = t.TempDir()
	r.Smoke = config.Command{Command: "smoke"}
	g.required = []string{"ci-ok"}
	adapter := &deployChecksGH{fakeGH: g, names: map[string]map[string]github.CI{
		shaB: {"ci-ok": github.Green, "deploy-ok": github.Red},
	}}
	s.GH = adapter
	s.Runner = smokeRunner{}

	if e := s.Release(context.Background(), r, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(adapter.published) != 1 || adapter.published[0].Deploy != nil {
		t.Fatalf("no deploy stage must publish no evidence: %+v", adapter.published)
	}
	g.m = adapter.published[0]
	if e := s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if sink.records != 1 {
		t.Fatal("rollout not reported")
	}
}

// TestRolloutAdmitsNewestDeployOKAncestor is (d): rollout installs the newest
// candidate on main whose deploy checks are green and that no red range covers.
func TestRolloutAdmitsNewestDeployOKAncestor(t *testing.T) {
	for _, tc := range []struct {
		name        string
		head        string
		candidates  []string
		checks      map[string]map[string]github.CI
		deny        string
		notAncestor [][2]string
		want        string
	}{
		{
			name:       "newest deploy-ok ancestor",
			head:       shaD,
			candidates: []string{shaC, shaB},
			checks:     map[string]map[string]github.CI{shaC: {"deploy-ok": github.Green}, shaB: {"deploy-ok": github.Green}},
			want:       shaC,
		},
		{
			name:        "skips a non-ancestor",
			head:        shaD,
			candidates:  []string{shaA, shaB},
			checks:      map[string]map[string]github.CI{shaA: {"deploy-ok": github.Green}, shaB: {"deploy-ok": github.Green}},
			notAncestor: [][2]string{{shaA, shaD}},
			want:        shaB,
		},
		{
			name:       "skips a red candidate",
			head:       shaF,
			candidates: []string{shaF, shaB},
			checks:     map[string]map[string]github.CI{shaF: {"deploy-ok": github.Red}, shaB: {"deploy-ok": github.Green}},
			want:       shaB,
		},
		{
			name:        "skips a denied sha",
			head:        shaD,
			candidates:  []string{shaC, shaB},
			checks:      map[string]map[string]github.CI{shaC: {"deploy-ok": github.Green}, shaB: {"deploy-ok": github.Green}},
			deny:        shaC + "\n",
			notAncestor: [][2]string{{shaC, shaB}},
			want:        shaB,
		},
		{
			name:        "denies everything after a culprit while main is red",
			head:        shaF,
			candidates:  []string{shaD, shaB},
			checks:      map[string]map[string]github.CI{shaF: {"deploy-ok": github.Red}, shaD: {"deploy-ok": github.Green}, shaB: {"deploy-ok": github.Green}},
			deny:        shaC + "\n",
			notAncestor: [][2]string{{shaC, shaB}},
			want:        shaB,
		},
		{
			name:       "the range ends at the next green",
			head:       shaF,
			candidates: []string{shaD},
			checks:     map[string]map[string]github.CI{shaF: {"deploy-ok": github.Green}, shaD: {"deploy-ok": github.Green}},
			deny:       shaC + "\n",
			want:       shaD,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, sink, _, r := setup(t)
			r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
			r.Root = t.TempDir()
			r.Smoke = config.Command{Command: "smoke"}
			g.required = []string{"ci-ok"}
			g.policyExtra = "deploy_checks: [deploy-ok]\n"
			g.head = tc.head
			if len(tc.notAncestor) > 0 {
				g.notAncestor = map[[2]string]bool{}
				for _, pair := range tc.notAncestor {
					g.notAncestor[pair] = true
				}
			}
			for _, sha := range tc.candidates {
				g.ms = append(g.ms, buildManifest(t, r.Name, sha))
			}
			s.GH = &deployChecksGH{fakeGH: g, names: tc.checks}
			s.Runner = smokeRunner{}
			if tc.deny != "" {
				if e := os.MkdirAll(s.state(r.Name), 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(s.state(r.Name), rolloutDenyName), []byte(tc.deny), 0600); e != nil {
					t.Fatal(e)
				}
			}

			if e := s.Rollout(context.Background(), r, Options{}); e != nil {
				t.Fatal(e)
			}
			var receipt struct {
				Manifest manifest.Manifest `json:"manifest"`
			}
			b, e := os.ReadFile(filepath.Join(s.state(r.Name), "rollout", "installed.json"))
			if e != nil {
				t.Fatal(e)
			}
			if e = json.Unmarshal(b, &receipt); e != nil {
				t.Fatal(e)
			}
			if receipt.Manifest.Commit != tc.want {
				t.Fatalf("installed %s, want %s", receipt.Manifest.Commit, tc.want)
			}
			if sink.records != 1 {
				t.Fatal("rollout not reported")
			}
		})
	}
}

func TestRolloutDenyFileRejectsMalformedSHA(t *testing.T) {
	s, _, _, _, r := setup(t)
	if e := os.MkdirAll(s.state(r.Name), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(s.state(r.Name), rolloutDenyName), []byte("not-a-sha\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := s.rolloutDeny(r.Name); e == nil || !strings.Contains(e.Error(), "full commit SHA") {
		t.Fatalf("malformed deny entry accepted: %v", e)
	}
}

// A missing deploy result does not prevent the independent main checkpoint.
func TestWatchMainGreenWithMissingDeployOK(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.required = []string{"ci-ok"}
	g.policyExtra = "watch_checks: [ci-ok]\ndeploy_checks: [deploy-ok]\n"
	g.head = shaB
	adapter := &deployChecksGH{fakeGH: g, names: map[string]map[string]github.CI{
		shaB: {"ci-ok": github.Green},
	}}
	s.GH = adapter

	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(adapter.reads, "ci-ok") || !slices.Contains(adapter.reads, "deploy-ok") {
		t.Fatalf("watch reads %v", adapter.reads)
	}
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "last-green"))
	if e != nil || string(b) != shaB {
		t.Fatalf("missing deploy-ok was not green: %q %v", b, e)
	}
}
