package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
)

var shaA = strings.Repeat("a", 40)
var shaB = strings.Repeat("b", 40)
var shaC = strings.Repeat("c", 40)
var shaD = strings.Repeat("d", 40)

type fakeGH struct {
	GitHub
	pullCalls                                                    int
	nextHead                                                     string
	pull                                                         github.Pull
	enabled, blocking, reviewed                                  bool
	findings                                                     int
	ci                                                           github.CI
	mergeCount, revertCount, postCount, claimCount, publishCount int
	head                                                         string
	checks                                                       map[string]github.CI
	parents                                                      map[string]string
	latch, headChanged                                           bool
	policyReads                                                  []string
	m                                                            manifest.Manifest
}

func (g *fakeGH) Pull(context.Context, string, int) (github.Pull, error) {
	g.pullCalls++
	p := g.pull
	if g.pullCalls > 1 && g.nextHead != "" {
		p.Head.SHA = g.nextHead
	}
	return p, nil
}
func (g *fakeGH) Head(_ context.Context, _, branch string) (string, error) {
	if branch == github.RecoveryBranch {
		if g.latch {
			return shaD, nil
		}
		return "", &github.APIError{Code: 404}
	}
	if g.head != "" {
		return g.head, nil
	}
	return shaB, nil
}
func (g *fakeGH) ReadFile(_ context.Context, _, sha, name string) ([]byte, error) {
	if name == ".mergewake/policy.yaml" {
		g.policyReads = append(g.policyReads, sha)
		text := "version: 1\nrequired_checks: [CI]\n"
		if g.enabled {
			text += "enabled: true\n"
		}
		if g.blocking {
			text += "blocking:\n  - paths: ['**']\n    reason: irreversible effects\n"
		}
		return []byte(text), nil
	}
	return []byte("payload"), nil
}
func (g *fakeGH) Files(context.Context, string, int) ([]policy.File, error) {
	return []policy.File{{Path: "tool", Kind: "modified"}}, nil
}
func (g *fakeGH) Check(_ context.Context, _, sha string, _ []string) (github.CI, error) {
	if g.checks != nil {
		return g.checks[sha], nil
	}
	return g.ci, nil
}
func (g *fakeGH) Reviewed(context.Context, string, int, string, string) (bool, error) {
	return g.reviewed, nil
}
func (g *fakeGH) Unresolved(context.Context, string, int) (int, error) { return g.findings, nil }
func (g *fakeGH) Merge(_ context.Context, _ string, _ int, sha string) error {
	if sha != g.pull.Head.SHA {
		return errors.New("unbound head")
	}
	g.mergeCount++
	return nil
}
func (g *fakeGH) Commit(_ context.Context, _, sha string) (github.Commit, error) {
	v := github.Commit{SHA: sha}
	if p := g.parents[sha]; p != "" {
		v.Parents = []github.Ref{{SHA: p}}
	}
	return v, nil
}
func (g *fakeGH) PRsForCommit(context.Context, string, string) ([]github.Pull, error) {
	return []github.Pull{g.pull}, nil
}
func (g *fakeGH) Revert(context.Context, string, string, string, string, int) (string, error) {
	g.revertCount++
	g.latch = true
	return "https://github.com/example/project/pull/9", nil
}
func (g *fakeGH) Diff(context.Context, string, int) (string, error) {
	return "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old\n+new\n", nil
}
func (g *fakeGH) CreateRef(context.Context, string, string, string) error { g.claimCount++; return nil }
func (g *fakeGH) PostReview(_ context.Context, _ string, _ int, _ string, f []github.Finding) error {
	g.postCount++
	g.reviewed = true
	return nil
}
func (g *fakeGH) Ancestor(context.Context, string, string, string) (bool, error) { return true, nil }
func (g *fakeGH) Publish(context.Context, manifest.Manifest) error               { g.publishCount++; return nil }
func (g *fakeGH) Latest(context.Context, string) (manifest.Manifest, error)      { return g.m, nil }

type fakeSink struct{ records, reopens int }

func (s *fakeSink) Reopen(context.Context, string, string) error { s.reopens++; return nil }
func (s *fakeSink) Record(context.Context, string, []byte) error { s.records++; return nil }

type fakeNotifier struct{ count int }

func (n *fakeNotifier) Notify(context.Context, integrations.Event) error { n.count++; return nil }

type fakeRunner struct {
	calls      int
	input      string
	args       []string
	familyFail bool
}

func (r *fakeRunner) Run(_ context.Context, command string, args []string, input, dir string) ([]byte, error) {
	r.calls++
	r.input = input
	r.args = args
	if dir != "" {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return nil, e
		}
		for _, f := range entries {
			if f.Name() != ".commandcode" {
				return nil, errors.New("review has checkout context")
			}
		}
	}
	return []byte(`{"findings":[{"path":"tool","line":1,"body":"A concrete bug"}]}`), nil
}
func setup(t *testing.T) (*Service, *fakeGH, *fakeSink, *fakeNotifier, config.Repo) {
	t.Helper()
	g := &fakeGH{ci: github.Green}
	g.pull = github.Pull{Number: 7, State: "open", ChangedFiles: 1, Head: github.Ref{SHA: shaA}, Base: github.Ref{Ref: "main", SHA: shaB}}
	sink := &fakeSink{}
	n := &fakeNotifier{}
	r := config.Repo{Name: "example/project", Branch: "main", Policy: ".mergewake/policy.yaml", SmokeTimeout: "5s"}
	s := &Service{Config: config.Config{Version: 1, Host: "host-a", StateDir: filepath.Join(t.TempDir(), "state"), WatchLimit: 20, Review: config.Review{Login: "review-bot", Timeout: "5s", Pairings: map[string][]string{"openai": {"claude"}}, Profiles: map[string]config.Reviewer{"claude": {Family: "anthropic", Command: "pi", Args: []string{"--model", "example-model"}}}}}, GH: g, Runner: &fakeRunner{}, Sink: sink, Notifier: n, Out: io.Discard}
	return s, g, sink, n, r
}
func TestLand(t *testing.T) {
	for _, tt := range []struct {
		name                        string
		enabled, blocking, reviewed bool
		findings                    int
		ci                          github.CI
		dry, shadow                 bool
		merges                      int
	}{
		{name: "shadow default", ci: github.Green}, {name: "routine enabled", enabled: true, ci: github.Green, merges: 1}, {name: "red never merges", enabled: true, ci: github.Red}, {name: "missing checks", enabled: true, ci: github.Pending}, {name: "blocking needs review", enabled: true, blocking: true, ci: github.Green}, {name: "blocking unresolved", enabled: true, blocking: true, reviewed: true, findings: 1, ci: github.Green}, {name: "blocking resolved", enabled: true, blocking: true, reviewed: true, ci: github.Green, merges: 1}, {name: "dry run", enabled: true, ci: github.Green, dry: true}, {name: "forced shadow", enabled: true, ci: github.Green, shadow: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = tt.enabled
			g.blocking = tt.blocking
			g.reviewed = tt.reviewed
			g.findings = tt.findings
			g.ci = tt.ci
			if e := s.Land(context.Background(), r, 7, Options{DryRun: tt.dry, Shadow: tt.shadow}); e != nil {
				t.Fatal(e)
			}
			if g.mergeCount != tt.merges {
				t.Fatalf("merged %d times", g.mergeCount)
			}
			for _, sha := range g.policyReads {
				if sha != shaB {
					t.Fatal("read untrusted head policy")
				}
			}
			if tt.dry {
				if _, e := os.Stat(s.Config.StateDir); !os.IsNotExist(e) {
					t.Fatal("dry run wrote state")
				}
			}
		})
	}
}
func TestBisect(t *testing.T) {
	for _, tt := range []struct {
		name   string
		states []github.CI
		index  int
		fail   bool
	}{
		{"first", []github.CI{github.Green, github.Red, github.Red, github.Red}, 1, false}, {"last", []github.CI{github.Green, github.Green, github.Green, github.Red}, 3, false}, {"pending", []github.CI{github.Green, github.Pending, github.Red}, 0, true}, {"no green", []github.CI{github.Red, github.Red}, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ids := []string{}
			states := map[string]github.CI{}
			for i, v := range tt.states {
				id := string(rune('a' + i))
				ids = append(ids, id)
				states[id] = v
			}
			i, e := Bisect(ids, func(sha string) (github.CI, error) { return states[sha], nil })
			if (e != nil) != tt.fail || !tt.fail && i != tt.index {
				t.Fatalf("%d %v", i, e)
			}
		})
	}
}
func TestWatchRecoveryLatchAndAdapters(t *testing.T) {
	for _, tt := range []struct {
		name         string
		enabled, dry bool
		reverts      int
	}{{"shadow", false, false, 0}, {"dry", true, true, 0}, {"acting", true, false, 1}} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, sink, n, r := setup(t)
			g.enabled = tt.enabled
			g.head = shaC
			g.parents = map[string]string{shaC: shaB, shaB: shaA}
			g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green}
			merged := "2026-01-01T00:00:00Z"
			g.pull.MergedAt = &merged
			g.pull.MergeCommitSHA = shaB
			g.pull.Body = "Mergewake-Task: 42"
			s.Config.Sink.Type = "handoffkeep"
			if e := s.Watch(context.Background(), r, Options{DryRun: tt.dry}); e != nil {
				t.Fatal(e)
			}
			if g.revertCount != tt.reverts {
				t.Fatalf("%d reverts", g.revertCount)
			}
			if sink.reopens != tt.reverts || n.count != tt.reverts {
				t.Fatal("adapter side effects mismatch")
			}
			if tt.reverts > 0 {
				if e := s.Watch(context.Background(), r, Options{}); e != nil {
					t.Fatal(e)
				}
				if g.revertCount != 1 || sink.reopens != 1 || n.count != 1 {
					t.Fatal("second recovery bypassed latch")
				}
			}
		})
	}
}
func TestReviewOnePassAndIsolation(t *testing.T) {
	s, g, _, _, r := setup(t)
	runner := s.Runner.(*fakeRunner)
	if e := s.Review(context.Background(), r, 7, "openai", Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 0 || g.claimCount != 0 {
		t.Fatal("dry review acted")
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if g.postCount != 1 || g.claimCount != 1 || !strings.Contains(runner.input, "UNTRUSTED DIFF") {
		t.Fatal("review did not run")
	}
	if !strings.Contains(strings.Join(runner.args, " "), "--no-tools") {
		t.Fatal("tools enabled")
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if runner.calls != 1 {
		t.Fatal("review ran twice")
	}
	g.reviewed = false
	if e := s.Review(context.Background(), r, 7, "anthropic", Options{}); e == nil {
		t.Fatal("same-family fallback accepted")
	}
}
func TestFindingValidation(t *testing.T) {
	diff := "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -10,2 +10,2 @@\n old\n-gone\n+new\n"
	for _, tt := range []struct {
		body string
		fail bool
	}{{`{"findings":[{"path":"a","line":11,"body":"bug"}]}`, false}, {`{"findings":[]}`, false}, {`{"findings":[{"path":"a","line":12,"body":"bug"}]}`, true}, {`{"findings":null}`, true}, {`{"findings":[]} garbage`, true}, {`{"findings":[],"extra":true}`, true}} {
		_, e := parseFindings([]byte(tt.body), diff)
		if (e != nil) != tt.fail {
			t.Errorf("%s: %v", tt.body, e)
		}
	}
}
func TestReleaseAndRolloutAdapters(t *testing.T) {
	s, g, sink, n, r := setup(t)
	r.Install = []config.Install{{Source: "tool", Target: "bin/tool", Mode: "0755"}}
	r.Root = t.TempDir()
	r.Smoke = config.Command{Command: "smoke"}
	if e := s.Release(context.Background(), r, "", Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if g.publishCount != 0 {
		t.Fatal("dry release published")
	}
	if e := s.Release(context.Background(), r, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if g.publishCount != 1 {
		t.Fatal("missing publication")
	}
	m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	// Smoke uses a separate fake: review's empty-directory assertion is irrelevant here.
	s.Runner = smokeRunner{}
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if sink.records != 1 || n.count != 1 {
		t.Fatal("rollout not reported")
	}
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if sink.records != 1 || n.count != 1 {
		t.Fatal("healthy poll repeated reporting")
	}
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "rollout", "installed.json"))
	if e != nil {
		t.Fatal(e)
	}
	if !json.Valid(b) {
		t.Fatal("invalid receipt")
	}
}

type smokeRunner struct{}

func (smokeRunner) Run(context.Context, string, []string, string, string) ([]byte, error) {
	return nil, nil
}

func TestChangedHeadStopsReviewAndLand(t *testing.T) {
	for _, command := range []string{"review", "land"} {
		t.Run(command, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.nextHead = shaC
			var e error
			if command == "review" {
				e = s.Review(context.Background(), r, 7, "openai", Options{})
			} else {
				e = s.Land(context.Background(), r, 7, Options{})
			}
			if e == nil || g.mergeCount != 0 || g.postCount != 0 {
				t.Fatalf("head race acted: %v merges=%d posts=%d", e, g.mergeCount, g.postCount)
			}
		})
	}
}
func TestLaterGreenSupersedesOldCheckpoint(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.head = shaD
	g.parents = map[string]string{shaD: shaC, shaC: shaB, shaB: shaA}
	g.checks = map[string]github.CI{shaD: github.Red, shaC: github.Green, shaB: github.Red, shaA: github.Green}
	g.pull.Merged = true
	g.pull.MergeCommitSHA = shaD
	if e := os.MkdirAll(s.state(r.Name), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(s.state(r.Name), "last-green"), []byte(shaA), 0600); e != nil {
		t.Fatal(e)
	}
	var out strings.Builder
	s.Out = &out
	if e := s.Watch(context.Background(), r, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), `"culprit":"`+shaD+`"`) {
		t.Fatal(out.String())
	}
}
