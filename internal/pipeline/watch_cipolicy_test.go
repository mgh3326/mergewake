package pipeline

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// ciPolicyGH serves workflow files on top of the pipeline fake; the policy file
// still comes from fakeGH so the blob SHA follows its content.
type ciPolicyGH struct {
	*fakeGH
	workflows map[string]string
}

func blobOf(s string) string { h := sha1.Sum([]byte(s)); return hex.EncodeToString(h[:]) }

func (g *ciPolicyGH) ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
	if text, ok := g.workflows[name]; ok {
		return []byte(text), nil
	}
	return g.fakeGH.ReadFile(ctx, repo, sha, name)
}

func (g *ciPolicyGH) PathBlob(ctx context.Context, repo, sha, name string) (string, error) {
	b, e := g.ReadFile(ctx, repo, sha, name)
	return blobOf(string(b)), e
}

func (g *ciPolicyGH) Dir(_ context.Context, _, _, dir string) ([]github.TreeEntry, error) {
	var out []github.TreeEntry
	for name, text := range g.workflows {
		if strings.HasPrefix(name, dir+"/") {
			out = append(out, github.TreeEntry{Path: name, Type: "blob", Mode: "100644", SHA: blobOf(text)})
		}
	}
	return out, nil
}

const watchCI = "name: CI\non:\n  push:\n    branches: [main]\nconcurrency:\n  group: ci-${{ github.ref }}\n  cancel-in-progress: CANCEL\njobs:\n  CI:\n    runs-on: x\n"

// (f) Watch warns once per change of the workflow or policy blobs, tells the
// repo's lane once as info, and stays quiet on the next pass.
func TestWatchCIPolicyWarnsOncePerBlobChange(t *testing.T) {
	s, f, _, n, r := setup(t)
	r.Lane = "repo-lane"
	g := &ciPolicyGH{fakeGH: f, workflows: map[string]string{".github/workflows/ci.yml": strings.Replace(watchCI, "CANCEL", "false", 1)}}
	s.GH = g
	var out strings.Builder
	s.Out = &out
	warnings := func() int { return strings.Count(out.String(), `"command":"ci-policy"`) }
	pass := func(o Options) {
		t.Helper()
		if e := s.Watch(context.Background(), r, o); e != nil {
			t.Fatal(e)
		}
	}

	pass(Options{})
	if warnings() != 1 || len(n.events) != 1 {
		t.Fatalf("first pass: %d warnings, %d events\n%s", warnings(), len(n.events), out.String())
	}
	ev := n.events[0]
	if ev.Type != "info" || ev.Lane != "repo-lane" || !strings.Contains(ev.Text, "cancel-in-progress is false") || !strings.Contains(ev.Text, "any_ref_workflow is not set") {
		t.Fatalf("event %+v", ev)
	}

	pass(Options{})
	if warnings() != 1 || len(n.events) != 1 {
		t.Fatalf("unchanged blobs warned again: %d warnings, %d events", warnings(), len(n.events))
	}

	// A workflow edit that still leaves a problem is a new key, so one more warning.
	g.workflows[".github/workflows/ci.yml"] = strings.Replace(watchCI, "CANCEL", "true", 1)
	pass(Options{})
	if warnings() != 2 || len(n.events) != 2 || strings.Contains(n.events[1].Text, "cancel-in-progress") {
		t.Fatalf("workflow change: %d warnings, events %+v", warnings(), n.events)
	}
	pass(Options{})
	if warnings() != 2 || len(n.events) != 2 {
		t.Fatalf("repeat after change: %d warnings, %d events", warnings(), len(n.events))
	}

	// Fixing the policy makes the repo compliant: no warning, no event.
	f.policyExtra = "any_ref_workflow: any.yml\n"
	g.workflows[".github/workflows/any.yml"] = "on: workflow_dispatch\njobs:\n  CI:\n    runs-on: x\n"
	pass(Options{})
	pass(Options{})
	if warnings() != 2 || len(n.events) != 2 {
		t.Fatalf("compliant repo warned: %d warnings, %d events", warnings(), len(n.events))
	}
}

// A dry run audits the warning but sends nothing and records nothing, so the
// next acting pass still warns.
func TestWatchCIPolicyDryRunSendsNothing(t *testing.T) {
	s, f, _, n, r := setup(t)
	s.GH = &ciPolicyGH{fakeGH: f, workflows: map[string]string{".github/workflows/ci.yml": strings.Replace(watchCI, "CANCEL", "false", 1)}}
	var out strings.Builder
	s.Out = &out
	if e := s.Watch(context.Background(), r, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), `"command":"ci-policy"`) || len(n.events) != 0 {
		t.Fatalf("dry run: events %d\n%s", len(n.events), out.String())
	}
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(n.events) != 1 {
		t.Fatalf("acting pass after dry run sent %d events", len(n.events))
	}
}
