package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/manifest"
	"github.com/mgh3326/mergewake/internal/policy"
	"github.com/mgh3326/mergewake/internal/rollout"
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
	enabled, blocking, reviewed, gate                            bool
	requiredBots                                                 []string
	findings                                                     int
	passes                                                       map[string][]github.PassReceipt
	feedback                                                     github.ReviewFeedback
	refs                                                         map[string]bool
	leaseTimes                                                   map[string]time.Time
	files                                                        []policy.File
	ci                                                           github.CI
	mergeCount, revertCount, postCount, claimCount, publishCount int
	head                                                         string
	checks                                                       map[string]github.CI
	parents                                                      map[string]string
	parents2                                                     map[string][]string
	commitPRs                                                    map[string][]github.Pull
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
func (g *fakeGH) RecoveryActive(context.Context, string) (bool, error) { return g.latch, nil }
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
		if g.gate {
			if g.requiredBots != nil {
				text += "review_gate:\n  required_bots: [" + strings.Join(g.requiredBots, ",") + "]\n"
			} else {
				text += "review_gate: {}\n"
			}
		}
		return []byte(text), nil
	}
	return []byte("payload"), nil
}
func (g *fakeGH) Files(context.Context, string, int) ([]policy.File, error) {
	if g.files != nil {
		return g.files, nil
	}
	return []policy.File{{Path: "tool", Kind: "modified"}}, nil
}
func (g *fakeGH) Check(_ context.Context, _, sha string, _ []string) (github.CI, error) {
	if g.checks != nil {
		return g.checks[sha], nil
	}
	return g.ci, nil
}
func (g *fakeGH) ReviewPasses(_ context.Context, _ string, _ int, sha, _ string) ([]github.PassReceipt, error) {
	if g.reviewed && sha == g.pull.Head.SHA {
		return []github.PassReceipt{{Pass: 1, Family: "anthropic", Lens: "correctness"}, {Pass: 2, Family: "openai", Lens: "state-risk"}}, nil
	}
	return g.passes[sha], nil
}
func (g *fakeGH) Reviewed(ctx context.Context, repo string, n int, sha, login string) (bool, error) {
	passes, e := g.ReviewPasses(ctx, repo, n, sha, login)
	return len(passes) > 0, e
}
func (g *fakeGH) ReviewClaimed(_ context.Context, _ string, n int, sha string) (bool, error) {
	return g.refs[fmt.Sprintf("mergewake/reviews/%d/%s", n, sha)], nil
}
func (g *fakeGH) ReviewLease(_ context.Context, _ string, branch string, ttl time.Duration) (string, bool, error) {
	if !g.refs[branch] {
		return "", false, nil
	}
	return shaA, time.Since(g.leaseTimes[branch]) < ttl, nil
}
func (g *fakeGH) ClaimReview(ctx context.Context, repo, branch, head string, ttl time.Duration) (bool, error) {
	_, active, err := g.ReviewLease(ctx, repo, branch, ttl)
	if err != nil || active {
		return false, err
	}
	if g.refs == nil {
		g.refs = map[string]bool{}
	}
	if g.leaseTimes == nil {
		g.leaseTimes = map[string]time.Time{}
	}
	g.refs[branch] = true
	g.leaseTimes[branch] = time.Now()
	g.claimCount++
	return true, nil
}
func (g *fakeGH) ReviewFeedback(_ context.Context, _ string, _ int) (github.ReviewFeedback, error) {
	fb := g.feedback
	fb.HeadSHA = g.pull.Head.SHA
	return fb, nil
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
	if ps := g.parents2[sha]; len(ps) > 0 {
		for _, p := range ps {
			v.Parents = append(v.Parents, github.Ref{SHA: p})
		}
	} else if p := g.parents[sha]; p != "" {
		v.Parents = []github.Ref{{SHA: p}}
	}
	return v, nil
}
func (g *fakeGH) PRsForCommit(_ context.Context, _, sha string) ([]github.Pull, error) {
	if prs, ok := g.commitPRs[sha]; ok {
		return prs, nil
	}
	return []github.Pull{g.pull}, nil
}
func (g *fakeGH) Revert(ctx context.Context, repo, branch, head, culprit string, pr int) (string, error) {
	commit, err := g.Commit(ctx, repo, culprit)
	if err != nil {
		return "", err
	}
	if len(commit.Parents) != 1 {
		return "", errors.New("automatic revert requires a squash commit")
	}
	g.revertCount++
	g.latch = true
	return "https://github.com/example/project/pull/9", nil
}
func (g *fakeGH) Diff(context.Context, string, int) (string, error) {
	return "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old\n+new\n", nil
}
func (g *fakeGH) CreateRef(_ context.Context, _, branch, _ string) error {
	g.claimCount++
	if g.refs == nil {
		g.refs = map[string]bool{}
	}
	if g.refs[branch] {
		return errors.New("ref exists")
	}
	g.refs[branch] = true
	return nil
}
func (g *fakeGH) PostReview(_ context.Context, _ string, _ int, sha string, f []github.Finding, meta github.PassReceipt) error {
	g.postCount++
	if g.passes == nil {
		g.passes = map[string][]github.PassReceipt{}
	}
	g.passes[sha] = append(g.passes[sha], meta)
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
	calls     int
	input     string
	inputs    []string
	args      []string
	recommend []string
}

func (r *fakeRunner) Run(_ context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "scopefuel" {
		if len(args) > 0 && args[0] == "--json" {
			return []byte(idleUsage), nil
		}
		if len(args) > 0 && args[0] == "--recommend" {
			if len(r.recommend) == 0 {
				r.recommend = []string{"claude"}
			}
			rows := []string{}
			for _, p := range r.recommend {
				rows = append(rows, `{"profile":"`+p+`","kind":"candidate","pool":"idle"}`)
			}
			return []byte(`{"rows":[` + strings.Join(rows, ",") + `]}`), nil
		}
		return nil, nil
	}
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
	g := &fakeGH{ci: github.Green, feedback: github.ReviewFeedback{PRAuthor: "pr-author", ReviewAuthors: map[string]int{"coderabbitai": 1, "chatgpt-codex-connector": 1, "review-bot": 1}}}
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
		{name: "shadow default", ci: github.Green}, {name: "routine enabled", enabled: true, reviewed: true, ci: github.Green, merges: 1}, {name: "red never merges", enabled: true, ci: github.Red}, {name: "missing checks", enabled: true, ci: github.Pending}, {name: "blocking needs review", enabled: true, blocking: true, ci: github.Green}, {name: "blocking unresolved", enabled: true, blocking: true, reviewed: true, findings: 1, ci: github.Green}, {name: "blocking resolved", enabled: true, blocking: true, reviewed: true, ci: github.Green, merges: 1}, {name: "dry run", enabled: true, ci: github.Green, dry: true}, {name: "forced shadow", enabled: true, ci: github.Green, shadow: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = tt.enabled
			g.blocking = tt.blocking
			g.reviewed = tt.reviewed
			g.findings = tt.findings
			if tt.findings > 0 {
				g.feedback.Threads = []github.ReviewThread{{Comments: []github.ReviewComment{{Author: "review-bot", CommitSHA: shaA, Body: "<!-- mergewake-severity:high --> bug"}}}}
			}
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
func landAction(t *testing.T, s *Service, repo string) string {
	t.Helper()
	state, e := s.ledger().Read()
	if e != nil {
		t.Fatal(e)
	}
	return state.Latest["https://github.com/"+repo+"/pull/7"].Action
}

// An unreviewed blocking path stays held until two head-bound passes exist.
func TestLandBlockingNeedsTwoPasses(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.blocking = true
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "wait-for-independent-review" {
		t.Fatalf("merged without reviews: %d", g.mergeCount)
	}
	g.passes = map[string][]github.PassReceipt{g.pull.Head.SHA: {{Pass: 1, Family: "anthropic", Lens: "correctness"}}}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 0 {
		t.Fatal("merged on a single pass")
	}
	g.passes[g.pull.Head.SHA] = append(g.passes[g.pull.Head.SHA], github.PassReceipt{Pass: 2, Family: "openai", Lens: "state-risk"})
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 || landAction(t, s, r.Name) != "merge" {
		t.Fatalf("two clean passes did not land: %d", g.mergeCount)
	}
}

// A PR editing its own policy file is judged under main's policy and counts
// as a blocking path even when main names no blocking rules.
func TestLandPolicyFileIsBlocking(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.pull.ChangedFiles = 1
	g.files = []policy.File{{Path: ".mergewake/policy.yaml", Kind: "modified"}}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 0 || landAction(t, s, r.Name) != "wait-for-independent-review" {
		t.Fatal("policy-file PR landed without two reviews")
	}
	for _, sha := range g.policyReads {
		if sha != shaB {
			t.Fatal("policy read from untrusted head")
		}
	}
	g.reviewed = true
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 {
		t.Fatal("reviewed policy-file PR did not land")
	}
}

// Reviews bound to an older head never count toward the current head.
func TestLandHeadChangeInvalidatesReviews(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.blocking = true
	old := g.pull.Head.SHA
	g.passes = map[string][]github.PassReceipt{old: {{Pass: 1, Family: "anthropic", Lens: "correctness"}, {Pass: 2, Family: "openai", Lens: "state-risk"}}}
	g.pull.Head.SHA = shaC
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 0 {
		t.Fatal("stale-head reviews merged")
	}
	g.passes[shaC] = []github.PassReceipt{{Pass: 1, Family: "anthropic", Lens: "correctness"}, {Pass: 2, Family: "openai", Lens: "state-risk"}}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 {
		t.Fatal("current-head reviews did not land")
	}
}

// With no other-family quota the review still runs, records same_family, and
// the blocking PR lands on the two head-bound passes.
func TestLandSameFamilyReviewLands(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	g.blocking = true
	g.pull.Body = "Mergewake-Author-Family: anthropic\nMergewake-Author-Profile: claude@high\n"
	s.Config.Review.Profiles["claude-sonnet"] = config.Reviewer{Family: "anthropic", Command: "pi", Args: []string{"--model", "other-model"}}
	s.Runner.(*fakeRunner).recommend = []string{"claude", "claude-sonnet"}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	if e := s.Land(context.Background(), r, 7, Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 {
		t.Fatal("same-family reviewed blocking PR did not land")
	}
	state, e := s.ledger().Read()
	if e != nil {
		t.Fatal(e)
	}
	v := state.Latest["https://github.com/"+r.Name+"/pull/7"]
	if !v.SameFamily || v.Action != "merge" {
		t.Fatalf("%+v", v)
	}
}

// review_gate folds bot threads into the decision graded by author and
// severity instead of the raw unresolved count.
func TestLandReviewGateFindings(t *testing.T) {
	thread := func(author, label string, resolved, answered bool) github.ReviewThread {
		comments := []github.ReviewComment{{Author: author, CommitSHA: shaA, Body: label + " problem here"}}
		if answered {
			comments = append(comments, github.ReviewComment{Author: "dev", AuthorType: "User", Body: "fixed in the latest commit"})
		}
		return github.ReviewThread{ID: author + label, Path: "tool", Line: 1, IsResolved: resolved, ResolvedBy: author, ResolvedByType: "Bot", Comments: comments}
	}
	for _, tt := range []struct {
		name     string
		blocking bool
		reviewed bool
		threads  []github.ReviewThread
		action   string
		merges   int
	}{
		{"critical blocks routine", false, false, []github.ReviewThread{thread("coderabbitai[bot]", "Critical", false, false)}, "wait-for-blocking-findings", 0},
		{"major needs triage on blocking", true, true, []github.ReviewThread{thread("coderabbitai[bot]", "Major", false, false)}, "wait-for-triage-reply", 0},
		{"p1 blocks blocking", true, true, []github.ReviewThread{thread("chatgpt-codex-connector[bot]", "P1", false, false)}, "wait-for-blocking-findings", 0},
		{"unanswered major holds routine", false, false, []github.ReviewThread{thread("coderabbitai[bot]", "Major", false, false)}, "wait-for-triage-reply", 0},
		{"answered major lands", false, false, []github.ReviewThread{thread("coderabbitai[bot]", "Major", false, true)}, "merge", 1},
		{"answer does not clear cross-bot P1", false, false, []github.ReviewThread{thread("coderabbitai[bot]", "Major", false, true), thread("chatgpt-codex-connector[bot]", "P1", false, false)}, "wait-for-blocking-findings", 0},
		{"resolved critical lands", false, false, []github.ReviewThread{thread("coderabbitai[bot]", "Critical", true, false)}, "merge", 1},
		{"ignored bot lands", false, false, []github.ReviewThread{thread("qlty[bot]", "Critical", false, false)}, "merge", 1},
		{"minor advisory lands", false, false, []github.ReviewThread{thread("coderabbitai[bot]", "Minor", false, false)}, "merge", 1},
		{"blocking reviewed lands", true, true, []github.ReviewThread{thread("coderabbitai[bot]", "Minor", false, false)}, "merge", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.blocking = tt.blocking
			g.reviewed = true
			g.gate = true
			g.feedback = github.ReviewFeedback{PRAuthor: "pr-author", Threads: tt.threads, ReviewAuthors: map[string]int{"coderabbitai": 1, "chatgpt-codex-connector": 1, "review-bot": 1}}
			if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
				t.Fatal(e)
			}
			if g.mergeCount != tt.merges || landAction(t, s, r.Name) != tt.action {
				t.Fatalf("action=%s merges=%d", landAction(t, s, r.Name), g.mergeCount)
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

// Replay: green → cancelled → non-squash merge → red. The cancelled run is
// unknown evidence, so the watch holds instead of blaming a boundary. Without
// the unknown commit, the merge commit maps to its PR through the recorded
// landing SHA; a blocking-path culprit is reported, never auto-reverted.
func TestWatchCulpritMergeCommitAndUnknownEvidence(t *testing.T) {
	shaE := strings.Repeat("e", 40)
	mergedPull := func() github.Pull {
		merged := "2026-01-01T00:00:00Z"
		p := github.Pull{Number: 7, State: "closed", Merged: true, MergedAt: &merged, MergeCommitSHA: shaC, ChangedFiles: 1, Base: github.Ref{Ref: "main"}, Head: github.Ref{SHA: shaE}}
		p.Body = "Mergewake-Task: 42"
		return p
	}
	for _, tt := range []struct {
		name     string
		checks   map[string]github.CI
		blocking bool
		action   string
		reverts  int
		reports  int
	}{
		{"unknown evidence holds", map[string]github.CI{shaD: github.Red, shaC: github.Red, shaB: github.Pending, shaA: github.Green}, false, "hold", 0, 1},
		{"merge commit culprit holds", map[string]github.CI{shaD: github.Red, shaC: github.Red, shaA: github.Green}, false, "hold", 0, 1},
		{"blocking culprit reported", map[string]github.CI{shaD: github.Red, shaC: github.Red, shaA: github.Green}, true, "report-culprit", 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, sink, n, r := setup(t)
			g.enabled = true
			g.blocking = tt.blocking
			g.head = shaD
			g.checks = tt.checks
			// shaC is a non-squash merge: first parent walks mainline, the
			// merged side branch shaE stays off the search path.
			g.parents2 = map[string][]string{shaD: {shaC}, shaC: {shaA, shaE}, shaA: {}}
			if _, ok := tt.checks[shaB]; ok {
				g.parents2[shaC] = []string{shaB, shaE}
				g.parents2[shaB] = []string{shaA}
			}
			g.commitPRs = map[string][]github.Pull{shaC: {mergedPull()}}
			s.Config.Sink.Type = "handoffkeep"
			var out strings.Builder
			s.Out = &out
			if e := s.Watch(context.Background(), r, Options{}); e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(out.String(), `"action":"`+tt.action+`"`) {
				t.Fatal(out.String())
			}
			if g.revertCount != tt.reverts {
				t.Fatalf("reverts=%d", g.revertCount)
			}
			if sink.records != tt.reports+tt.reverts || n.count != tt.reports+tt.reverts {
				t.Fatalf("records=%d notify=%d", sink.records, n.count)
			}
			if tt.reports > 0 {
				// A second poll on the same head stays quiet.
				if e := s.Watch(context.Background(), r, Options{}); e != nil {
					t.Fatal(e)
				}
				if sink.records != tt.reports || n.count != tt.reports {
					t.Fatal("repeat report was not deduplicated")
				}
			}
		})
	}
}

// Red or unknown main pauses ordinary landing; a recovery PR from the watch
// latch may still land, and shadow records the pause as would-pause.
func TestLandRedMainPause(t *testing.T) {
	for _, tt := range []struct {
		name     string
		mainCI   github.CI
		recovery bool
		shadow   bool
		action   string
		merges   int
	}{
		{"red main pauses", github.Red, false, false, "pause-red-main", 0},
		{"unknown main pauses", github.Pending, false, false, "pause-main-unknown", 0},
		{"shadow records would-pause", github.Red, false, true, "would-pause-red-main", 0},
		{"recovery lands on red main", github.Red, true, false, "merge", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.reviewed = true
			g.checks = map[string]github.CI{shaB: tt.mainCI, shaA: github.Green}
			if tt.recovery {
				g.pull.Head.Ref = github.RecoveryBranch
				g.pull.Head.Repo.FullName = r.Name
			}
			if e := s.Land(context.Background(), r, 7, Options{Shadow: tt.shadow}); e != nil {
				t.Fatal(e)
			}
			if g.mergeCount != tt.merges {
				t.Fatalf("merges=%d", g.mergeCount)
			}
			state, e := s.ledger().Read()
			if e != nil {
				t.Fatal(e)
			}
			if v := state.Latest["https://github.com/"+r.Name+"/pull/7"]; v.Action != tt.action {
				t.Fatalf("action=%s", v.Action)
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
	if g.postCount != 1 || g.claimCount != 2 || !strings.Contains(runner.input, "UNTRUSTED DIFF") {
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
}

// A blocking PR reviewed by two same-family profiles gets the split lenses and
// records same_family, while two different families both keep the default lens.
func TestReviewTwoPassesLensesAndSameFamily(t *testing.T) {
	for _, tt := range []struct {
		name       string
		profiles   map[string]config.Reviewer
		recommend  []string
		lenses     []string
		sameFamily []bool
	}{
		{
			name: "same family gets split lenses",
			profiles: map[string]config.Reviewer{
				"claude":        {Family: "anthropic", Command: "pi", Args: []string{"--model", "example-model"}},
				"claude-sonnet": {Family: "anthropic", Command: "pi", Args: []string{"--model", "other-model"}},
			},
			recommend:  []string{"claude", "claude-sonnet"},
			lenses:     []string{"correctness", "state-risk"},
			sameFamily: []bool{true, true},
		},
		{
			name: "different families keep default lens",
			profiles: map[string]config.Reviewer{
				"claude": {Family: "anthropic", Command: "pi", Args: []string{"--model", "example-model"}},
				"gpt":    {Family: "openai", Command: "pi", Args: []string{"--model", "gpt-model"}},
			},
			recommend:  []string{"claude", "gpt"},
			lenses:     []string{"correctness", "state-risk"},
			sameFamily: []bool{false, true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			runner := s.Runner.(*fakeRunner)
			g.blocking = true
			g.pull.Body = "Mergewake-Author-Family: anthropic\nMergewake-Author-Profile: claude@high\n"
			s.Config.Review.Profiles = tt.profiles
			runner.recommend = tt.recommend
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			passes := g.passes[g.pull.Head.SHA]
			if len(passes) != 2 || runner.calls != 2 || g.claimCount != 3 {
				t.Fatalf("passes=%d runs=%d claims=%d", len(passes), runner.calls, g.claimCount)
			}
			if passes[0].Reviewer == passes[1].Reviewer {
				t.Fatal("one profile ran both passes")
			}
			wantOutcome := false
			for i, pass := range passes {
				if pass.Lens != tt.lenses[i] || pass.SameFamily != tt.sameFamily[i] {
					t.Fatalf("pass %d: %+v", i, pass)
				}
				wantOutcome = wantOutcome || pass.SameFamily
			}
			state, e := s.ledger().Read()
			if e != nil {
				t.Fatal(e)
			}
			if v := state.Latest["https://github.com/"+r.Name+"/pull/7"]; v.SameFamily != wantOutcome {
				t.Fatalf("outcome same_family=%v", v.SameFamily)
			}
		})
	}
}

// When quota admits nobody outside the author's family, review still runs: a
// same-family reviewer with a different model, or the same model in a fresh
// call when nothing else exists.
func TestReviewFamilyFallbackOrder(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.pull.Body = "Mergewake-Author-Family: anthropic\nMergewake-Author-Profile: claude@high\n"
	s.Config.Review.Profiles["claude-sonnet"] = config.Reviewer{Family: "anthropic", Command: "pi", Args: []string{"--model", "other-model"}}
	runner := s.Runner.(*fakeRunner)
	runner.recommend = []string{"claude", "claude-sonnet"}
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	pass := g.passes[g.pull.Head.SHA]
	if len(pass) != 1 || pass[0].Reviewer != "claude-sonnet" || !pass[0].SameFamily {
		t.Fatalf("%+v", pass)
	}

	s, g, _, _, r = setup(t)
	g.pull.Body = "Mergewake-Author-Family: anthropic\nMergewake-Author-Profile: claude@high\n"
	if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	pass = g.passes[g.pull.Head.SHA]
	if len(pass) != 1 || pass[0].Reviewer != "claude" || !pass[0].SameFamily {
		t.Fatalf("%+v", pass)
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

// dockerCLI fakes only the docker/podman commands the container runtime uses.
type dockerCLI struct {
	containers map[string]string
	running    map[string]bool
	images     map[string]string
}

func newDockerCLI() *dockerCLI {
	return &dockerCLI{containers: map[string]string{}, running: map[string]bool{}, images: map[string]string{}}
}
func (d *dockerCLI) add(ref string) string {
	id := "img-" + ref
	d.images[ref] = id
	return id
}
func (d *dockerCLI) Run(_ context.Context, command string, args []string, _, _ string) ([]byte, error) {
	if command != "docker" || len(args) == 0 {
		return nil, errors.New("unexpected runtime command")
	}
	switch args[0] {
	case "pull":
		d.add(args[1])
		return nil, nil
	case "run":
		name, ref := args[2], args[len(args)-1]
		if _, ok := d.containers[name]; ok {
			return nil, errors.New("container name in use")
		}
		d.containers[name] = ref
		d.running[name] = true
		return nil, nil
	case "stop":
		d.running[args[len(args)-1]] = false
		return nil, nil
	case "start":
		d.running[args[len(args)-1]] = true
		return nil, nil
	case "rename":
		from, to := args[1], args[2]
		if _, ok := d.containers[from]; !ok {
			return nil, errors.New("no such container")
		}
		d.containers[to], d.running[to] = d.containers[from], d.running[from]
		delete(d.containers, from)
		delete(d.running, from)
		return nil, nil
	case "rm":
		name := args[len(args)-1]
		delete(d.containers, name)
		delete(d.running, name)
		return nil, nil
	case "rmi":
		delete(d.images, args[1])
		return nil, nil
	case "ps":
		var names []string
		for name := range d.containers {
			names = append(names, name)
		}
		return []byte(strings.Join(names, "\n")), nil
	case "images":
		var ids []string
		for _, id := range d.images {
			ids = append(ids, id)
		}
		return []byte(strings.Join(ids, "\n")), nil
	case "inspect":
		name := args[len(args)-1]
		for _, n := range args[3:] {
			if _, ok := d.containers[n]; !ok {
				continue
			}
			name = n
		}
		ref, ok := d.containers[name]
		if !ok {
			return nil, errors.New("no such object")
		}
		switch {
		case strings.Contains(args[2], "State.Running"):
			return []byte(fmt.Sprintf("%t\t%s\t%s", d.running[name], ref, d.images[ref])), nil
		case strings.Contains(args[2], ".Image"):
			var out []string
			for _, n := range args[3:] {
				if r, ok := d.containers[n]; ok {
					out = append(out, d.images[r])
				}
			}
			return []byte(strings.Join(out, "\n")), nil
		}
		return nil, errors.New("unexpected inspect format")
	case "image":
		id := args[len(args)-1]
		for ref, imageID := range d.images {
			if imageID != id {
				continue
			}
			if strings.Contains(args[2], "RepoDigests") {
				return []byte(ref), nil
			}
			return []byte(id + "\t2026-01-01T00:00:00Z\t" + ref + ","), nil
		}
		return nil, errors.New("no such image")
	}
	return nil, errors.New("unexpected runtime arguments")
}

func TestContainerRolloutAdoptAndDeploy(t *testing.T) {
	s, g, sink, _, r := setup(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	old := "sha256:" + strings.Repeat("a", 64)
	next := "sha256:" + strings.Repeat("b", 64)
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api",
		Health: &config.Health{URL: server.URL, Status: 200, Timeout: "1s", Retries: 1}, Runtime: "docker"}
	r.Install = []config.Install{spec}
	r.Root, r.Smoke = "", config.Command{}
	r.SmokeTimeout = "1s"
	d := newDockerCLI()
	ref := spec.Image + "@" + old
	d.containers[spec.Name] = ref
	d.running[spec.Name] = true
	d.add(ref)
	s.Runner = d
	pinned := spec
	pinned.Digest = next
	m, e := manifest.Build(r.Name, shaB, []config.Install{pinned}, nil)
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	if e = s.Rollout(context.Background(), r, Options{}); e == nil || !strings.Contains(e.Error(), "unmanaged") {
		t.Fatalf("unmanaged container replaced: %v", e)
	}
	if e = s.Rollout(context.Background(), r, Options{Adopt: "api"}); e != nil {
		t.Fatal(e)
	}
	records := sink.records
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if sink.records != records+1 {
		t.Fatalf("container deploy not reported: %d", sink.records)
	}
	receipt, _, e := rollout.LoadContainerReceipt(filepath.Join(s.state(r.Name), "rollout"), "host-a", r.Name)
	if e != nil || receipt.Targets["api"].Digest != next || !reflect.DeepEqual(receipt.Targets["api"].Previous, []string{old}) {
		t.Fatalf("receipt %+v %v", receipt, e)
	}
	if e = s.Rollout(context.Background(), r, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
}

func TestContainerRolloutDryRunWritesNoState(t *testing.T) {
	s, g, _, _, r := setup(t)
	digest := "sha256:" + strings.Repeat("c", 64)
	spec := config.Install{Kind: "container", Target: "api", Image: "registry.example/api", Name: "fleet-api",
		Health: &config.Health{URL: "http://127.0.0.1:1/healthz", Status: 200, Timeout: "1s", Retries: 1}, Runtime: "docker"}
	r.Install = []config.Install{spec}
	r.Root, r.Smoke, r.SmokeTimeout = "", config.Command{}, "1s"
	s.Runner = newDockerCLI()
	pinned := spec
	pinned.Digest = digest
	m, e := manifest.Build(r.Name, shaB, []config.Install{pinned}, nil)
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	if e = s.Rollout(context.Background(), r, Options{DryRun: true, Digest: spec.Image + "@" + digest}); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(s.state(r.Name), "rollout", rollout.ContainerReceiptName)); !os.IsNotExist(e) {
		t.Fatal("dry-run wrote a container receipt")
	}
}

func TestChangedHeadStopsReviewAndLand(t *testing.T) {
	for _, command := range []string{"review", "land"} {
		t.Run(command, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.nextHead = shaC
			if command == "land" {
				g.reviewed = true
			}
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

func TestFileRolloutCanaryGate(t *testing.T) {
	s, g, _, _, r := setup(t)
	s.Config.Host = "host-b"
	spec := config.Install{Source: "tool", Target: "bin/tool", Mode: "0755", Canary: "host-a", ReceiptsDir: t.TempDir()}
	r.Install = []config.Install{spec}
	r.Root = t.TempDir()
	r.Smoke = config.Command{Command: "smoke"}
	s.Runner = smokeRunner{}
	m, e := manifest.Build(r.Name, shaB, r.Install, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	g.m = m
	if e = s.Rollout(context.Background(), r, Options{}); e == nil {
		t.Fatal("file install ignored canary")
	}
	if _, e = os.Stat(filepath.Join(r.Root, "bin/tool")); !os.IsNotExist(e) {
		t.Fatal("waiting follower mutated files")
	}
	if e = rollout.PublishGate(spec, "host-a", r.Name, m.Commit); e != nil {
		t.Fatal(e)
	}
	if e = s.Rollout(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	if e = rollout.CheckGate(config.Install{Canary: "host-b", ReceiptsDir: spec.ReceiptsDir, Target: spec.Target}, "host-c", r.Name, m.Commit); e != nil {
		t.Fatal("follower did not publish success receipt:", e)
	}
}
