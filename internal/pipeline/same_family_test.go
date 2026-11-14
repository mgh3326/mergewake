package pipeline

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// outcomeSameFamily reads the PR-level same_family the outcome ledger holds.
func outcomeSameFamily(t *testing.T, s *Service, repo string) bool {
	t.Helper()
	state, e := s.ledger().Read()
	if e != nil {
		t.Fatal(e)
	}
	return state.Latest["https://github.com/"+repo+"/pull/7"].SameFamily
}

// sortedPasses returns the head's posted receipts in pass order; the passes of
// a head post in whatever order they finish.
func sortedPasses(passes []github.PassReceipt) []github.PassReceipt {
	out := slices.Clone(passes)
	slices.SortFunc(out, func(a, b github.PassReceipt) int { return a.Pass - b.Pass })
	return out
}

// (a) A reviewer whose family cannot be named is not the author's family: the
// receipt says same_family false and names no family, and the hk record omits
// same_family rather than claiming a comparison that never happened.
func TestSameFamilyUnknownReviewerFamily(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	pane := s.Runner.(*fakeRunner).pane
	pane.family, pane.noEvents = "", true
	if e := s.Review(context.Background(), r, 7, "anthropic", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	passes := g.passes[shaA]
	if len(passes) != 1 {
		t.Fatalf("passes %+v", passes)
	}
	if passes[0].Family != "" || passes[0].SameFamily {
		t.Fatalf("unknown reviewer family recorded as same-family: %+v", passes[0])
	}
	got := hk.received()
	if len(got) != 1 || len(got[0].Reviews) != 1 {
		t.Fatalf("events %+v", got)
	}
	if got[0].Reviews[0].SameFamily != nil {
		t.Fatalf("same_family sent without a known reviewer family: %v", *got[0].Reviews[0].SameFamily)
	}
}

// A pass record leaves same_family out when the author family is not on the
// receipt: the reviewer family alone cannot say whether the families matched.
func TestPassRecordSameFamilyNeedsBothFamilies(t *testing.T) {
	pr := github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}
	at := time.Now()
	rec := passRecord(pr, github.PassReceipt{Pass: 1, Round: 1, Lens: "default", Family: "openai", SameFamily: false}, nil, "", at, at)
	if rec.SameFamily != nil {
		t.Fatalf("same_family sent without a known author family: %v", *rec.SameFamily)
	}
}

// (b) and (c) same_family is a property of the head being judged: a
// same-family review on an earlier head cannot hold the flag true for a later
// head whose own passes came from another family, and a head whose counted
// passes all match the author still reads true.
func TestSameFamilyCountsOnlyTheJudgedHead(t *testing.T) {
	s, g, _, _, r := reviewSetup(t)
	g.enabled = true
	g.blocking = true
	g.pull.Body = "Mergewake-Author-Family: anthropic\n"
	runner := s.Runner.(*fakeRunner)
	runner.pane.family = "anthropic"
	ctx := context.Background()
	if e := s.Review(ctx, r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	for _, pass := range sortedPasses(g.passes[shaA]) {
		if pass.Family != "anthropic" || !pass.SameFamily {
			t.Fatalf("same-family pass %+v", pass)
		}
	}
	if !outcomeSameFamily(t, s, r.Name) {
		t.Fatal("same-family head did not record same_family")
	}

	// The head moves to a different patch and is reviewed by another family.
	g.pull.Head.SHA = shaC
	g.diffs = map[string]string{shaC: "diff --git a/other b/other\n--- a/other\n+++ b/other\n@@ -1 +1 @@\n-old\n+new\n"}
	runner.pane.family = "openai"
	if e := s.Review(ctx, r, 7, "", Options{}); e != nil {
		t.Fatal(e)
	}
	for _, pass := range sortedPasses(g.passes[shaC]) {
		if pass.Family != "openai" || pass.SameFamily {
			t.Fatalf("other-family pass %+v", pass)
		}
	}
	if outcomeSameFamily(t, s, r.Name) {
		t.Fatal("an earlier head's same-family pass held same_family true for the new head")
	}
	p, e := s.Policy(ctx, r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.mergeGate(ctx, r, g.pull, p, shaB)
	if e != nil {
		t.Fatal(e)
	}
	if v.SameFamily {
		t.Fatal("gate verdict read same_family for a head reviewed by another family")
	}
	if e = s.Land(ctx, r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if outcomeSameFamily(t, s, r.Name) {
		t.Fatal("land kept same_family from the earlier head")
	}
}

// When the verdict inherits an earlier head's review, the counted passes are
// the inherited ones alone: the current head's own partial receipts do not
// join them, and one inherited pass from another family reads false even
// though another inherited pass was same-family.
func TestSameFamilyVerdictUsesInheritedSetOnly(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.blocking = true
	ctx := context.Background()
	diff, e := g.Diff(ctx, "", 0)
	if e != nil {
		t.Fatal(e)
	}
	patch := reviewPatchID(diff)
	g.passes = map[string][]github.PassReceipt{
		shaA: {
			{Pass: 1, Lens: "correctness", Family: "anthropic", SameFamily: true, Patch: patch},
			{Pass: 2, Lens: "state-risk", Family: "openai", SameFamily: false, Patch: patch},
		},
		// One own pass of the current head is not a complete review, so the
		// verdict inherits shaA's.
		shaC: {{Pass: 1, Lens: "correctness", Family: "anthropic", SameFamily: true, Patch: patch}},
	}
	g.pull.Head.SHA = shaC
	p, e := s.Policy(ctx, r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.mergeGate(ctx, r, g.pull, p, shaB)
	if e != nil {
		t.Fatal(e)
	}
	if v.Review.Inherited != shaA {
		t.Fatalf("verdict inherited %q, want %s", v.Review.Inherited, shaA)
	}
	if v.SameFamily {
		t.Fatal("own and inherited passes were OR-ed: one inherited other-family pass must read false")
	}
}

// (d) A receipt that names no reviewer family is unknown, not same-family,
// even one written before the flag meant anything: the gate cannot call it
// the author's family. A receipt whose family is known and marked same still
// counts.
func TestSameFamilyUnknownFamilyReceipt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		receipt github.PassReceipt
		sameFam bool
	}{
		{"unknown reviewer family stored as same", github.PassReceipt{Pass: 1, Lens: "default", Family: "", SameFamily: true}, false},
		{"known same family", github.PassReceipt{Pass: 1, Lens: "default", Family: "anthropic", SameFamily: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.passes = map[string][]github.PassReceipt{shaA: {tc.receipt}}
			g.refs = map[string]bool{doneRef(7, shaA): true}
			p, e := s.Policy(context.Background(), r, shaB)
			if e != nil {
				t.Fatal(e)
			}
			v, e := s.mergeGate(context.Background(), r, g.pull, p, shaB)
			if e != nil {
				t.Fatal(e)
			}
			if v.SameFamily != tc.sameFam {
				t.Fatalf("same_family=%v want %v", v.SameFamily, tc.sameFam)
			}
		})
	}
}
