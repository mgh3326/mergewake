package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

// freshReviewerSparses clears the process-wide memory of a refused sparse
// checkout: one test's old hub must not decide another's spawn.
func freshReviewerSparses(t *testing.T) {
	t.Helper()
	reset := func() {
		reviewerSparses.mu.Lock()
		reviewerSparses.legacyTo = time.Time{}
		reviewerSparses.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

func sparseDiff() string {
	return "diff --git a/keep.txt b/keep.txt\n--- a/keep.txt\n+++ b/keep.txt\n@@ -1 +1 @@\n-old\n+new\n" +
		"diff --git a/added.txt b/added.txt\nnew file mode 100644\n--- /dev/null\n+++ b/added.txt\n@@ -0,0 +1 @@\n+new\n" +
		"diff --git a/old.txt b/old.txt\ndeleted file mode 100644\n--- a/old.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-gone\n" +
		"diff --git a/was.txt b/is.txt\nsimilarity index 90%\nrename from was.txt\nrename to is.txt\n"
}

// With an empty list the spawn args are byte-identical to today's.
func TestSparseCheckoutEmptyListMatchesToday(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, _, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	sp := runner.spawns[0]
	for _, f := range []string{"--checkout-exclude", "--checkout-include"} {
		if slices.Contains(sp, f) {
			t.Fatalf("empty list named sparse flags: %v", sp)
		}
	}
}

// A non-empty list produces exclude flags plus include flags for changed
// files only (deleted files excluded).
func TestSparseCheckoutNonEmptyIncludesChangedOnly(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	r.Review.CheckoutExclude = []string{"Assets", "Docs"}
	g.diffs = map[string]string{shaA: sparseDiff()}
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	sp := runner.spawns[0]
	for _, want := range []string{"Assets", "Docs"} {
		found := false
		for i := 0; i+1 < len(sp); i++ {
			if sp[i] == "--checkout-exclude" && sp[i+1] == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing --checkout-exclude %q in %v", want, sp)
		}
	}
	includes := []string{}
	for i := 0; i+1 < len(sp); i++ {
		if sp[i] == "--checkout-include" {
			includes = append(includes, sp[i+1])
		}
	}
	for _, want := range []string{"keep.txt", "added.txt", "is.txt"} {
		if !slices.Contains(includes, want) {
			t.Fatalf("missing include %q in %v", want, includes)
		}
	}
	for _, banned := range []string{"old.txt", "was.txt"} {
		if slices.Contains(includes, banned) {
			t.Fatalf("deleted/renamed-source %q must not be included: %v", banned, includes)
		}
	}
}

// More than 500 changed paths produce no sparse flags and an audit row.
func TestSparseCheckoutTooManyPathsSkipsAndAudits(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	r.Review.CheckoutExclude = []string{"Assets"}
	// Build 501 distinct paths deterministically.
	var diff strings.Builder
	for i := 0; i < 501; i++ {
		name := "f" + strings.Repeat("x", 2) + "-" + strings.Repeat("y", i%7) + "-" + string(rune('a'+i%26)) + string(rune('0'+(i/26)%10)) + string(rune('A'+(i/182)%26)) + ".txt"
		diff.WriteString("diff --git a/" + name + " b/" + name + "\n--- a/" + name + "\n+++ b/" + name + "\n@@ -1 +1 @@\n-old\n+new\n")
	}
	g.diffs = map[string]string{shaA: diff.String()}
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	sp := runner.spawns[0]
	if slices.Contains(sp, "--checkout-exclude") || slices.Contains(sp, "--checkout-include") {
		t.Fatalf("over-limit PR must take the full checkout: %v", sp)
	}
	rows := auditRows(t, s, r, "reviewer-checkout-sparse-skipped")
	if len(rows) != 1 || rows[0]["reason"] != "too-many-paths" {
		t.Fatalf("sparse-skipped rows: %v", rows)
	}
}

// Config validation refuses !x, /x, a/../b and more than 64 patterns.
func TestSparseCheckoutConfigValidation(t *testing.T) {
	for _, p := range []string{"!x", "/x", "-x", "--routing-role", "a/../b", "a\x00b", "a\nb", ""} {
		if e := config.ValidateCheckoutExcludePattern(p); e == nil {
			t.Fatalf("pattern %q accepted", p)
		}
	}
	if e := config.ValidateCheckoutExcludePattern("Assets"); e != nil {
		t.Fatalf("valid pattern refused: %v", e)
	}
}

// The prompt paragraph appears only when the list is non-empty.
func TestSparseCheckoutPromptParagraph(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, _, r := panewireSetup(t, runner)
	x := &panewireExecutor{s: s, repo: r, pr: s.GH.(*fakeGH).pull, diff: "x", authorFamily: "openai"}
	got := x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
	if strings.Contains(got, "sparse-checkout") {
		t.Fatalf("empty list must not name sparse checkout:\n%s", got)
	}
	r.Review.CheckoutExclude = []string{"Assets", "Docs"}
	x = &panewireExecutor{s: s, repo: r, pr: s.GH.(*fakeGH).pull, diff: "x", authorFamily: "openai"}
	got = x.brief(context.Background(), &passPlan{pass: 1, lens: "default"}, "label", "/tmp/job", "openai")
	for _, want := range []string{"Assets", "Docs", "git sparse-checkout list", "core.sparseCheckout", "git show HEAD:<path>", "not a finding"} {
		if !strings.Contains(got, want) {
			t.Fatalf("sparse paragraph missing %q:\n%s", want, got)
		}
	}
}

// A refusal of the new flags falls back to a full spec spawn.
func TestSparseCheckoutRefusalFallsBackToFullSpec(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", sparseRefused: true}
	s, g, r := panewireSetup(t, runner)
	r.Review.CheckoutExclude = []string{"Assets"}
	g.diffs = map[string]string{shaA: sparseDiff()}
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	if !slices.Contains(runner.spawns[0], "--checkout-exclude") {
		t.Fatalf("first spawn must name sparse flags: %v", runner.spawns[0])
	}
	retry := runner.spawns[1]
	if !slices.Contains(retry, "--checkout-origin") {
		t.Fatalf("retry must keep the checkout spec: %v", retry)
	}
	if slices.Contains(retry, "--checkout-exclude") || slices.Contains(retry, "--checkout-include") {
		t.Fatalf("retry must be the full spec: %v", retry)
	}
	if !reviewerSparses.legacy() {
		t.Fatal("the sparse refusal was not remembered")
	}
	if rows := auditRows(t, s, r, "reviewer-checkout-sparse-refused"); len(rows) != 1 {
		t.Fatalf("sparse-refused rows: %v", rows)
	}
}

// checkout_mode from the spawn answer lands on the reviewer's audit row.
func TestSparseCheckoutModeRecorded(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	r.Review.CheckoutExclude = []string{"Assets"}
	g.diffs = map[string]string{shaA: sparseDiff()}
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	runner.spawnOut = "SPAWNED job=l host=node-a profile=seat-x pane=p1 checkout_mode=sparse\nwait job=l outcome=completed report= pane=p1\n"
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	rows := auditRows(t, s, r, "reviewer-spawn")
	found := false
	for _, row := range rows {
		if row["checkout_mode"] == "sparse" {
			found = true
		}
	}
	if !found {
		t.Fatalf("reviewer-spawn rows lack checkout_mode=sparse: %v", rows)
	}
}

// A PR path that would read as a flag takes the full checkout instead of a
// corruptible sparse spawn, with an audit row naming the reason.
func TestSparseCheckoutFlagLikePathSkipsAndAudits(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	r.Review.CheckoutExclude = []string{"Assets"}
	g.diffs = map[string]string{shaA: "diff --git a/keep.txt b/keep.txt\n--- a/keep.txt\n+++ b/keep.txt\n@@ -1 +1 @@\n-old\n+new\n" +
		"diff --git a/--role b/--role\n--- a/--role\n+++ b/--role\n@@ -1 +1 @@\n-old\n+new\n"}
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	sp := runner.spawns[0]
	if slices.Contains(sp, "--checkout-exclude") || slices.Contains(sp, "--checkout-include") {
		t.Fatalf("flag-like path must take the full checkout: %v", sp)
	}
	rows := auditRows(t, s, r, "reviewer-checkout-sparse-skipped")
	if len(rows) != 1 || rows[0]["reason"] != "flag-like-path" {
		t.Fatalf("sparse-skipped rows: %v", rows)
	}
}

// A hub that refuses both the sparse and the spec flags still reaches the
// local checkout: the sparse retry runs through place, not past it.
func TestSparseCheckoutDoubleRefusalFallsBackToLocal(t *testing.T) {
	freshReviewerSpecs(t)
	freshReviewerSparses(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", sparseRefused: true, specRefused: true}
	s, g, r := panewireSetup(t, runner)
	r.Review.CheckoutExclude = []string{"Assets"}
	g.diffs = map[string]string{shaA: sparseDiff()}
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 3 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	if !slices.Contains(runner.spawns[0], "--checkout-exclude") {
		t.Fatalf("first spawn must name sparse flags: %v", runner.spawns[0])
	}
	retry := runner.spawns[1]
	if !slices.Contains(retry, "--checkout-origin") || slices.Contains(retry, "--checkout-exclude") {
		t.Fatalf("second spawn must be the full spec: %v", retry)
	}
	local := runner.spawns[2]
	if slices.Contains(local, "--checkout-origin") || arg(local, "-c") == "" {
		t.Fatalf("third spawn must be the local checkout: %v", local)
	}
	if len(auditRows(t, s, r, "reviewer-checkout-sparse-refused")) != 1 {
		t.Fatalf("sparse-refused rows: %v", auditRows(t, s, r, "reviewer-checkout-sparse-refused"))
	}
	if len(auditRows(t, s, r, "reviewer-checkout-spec-refused")) != 1 {
		t.Fatalf("spec-refused rows: %v", auditRows(t, s, r, "reviewer-checkout-spec-refused"))
	}
	if !reviewerSpecs.legacy() {
		t.Fatal("the spec refusal was not remembered")
	}
}

// panewireSpawnArgs canonicalizes the real family flag while leaving
// checkout values that spell flags untouched, including across the
// --routing-role deletion.
func TestPanewireSpawnArgsSkipsCheckoutValues(t *testing.T) {
	args := []string{"spawn", "--checkout-include", "--builder-family", "--checkout-include", "--author-family",
		"--author-family", "claude", "--role", "reviewer"}
	got := panewireSpawnArgs(args)
	if i := indexFlag(got, "--builder-family"); i >= 0 {
		t.Fatalf("--builder-family value leaked into a flag lookup: %v", got)
	}
	i := indexFlag(got, "--author-family")
	if i < 0 || got[i+1] != "anthropic" {
		t.Fatalf("real family flag not canonicalized: %v", got)
	}
	keep := 0
	for j := 0; j+1 < len(got); j++ {
		if got[j] == "--checkout-include" && (got[j+1] == "--builder-family" || got[j+1] == "--author-family") {
			keep++
		}
	}
	if keep != 2 {
		t.Fatalf("checkout pairs not preserved: %v", got)
	}
	routed := []string{"spawn", "--checkout-exclude", "--routing-role", "--routing-role", "seat", "--role", "reviewer"}
	// No family flag present, so the default --author-family other is appended.
	if want, got := []string{"spawn", "--checkout-exclude", "--routing-role", "--role", "reviewer", "--author-family", "other"}, panewireSpawnArgs(routed); !slices.Equal(got, want) {
		t.Fatalf("routing-role rewrite: got %v want %v", got, want)
	}
}

// A later mode enriches a label recorded without one, and never overwrites
// an already recorded mode or counts the spawn twice.
func TestSparseCheckoutModeEnrichment(t *testing.T) {
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull}
	plan := &passPlan{pass: 1}
	label := "mwr-7-" + short7(shaA) + "-p1-1700000000"
	x.auditReviewerSpawn(plan, label, "node-a", "")
	x.auditReviewerSpawn(plan, label, "node-a", "sparse")
	rows := auditRows(t, s, r, "reviewer-spawn")
	if len(rows) != 2 {
		t.Fatalf("enrichment rows: %v", rows)
	}
	modes := map[string]bool{}
	for _, row := range rows {
		if m, ok := row["checkout_mode"].(string); ok {
			modes[m] = true
		}
	}
	if len(modes) != 1 || !modes["sparse"] {
		t.Fatalf("mode rows: %v", rows)
	}
	x.auditReviewerSpawn(plan, label, "node-a", "sparse")
	x.auditReviewerSpawn(plan, label, "node-a", "full")
	if rows := auditRows(t, s, r, "reviewer-spawn"); len(rows) != 2 {
		t.Fatalf("repeat/mismatch rows: %v", rows)
	}
}
