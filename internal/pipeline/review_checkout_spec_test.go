package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// freshReviewerSpecs clears the process-wide memory of a refused checkout
// spec: one test's old hub must not decide another's spawn.
func freshReviewerSpecs(t *testing.T) {
	t.Helper()
	reset := func() {
		reviewerSpecs.mu.Lock()
		reviewerSpecs.legacyTo = time.Time{}
		reviewerSpecs.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

// reviewOrigin scripts the origin probe a reviewer reads before it names a
// checkout spec.
func reviewOrigin(runner *panewireRunner, root, origin string) {
	if runner.gitOuts == nil {
		runner.gitOuts = map[string]string{}
	}
	runner.gitOuts[key("git", "-C", root, "remote", "get-url", "origin")] = origin + "\n"
}

// A hub that advertises checkout_spec_v1 is named the reviewed head with the
// three --checkout-* flags instead of -c, and the daemon adds no worktree
// under review.panewire.checkouts for the reviewer (hk 4807).
func TestReviewerSpawnNamesTheCheckoutSpecWhenTheHubAdvertisesIt(t *testing.T) {
	freshReviewerSpecs(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	var record []byte
	runner.afterRun = func() {
		record, _ = os.ReadFile(s.reviewerJobPath(r.Name, 7, 1))
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	sp := runner.spawns[0]
	if got := arg(sp, "--checkout-origin"); got != "git@github.com:example/project.git" {
		t.Fatalf("--checkout-origin=%q argv %v", got, sp)
	}
	if arg(sp, "--checkout-commit") != shaA || arg(sp, "--checkout-ref") != "pull/7/head" {
		t.Fatalf("spec flags: %v", sp)
	}
	if slices.Contains(sp, "-c") {
		t.Fatalf("-c beside a checkout spec: %v", sp)
	}
	if n := worktreeAdds(runner.git); n != 0 {
		t.Fatalf("worktree adds: %d, git %v", n, gitCalls(runner, r.Root))
	}
	if _, e := os.Stat(s.Config.Review.Panewire.Checkouts); !os.IsNotExist(e) {
		t.Fatalf("a checkout directory was made: %v", e)
	}
	if len(g.passes[shaA]) != 1 || g.passes[shaA][0].Executor != "panewire" {
		t.Fatalf("pass: %+v", g.passes[shaA])
	}
	// The recorded spawn argv is what a rollout reads, and it names no local
	// checkout: nothing may try to remove one.
	if len(record) == 0 || !bytes.Contains(record, []byte("--checkout-origin")) || bytes.Contains(record, []byte(`"checkout"`)) {
		t.Fatalf("record: %s", record)
	}
}

// An old hub, one that does not advertise checkout_spec_v1, refuses the spec
// before submitting anything: the pass then makes the local checkout it always
// made and places the reviewer from it, and remembers the refusal.
func TestReviewerSpawnFallsBackToALocalCheckoutWhenTheHubLacksTheSpec(t *testing.T) {
	freshReviewerSpecs(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", specRefused: true}
	s, g, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	var record reviewerJob
	runner.afterRun = func() {
		if b, e := os.ReadFile(s.reviewerJobPath(r.Name, 7, 1)); e == nil {
			_ = json.Unmarshal(b, &record)
		}
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 2 || !slices.Contains(runner.spawns[0], "--checkout-origin") {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	retry := runner.spawns[1]
	if slices.Contains(retry, "--checkout-origin") || arg(retry, "-c") == "" {
		t.Fatalf("the retry is not the local checkout: %v", retry)
	}
	if arg(retry, "-c") != filepath.Join(s.Config.Review.Panewire.Checkouts, arg(retry, "-l")) {
		t.Fatalf("-c %q does not name the reviewer's checkout", arg(retry, "-c"))
	}
	if n := worktreeAdds(runner.git); n != 1 {
		t.Fatalf("worktree adds: %d", n)
	}
	// The record follows the retry: it names the local checkout, so a later
	// resume keeps cleaning it up.
	if record.Checkout != arg(retry, "-c") || slices.Contains(record.Args, "--checkout-origin") {
		t.Fatalf("record: %+v", record)
	}
	if len(g.passes[shaA]) != 1 || g.passes[shaA][0].Executor != "panewire" {
		t.Fatalf("pass: %+v", g.passes[shaA])
	}
	if !reviewerSpecs.legacy() {
		t.Fatal("the hub's refusal was not remembered")
	}
}

// While a refusal is remembered the pass creates the local checkout first, so
// an old hub costs one refused attempt per refusal window, not one per spawn.
func TestReviewerSpawnSkipsTheSpecWhileTheRefusalIsRemembered(t *testing.T) {
	freshReviewerSpecs(t)
	reviewerSpecs.refused()
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, _, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "git@github.com:example/project.git")
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 || slices.Contains(runner.spawns[0], "--checkout-origin") || arg(runner.spawns[0], "-c") == "" {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	if n := worktreeAdds(runner.git); n != 1 {
		t.Fatalf("worktree adds: %d", n)
	}
}

// A PR GitHub has no pull/<n>/head for names its branch, as fetchPRHead already
// decides, and the reviewer still takes the spec instead of a worktree.
func TestReviewerSpawnNamesTheBranchWhenThePullRefIsGone(t *testing.T) {
	freshReviewerSpecs(t)
	o := seedPROrigin(t, false)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai", realGit: true}
	runner.gitOuts = map[string]string{key("git", "-C", o.root, "remote", "get-url", "origin"): "https://github.com/example/project.git\n"}
	s, g, r := panewireSetup(t, runner)
	r.Root = o.root
	g.pull.Head.SHA = o.head
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns: %v", runner.spawns)
	}
	sp := runner.spawns[0]
	if arg(sp, "--checkout-origin") != "https://github.com/example/project.git" || arg(sp, "--checkout-commit") != o.head || arg(sp, "--checkout-ref") != "refs/heads/feat/thing" {
		t.Fatalf("spec flags: %v", sp)
	}
	if slices.Contains(sp, "-c") || worktreeAdds(runner.git) != 0 {
		t.Fatalf("local checkout used: %v", gitCalls(runner, o.root))
	}
	// The branch fetch still landed the head in the ref panewire reads.
	if got := gitAt(t, o.root, "rev-parse", "refs/remotes/origin/feat/thing"); got != o.head {
		t.Fatalf("refs/remotes/origin/feat/thing=%s want %s", got, o.head)
	}
}

// A spec-placed reviewer holds no local checkout: its record names none, so
// nothing removes a checkout that was never made and the orphan sweep writes
// no row for it (hk 4807).
func TestSpecPlacedReviewerHoldsNoLocalCheckout(t *testing.T) {
	f := newOrphanFix(t)
	f.run.outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	f.run.outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	dir := t.TempDir()
	f.s.Config.Review.Panewire.Checkouts = dir
	label := reviewerLabelFor(12, 7*time.Hour)
	if e := fsutil.WriteJSON(f.s.reviewerJobPath(testRepo.Name, 12, 1), reviewerJob{PR: 12, Head: headSHA, Pass: 1, Label: label,
		Args: []string{"spawn", "--checkout-origin", "https://github.com/example/project.git", "--checkout-commit", headSHA, "--checkout-ref", "pull/12/head", "--role", "reviewer"}}); e != nil {
		t.Fatal(e)
	}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := orphanRows(t, f); len(rows) != 0 {
		t.Fatalf("orphan rows for a checkout that was never made: %v", rowSummary(rows))
	}
	if f.run.called(key("git", "-C", "/repo", "worktree", "remove", filepath.Join(dir, label))) {
		t.Fatalf("removal of a checkout that was never made: %q", f.run.calls)
	}
	for _, row := range f.rows(t) {
		if row.Worktree == filepath.Join(dir, label) {
			t.Fatalf("reviewer sweep claimed the local checkout: %v", rowSummary(f.rows(t)))
		}
	}
}

// A spec-placed reviewer's node checkout is named <label>-checkout, not
// <label>: the sweep still recognises it and removes it through the hub's
// guarded removal.
func TestReapReviewerRemovesSpecPlacedNodeCheckout(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	cwd := "/repo/" + label + "-checkout"
	f.addPane("w1:p5", label, cwd)
	f.hub.detached[cwd] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5", Cwd: cwd}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.hub.ops("worktree.remove")) != 0 {
		t.Fatalf("removed under a running pane: %v", f.hub.commands)
	}
	// panewire's node closes the pane; the sweep then removes its worktree.
	f.hub.agents["agents"], f.hub.agents["panes"] = []any{}, []any{}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != cwd || removes[0]["expect_head"] != headSHA {
		t.Fatalf("removes: %v", removes)
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[1].Result != "removed" || rows[1].Worktree != cwd || rows[1].Label != label {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// A checkout whose origin carries a credential keeps the local path: such an
// origin would otherwise sit in the spawn argv (visible in ps) and in the
// reviewer-job record, which outlives the attempt.
func TestReviewerSpawnKeepsACredentialedOriginOutOfTheArgv(t *testing.T) {
	freshReviewerSpecs(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, _, r := panewireSetup(t, runner)
	reviewOrigin(runner, r.Root, "https://user:secret-token@github.com/example/project.git")
	var record []byte
	runner.afterRun = func() {
		record, _ = os.ReadFile(s.reviewerJobPath(r.Name, 7, 1))
	}
	if e := s.Review(context.Background(), r, 7, "openai", Options{}); e != nil {
		t.Fatal(e)
	}
	sp := runner.spawns[0]
	if slices.Contains(sp, "--checkout-origin") || arg(sp, "-c") == "" {
		t.Fatalf("spec named for a credentialed origin: %v", sp)
	}
	for _, seen := range []string{strings.Join(sp, " "), string(record)} {
		if strings.Contains(seen, "secret-token") {
			t.Fatalf("credential reached the spawn or the record: %s", seen)
		}
	}
}

// Only an origin that can carry a credential keeps the local path: any https
// userinfo can be a token, while an ssh or git login name is not a credential.
// The same line panewire's own rule draws.
func TestSpecOriginCarriedRefusesOnlyCredentials(t *testing.T) {
	for _, tc := range []struct {
		origin string
		want   bool
	}{
		{"https://github.com/example/project.git", true},
		{"https://user@github.com/example/project.git", false},
		{"https://user:token@github.com/example/project.git", false},
		{"ssh://git@github.com/example/project.git", true},
		{"ssh://deploy:secret@github.com/example/project.git", false},
		{"git@github.com:example/project.git", true},
		{"deploy:secret@github.com:example/project.git", false},
		{"/tmp/origin.git", false},
		{"file:///tmp/origin.git", false},
		{"", false},
	} {
		if got := specOriginCarried(tc.origin); got != tc.want {
			t.Errorf("%q: got %v, want %v", tc.origin, got, tc.want)
		}
	}
}
