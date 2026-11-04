package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// cleanupOpts shape one review pass whose reviewer pane lands on node-a, the
// hub's machine, in /repo/<label>. The zero value is a terminal pass with an
// empty composer and a live pane named for its label.
type cleanupOpts struct {
	paneName         func(label string) string // the live pane's name; default is the label
	composer         string                    // pane.read's composer answer; default "empty"
	removeOutcome    string                    // worktree.remove's answer; default "removed"
	forbid           []string                  // hub ops answered 403
	waitTimeout      bool                      // the spawn's --wait timed out after its pane started
	expireAfterSpawn bool                      // the review context expires once the spawn returns
}

type cleanupResult struct {
	out    passOutcome
	err    error
	ctx    context.Context
	s      *Service
	repo   config.Repo
	hub    *fakeReapHub
	runner *panewireRunner
}

func runReviewerCleanup(t *testing.T, o cleanupOpts) cleanupResult {
	t.Helper()
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	hub := newFakeReapHub()
	composer := o.composer
	if composer == "" {
		composer = "empty"
	}
	hub.composer = map[string]any{"outcome": "read", "composer": composer}
	hub.removeOutcome = o.removeOutcome
	hub.forbid = map[string]bool{}
	for _, op := range o.forbid {
		hub.forbid[op] = true
	}
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	g.pull.Head.SHA = headSHA

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	runner.placement = " placement=" + base64.RawURLEncoding.EncodeToString([]byte(`{"chosen_host":"node-a","pane_id":"w1:p1"}`))
	runner.waitTimeout = o.waitTimeout
	runner.afterRun = func() {
		label := arg(runner.spawns[len(runner.spawns)-1], "-l")
		cwd := "/repo/" + label
		name := label
		if o.paneName != nil {
			name = o.paneName(label)
		}
		runner.sink.jobs[label] = integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`), Host: "node-a", PaneID: "w1:p1", Cwd: cwd}
		pane := map[string]any{"pane_id": "w1:p1", "name": name, "agent_status": "idle", "agent": "claude", "cwd": cwd}
		hub.agents["agents"] = []any{pane}
		hub.agents["panes"] = []any{pane}
		hub.wtPaths = map[string]bool{cwd: true}
		if o.expireAfterSpawn {
			cancel()
		}
	}
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: "diff --git a/tool b/tool\n+new\n"}
	out, err := x.runPass(ctx, &passPlan{pass: 1, lens: "default"}, func() (bool, error) { return true, nil })
	return cleanupResult{out: out, err: err, ctx: ctx, s: s, repo: r, hub: hub, runner: runner}
}

// hubOps lists the hub ops the run reached, in order.
func (c cleanupResult) hubOps() []string {
	var ops []string
	for _, cmd := range c.hub.commands {
		op, _ := cmd["op"].(string)
		ops = append(ops, op)
	}
	return ops
}

// receipts are this run's reviewer-cleanup rows in reap.jsonl.
func (c cleanupResult) receipts(t *testing.T) []reapReceipt {
	t.Helper()
	all, err := c.s.reapReceipts(c.repo.Name)
	if err != nil {
		t.Fatal(err)
	}
	var out []reapReceipt
	for _, row := range all {
		if row.Action == "reviewer-cleanup" {
			out = append(out, row)
		}
	}
	return out
}

// audits are this run's reviewer-cleanup records in decisions.jsonl.
func (c cleanupResult) audits(t *testing.T) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(c.s.state(c.repo.Name), "decisions.jsonl"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var rec struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Command == "reviewer-cleanup" {
			out = append(out, rec.Value)
		}
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReviewerCleanupClosesThenRemovesWithReceipt(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{})
	if res.err != nil || res.out.executor != "panewire" {
		t.Fatalf("outcome=%+v error=%v", res.out, res.err)
	}
	if want := []string{"pane.read", "pane.close", "worktree.inspect", "worktree.remove"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "removed" || receipts[0].Pane != "w1:p1" || !strings.HasPrefix(receipts[0].Worktree, "/repo/") {
		t.Fatalf("receipts = %+v", receipts)
	}
	audits := res.audits(t)
	if len(audits) != 1 || audits[0]["result"] != "removed" || audits[0]["pane"] != "w1:p1" || audits[0]["head"] != headSHA || audits[0]["pass"] != float64(1) || audits[0]["pr"] != float64(7) {
		t.Fatalf("audits = %+v", audits)
	}
}

func TestReviewerCleanupKeepsPaneWithComposerText(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{composer: "has_text"})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if want := []string{"pane.read"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "kept" || !strings.Contains(receipts[0].Reason, "composer") {
		t.Fatalf("receipts = %+v", receipts)
	}
}

func TestReviewerCleanupKeepsPaneOnIdentityMismatch(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{paneName: func(string) string { return "someone-else" }})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if len(res.hubOps()) != 0 {
		t.Fatalf("a mismatched pane was touched: hub ops = %v", res.hubOps())
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "kept" || receipts[0].Reason != "pane name/cwd mismatch" {
		t.Fatalf("receipts = %+v", receipts)
	}
}

func TestReviewerCleanupRunsAfterExpiredReviewContext(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{expireAfterSpawn: true})
	if res.ctx.Err() == nil {
		t.Fatal("the review context did not expire")
	}
	if want := []string{"pane.read", "pane.close", "worktree.inspect", "worktree.remove"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	if receipts := res.receipts(t); len(receipts) != 1 || receipts[0].Result != "removed" {
		t.Fatalf("receipts = %+v", receipts)
	}
}

func TestReviewerCleanupRecordsHubErrorInReceipt(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{forbid: []string{"pane.close"}})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if want := []string{"pane.read"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "error" || !strings.Contains(receipts[0].Reason, "pane close") {
		t.Fatalf("receipts = %+v", receipts)
	}
	if audits := res.audits(t); len(audits) != 1 || audits[0]["result"] != "error" {
		t.Fatalf("audits = %+v", audits)
	}
}

func TestReviewerCleanupRecordsRefusedRemovalAsPending(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{removeOutcome: "detached"})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if want := []string{"pane.read", "pane.close", "worktree.inspect", "worktree.remove"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "pending-cleanup" || receipts[0].Reason != "hub guarded removal refused: detached" {
		t.Fatalf("receipts = %+v", receipts)
	}
}

func TestReviewerCleanupDoesNotCloseAfterWaitTimeout(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{waitTimeout: true})
	if len(res.hubOps()) != 0 {
		t.Fatalf("a pane whose wait timed out was touched: hub ops = %v", res.hubOps())
	}
	if receipts := res.receipts(t); len(receipts) != 0 {
		t.Fatalf("receipts = %+v", receipts)
	}
	if audits := res.audits(t); len(audits) != 0 {
		t.Fatalf("audits = %+v", audits)
	}
}
