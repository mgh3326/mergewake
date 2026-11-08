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
	refuseAll        bool                      // both the reviewer and the worker seat are refused before any pane starts
	noPlacement      bool                      // the spawn's OK line names no placement receipt
	jobState         string                    // the job record's state after the spawn; "completed" when empty
	noJobPlacement   bool                      // the completed job record names no host and pane either
	expireAfterSpawn bool                      // the review context expires once the spawn returns
	spawnOut         string                    // the spawn's whole stdout, replacing the OK line
	unnamedPanes     bool                      // the full pane list carries no names, as on a hub node
	paneListStatus   string                    // the full pane list's agent_status when unnamedPanes; "idle" when empty
	agentStatus      string                    // the agent list's agent_status; "idle" when empty
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
	runner.unplaced = o.noPlacement
	if !o.noPlacement {
		runner.placement = " placement=" + base64.RawURLEncoding.EncodeToString([]byte(`{"chosen_host":"node-a","pane_id":"w1:p1"}`))
	}
	runner.waitTimeout = o.waitTimeout
	runner.spawnOut = o.spawnOut
	if o.refuseAll {
		runner.refuse = map[string]int{"reviewer": panewireRefused, "worker": panewireRefused}
	}
	runner.afterRun = func() {
		label := arg(runner.spawns[len(runner.spawns)-1], "-l")
		cwd := "/repo/" + label
		name := label
		if o.paneName != nil {
			name = o.paneName(label)
		}
		job := integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`), Host: "node-a", PaneID: "w1:p1", Cwd: cwd}
		if o.jobState != "" {
			job.State = o.jobState
		}
		if o.noJobPlacement {
			job.Host, job.PaneID, job.Cwd = "", "", ""
		}
		runner.sink.jobs[label] = job
		pane := map[string]any{"pane_id": "w1:p1", "name": name, "agent_status": "idle", "agent": "claude", "cwd": cwd}
		hub.agents["agents"] = []any{pane}
		hub.agents["panes"] = []any{pane}
		if o.unnamedPanes {
			// A hub node's full pane list comes from herdr's pane list,
			// which carries no names; only the agent list names panes.
			status := "idle"
			if o.paneListStatus != "" {
				status = o.paneListStatus
			}
			hub.agents["panes"] = []any{map[string]any{"pane_id": "w1:p1", "name": nil, "agent_status": status, "cwd": cwd}}
		}
		if o.agentStatus != "" {
			pane["agent_status"] = o.agentStatus
		}
		// Reviewer checkouts are detached, as on a real node.
		hub.detached = map[string]bool{cwd: true}
		if o.expireAfterSpawn {
			cancel()
		}
	}
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: "diff --git a/tool b/tool\n+new\n"}
	out, err := x.runPass(ctx, &passPlan{pass: 1, lens: "default"}, func() (string, bool, error) { return "lease", true, nil })
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

// A spawn the hub coordinator placed reports a SPAWNED line, not an OK line.
// Its host and pane are the placement, so the pass closes the pane and removes
// its worktree instead of keeping both as pending-cleanup.
func TestReviewerCleanupReadsHubSpawnedReceipt(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{spawnOut: "ACCEPTED job=l request=r1\nSPAWNED job=l host=node-a profile=seat-y pane=w1:p1\nwait job=l outcome=completed report= pane=\n"})
	if res.err != nil || res.out.executor != "panewire" || res.out.reviewer != "panewire:seat-y" {
		t.Fatalf("outcome=%+v error=%v", res.out, res.err)
	}
	if want := []string{"pane.read", "pane.close", "worktree.inspect", "worktree.remove"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "removed" || receipts[0].Pane != "w1:p1" || receipts[0].Host != "node-a" {
		t.Fatalf("receipts = %+v", receipts)
	}
}

// On a hub node the full pane list has no names. The pass takes the name from
// the agent list for the same pane id and cwd, and closes the pane.
func TestReviewerCleanupNamesPaneFromAgentList(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{unnamedPanes: true})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if want := []string{"pane.read", "pane.close", "worktree.inspect", "worktree.remove"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	if r := res.receipts(t); len(r) != 1 || r[0].Result != "removed" {
		t.Fatalf("receipts = %+v", r)
	}
}

// The full pane list's status need not be the agent's: the close sends the
// agent list's status, which the hub accepts.
func TestReviewerCleanupTakesStatusFromAgentList(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{unnamedPanes: true, paneListStatus: "unknown", agentStatus: "done"})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	closes := res.hub.ops("pane.close")
	if len(closes) != 1 || closes[0]["agent_status"] != "done" {
		t.Fatalf("pane.close = %v", closes)
	}
	if r := res.receipts(t); len(r) != 1 || r[0].Result != "removed" {
		t.Fatalf("receipts = %+v", r)
	}
}

// An agent still working is not sent to the hub's close, which would refuse it.
func TestReviewerCleanupKeepsWorkingPane(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{unnamedPanes: true, agentStatus: "working"})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if len(res.hub.ops("pane.close")) != 0 || len(res.hub.ops("worktree.remove")) != 0 {
		t.Fatalf("a working pane was closed: %v", res.hubOps())
	}
	if r := res.receipts(t); len(r) != 1 || r[0].Result != "kept" || !strings.Contains(r[0].Reason, "working") {
		t.Fatalf("receipts = %+v", r)
	}
}

// An agent entry with another name for the pane id still refuses the close.
func TestReviewerCleanupUnnamedPaneWithOtherAgentNameIsKept(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{unnamedPanes: true, paneName: func(string) string { return "mw-author" }})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if r := res.receipts(t); len(r) != 1 || r[0].Result != "kept" || r[0].Reason != "pane name/cwd mismatch" {
		t.Fatalf("receipts = %+v", r)
	}
	for _, op := range res.hubOps() {
		if op == "pane.close" || op == "worktree.remove" {
			t.Fatalf("a mismatched pane was touched: %v", res.hubOps())
		}
	}
}

func TestSpawnedPlacement(t *testing.T) {
	for _, tc := range []struct {
		out  string
		host string
		pane string
		ok   bool
	}{
		{"SPAWNED job=l host=node-a profile=p pane=w1:p1\n", "node-a", "w1:p1", true},
		{"SPAWNED job=l host=node-a profile=p pane=none\n", "", "", false},
		{"SPAWNED job=l host=none profile=none pane=none\n", "", "", false},
		{"SPAWNED job=l host=old profile=p pane=w1:p0\nSPAWNED job=l host=node-b profile=p pane=w2:p2 lane=unregistered\n", "node-b", "w2:p2", true},
		{"ACCEPTED job=l request=r\n", "", "", false},
	} {
		got, ok := spawnPlacement(tc.out)
		if ok != tc.ok || got.Host != tc.host || got.PaneID != tc.pane {
			t.Errorf("%q: got %+v ok=%v", tc.out, got, ok)
		}
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

// A timed-out wait may leave its pane running in the local checkout. The pass
// must close nothing, remove no worktree, and name the kept checkout for later.
func TestReviewerCleanupDoesNotCloseAfterWaitTimeout(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{waitTimeout: true})
	if len(res.hubOps()) != 0 {
		t.Fatalf("a pane whose wait timed out was touched: hub ops = %v", res.hubOps())
	}
	for _, call := range gitCalls(res.runner, res.repo.Root) {
		if strings.Contains(call, "worktree remove") {
			t.Fatalf("the checkout of a timed-out wait was removed: git %q", call)
		}
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "pending-cleanup" || !strings.Contains(receipts[0].Worktree, "checkouts") {
		t.Fatalf("receipts = %+v", receipts)
	}
	if audits := res.audits(t); len(audits) != 1 || audits[0]["result"] != "pending-cleanup" {
		t.Fatalf("audits = %+v", audits)
	}
}

// A refused spawn starts no pane, so its checkout is removed as before.
func TestReviewerCleanupRemovesCheckoutWhenSpawnRefused(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{refuseAll: true})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	removed := false
	for _, call := range gitCalls(res.runner, res.repo.Root) {
		removed = removed || strings.Contains(call, "worktree remove")
	}
	if !removed {
		t.Fatalf("a refused spawn kept its checkout: git %v", gitCalls(res.runner, res.repo.Root))
	}
	if len(res.hubOps()) != 0 || len(res.receipts(t)) != 0 {
		t.Fatalf("a refused spawn has no pane: hub ops=%v", res.hubOps())
	}
}

// The spawn's placement is read only from its OK line, so a placement without
// chosen_host must keep the host the OK line already named.
func TestSpawnPlacementKeepsHostWhenPlacementOmitsIt(t *testing.T) {
	placement := base64.RawURLEncoding.EncodeToString([]byte(`{"pane_id":"w1:p1"}`))
	got, ok := spawnPlacement("OK pane=p1 host=node-a placement=" + placement + "\n")
	if !ok || got.Host != "node-a" || got.PaneID != "w1:p1" {
		t.Fatalf("placement = %+v confirmed=%v", got, ok)
	}
}

// Without a placement, from the spawn or from the job record, the pane's host
// is unknown, so the pass cannot rule out a pane running in the local checkout.
// The checkout stays, named for later.
func TestReviewerCleanupKeepsCheckoutWhenPlacementUnconfirmed(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{noPlacement: true, noJobPlacement: true})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if len(res.hubOps()) != 0 {
		t.Fatalf("an unplaced pane was touched: hub ops = %v", res.hubOps())
	}
	for _, call := range gitCalls(res.runner, res.repo.Root) {
		if strings.Contains(call, "worktree remove") {
			t.Fatalf("the checkout of an unplaced pane was removed: git %q", call)
		}
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "pending-cleanup" || !strings.Contains(receipts[0].Worktree, "checkouts") {
		t.Fatalf("receipts = %+v", receipts)
	}
}

// A spawn whose placement receipt came late still ran: when the completed job
// record names the host and pane, the pass cleans up as if the spawn had named
// them.
func TestReviewerCleanupResolvesUnconfirmedPlacementFromJobRecord(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{noPlacement: true})
	if res.err != nil {
		t.Fatalf("error=%v", res.err)
	}
	if want := []string{"pane.read", "pane.close", "worktree.inspect", "worktree.remove"}; !equalStrings(res.hubOps(), want) {
		t.Fatalf("hub ops = %v, want %v", res.hubOps(), want)
	}
	receipts := res.receipts(t)
	if len(receipts) != 1 || receipts[0].Result != "removed" || receipts[0].Pane != "w1:p1" || !strings.HasPrefix(receipts[0].Worktree, "/repo/") {
		t.Fatalf("receipts = %+v", receipts)
	}
}

// A job record that is not completed does not confirm a placement.
func TestReviewerCleanupIgnoresPlacementOfJobNotCompleted(t *testing.T) {
	res := runReviewerCleanup(t, cleanupOpts{noPlacement: true, jobState: "running"})
	if len(res.hubOps()) != 0 {
		t.Fatalf("hub ops = %v", res.hubOps())
	}
	if receipts := res.receipts(t); len(receipts) != 1 || receipts[0].Result != "pending-cleanup" {
		t.Fatalf("receipts = %+v", receipts)
	}
}
