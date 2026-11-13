package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reviewerNow is the sweep's clock in these tests; review.timeout is 10m, so
// a label is old enough once it is more than 11m behind it.
var reviewerNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

func reviewerLabelFor(pr int, age time.Duration) string {
	return fmt.Sprintf("mwr-%d-%s-p1-%d", pr, headSHA[:7], reviewerNow.Add(-age).Unix())
}

// reviewerFix is a hub-transport reap whose node-a carries reviewer panes.
// Its jobs are hk job records; its runner answers only what a test adds.
type reviewerFix struct {
	s    *Service
	hub  *fakeReapHub
	run  *scriptRunner
	sink *jobSink
}

func newReviewerFix(t *testing.T, hosts ...reap.Host) *reviewerFix {
	t.Helper()
	hub := newFakeReapHub()
	hub.agents["agents"], hub.agents["panes"] = []any{}, []any{}
	hub.composer = map[string]any{"outcome": "read", "composer": "empty"}
	hub.detached = map[string]bool{}
	run := &scriptRunner{outs: map[string]string{}}
	s, _ := hubSvc(t, hub, run, hosts...)
	sink := &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
	s.Sink = sink
	s.Now = func() time.Time { return reviewerNow }
	s.Config.Review.Timeout = "10m"
	return &reviewerFix{s: s, hub: hub, run: run, sink: sink}
}

// addPane puts an agent pane on node-a and lists it in the full pane list.
func (f *reviewerFix) addPane(id, name, cwd string) {
	p := map[string]any{"pane_id": id, "name": name, "agent_status": "idle", "agent": "claude", "cwd": cwd}
	f.hub.agents["agents"] = append(f.hub.agents["agents"].([]any), p)
	f.hub.agents["panes"] = append(f.hub.agents["panes"].([]any), p)
}

// addReviewer is a remote reviewer: its pane on node-a in /repo/<label>, a
// detached worktree there at the reviewed head, and a completed job record.
func (f *reviewerFix) addReviewer(label, pane string) {
	cwd := "/repo/" + label
	f.addPane(pane, label, cwd)
	f.hub.detached[cwd] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: pane, Cwd: cwd}
}

// acts counts the hub ops that read a composer, close a pane or remove a
// worktree; the host scan's worktree.inspect of each pane cwd is not one.
func (f *reviewerFix) acts() int {
	return len(f.hub.ops("pane.read")) + len(f.hub.ops("pane.close")) + len(f.hub.ops("worktree.remove"))
}

func (f *reviewerFix) reap(t *testing.T, pr int, o Options) error {
	t.Helper()
	return f.s.Reap(context.Background(), testRepo, pr, o)
}

// rows are the reviewer-reap rows in reap.jsonl.
func (f *reviewerFix) rows(t *testing.T) []reapReceipt {
	t.Helper()
	all, err := f.s.reapReceipts(testRepo.Name)
	if err != nil {
		t.Fatal(err)
	}
	var out []reapReceipt
	for _, v := range all {
		if v.Action == "reviewer-reap" {
			out = append(out, v)
		}
	}
	return out
}

// audits are the reviewer-reap records in decisions.jsonl.
func (f *reviewerFix) audits(t *testing.T) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.s.state(testRepo.Name), "decisions.jsonl"))
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
		if rec.Command == "reviewer-reap" {
			out = append(out, rec.Value)
		}
	}
	return out
}

func rowSummary(rows []reapReceipt) []string {
	var out []string
	for _, v := range rows {
		out = append(out, fmt.Sprintf("%s pane=%s wt=%s %s %s", v.Label, v.Pane, v.Worktree, v.Result, v.Reason))
	}
	return out
}

// (a) A completed reviewer job on a remote host, its pane matching the job
// record with an empty composer, and a clean detached worktree: the pane
// closes, the hub's guarded removal removes the worktree, one row and one
// audit record per action.
func TestReapReviewer_A_I1_I2_I4_I5_I6_ClosesPaneAndRemovesWorktree(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	closes := f.hub.ops("pane.close")
	if len(closes) != 1 || closes[0]["pane_id"] != "w1:p5" || closes[0]["name"] != label || closes[0]["cwd"] != "/repo/"+label {
		t.Fatalf("closes: %v", closes)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != "/repo/"+label || removes[0]["detached"] != true || removes[0]["expect_head"] != headSHA || removes[0]["base"] != "main" {
		t.Fatalf("removes: %v", removes)
	}
	if len(f.run.calls) != 0 {
		t.Fatalf("hub host ran local or ssh commands: %q", f.run.calls)
	}
	rows := f.rows(t)
	if len(rows) != 2 ||
		rows[0].Label != label || rows[0].Host != "node" || rows[0].Pane != "w1:p5" || rows[0].Worktree != "" || rows[0].Result != "closed" ||
		rows[1].Label != label || rows[1].Host != "node" || rows[1].Worktree != "/repo/"+label || rows[1].Result != "removed" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
	audits := f.audits(t)
	if len(audits) != 2 || audits[0]["result"] != "closed" || audits[1]["result"] != "removed" || audits[0]["label"] != label {
		t.Fatalf("audits: %v", audits)
	}
}

// (b) The same pane whose job is running, escalated, lost or has no record:
// kept with the state as the reason, never read or closed.
func TestReapReviewer_B_I2_KeepsPaneOfUnfinishedJob(t *testing.T) {
	for _, state := range []string{"running", "escalated", "lost", "missing"} {
		t.Run(state, func(t *testing.T) {
			f := newReviewerFix(t)
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			want := "job:" + state
			if state == "missing" {
				delete(f.sink.recs, label)
				want = "job-record-missing"
			} else {
				rec := f.sink.recs[label]
				rec.State = state
				f.sink.recs[label] = rec
			}
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if n := len(f.hub.ops("pane.close")) + len(f.hub.ops("pane.read")) + len(f.hub.ops("worktree.remove")); n != 0 {
				t.Fatalf("hub ops: %v", f.hub.commands)
			}
			rows := f.rows(t)
			if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != want || rows[0].Pane != "w1:p5" {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
	// A completed job of another role (an author's lead) is no reviewer.
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.Role = "lead"
	f.sink.recs[label] = rec
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(f.hub.ops("pane.close")) != 0 || len(rows) != 1 || rows[0].Reason != "role:lead" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

func TestReapReviewerAcceptsOldAndRenamedWorkerRoles(t *testing.T) {
	for _, role := range []string{"worker", "implementer", "tester", "counsel", "consult"} {
		t.Run(role, func(t *testing.T) {
			f := newReviewerFix(t)
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			rec := f.sink.recs[label]
			rec.Role = role
			f.sink.recs[label] = rec
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			rows := f.rows(t)
			if len(rows) < 1 || rows[0].Result != "closed" || rows[0].Reason == "role:"+role {
				t.Fatalf("role %q not accepted: %v", role, rowSummary(rows))
			}
		})
	}
}

// (c) A label younger than review.timeout + 1m may still belong to a running
// pass: no row, no hub op.
func TestReapReviewer_C_I3_LeavesYoungLabel(t *testing.T) {
	for _, age := range []time.Duration{time.Minute, 10*time.Minute + 30*time.Second, 11 * time.Minute} {
		f := newReviewerFix(t)
		label := reviewerLabelFor(12, age)
		f.addReviewer(label, "w1:p5")
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if f.acts() != 0 || len(f.rows(t)) != 0 {
			t.Fatalf("age %s: ops %v rows %v", age, f.hub.commands, rowSummary(f.rows(t)))
		}
	}
}

// (d) The pane id, cwd or host differs from the job record: kept.
func TestReapReviewer_D_I4_KeepsPaneTheJobDoesNotName(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(f *reviewerFix, label string)
	}{
		{"pane id", func(f *reviewerFix, label string) {
			rec := f.sink.recs[label]
			rec.PaneID = "w1:p9"
			f.sink.recs[label] = rec
		}},
		{"pane id unrecorded", func(f *reviewerFix, label string) {
			rec := f.sink.recs[label]
			rec.PaneID = ""
			f.sink.recs[label] = rec
		}},
		{"cwd", func(f *reviewerFix, label string) {
			for _, list := range []string{"agents", "panes"} {
				f.hub.agents[list].([]any)[0].(map[string]any)["cwd"] = "/repo/elsewhere"
			}
			// Still a worktree of this repo, so the pane is this sweep's
			// candidate and the job/pane disagreement is what keeps it.
			f.hub.detached["/repo/elsewhere"] = true
		}},
		{"job cwd", func(f *reviewerFix, label string) {
			// Same base name, but not the cwd the job record names.
			rec := f.sink.recs[label]
			rec.Cwd = "/repo/other/" + label
			f.sink.recs[label] = rec
		}},
		{"host", func(f *reviewerFix, label string) {
			rec := f.sink.recs[label]
			rec.Host = "node-b"
			f.sink.recs[label] = rec
		}},
		{"no host", func(f *reviewerFix, label string) {
			rec := f.sink.recs[label]
			rec.Host = ""
			f.sink.recs[label] = rec
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReviewerFix(t)
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			test.edit(f, label)
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if f.acts() != 0 {
				t.Fatalf("hub ops: %v", f.hub.commands)
			}
			rows := f.rows(t)
			if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "job record and pane disagree" {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// (e) A composer with text, one that cannot be read, or a close whose recheck
// does not close: kept, and the worktree is not removed.
func TestReapReviewer_E_I5_KeepsPaneWithComposerOrFailedRecheck(t *testing.T) {
	for _, test := range []struct {
		name, composer string
		forbid         bool
		close          string
		reason         string
		closes         int
	}{
		{"draft", "has_text", false, "closed", "composer-text", 0},
		{"unknown", "unknown", false, "closed", "composer-unknown", 0},
		{"unreadable", "empty", true, "closed", "composer-unknown", 0},
		{"recheck changed", "empty", false, "pane-changed", "pane close recheck: pane-changed", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReviewerFix(t)
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			f.hub.composer = map[string]any{"outcome": "read", "composer": test.composer}
			if test.forbid {
				f.hub.forbid = map[string]bool{"pane.read": true}
			}
			f.hub.close = map[string]any{"outcome": test.close}
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			// Only the host scan inspects the pane's cwd; the sweep never reaches the worktree.
			if len(f.hub.ops("pane.close")) != test.closes || len(f.hub.ops("worktree.remove")) != 0 || len(f.hub.ops("worktree.inspect")) != 1 {
				t.Fatalf("hub ops: %v", f.hub.commands)
			}
			rows := f.rows(t)
			if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != test.reason {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// (f) The hub refuses the removal: pending-cleanup, and nothing removes the
// worktree another way. Another pane working inside the worktree: no removal.
func TestReapReviewer_F_I6_RemovesOnlyThroughHubGuard(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		f := newReviewerFix(t)
		label := reviewerLabelFor(12, time.Hour)
		f.addReviewer(label, "w1:p5")
		f.hub.removeOutcome = "dirty"
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.hub.ops("worktree.remove")) != 1 || len(f.run.calls) != 0 {
			t.Fatalf("hub ops %v local %q", f.hub.commands, f.run.calls)
		}
		rows := f.rows(t)
		if len(rows) != 2 || rows[1].Result != "pending-cleanup" || rows[1].Reason != "hub guarded removal refused: dirty" {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
	})
	t.Run("pane in use", func(t *testing.T) {
		f := newReviewerFix(t)
		label := reviewerLabelFor(12, time.Hour)
		f.addReviewer(label, "w1:p5")
		f.hub.agents["panes"] = append(f.hub.agents["panes"].([]any), map[string]any{"pane_id": "w1:p6", "cwd": "/repo/" + label + "/sub"})
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.hub.ops("worktree.remove")) != 0 || len(f.run.calls) != 0 {
			t.Fatalf("hub ops %v local %q", f.hub.commands, f.run.calls)
		}
		rows := f.rows(t)
		if len(rows) != 2 || rows[0].Result != "closed" || rows[1].Result != "kept" || rows[1].Reason != "pane-in-use" {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
	})
	t.Run("identity changed", func(t *testing.T) {
		f := newReviewerFix(t)
		label := reviewerLabelFor(12, time.Hour)
		f.addReviewer(label, "w1:p5")
		f.hub.wtHead = "bbbb2222"
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		rows := f.rows(t)
		if len(f.hub.ops("worktree.remove")) != 0 || len(rows) != 2 || rows[1].Reason != "worktree identity changed" {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
	})
}

// A pane already gone whose job record names no cwd still has its remote
// worktree removed, at the path an earlier reviewer-cleanup receipt recorded.
func TestReapReviewer_F_I6_RemovesGoneReviewerAtReceiptedPath(t *testing.T) {
	f := newReviewerFix(t)
	label := reviewerLabelFor(12, time.Hour)
	cwd := "/repo/" + label
	f.hub.detached[cwd] = true
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Host: "node-a", Pane: "w1:p5", Cwd: cwd, Worktree: "/checkouts/" + label, Result: "kept", Reason: "reviewer job escalated; the pane stays for the operator"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != cwd || len(f.hub.ops("pane.close")) != 0 {
		t.Fatalf("hub ops: %v", f.hub.commands)
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[0].Reason != "pane already gone" || rows[1].Result != "removed" || rows[1].Worktree != cwd {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// localCheckout is a reviewer checkout <checkouts>/<label> on this machine,
// detached at the reviewed head; the local host runs pane in it when inUse.
func localCheckout(t *testing.T, f *reviewerFix, label string, inUse bool) string {
	t.Helper()
	dir := t.TempDir()
	f.s.Config.Review.Panewire.Checkouts = dir
	path := filepath.Join(dir, label)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	panes := `{"result":{"panes":[]}}`
	if inUse {
		panes = `{"result":{"panes":[{"pane_id":"w9:p1","cwd":"` + path + `"}]}}`
	}
	for k, v := range map[string]string{
		key("herdr", "agent", "list"): `{"result":{"agents":[]}}`,
		key("herdr", "pane", "list"):  panes,
		key("git", "-C", path, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel"): "/repo/.git/worktrees/" + label + "\n/repo/.git\n" + path + "\n",
		key("git", "-C", path, "branch", "--show-current"):                                               "\n",
		key("git", "-C", path, "rev-parse", "HEAD"):                                                      headSHA + "\n",
		key("git", "-C", path, "remote", "get-url", "origin"):                                            "git@github.com:example/project.git\n",
		key("git", "-C", path, "status", "--porcelain"):                                                  "",
		key("git", "-C", path, "status", "--porcelain", "--ignored"):                                     "",
		key("git", "-C", "/repo", "worktree", "remove", path):                                            "",
	} {
		f.run.outs[k] = v
	}
	return path
}

// (g) A local checkout a pass kept (spawn wait timed out, so its row is
// pending-cleanup) is removed once its job completed, clean and unused; dirty,
// in use or without a job record it is kept.
func TestReapReviewer_G_I7_RemovesLocalCheckoutOnlyWhenUnused(t *testing.T) {
	for _, test := range []struct {
		name   string
		inUse  bool
		dirty  bool
		noJob  bool
		result string
		reason string
	}{
		{"removed", false, false, false, "removed", ""},
		{"dirty", false, true, false, "kept", "dirty"},
		{"in use", true, false, false, "kept", "pane-in-use"},
		{"no job", false, false, true, "kept", "job-record-missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
			label := reviewerLabelFor(12, time.Hour)
			path := localCheckout(t, f, label, test.inUse)
			if test.dirty {
				f.run.outs[key("git", "-C", path, "status", "--porcelain")] = " M tool.go\n"
			}
			if !test.noJob {
				// Unconfirmed placement: the job names no host.
				f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label}
			}
			seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Worktree: path, Result: "pending-cleanup", Reason: "spawn ended before a pane result; the checkout may still run a pane"})
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			removed := f.run.called(key("git", "-C", "/repo", "worktree", "remove", path))
			if removed != (test.result == "removed") {
				t.Fatalf("removed=%v calls %q", removed, f.run.calls)
			}
			rows := f.rows(t)
			if len(rows) != 1 || rows[0].Result != test.result || rows[0].Reason != test.reason {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
			if !test.noJob && rows[0].Worktree != path {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// (g) A local pass's reviewer-cleanup row says "closed" once the pane closed;
// the pass then removes the checkout and may fail. The label stays open while
// the checkout is on disk, and the sweep removes it.
func TestReapReviewer_G_I7_RetriesCheckoutLeftAfterClosedPane(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
	label := reviewerLabelFor(12, time.Hour)
	path := localCheckout(t, f, label, false)
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "local", PaneID: "w9:p1"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Host: "local", Pane: "w9:p1", Worktree: path, Result: "closed", Reason: "pane closed; local checkout removed by the pass"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) {
		t.Fatalf("checkout not removed; calls %q", f.run.calls)
	}
	rows := f.rows(t)
	if got := rowSummary(rows); len(rows) != 2 || rows[0].Reason != "pane already gone" || rows[1].Result != "removed" || rows[1].Worktree != path {
		t.Fatalf("rows: %v", got)
	}

	// Once the checkout is gone the closed row is terminal again: no visit.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	before := len(f.run.calls)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(rows) != 2 {
		t.Fatalf("second run rows: %v", rowSummary(rows))
	}
	for _, c := range f.run.calls[before:] {
		if strings.Contains(c, path) {
			t.Fatalf("second run touched the checkout: %q", c)
		}
	}
}

// (h) Shadow (reap.act off, or a shadow run) and --dry-run: rows say what
// would happen; no pane closes and nothing is removed.
func TestReapReviewer_H_I8_ShadowAndDryRunNeverAct(t *testing.T) {
	for _, test := range []struct {
		name  string
		o     Options
		noAct bool
	}{
		{"reap.act off", Options{}, true},
		{"shadow", Options{Shadow: true}, false},
		{"dry-run", Options{DryRun: true}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			path := localCheckout(t, f, label, false)
			f.s.Config.Reap.Act = !test.noAct
			var out strings.Builder
			f.s.Out = &out
			if err := f.reap(t, 0, test.o); err != nil {
				t.Fatal(err)
			}
			if n := len(f.hub.ops("pane.close")) + len(f.hub.ops("worktree.remove")); n != 0 || f.run.called("worktree\x00remove") {
				t.Fatalf("acted: hub %v local %q", f.hub.commands, f.run.calls)
			}
			for _, want := range []string{`"result":"would-close"`, `"worktree":"/repo/` + label + `","result":"would-remove"`, `"worktree":"` + path + `","result":"would-remove"`} {
				if !strings.Contains(out.String(), want) {
					t.Fatalf("missing %s in %s", want, out.String())
				}
			}
			rows := f.rows(t)
			if test.o.DryRun && len(rows) != 0 || !test.o.DryRun && len(rows) != 3 {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// unknownPathFix is a completed reviewer job on remote node-a whose pane is
// gone and whose remote worktree path is known nowhere (the job record has no
// cwd and no receipt names one), with a clean local checkout. An earlier
// reviewer-cleanup row names only the local checkout, as the pre-hk-2955 labels
// had.
func unknownPathFix(t *testing.T) (*reviewerFix, string, string) {
	t.Helper()
	f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
	label := reviewerLabelFor(12, time.Hour)
	path := localCheckout(t, f, label, false)
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5"}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Host: "node-a", Pane: "w1:p5", Worktree: path, Result: "pending-cleanup", Reason: "spawn ended before a pane result; the checkout may still run a pane"})
	return f, label, path
}

// L1: a completed job on a remote host, pane gone, no cwd anywhere, clean
// local checkout: the remote side ends kept (final) and the local checkout
// is removed.
func TestReapReviewer_L1_UnknownRemotePathDoesNotBlockLocalRemoval(t *testing.T) {
	f, label, path := unknownPathFix(t)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) {
		t.Fatalf("checkout not removed; calls %q", f.run.calls)
	}
	if len(f.hub.ops("worktree.remove")) != 0 {
		t.Fatalf("a remote removal ran without a path: %v", f.hub.commands)
	}
	var remote, local *reapReceipt
	rows := f.rows(t)
	for i := range rows {
		switch {
		case rows[i].Host == "local":
			local = &rows[i]
		case rows[i].Host != "" && rows[i].Worktree == label:
			remote = &rows[i]
		}
	}
	if remote == nil || remote.Result != "kept" || remote.Reason != reviewerPathUnknown {
		t.Fatalf("remote row missing: %v", rowSummary(rows))
	}
	if local == nil || local.Result != "removed" || local.Worktree != path {
		t.Fatalf("local row missing: %v", rowSummary(rows))
	}
}

// L2: the remote path is known and the hub refuses the removal: the visit
// stops, the local checkout stays.
func TestReapReviewer_L2_RefusedRemoteStopsBeforeLocalRemoval(t *testing.T) {
	f, label, path := unknownPathFix(t)
	cwd := "/repo/" + label
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label, Host: "node-a", PaneID: "w1:p5", Cwd: cwd}
	f.hub.detached[cwd] = true
	f.hub.removeOutcome = "dirty"
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(f.hub.ops("worktree.remove")) != 1 {
		t.Fatalf("hub ops: %v", f.hub.commands)
	}
	if f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) {
		t.Fatalf("local checkout removed after a refused remote removal; calls %q", f.run.calls)
	}
	rows := f.rows(t)
	for _, v := range rows {
		if v.Host == "local" {
			t.Fatalf("local row after a refused remote removal: %v", rowSummary(rows))
		}
	}
	if last := rows[len(rows)-1]; last.Result != "pending-cleanup" || last.Reason != "hub guarded removal refused: dirty" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// L3: the same as L1 with a dirty local checkout: kept dirty.
func TestReapReviewer_L3_UnknownRemotePathKeepsDirtyLocalCheckout(t *testing.T) {
	f, label, path := unknownPathFix(t)
	f.run.outs[key("git", "-C", path, "status", "--porcelain")] = " M tool.go\n"
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) {
		t.Fatalf("dirty checkout removed; calls %q", f.run.calls)
	}
	var local []reapReceipt
	for _, v := range f.rows(t) {
		if v.Host == "local" {
			local = append(local, v)
		}
	}
	if len(local) != 1 || local[0].Result != "kept" || local[0].Reason != "dirty" || local[0].Label != label {
		t.Fatalf("local rows: %v", rowSummary(local))
	}
}

// L4: the same as L1 with the job still spawned: kept job:spawned, and
// nothing is removed.
func TestReapReviewer_L4_UnknownRemotePathKeepsSpawnedJobsCheckout(t *testing.T) {
	f, label, path := unknownPathFix(t)
	rec := f.sink.recs[label]
	rec.State = "spawned"
	f.sink.recs[label] = rec
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) || f.acts() != 0 {
		t.Fatalf("acted on a spawned job: hub %v local %q", f.hub.commands, f.run.calls)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "job:spawned" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}

// L5: the same as L1 under reap.act off, shadow and --dry-run: nothing is
// removed; the local checkout is only reported as would-remove.
func TestReapReviewer_L5_UnknownRemotePathNeverActsInShadowOrDryRun(t *testing.T) {
	for _, test := range []struct {
		name  string
		o     Options
		noAct bool
	}{
		{"reap.act off", Options{}, true},
		{"shadow", Options{Shadow: true}, false},
		{"dry-run", Options{DryRun: true}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _, path := unknownPathFix(t)
			f.s.Config.Reap.Act = !test.noAct
			var out strings.Builder
			f.s.Out = &out
			if err := f.reap(t, 0, test.o); err != nil {
				t.Fatal(err)
			}
			if f.acts() != 0 || f.run.called("worktree\x00remove") {
				t.Fatalf("acted: hub %v local %q", f.hub.commands, f.run.calls)
			}
			if want := `"worktree":"` + path + `","result":"would-remove"`; !strings.Contains(out.String(), want) {
				t.Fatalf("missing %s in %s", want, out.String())
			}
			if strings.Contains(out.String(), `"result":"removed"`) {
				t.Fatalf("removed row in %s", out.String())
			}
		})
	}
}

// (i) A second run over finished labels writes no row and makes no hub call;
// a label still pending writes no repeat row either.
func TestReapReviewer_I_I9_SecondRunIsQuiet(t *testing.T) {
	t.Run("finished", func(t *testing.T) {
		f := newReviewerFix(t)
		f.hub.dropClosed, f.hub.dropRemoved = true, true
		label := reviewerLabelFor(12, time.Hour)
		f.addReviewer(label, "w1:p5")
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		rows, audits, ops := len(f.rows(t)), len(f.audits(t)), len(f.hub.commands)
		if rows != 2 || audits != 2 {
			t.Fatalf("first run rows %v", rowSummary(f.rows(t)))
		}
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		if len(f.rows(t)) != rows || len(f.audits(t)) != audits || len(f.hub.commands) != ops {
			t.Fatalf("second run: rows %v ops %v", rowSummary(f.rows(t)), f.hub.commands[ops:])
		}
	})
	t.Run("pending", func(t *testing.T) {
		f := newReviewerFix(t)
		f.hub.dropClosed = true
		f.hub.removeOutcome = "dirty"
		label := reviewerLabelFor(12, time.Hour)
		f.addReviewer(label, "w1:p5")
		for range 3 {
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
		}
		rows := f.rows(t)
		if len(rows) != 2 || rows[0].Result != "closed" || rows[1].Result != "pending-cleanup" || len(f.audits(t)) != 2 {
			t.Fatalf("rows: %v", rowSummary(rows))
		}
		// Still pending, but the hub is asked again only once a day.
		if n := len(f.hub.ops("worktree.remove")); n != 1 {
			t.Fatalf("removes: %d", n)
		}
	})
}

// (j) An author pane (mw-…, its job completed, in a cwd named for it) beside
// a reviewer pane is never touched by the sweep; and a reap with no reviewers
// writes exactly what it wrote before the sweep existed.
func TestReapReviewer_J_I1_I10_LeavesAuthorPanes(t *testing.T) {
	t.Run("author beside reviewer", func(t *testing.T) {
		f := newReviewerFix(t)
		label := reviewerLabelFor(12, time.Hour)
		f.addReviewer(label, "w1:p5")
		for _, name := range []string{"mw-77-fix", "wk-7", "shell"} {
			id := "w1:" + name
			f.addPane(id, name, "/repo/"+name)
			f.hub.detached["/repo/"+name] = true
			f.sink.recs[name] = integrations.JobRecord{State: "completed", Role: "worker", Label: name, Host: "node-a", PaneID: id, Cwd: "/repo/" + name}
		}
		if err := f.reap(t, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		for _, op := range []string{"pane.read", "pane.close"} {
			for _, args := range f.hub.ops(op) {
				if args["pane_id"] != "w1:p5" {
					t.Fatalf("%s on %v", op, args)
				}
			}
		}
		for _, args := range f.hub.ops("worktree.remove") {
			if args["cwd"] != "/repo/"+label {
				t.Fatalf("removed %v", args)
			}
		}
		for _, v := range f.rows(t) {
			if v.Label != label {
				t.Fatalf("rows: %v", rowSummary(f.rows(t)))
			}
		}
	})
	t.Run("no reviewers", func(t *testing.T) {
		hub := newFakeReapHub()
		r := &scriptRunner{}
		s, _ := hubSvc(t, hub, r)
		s.Config.Review.Timeout = "10m"
		if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, row := range reapLogRows(t, s, testRepo.Name) {
			reason, _ := row["reason"].(string)
			got = append(got, fmt.Sprintf("%s %s %s", row["action"], row["result"], reason))
		}
		want := []string{"close-pane skipped composer-unknown", "remove-worktree removed ", "delete-branch deleted "}
		if !equalStrings(got, want) {
			t.Fatalf("rows %q, want %q", got, want)
		}
		b, err := os.ReadFile(filepath.Join(s.state(testRepo.Name), "decisions.jsonl"))
		if err != nil || strings.Contains(string(b), "reviewer-reap") {
			t.Fatalf("decisions: %v %s", err, b)
		}
	})
}

// closedGH serves PR 34, closed without a merge.
type closedGH struct{ reapGH }

func (closedGH) Pull(_ context.Context, _ string, pr int) (github.Pull, error) {
	return github.Pull{Number: pr, State: "closed", Head: github.Ref{Ref: "wk/y", SHA: headSHA}, Base: github.Ref{Ref: "main"}}, nil
}

// (k) A reviewer of a closed, unmerged PR is cleaned like (a), in the full
// sweep and with --pr naming that PR; --pr limits the sweep to its labels.
func TestReapReviewer_K_I2_I6_CleansReviewerOfClosedPR(t *testing.T) {
	for _, pr := range []int{0, 34} {
		t.Run(fmt.Sprintf("pr=%d", pr), func(t *testing.T) {
			f := newReviewerFix(t)
			f.s.GH = &closedGH{}
			label := reviewerLabelFor(34, time.Hour)
			other := reviewerLabelFor(35, time.Hour)
			f.addReviewer(label, "w1:p5")
			f.addReviewer(other, "w1:p6")
			err := f.reap(t, pr, Options{})
			if pr == 0 && err != nil || pr == 34 && (err == nil || !strings.Contains(err.Error(), "not a merged PR")) {
				t.Fatalf("err: %v", err)
			}
			var mine []reapReceipt
			for _, v := range f.rows(t) {
				if v.Label == label {
					mine = append(mine, v)
				}
			}
			if len(mine) != 2 || mine[0].PR != 34 || mine[0].Result != "closed" || mine[1].Result != "removed" {
				t.Fatalf("rows: %v", rowSummary(f.rows(t)))
			}
			others := len(f.rows(t)) - len(mine)
			if pr == 34 && others != 0 || pr == 0 && others != 2 {
				t.Fatalf("other labels: %v", rowSummary(f.rows(t)))
			}
		})
	}
}

// parseReviewerLabel accepts exactly the label a review pass writes.
// (l) A label names a PR number, not a repo. A completed reviewer pane whose
// worktree belongs to another repository is not this repo's sweep's: it is not
// closed, read or receipted here, and this repo's own reviewer still is.
func TestReapReviewer_L_I1_LeavesAnotherReposReviewer(t *testing.T) {
	f := newReviewerFix(t)
	other := reviewerLabelFor(141, time.Hour)
	own := reviewerLabelFor(12, time.Hour)
	f.addReviewer(other, "w1:p6")
	f.addReviewer(own, "w1:p5")
	f.hub.foreign = map[string]bool{"/repo/" + other: true}
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"pane.read", "pane.close", "worktree.remove"} {
		for _, c := range f.hub.ops(op) {
			if c["pane_id"] == "w1:p6" || c["cwd"] == "/repo/"+other {
				t.Fatalf("%s touched another repo's reviewer: %v", op, c)
			}
		}
	}
	closed := false
	for _, r := range f.rows(t) {
		if r.Label == other {
			t.Fatalf("another repo's reviewer was receipted here: %v", rowSummary(f.rows(t)))
		}
		if r.Label == own && r.Result == "closed" {
			closed = true
		}
	}
	if !closed {
		t.Fatalf("own reviewer not closed: %v", rowSummary(f.rows(t)))
	}
}

func TestReapReviewerLabelParse(t *testing.T) {
	for name, ok := range map[string]bool{
		"mwr-12-aaaa111-p1-1700000000":  true,
		"mwr-12-aaaa111-p10-1700000000": true,
		"mw-12-aaaa111-p1-1700000000":   false,
		"mwr-12-AAAA111-p1-1700000000":  false,
		"mwr-12-aaaa11-p1-1700000000":   false,
		"mwr-12-aaaa111-p1-1700000000x": false,
		"xmwr-12-aaaa111-p1-1700000000": false,
		"mwr-0-aaaa111-p1-1700000000":   false,
		"wk-7":                          false,
	} {
		if _, got := parseReviewerLabel(name); got != ok {
			t.Errorf("%s: %v", name, got)
		}
	}
}

// The review path sweeps an open PR's reviewers through Reap and wants no
// "not a merged PR" refusal, and no reap-error audit row for it.
func TestReapReviewersOnlyOfOpenPR(t *testing.T) {
	f := newReviewerFix(t)
	f.s.GH = &closedGH{}
	label := reviewerLabelFor(34, time.Hour)
	f.addReviewer(label, "w1:p5")
	ctx := context.WithValue(context.Background(), reviewersOnlyKey{}, true)
	if err := f.s.Reap(ctx, testRepo, 34, Options{}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if rows := f.rows(t); len(rows) != 2 || rows[0].Result != "closed" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
	if b, err := os.ReadFile(filepath.Join(f.s.state(testRepo.Name), "decisions.jsonl")); err == nil && strings.Contains(string(b), "reap-error") {
		t.Fatalf("reap-error audited: %s", b)
	}
}

// (l) The PR merges while a review run is still in flight: the held path's
// reviewersOnly sweep closes the reviewer pane, but the merged-PR cleanup
// (the author's job pane, worktree and branch) does not run at all.
func TestReapReviewersOnlyOfMergedPRSkipsCleanup(t *testing.T) {
	f := newReviewerFix(t)
	merged := reviewerNow.Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged,
		Head: github.Ref{Ref: "wk/x", SHA: headSHA}, Base: github.Ref{Ref: "main"}}}
	// The author's pane sits in the merged PR's own worktree, so the merged path
	// would close it and remove the worktree and branch.
	f.addPane("w1:p1", "wk-7", "/repo.wt")
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	ctx := context.WithValue(context.Background(), reviewersOnlyKey{}, true)
	if err := f.s.Reap(ctx, testRepo, 12, Options{}); err != nil {
		t.Fatal(err)
	}
	closes := f.hub.ops("pane.close")
	if len(closes) != 1 || closes[0]["pane_id"] != "w1:p5" {
		t.Fatalf("closes=%v, want only the reviewer pane", closes)
	}
	removes := f.hub.ops("worktree.remove")
	if len(removes) != 1 || removes[0]["cwd"] != "/repo/"+label {
		t.Fatalf("removes=%v, want only the reviewer worktree", removes)
	}
	for _, v := range f.rows(t) {
		if v.Label != label {
			t.Fatalf("merged-PR cleanup receipted: %v", rowSummary(f.rows(t)))
		}
	}
}

// parseReviewerLabel reads the pass of a label.
func TestParseReviewerLabelPass(t *testing.T) {
	l, ok := parseReviewerLabel("mwr-283-7e8272c-p2-1700000000")
	if !ok || l.pr != 283 || l.sha7 != "7e8272c" || l.pass != 2 {
		t.Fatalf("label=%+v ok=%v", l, ok)
	}
}

// (a) A hub replacement label <old>-q<n> parses to the same PR, head, pass and
// start as the original, and keeps n.
func TestReapReviewerLabel_ReplacementSuffixParses(t *testing.T) {
	for name, want := range map[string]reviewerLabel{
		"mwr-283-7e8272c-p1-1794000000":     {pr: 283, sha7: "7e8272c", pass: 1, started: time.Unix(1794000000, 0)},
		"mwr-283-7e8272c-p1-1794000000-q1":  {pr: 283, sha7: "7e8272c", pass: 1, started: time.Unix(1794000000, 0), replacement: 1},
		"mwr-283-7e8272c-p2-1794000000-q2":  {pr: 283, sha7: "7e8272c", pass: 2, started: time.Unix(1794000000, 0), replacement: 2},
		"mwr-283-7e8272c-p1-1794000000-q12": {pr: 283, sha7: "7e8272c", pass: 1, started: time.Unix(1794000000, 0), replacement: 12},
	} {
		want.name = name
		got, ok := parseReviewerLabel(name)
		if !ok || got != want {
			t.Errorf("%s: got %+v ok=%v, want %+v", name, got, ok, want)
		}
	}
}

// (c) A malformed replacement suffix is not a reviewer label.
func TestReapReviewerLabel_MalformedReplacementRejected(t *testing.T) {
	for _, suffix := range []string{"-q", "-q0", "-q01", "-qx", "-q1x", "-q-1", "-q1-q2", "q1", "-Q1"} {
		if _, ok := parseReviewerLabel("mwr-283-7e8272c-p1-1794000000" + suffix); ok {
			t.Errorf("suffix %q accepted", suffix)
		}
	}
}

// (b) A pending cleanup recorded under a replacement label is visited: its
// clean, unused local checkout is removed, as for an original label.
func TestReapReviewer_ReplacementLabelCleanupVisited(t *testing.T) {
	f := newReviewerFix(t, reap.Host{Name: "local", Jobs: "/jobs"})
	label := reviewerLabelFor(12, time.Hour) + "-q1"
	path := localCheckout(t, f, label, false)
	f.sink.recs[label] = integrations.JobRecord{State: "completed", Role: "reviewer", Label: label}
	seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-cleanup", Repo: testRepo.Name, PR: 12, Worktree: path, Result: "pending-cleanup", Reason: "spawn ended before a pane result; the checkout may still run a pane"})
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !f.run.called(key("git", "-C", "/repo", "worktree", "remove", path)) {
		t.Fatalf("checkout not removed; calls %q", f.run.calls)
	}
	rows := f.rows(t)
	if len(rows) != 1 || rows[0].Result != "removed" || rows[0].Worktree != path {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}
