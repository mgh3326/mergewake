package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// stuckFix is a reviewer fix whose clock the test moves and whose composer
// reads as unreadable, as herdr reports for an omp pane.
type stuckFix struct {
	*reviewerFix
	now   time.Time
	label string
}

func newStuckFix(t *testing.T) *stuckFix {
	t.Helper()
	f := &stuckFix{reviewerFix: newReviewerFix(t), now: reviewerNow}
	f.s.Now = func() time.Time { return f.now }
	f.hub.composer = map[string]any{"outcome": "read", "composer": "unknown"}
	f.label = reviewerLabelFor(12, time.Hour)
	return f
}

// setPane edits one field of the pane's entry in the agent and pane lists.
func (f *reviewerFix) setPane(id, key string, value any) {
	for _, list := range []string{"agents", "panes"} {
		for _, p := range f.hub.agents[list].([]any) {
			if m := p.(map[string]any); m["pane_id"] == id {
				m[key] = value
			}
		}
	}
}

func (f *stuckFix) setJob(edit func(*integrations.JobRecord)) {
	rec := f.sink.recs[f.label]
	edit(&rec)
	f.sink.recs[f.label] = rec
}

// passAfter moves the clock d past the start and runs one acting pass.
func (f *stuckFix) passAfter(t *testing.T, d time.Duration) {
	t.Helper()
	f.now = reviewerNow.Add(d)
	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
}

func (f *stuckFix) closes() int { return len(f.hub.ops("pane.close")) }

func (f *reviewerFix) jobCloses() [][]string {
	var out [][]string
	for _, c := range f.run.calls {
		if args := strings.Split(c, "\x00"); len(args) > 2 && args[1] == "job" && args[2] == "close" {
			out = append(out, args[1:])
		}
	}
	return out
}

func (f *reviewerFix) allowJobClose() {
	f.run.fn = func(k string) (string, bool) { return "", strings.Contains(k, "\x00job\x00close\x00") }
}

// spawnedStuckFix is a stuck reviewer whose review is over but whose hk job
// record still reads spawned: the abandon close leaves the record spawned until
// panewire's sweep ends the job, so the composer clock still governs its pane.
func spawnedStuckFix(t *testing.T) *stuckFix {
	t.Helper()
	f := newStuckFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	f.allowJobClose()
	f.addReviewer(f.label, "w1:p5")
	f.setJob(func(j *integrations.JobRecord) { j.State, j.OwnerLane = "spawned", "lane-x" })
	return f
}

// (a) A finished omp reviewer pane whose composer cannot be read is never read
// at all: the sweep keeps it for panewire's node to close, however long it
// sits idle.
func TestReapReviewerUnreadableComposerKeepsPaneForNode(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.setPane("w1:p5", "agent_status", "done")
	f.passAfter(t, 0)
	f.passAfter(t, 3*time.Hour)
	rows := f.rows(t)
	if f.closes() != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "pane-in-use" {
		t.Fatalf("closes=%d rows: %v", f.closes(), rowSummary(rows))
	}
}

// (b) A worker pane with an unreadable composer is kept however long it waits.
func TestReapReviewerUnreadableComposerKeepsWorkerPane(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.setJob(func(j *integrations.JobRecord) { j.Role = "worker" })
	f.setPane("w1:p5", "agent_status", "done")
	f.passAfter(t, 0)
	f.passAfter(t, 3*time.Hour)
	f.passAfter(t, 30*time.Hour)
	rows := f.rows(t)
	if f.closes() != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "pane-in-use" {
		t.Fatalf("closes=%d rows: %v", f.closes(), rowSummary(rows))
	}
}

// (c) A focused reviewer pane stays: its worktree waits for panewire's node.
func TestReapReviewerUnreadableComposerKeepsFocusedPane(t *testing.T) {
	f := newStuckFix(t)
	f.addReviewer(f.label, "w1:p5")
	f.setPane("w1:p5", "agent_status", "done")
	f.setPane("w1:p5", "focused", true)
	f.passAfter(t, 0)
	f.passAfter(t, 3*time.Hour)
	rows := f.rows(t)
	if f.closes() != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "pane-in-use" {
		t.Fatalf("closes=%d rows: %v", f.closes(), rowSummary(rows))
	}
}

// stateGH answers every PR read with one pull.
type stateGH struct {
	reapGH
	pull github.Pull
}

func (g stateGH) Pull(context.Context, string, int) (github.Pull, error) { return g.pull, nil }

// (d) A reviewer job that never left spawned, whose pane is done, is closed
// abandoned once its PR is merged or closed or has moved past the reviewed
// head; a PR still open at that head leaves the job and its pane alone.
func TestReapReviewerAbandonsSpawnedJob(t *testing.T) {
	merged := "2026-11-04T10:00:00Z"
	for _, tt := range []struct {
		name    string
		pull    github.Pull
		abandon string // the reason, or "" when the job must stay
	}{
		{"merged", github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}, "PR #12 merged"},
		{"closed", github.Pull{Number: 12, State: "closed", Head: github.Ref{SHA: headSHA}}, "PR #12 closed"},
		{"head moved", github.Pull{Number: 12, State: "open", Head: github.Ref{SHA: "bbbb2222"}}, "PR #12 head moved past reviewed aaaa111"},
		{"open at reviewed head", github.Pull{Number: 12, State: "open", Head: github.Ref{SHA: headSHA}}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newReviewerFix(t)
			f.s.GH = &stateGH{pull: tt.pull}
			f.allowJobClose()
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			rec := f.sink.recs[label]
			rec.State, rec.OwnerLane = "spawned", "lane-x"
			f.sink.recs[label] = rec
			f.setPane("w1:p5", "agent_status", "done")
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			closes := f.jobCloses()
			rows := f.rows(t)
			if tt.abandon == "" {
				if len(closes) != 0 || f.acts() != 0 || len(rows) != 1 || rows[0].Result != "kept" || rows[0].Reason != "job:spawned" {
					t.Fatalf("job closes=%v acts=%d rows: %v", closes, f.acts(), rowSummary(rows))
				}
				return
			}
			want := append([]string{"job", "close", label, "--lane", "lane-x", "--outcome", "abandoned", "--reason", tt.abandon}, hubFlagArgs(f.s.Config.Reap.Hub)...)
			if len(closes) != 1 || strings.Join(closes[0], " ") != strings.Join(want, " ") {
				t.Fatalf("job closes=%v, want %v", closes, want)
			}
			if len(rows) < 2 || rows[0].Result != "abandoned" || rows[0].Reason != tt.abandon || rows[1].Result != "kept" || rows[1].Reason != "pane-in-use" || len(f.hub.ops("pane.close")) != 0 {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// A spawned job whose pane is still working is never closed, and a shadow pass
// only plans the close.
func TestReapReviewerAbandonNeedsFinishedPaneAndActing(t *testing.T) {
	merged := "2026-11-04T10:00:00Z"
	for _, tt := range []struct {
		name   string
		status string
		opts   Options
		result string
	}{
		{"working pane", "working", Options{}, "kept"},
		{"shadow", "done", Options{Shadow: true}, "would-abandon"},
		{"dry run", "done", Options{DryRun: true}, "would-abandon"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newReviewerFix(t)
			f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
			f.allowJobClose()
			label := reviewerLabelFor(12, time.Hour)
			f.addReviewer(label, "w1:p5")
			rec := f.sink.recs[label]
			rec.State, rec.OwnerLane = "spawned", "lane-x"
			f.sink.recs[label] = rec
			f.setPane("w1:p5", "agent_status", tt.status)
			if err := f.reap(t, 0, tt.opts); err != nil {
				t.Fatal(err)
			}
			if n := len(f.jobCloses()); n != 0 || len(f.hub.ops("pane.close")) != 0 {
				t.Fatalf("acted: job closes=%d pane closes=%d", n, len(f.hub.ops("pane.close")))
			}
			if rows := f.rows(t); !tt.opts.DryRun && (len(rows) == 0 || rows[0].Result != tt.result) {
				t.Fatalf("rows: %v", rowSummary(rows))
			}
		})
	}
}

// An abandoned job whose pane stays is not closed a second time: the job close
// ran once, the pane waits for panewire's node, and no pane close ever runs.
func TestReapReviewerAbandonedJobPaneIsReapedLater(t *testing.T) {
	f := newStuckFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	f.allowJobClose()
	f.addReviewer(f.label, "w1:p5")
	f.setJob(func(j *integrations.JobRecord) { j.State, j.OwnerLane = "spawned", "lane-x" })
	f.setPane("w1:p5", "agent_status", "done")
	f.passAfter(t, 0)
	f.passAfter(t, 30*time.Minute)
	if n := len(f.jobCloses()); n != 1 || f.closes() != 0 {
		t.Fatalf("job closes=%d pane closes=%d: %v", n, f.closes(), rowSummary(f.rows(t)))
	}
	// hk now reports the closed job lost: still no pane close, still one job
	// close, and the pane-in-use row is not rewritten.
	f.setJob(func(j *integrations.JobRecord) { j.State = "lost" })
	f.passAfter(t, 3*time.Hour)
	if n := len(f.jobCloses()); n != 1 || f.closes() != 0 {
		t.Fatalf("job closes=%d pane closes=%d: %v", n, f.closes(), rowSummary(f.rows(t)))
	}
	rows := f.rows(t)
	if len(rows) != 2 || rows[0].Result != "abandoned" || rows[1].Result != "kept" || rows[1].Reason != "pane-in-use" {
		t.Fatalf("rows: %v", rowSummary(rows))
	}
}
