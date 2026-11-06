package pipeline

import (
	"context"
	"github.com/mgh3326/mergewake/internal/github"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/prstatus"
)

// (a) A pass that blocks a PR on review_missing leaves a blocked snapshot with
// the reasons, the required checks and when the gate decided.
func TestPRStatusSnapshotWrittenForReviewMissing(t *testing.T) {
	s, g, _, r := notifySetup(t)
	g.reviewed = false
	delete(g.refs, doneRef(7, shaA))
	at := time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC)
	s.Now = func() time.Time { return at }
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	snap, e := prstatus.Read(s.Config.StateDir, r.Name, 7)
	if e != nil {
		t.Fatal(e)
	}
	if snap.State != "blocked" || len(snap.Reasons) != 1 || snap.Reasons[0] != "review_missing" {
		t.Fatalf("snapshot: %+v", snap)
	}
	if snap.Repo != r.Name || snap.PR != 7 || snap.Head != shaA || snap.Source != "last_pass" || !snap.ObservedAt.Equal(at) {
		t.Fatalf("snapshot identity: %+v", snap)
	}
	if snap.Review.Done || len(snap.Review.Passes) != 0 || snap.BaseFresh == nil || !*snap.BaseFresh {
		t.Fatalf("snapshot review/base: %+v", snap)
	}
	if len(snap.Checks) == 0 || snap.Checks[0].State != "green" {
		t.Fatalf("checks: %+v", snap.Checks)
	}
}

// A dry run decides but records nothing, like the rest of NotifyPR.
func TestPRStatusDryRunWritesNothing(t *testing.T) {
	s, _, _, r := notifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	if _, e := prstatus.Read(s.Config.StateDir, r.Name, 7); e != prstatus.ErrNotFound {
		t.Fatalf("dry run wrote a snapshot: %v", e)
	}
}

// A PR that left the open list keeps its last gate fields under its final
// state.
func TestPRStatusFinalSnapshotForMergedPR(t *testing.T) {
	s, g, _, r := notifySetup(t)
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	g.pull.State, g.pull.Merged = "closed", true
	s.finalizeStatus(r, g.pull)
	snap, e := prstatus.Read(s.Config.StateDir, r.Name, 7)
	if e != nil {
		t.Fatal(e)
	}
	if snap.State != "merged" || len(snap.Checks) == 0 || snap.Open() {
		t.Fatalf("final snapshot: %+v", snap)
	}
}

func findingsSnapshot(t *testing.T, extra string, threads ...github.ReviewThread) (prstatus.Snapshot, string) {
	t.Helper()
	s, g, _, r := notifySetup(t)
	g.enabled = true
	g.policyExtra = extra
	g.pull.User.Login = "author"
	g.feedback.Threads = threads
	for _, th := range threads {
		if !th.IsResolved {
			g.findings++
		}
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	snap, e := prstatus.Read(s.Config.StateDir, r.Name, 7)
	if e != nil {
		t.Fatal(e)
	}
	return snap, strings.Join(snap.Reasons, ";")
}

// The snapshot lists each unresolved thread with the fields a reader needs to
// act on it, from the feedback the pass already read.
func TestPRStatusSnapshotCarriesOpenFindings(t *testing.T) {
	long := strings.Repeat("x", 300)
	high := gradedThread("review-bot", "high", false, shaB, [2]string{"author", "User"})
	high.ID, high.Path, high.Line = "T_high", "internal/a.go", 42
	high.Comments[0].Body = "<!-- mergewake-severity:high -->\n\n" + long + "\nsecond line"
	high.Comments[0].URL = "https://example.test/pull/7#discussion_r1"
	medium := gradedThread("review-bot", "medium", false, shaB)
	medium.ID, medium.Path, medium.Line = "T_medium", "b.go", 7
	resolved := gradedThread("review-bot", "high", true, shaB)
	resolved.ID = "T_resolved"

	snap, _ := findingsSnapshot(t, gradedPolicy, high, medium, resolved)
	if len(snap.Findings) != 2 || snap.Threads.Unresolved == nil || *snap.Threads.Unresolved != 2 {
		t.Fatalf("findings: %+v threads: %+v", snap.Findings, snap.Threads)
	}
	f := snap.Findings[0]
	if f.ID != "T_high" || f.Location != "internal/a.go:42" || f.Severity != "high" || !f.Blocking || !f.AuthorReplied || f.URL != "https://example.test/pull/7#discussion_r1" || f.Summary != long[:prstatus.MaxSummary] {
		t.Fatalf("high finding: %+v", f)
	}
	m := snap.Findings[1]
	if m.ID != "T_medium" || m.Location != "b.go:7" || m.Severity != "medium" || m.Blocking || m.AuthorReplied || m.Summary != "finding" {
		t.Fatalf("medium finding: %+v", m)
	}
}

// Under the counted gate every unresolved thread holds it.
func TestPRStatusFindingsAllBlockUnderCountedGate(t *testing.T) {
	th := gradedThread("review-bot", "low", false, shaB)
	th.ID = "T_low"
	snap, reasons := findingsSnapshot(t, "", th)
	if len(snap.Findings) != 1 || !snap.Findings[0].Blocking || reasons != "threads_open:1" {
		t.Fatalf("findings %+v reasons %q", snap.Findings, reasons)
	}
}

// The pass that decides a PR clears a re-evaluation marker set before it
// began, and keeps one set after.
func TestPRStatusPassClearsEarlierDirtyMarker(t *testing.T) {
	s, _, _, r := notifySetup(t)
	at := time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC)
	s.Now = func() time.Time { return at }
	for marked, wantDirty := range map[time.Time]bool{at.Add(-time.Minute): false, at.Add(time.Minute): true} {
		if e := prstatus.MarkDirty(s.Config.StateDir, r.Name, 7, marked); e != nil {
			t.Fatal(e)
		}
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
		if got := prstatus.DirtyPRs(s.Config.StateDir, r.Name)[7]; got != wantDirty {
			t.Fatalf("marker at %v: dirty=%v, want %v", marked, got, wantDirty)
		}
	}
}
