package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
)

// Own-lane scope_forbidden is an error plus one notice a day (hk 6010).
func TestReapReviewerScopeForbiddenOwnLaneIsErrorAndNotified(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	f.s.Config.Review.Panewire.Owner = "lane-x"
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f, "panewire job: close refused: scope_forbidden")

	if err := f.reap(t, 0, Options{}); err == nil {
		t.Fatalf("own-lane scope_forbidden did not fail its pass: %v", rowSummary(f.rows(t)))
	}
	var errs []reapReceipt
	for _, r := range f.rows(t) {
		if r.Result == "error" {
			errs = append(errs, r)
		}
	}
	if len(errs) != 1 || !strings.HasPrefix(errs[0].Reason, "abandon job: ") || !strings.Contains(errs[0].Reason, "scope_forbidden") {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
	rows := auditRows(t, f.s, testRepo, "notice-cause")
	var got []map[string]any
	for _, r := range rows {
		if r["cause"] == causeReapCloseScopeForbidden {
			got = append(got, r)
		}
	}
	if len(got) != 1 || got[0]["sent"] != true {
		t.Fatalf("notice-cause rows: %v", rows)
	}
	n, ok := f.s.Notifier.(*fakeNotifier)
	if !ok || n.count != 1 {
		t.Fatalf("notifier events: %+v", n)
	}
}

// Another lane's scope_forbidden stays benign: kept, no error, no notice.
func TestReapReviewerScopeForbiddenOtherLaneStaysKept(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	label := reviewerLabelFor(12, time.Hour)
	f.addReviewer(label, "w1:p5")
	rec := f.sink.recs[label]
	rec.State, rec.OwnerLane = "spawned", "lane-x"
	f.sink.recs[label] = rec
	f.setPane("w1:p5", "agent_status", "done")
	f.s.Config.Review.Panewire.Owner = "own-lane"
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f, "panewire job: close refused: scope_forbidden")

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	var errs, kept []reapReceipt
	for _, r := range f.rows(t) {
		if r.Result == "error" {
			errs = append(errs, r)
		}
		if r.Result == "kept" {
			kept = append(kept, r)
		}
	}
	if len(errs) != 0 {
		t.Fatalf("error rows: %v", rowSummary(f.rows(t)))
	}
	if len(kept) != 1 || kept[0].Reason != reviewerOutOfScope {
		t.Fatalf("kept rows: %v", rowSummary(f.rows(t)))
	}
	for _, r := range auditRows(t, f.s, testRepo, "notice-cause") {
		if r["cause"] == causeReapCloseScopeForbidden {
			t.Fatalf("other-lane close sent a notice: %v", r)
		}
	}
}

// Three scope_forbidden closes in one pass log one ERROR line (hk 6010).
func TestReapReviewerScopeForbiddenThreeLogsCredentialError(t *testing.T) {
	f := newReviewerFix(t)
	merged := "2026-11-04T10:00:00Z"
	f.s.GH = &stateGH{pull: github.Pull{Number: 12, State: "closed", Merged: true, MergedAt: &merged, Head: github.Ref{SHA: headSHA}}}
	f.s.Config.Review.Panewire.Owner = "own-lane"
	var labels []string
	for i, age := range []time.Duration{time.Hour, 2 * time.Hour, 3 * time.Hour} {
		label := reviewerLabelFor(12, age+time.Duration(i)*time.Minute)
		labels = append(labels, label)
		pane := fmt.Sprintf("w1:p%d", 5+i)
		f.addReviewer(label, pane)
		rec := f.sink.recs[label]
		rec.State, rec.OwnerLane = "spawned", "lane-other"
		f.sink.recs[label] = rec
	}
	// All panes done so every label attempts its close.
	for _, list := range []string{"agents", "panes"} {
		for _, p := range f.hub.agents[list].([]any) {
			p.(map[string]any)["agent_status"] = "done"
		}
	}
	f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
	refuseJobClose(f, "panewire job: close refused: scope_forbidden")
	var logs bytes.Buffer
	f.s.Log = slog.New(slog.NewTextHandler(&logs, nil))

	if err := f.reap(t, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.jobCloses()); n != 3 {
		t.Fatalf("job closes=%d, want 3: %v", n, rowSummary(f.rows(t)))
	}
	out := logs.String()
	if strings.Count(out, "may have lost its close grant") != 1 {
		t.Fatalf("ERROR lines naming the credential = %d, want 1: %q", strings.Count(out, "may have lost its close grant"), out)
	}
	if !strings.Contains(out, "count=3") && !strings.Contains(out, "count\":3") && !strings.Contains(out, ":3") {
		t.Fatalf("ERROR line does not name the count: %q", out)
	}
	sample := false
	for _, l := range labels {
		if strings.Contains(out, l) {
			sample = true
		}
	}
	if !sample {
		t.Fatalf("ERROR line names no sample label: %q labels=%v", out, labels)
	}
}

// A kept not-closable row restores the close backoff after a restart (hk 6010).
func TestReapReviewerKeptNotClosableRestoresBackoffAfterRestart(t *testing.T) {
	for _, why := range []string{reviewerNotClosable, reviewerOutOfScope} {
		t.Run(why, func(t *testing.T) {
			f := newStuckJobFix(t, 7*time.Hour)
			f.hub.jobResult = json.RawMessage(`{"found":true,"completed":false}`)
			f.clearPanes()
			seedReapRows(t, f.s, testRepo.Name, reapReceipt{Action: "reviewer-reap", Repo: testRepo.Name, PR: 12, Label: f.label, Result: "kept", Reason: why})
			stderr := jobUnknownStderr(f.label)
			if why == reviewerOutOfScope {
				stderr = "panewire job: close refused: scope_forbidden"
			}
			refuseJobClose(f.reviewerFix, stderr)
			// Restart: the in-memory retry state is gone; the kept row remains.
			f.s.reapRetryState = nil
			f.now = reviewerNow.Add(30 * time.Minute)
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if n := len(f.jobCloses()); n != 0 {
				t.Fatalf("kept %q closed again before its backoff after restart: closes=%d want 0: %v", why, n, rowSummary(f.rows(t)))
			}
			f.now = reviewerNow.Add(3 * time.Hour)
			if err := f.reap(t, 0, Options{}); err != nil {
				t.Fatal(err)
			}
			if n := len(f.jobCloses()); n < 1 {
				t.Fatalf("kept %q never retried past its backoff: closes=%d: %v", why, n, rowSummary(f.rows(t)))
			}
		})
	}
}

func TestReapReviewerScopeForbiddenNoticeOncePerDay(t *testing.T) {
	s, _, n, r := notifySetup(t)
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	causeAt(t, s, r, at, causeReapCloseScopeForbidden)
	causeAt(t, s, r, at.Add(5*time.Minute), causeReapCloseScopeForbidden)
	causeAt(t, s, r, at.Add(61*time.Minute), causeReapCloseScopeForbidden)
	if n.count != 1 {
		t.Fatalf("same-day reap notices = %d, want 1: %+v", n.count, n.events)
	}
	causeAt(t, s, r, at.Add(24*time.Hour+time.Minute), causeReapCloseScopeForbidden)
	if n.count != 2 {
		t.Fatalf("next-day reap notices = %d, want 2: %+v", n.count, n.events)
	}
	rows := auditRows(t, s, r, "notice-cause")
	if len(rows) != 4 || rows[0]["sent"] != true || rows[1]["sent"] != false || rows[2]["sent"] != false || rows[3]["sent"] != true {
		t.Fatalf("notice-cause rows = %v", rows)
	}
}
