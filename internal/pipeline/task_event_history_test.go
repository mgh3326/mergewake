package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// historyKeys are the review-history fields hk 3208 added to reviews[].
var historyKeys = []string{"reviewer", "same_family", "delta", "quiet", "outside_diff_blocking", "patch_digest"}

// hasHistoryField reports whether any reviews[] item of a raw task event
// carries a review-history key.
func hasHistoryField(raw []byte) bool {
	var ev struct {
		Reviews []map[string]json.RawMessage `json:"reviews"`
	}
	if json.Unmarshal(raw, &ev) != nil {
		return false
	}
	for _, rv := range ev.Reviews {
		for _, k := range historyKeys {
			if _, ok := rv[k]; ok {
				return true
			}
		}
	}
	return false
}

func recordJSON(t *testing.T, rec integrations.ReviewRecord) map[string]any {
	t.Helper()
	b, e := json.Marshal(rec)
	if e != nil {
		t.Fatal(e)
	}
	var m map[string]any
	if e := json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	return m
}

const (
	historyPatchID = "0123456789abcdef0123456789abcdef01234567"
	historyDigest  = "fedcba9876543210fedc"
)

// (a) A normal pass record carries the reviewer, the same-family flag and the
// digest half of the patch receipt, with patch_id staying the patch-id half.
// quiet=false is sent explicitly: hk reads a missing field as unknown and an
// explicit false as a fact, and mergewake knows this pass was not quiet.
func TestPassRecordReviewHistory(t *testing.T) {
	pr := github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}
	receipt := github.PassReceipt{Pass: 1, Round: 1, Lens: "default", Reviewer: "sonnet@high", Family: "anthropic", SameFamily: true, Patch: historyPatchID + ":" + historyDigest}
	at := time.Now()
	rec := passRecord(pr, receipt, nil, "", at, at)
	if rec.Reviewer == nil || *rec.Reviewer != "sonnet@high" {
		t.Fatalf("reviewer %v", rec.Reviewer)
	}
	if rec.SameFamily == nil || !*rec.SameFamily {
		t.Fatalf("same_family %v", rec.SameFamily)
	}
	if rec.PatchID != historyPatchID || rec.PatchDigest == nil || *rec.PatchDigest != historyDigest {
		t.Fatalf("patch %q digest %v", rec.PatchID, rec.PatchDigest)
	}
	m := recordJSON(t, rec)
	if v, ok := m["quiet"]; !ok || v != false {
		t.Fatalf("quiet must be an explicit false: %v", m)
	}
	if v, ok := m["delta"]; !ok || v != false {
		t.Fatalf("delta must be an explicit false for a whole-diff pass: %v", m)
	}
	if m["patch_digest"] != historyDigest || m["patch_id"] != historyPatchID || m["reviewer"] != "sonnet@high" || m["same_family"] != true {
		t.Fatalf("wire %v", m)
	}
}

// (b) A fix-delta pass sets delta=true, and a quiet pass quiet=true.
func TestPassRecordDeltaAndQuiet(t *testing.T) {
	pr := github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}
	at := time.Now()
	rec := passRecord(pr, github.PassReceipt{Pass: 1, Round: 2, Lens: "default", Delta: shaB, Quiet: true}, nil, "", at, at)
	if rec.Delta == nil || !*rec.Delta || rec.Quiet == nil || !*rec.Quiet {
		t.Fatalf("delta %v quiet %v", rec.Delta, rec.Quiet)
	}
	m := recordJSON(t, rec)
	if m["delta"] != true || m["quiet"] != true {
		t.Fatalf("wire %v", m)
	}
}

// (c) The count of blocking findings outside the diff rides on the record:
// critical and high count, advisory, medium and in-diff ones do not.
func TestPassRecordOutsideDiffBlocking(t *testing.T) {
	pr := github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}
	at := time.Now()
	fs := []github.Finding{
		{Severity: "critical", Anchor: github.AnchorOutside},
		{Severity: "high", Anchor: github.AnchorOutside},
		{Severity: "high", Anchor: github.AnchorOutside, Advisory: true},
		{Severity: "medium", Anchor: github.AnchorOutside},
		{Severity: "high"},
	}
	rec := passRecord(pr, github.PassReceipt{Pass: 1, Round: 1, Lens: "default"}, fs, "", at, at)
	if rec.OutsideDiffBlocking == nil || *rec.OutsideDiffBlocking != 2 {
		t.Fatalf("outside_diff_blocking %v", rec.OutsideDiffBlocking)
	}
	if m := recordJSON(t, rec); m["outside_diff_blocking"] != float64(2) {
		t.Fatalf("wire %v", m)
	}
	// A pass with none outside the diff knows the count is 0 and says so.
	rec = passRecord(pr, github.PassReceipt{Pass: 1, Round: 1, Lens: "default"}, fs[4:], "", at, at)
	if m := recordJSON(t, rec); m["outside_diff_blocking"] != float64(0) {
		t.Fatalf("wire %v", m)
	}
}

// (d) A value mergewake does not know is left out of the JSON: no reviewer, no
// family (so no same_family), no patch receipt (so no digest).
func TestPassRecordUnknownHistoryOmitted(t *testing.T) {
	pr := github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}
	at := time.Now()
	rec := passRecord(pr, github.PassReceipt{Pass: 1, Round: 1, Lens: "default", SameFamily: true, Patch: historyPatchID}, nil, "", at, at)
	m := recordJSON(t, rec)
	for _, k := range []string{"reviewer", "same_family", "patch_digest"} {
		if _, ok := m[k]; ok {
			t.Fatalf("%s sent without a known value: %v", k, m)
		}
	}
	if m["patch_id"] != historyPatchID {
		t.Fatalf("patch_id %v", m)
	}
	// reviewPatchID's own 12-hex digest is shorter than hk's 16: hk refuses the
	// whole record that carries it, so passRecord sends the patch-id half only.
	rec = passRecord(pr, github.PassReceipt{Pass: 1, Round: 1, Lens: "default", Patch: reviewPatchID(reviewTestDiff)}, nil, "", at, at)
	if rec.PatchID == "" || rec.PatchDigest != nil {
		t.Fatalf("patch %q digest %v", rec.PatchID, rec.PatchDigest)
	}
	// What hk would refuse is cleared, not sent: reviewer text hk rejects, and a
	// digest shorter than hk's 16 hex digits (mergewake's receipts carry 12).
	short, empty := "0123456789ab", ""
	bad := integrations.ReviewRecord{ReviewedHead: shaA, Pass: 1, Round: 1, Lens: "default", Reviewer: &empty, PatchDigest: &short}
	fit, cleared, drop := bad.ForHK()
	if drop != "" || fit.Reviewer != nil || fit.PatchDigest != nil || strings.Join(cleared, ",") != "patch_digest,reviewer" {
		t.Fatalf("fit %+v cleared %v drop %q", fit, cleared, drop)
	}
}

// A review through the real path sends the history fields on the wire and an hk
// holding the full rules accepts the event.
func TestTaskEventReviewHistoryOnWire(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.rules = true
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if len(got) != 1 || len(got[0].Reviews) != 2 || len(hk.why) != 0 {
		t.Fatalf("events %+v refusals %v", got, hk.why)
	}
	for _, rv := range got[0].Reviews {
		if rv.Reviewer == nil || *rv.Reviewer == "" || rv.Delta == nil || rv.Quiet == nil || rv.OutsideDiffBlocking == nil {
			t.Fatalf("history missing: %+v", rv)
		}
	}
}

// (f) An hk that predates the history fields answers 400 to them. The event is
// still recorded: the existing refused-fields path resends it without the
// reviews (head and base kept), then plain. The new fields ride inside
// reviews[], so that path covers them as is, at the cost of the whole reviews[]
// of that event rather than only the new fields.
func TestTaskEventHistoryRefusedStillRecorded(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.refuseHistory = true
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if len(got) != 2 || len(got[0].Reviews) != 2 || len(got[1].Reviews) != 0 || got[1].HeadSHA == "" {
		t.Fatalf("want the full post, then a resend with head and base and no reviews: %+v", got)
	}
	refused := eventAuditRows(t, s, r.Name, "task-event-fields-refused")
	rows := eventAuditRows(t, s, r.Name, "hk-task-event")
	if len(refused) != 1 || refused[0]["resend"] != integrations.ResendWithoutReviews || len(rows) != 1 || rows[0]["error"] != nil || rows[0]["result"] == nil {
		t.Fatalf("refused %+v rows %+v", refused, rows)
	}
}
