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

const reviewTestDiff = "diff --git a/tool b/tool\n--- a/tool\n+++ b/tool\n@@ -1 +1 @@\n-old\n+new\n"

var (
	recStart  = time.Date(2026, 11, 6, 1, 0, 0, 0, time.UTC)
	recFinish = recStart.Add(time.Minute)
)

func recordPR() github.Pull {
	return github.Pull{Number: 7, Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: shaB}}
}

// reviewEventBody is the JSON body PostTaskEvent sends for reviews.
func reviewEventBody(t *testing.T, recs ...integrations.ReviewRecord) []byte {
	t.Helper()
	b, e := json.Marshal(integrations.TaskEvent{Source: integrations.SourceReview, SourceRef: testPRURL, TaskID: 2909, PR: testPRURL, HeadSHA: shaA, BaseSHA: shaB, Reviews: recs})
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// recordCases are the records mergewake builds for the receipts it can hold: a
// pass of a single-lens repo, one stored before round counting, the patch id
// the review code puts in every receipt, and an ordinary two-lens pass.
var recordCases = []struct {
	name    string
	receipt github.PassReceipt
}{
	{"pass without a lens", github.PassReceipt{Pass: 1, Round: 1}},
	{"receipt with round 0", github.PassReceipt{Pass: 1, Lens: "correctness"}},
	{"patch id of a reviewed diff", github.PassReceipt{Pass: 1, Lens: "correctness", Round: 1, Patch: reviewPatchID(reviewTestDiff)}},
	{"two-lens pass", github.PassReceipt{Pass: 2, Lens: "state-risk", Round: 1, Family: "openai", Executor: "panewire"}},
}

// Contract: every record mergewake builds passes hk's review rules (a copy of
// handoffkeep 11ccc30 internal/store/change_payload.go). The failure message
// names the field hk refuses.
func TestHKReviewRecordsMeetHKRules(t *testing.T) {
	for _, tc := range recordCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := passRecord(recordPR(), tc.receipt, nil, "", recStart, recFinish)
			if why := hkRulesCheck(reviewEventBody(t, rec)); why != "" {
				b, _ := json.Marshal(rec)
				t.Fatalf("hk refuses %s\nrecord %s", why, b)
			}
		})
	}
}

// (a) Posted to an hk that applies those rules, each record is accepted with
// reviews, head and base: one post, no plain resend.
func TestHKReviewEventAcceptedWithReviews(t *testing.T) {
	for _, tc := range recordCases {
		t.Run(tc.name, func(t *testing.T) {
			s, g, hk, r := hkSetup(t)
			hk.rules = true
			rec := passRecord(recordPR(), tc.receipt, nil, "", recStart, recFinish)
			if got := s.postTaskEventWith(context.Background(), r, g.pull, integrations.SourceReview, nil, recStart, []integrations.ReviewRecord{rec}); got != postSent {
				t.Fatalf("outcome %v", got)
			}
			events := hk.received()
			if len(events) != 1 {
				t.Fatalf("hk saw %d posts (refusals %q); want 1 with reviews", len(events), hk.why)
			}
			ev := events[0]
			if len(ev.Reviews) != 1 || ev.HeadSHA != shaA || ev.BaseSHA != shaB {
				t.Fatalf("event lost fields: %+v", ev)
			}
			if refused := eventAuditRows(t, s, r.Name, "task-event-fields-refused"); len(refused) != 0 {
				t.Fatalf("refusal audited: %+v", refused)
			}
		})
	}
}

// The real path: a two-pass Review run records patch ids and posts a review
// event hk accepts whole.
func TestHKReviewRunEventAccepted(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.rules = true
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	events := hk.received()
	if len(events) != 1 {
		t.Fatalf("hk saw %d posts (refusals %q); want one", len(events), hk.why)
	}
	ev := events[0]
	if len(ev.Reviews) != 2 || ev.HeadSHA != shaA || ev.BaseSHA != shaB {
		t.Fatalf("event %+v", ev)
	}
	for _, rv := range ev.Reviews {
		if rv.PatchID == "" || strings.ContainsAny(rv.PatchID, ":-") {
			t.Fatalf("patch_id %q is not a bare hex id", rv.PatchID)
		}
	}
}

// (b) hk's 400 code and message reach the task-event-fields-refused audit row,
// bounded and redacted; the event itself still lands.
func TestHKReviewRefusalMessageAudited(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.refuseExtended = true
	hk.message = "invalid task event: reviews[0]: lens must be a token api_key=sk-live-0123456789abcdefghij " + strings.Repeat("x", 400)
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	rows := eventAuditRows(t, s, r.Name, "task-event-fields-refused")
	if len(rows) != 1 {
		t.Fatalf("refused audit %+v", rows)
	}
	msg, _ := rows[0]["message"].(string)
	if rows[0]["code"] != "invalid_event" || !strings.HasPrefix(msg, "invalid task event: reviews[0]: lens must be a token") {
		t.Fatalf("code/message not kept: %+v", rows[0])
	}
	if strings.Contains(msg, "sk-live") || len(msg) > 200 {
		t.Fatalf("message not redacted or bounded (%d bytes): %q", len(msg), msg)
	}
}

// (c) A record hk would still refuse is dropped alone: the other records, head
// and base are kept, and the drop is audited.
func TestHKReviewBadRecordDroppedAlone(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.rules = true
	good := passRecord(recordPR(), github.PassReceipt{Pass: 1, Lens: "correctness", Round: 1}, nil, "", recStart, recFinish)
	second := passRecord(recordPR(), github.PassReceipt{Pass: 2, Lens: "state-risk", Round: 1}, nil, "", recStart, recFinish)
	noPass := passRecord(recordPR(), github.PassReceipt{Lens: "correctness", Round: 1}, nil, "", recStart, recFinish)
	badHead := second
	badHead.ReviewedHead = "not-a-sha"
	badFields := second
	badFields.Pass = 3
	badFields.Family = "Open AI!"
	badFields.Model = strings.Repeat("m", 300)
	badFields.PatchID = "xyz"
	badFields.ReviewedBase = "short"
	badFields.FinishedAt = &recStart
	badFields.StartedAt = &recFinish
	recs := []integrations.ReviewRecord{noPass, good, badHead, badFields, second}
	if got := s.postTaskEventWith(context.Background(), r, g.pull, integrations.SourceReview, nil, recStart, recs); got != postSent {
		t.Fatalf("outcome %v", got)
	}
	events := hk.received()
	if len(events) != 1 {
		t.Fatalf("hk saw %d posts (refusals %q); want one", len(events), hk.why)
	}
	ev := events[0]
	if ev.HeadSHA != shaA || ev.BaseSHA != shaB {
		t.Fatalf("head/base lost: %+v", ev)
	}
	var passes []int
	for _, rv := range ev.Reviews {
		passes = append(passes, rv.Pass)
	}
	// noPass (pass 0) and badHead are dropped; badFields loses its bad fields
	// only, and keeps its lens, pass and round.
	if len(passes) != 3 || passes[0] != 1 || passes[1] != 3 || passes[2] != 2 {
		t.Fatalf("kept passes %v", passes)
	}
	kept := ev.Reviews[1]
	if kept.Family != "" || kept.Model != "" || kept.PatchID != "" || kept.ReviewedBase != "" || kept.FinishedAt != nil || kept.Lens != "state-risk" {
		t.Fatalf("bad fields not cleared alone: %+v", kept)
	}
	rows := eventAuditRows(t, s, r.Name, "task-event-review-record")
	if len(rows) != 3 {
		t.Fatalf("audit rows %+v", rows)
	}
	dropped := 0
	for _, row := range rows {
		if row["action"] == "dropped" {
			dropped++
		}
	}
	if dropped != 2 {
		t.Fatalf("dropped rows %d in %+v", dropped, rows)
	}
}

// (d) An hk that still answers 400 resends once with head and base and without
// reviews; if that fails too, the plain event goes. The event is never lost.
func TestHKReviewResendKeepsHeadBaseThenPlain(t *testing.T) {
	t.Run("reviews refused", func(t *testing.T) {
		s, g, hk, r := hkSetup(t)
		hk.refuseReviews = true
		g.blocking = true
		if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
			t.Fatal(e)
		}
		events := hk.received()
		if len(events) != 2 || len(events[0].Reviews) != 2 {
			t.Fatalf("posts %+v", events)
		}
		if len(events[1].Reviews) != 0 || events[1].HeadSHA != shaA || events[1].BaseSHA != shaB {
			t.Fatalf("resend must keep head/base and drop reviews: %+v", events[1])
		}
		wantEvent(t, events[1], integrations.SourceReview, testPRURL, testPRURL, "", "")
		refused := eventAuditRows(t, s, r.Name, "task-event-fields-refused")
		if len(refused) != 1 || refused[0]["resend"] != "without-reviews" {
			t.Fatalf("audit %+v", refused)
		}
	})
	t.Run("head and base refused too", func(t *testing.T) {
		s, g, hk, r := hkSetup(t)
		hk.refuseExtended = true
		g.blocking = true
		if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
			t.Fatal(e)
		}
		events := hk.received()
		if len(events) != 3 {
			t.Fatalf("posts %+v", events)
		}
		if len(events[1].Reviews) != 0 || events[1].HeadSHA != shaA {
			t.Fatalf("second post %+v", events[1])
		}
		if events[2].HeadSHA != "" || events[2].BaseSHA != "" || len(events[2].Reviews) != 0 {
			t.Fatalf("plain post %+v", events[2])
		}
		wantEvent(t, events[2], integrations.SourceReview, testPRURL, testPRURL, "", "")
		refused := eventAuditRows(t, s, r.Name, "task-event-fields-refused")
		if len(refused) != 1 || refused[0]["resend"] != "plain" {
			t.Fatalf("audit %+v", refused)
		}
	})
}

// (e) An ordinary two-lens pass record serialises exactly as it did before the
// records were made to fit hk's rules (the bytes captured from passRecord at
// 765eaaf). A record with a patch id is not in this golden: hk refused it.
func TestPassRecordTwoLensUnchanged(t *testing.T) {
	fs := []github.Finding{{Severity: "critical"}, {Severity: "high"}, {Severity: "medium"}, {Severity: "low"}}
	cases := []struct {
		receipt github.PassReceipt
		golden  string
	}{
		{github.PassReceipt{Pass: 1, Lens: "correctness", Family: "openai", Executor: "panewire", Round: 1}, `{"reviewed_head":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","reviewed_base":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","round":1,"pass":1,"lens":"correctness","family":"openai","executor":"panewire","contract_source":"linked-task-3118","critical":1,"high":1,"medium":1,"low":1,"started_at":"2026-11-06T01:00:00Z","finished_at":"2026-11-06T01:01:00Z"}`},
		{github.PassReceipt{Pass: 2, Lens: "state-risk", Family: "anthropic", Round: 2, Fallback: "headless"}, `{"reviewed_head":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","reviewed_base":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","round":2,"pass":2,"lens":"state-risk","family":"anthropic","fallback":true,"contract_source":"linked-task-3118","critical":1,"high":1,"medium":1,"low":1,"started_at":"2026-11-06T01:00:00Z","finished_at":"2026-11-06T01:01:00Z"}`},
	}
	for i, tc := range cases {
		b, e := json.Marshal(passRecord(recordPR(), tc.receipt, fs, "linked task 3118", recStart, recFinish))
		if e != nil {
			t.Fatal(e)
		}
		if string(b) != tc.golden {
			t.Fatalf("case %d:\n got %s\nwant %s", i, b, tc.golden)
		}
	}
}
