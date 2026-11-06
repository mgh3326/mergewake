package daemon

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	headA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	headB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func deliver(t *testing.T, d *Daemon, event, body string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(d.handleDelivery))
	defer srv.Close()
	resp := post(t, srv.Client(), srv.URL+"/github", event, "id", []byte(body), testSecret)
	resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("%s: HTTP %d", event, resp.StatusCode)
	}
}

func dirtyDaemon(dirty *Dirty) *Daemon {
	var triggered []string
	d := webhookDaemon(&triggered)
	d.Webhook.Dirty = dirty
	return &d
}

// A check_run that carries only its head commit (no pull_requests, as for a
// fork or a re-run) marks the PR with that head and leaves its siblings clean.
func TestCheckRunWithOnlyHeadSHAMarksOnlyThePRWithThatHead(t *testing.T) {
	var dirty Dirty
	start := dirty.Seq()
	deliver(t, dirtyDaemon(&dirty), "check_run", `{"repository":{"full_name":"o/r"},"check_run":{"head_sha":"`+headA+`","pull_requests":[]}}`)
	if !dirty.Since(start, "o/r", 1, headA) {
		t.Fatal("the PR whose head the check ran on stayed clean")
	}
	if dirty.Since(start, "o/r", 2, headB) {
		t.Fatal("a sibling PR with another head went dirty")
	}
	if dirty.Since(start, "o/other", 1, headA) {
		t.Fatal("the same head in another repository went dirty")
	}
}

func TestDeliveriesMarkThePRsTheyName(t *testing.T) {
	for _, c := range []struct {
		name, event, body string
	}{
		{"pull_request by number", "pull_request", `{"number":7,"pull_request":{"number":7,"head":{"sha":"` + headA + `"}}}`},
		{"review by number", "pull_request_review", `{"pull_request":{"number":7}}`},
		{"review thread by number", "pull_request_review_thread", `{"pull_request":{"number":7}}`},
		{"review comment by number", "pull_request_review_comment", `{"pull_request":{"number":7}}`},
		{"check_suite by listed PR", "check_suite", `{"check_suite":{"head_sha":"` + headB + `","pull_requests":[{"number":7}]}}`},
		{"check_run by listed PR", "check_run", `{"check_run":{"head_sha":"` + headB + `","pull_requests":[{"number":7}]}}`},
		{"status by sha", "status", `{"sha":"` + headA + `"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			var dirty Dirty
			start := dirty.Seq()
			body := c.body[:1] + `"repository":{"full_name":"o/r"},` + c.body[1:]
			deliver(t, dirtyDaemon(&dirty), c.event, body)
			if !dirty.Since(start, "o/r", 7, headA) {
				t.Fatal("PR 7 stayed clean")
			}
			if dirty.Since(start, "o/r", 8, "cccccccccccccccccccccccccccccccccccccccc") {
				t.Fatal("PR 8 went dirty")
			}
		})
	}
}

// Events that never reach a verdict, and repositories outside the config, mark
// nothing.
func TestIgnoredDeliveriesMarkNothing(t *testing.T) {
	var dirty Dirty
	start := dirty.Seq()
	d := dirtyDaemon(&dirty)
	deliver(t, d, "push", `{"repository":{"full_name":"o/r"},"sha":"`+headA+`"}`)
	deliver(t, d, "check_run", `{"repository":{"full_name":"x/y"},"check_run":{"head_sha":"`+headA+`"}}`)
	if dirty.Since(start, "o/r", 1, headA) || dirty.Since(start, "x/y", 1, headA) {
		t.Fatal("an ignored delivery marked a PR")
	}
}

// A mark made after an evaluation began stays dirty for the next pass; one made
// before it does not.
func TestDirtySinceIsRelativeToTheEvaluationStart(t *testing.T) {
	var d Dirty
	d.Mark("o/r", 1, "")
	start := d.Seq()
	if d.Since(start, "o/r", 1, headA) {
		t.Fatal("a mark from before the evaluation read as dirty")
	}
	d.Mark("o/r", 1, "")
	if !d.Since(start, "o/r", 1, headA) {
		t.Fatal("a mark that landed during the evaluation was lost")
	}
}

func TestDirtyPrunesExpiredMarksAndNeverForgetsLiveOnes(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	d := Dirty{Now: func() time.Time { return now }}
	start := d.Seq()
	for i := 1; i <= dirtyPrune; i++ {
		d.Mark("o/r", i, "")
	}
	now = now.Add(dirtyKeep + time.Minute)
	d.Mark("o/r", dirtyPrune+1, "")
	if len(d.prs) != 1 {
		t.Fatalf("%d marks kept after expiry", len(d.prs))
	}
	if !d.Since(start, "o/r", dirtyPrune+1, "") {
		t.Fatal("the live mark was pruned")
	}
	// Past the hard cap everything is forgotten, and every evaluation older than
	// that reads as dirty.
	d = Dirty{Now: func() time.Time { return now }}
	start = d.Seq()
	for i := 1; i <= dirtyMaxLen+1; i++ {
		d.Mark("o/r", i, "")
	}
	if !d.Since(start, "o/r", 999999, "") {
		t.Fatal("an evaluation older than a dropped mark read as clean")
	}
}

func TestNilDirtyIsAlwaysDirty(t *testing.T) {
	var d *Dirty
	if !d.Since(0, "o/r", 1, headA) {
		t.Fatal("without a webhook receiver every PR must be evaluated")
	}
}
