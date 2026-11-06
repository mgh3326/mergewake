package webhookstate

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The fixtures follow the shapes GitHub documents for each event; the values are made up.
func pullRequestEvent(action string, n int, head, updated string) []byte {
	return fmt.Appendf(nil, `{"action":%q,"number":%d,"repository":{"full_name":"o/r"},"pull_request":{"number":%d,"state":"open","draft":false,"merged":false,
		"title":"t","body":"body","user":{"login":"dev","type":"User"},"head":{"sha":%q,"ref":"topic","repo":{"full_name":"o/r"}},
		"base":{"sha":"b1","ref":"main","repo":{"full_name":"o/r"}},"labels":[{"name":"fast"}],"created_at":"2026-11-06T00:00:00Z","updated_at":%q,
		"changed_files":2,"additions":3,"deletions":1}}`, action, n, n, head, updated)
}

func checkRunEvent(id int64, name, status, conclusion, started, completed string) []byte {
	completedField := "null"
	if completed != "" {
		completedField = `"` + completed + `"`
	}
	return fmt.Appendf(nil, `{"action":"completed","repository":{"full_name":"o/r"},"check_run":{"id":%d,"name":%q,"head_sha":"h1","status":%q,"conclusion":%q,
		"started_at":%q,"completed_at":%s,"details_url":"https://ci.example/%d","check_suite":{"id":9},"app":{"slug":"ci"}}}`, id, name, status, conclusion, started, completedField, id)
}

func reviewEvent(action string, id int64, login, state, commit, body, submitted string) []byte {
	b := strings.NewReplacer("\n", `\n`, `"`, `\"`).Replace(body)
	return fmt.Appendf(nil, `{"action":%q,"repository":{"full_name":"o/r"},"pull_request":{"number":7},"review":{"id":%d,"user":{"login":%q},"state":%q,"commit_id":%q,"body":"%s","submitted_at":%q}}`,
		action, id, login, state, commit, b, submitted)
}

func apply(t *testing.T, s *Store, event, id string, body []byte) {
	t.Helper()
	if e := s.Apply("o/r", event, id, body); e != nil {
		t.Fatalf("%s %s: %v", event, id, e)
	}
}

// (a) synchronize stores the new head; an older delivery arriving later does not overwrite it.
func TestNewerPullRequestDeliveryWinsOverOlderOne(t *testing.T) {
	s := Open(t.TempDir())
	apply(t, s, "pull_request", "d1", pullRequestEvent("opened", 7, "h1", "2026-11-06T00:01:00Z"))
	apply(t, s, "pull_request", "d3", pullRequestEvent("synchronize", 7, "h3", "2026-11-06T00:03:00Z"))
	apply(t, s, "pull_request", "d2", pullRequestEvent("synchronize", 7, "h2", "2026-11-06T00:02:00Z"))
	pr, ok := s.PR("o/r", 7)
	if !ok || pr.HeadSHA != "h3" {
		t.Fatalf("stored head = %q (found %v), want h3", pr.HeadSHA, ok)
	}
	apply(t, s, "pull_request", "d4", pullRequestEvent("edited", 7, "h4", "2026-11-06T00:03:00Z"))
	if pr, _ := s.PR("o/r", 7); pr.HeadSHA != "h3" {
		t.Fatalf("an equal timestamp overwrote the head: %q", pr.HeadSHA)
	}
	if pr.Labels[0] != "fast" || pr.BaseRef != "main" || pr.Title != "t" || pr.Body != "body" {
		t.Fatalf("stored PR lost fields: %+v", pr)
	}
}

func TestDuplicateDeliveryIdIsNoOp(t *testing.T) {
	s := Open(t.TempDir())
	apply(t, s, "pull_request", "d1", pullRequestEvent("opened", 7, "h1", "2026-11-06T00:01:00Z"))
	// Same id, newer payload: the id was applied already, so nothing changes.
	apply(t, s, "pull_request", "d1", pullRequestEvent("synchronize", 7, "h2", "2026-11-06T00:02:00Z"))
	if pr, _ := s.PR("o/r", 7); pr.HeadSHA != "h1" {
		t.Fatalf("a repeated delivery id was applied again: head %q", pr.HeadSHA)
	}
}

// (b) check_run completed, then an older in_progress for the same check, keeps completed.
func TestCompletedCheckRunSurvivesOlderInProgress(t *testing.T) {
	s := Open(t.TempDir())
	apply(t, s, "check_run", "c1", checkRunEvent(5, "test", "completed", "success", "2026-11-06T00:00:10Z", "2026-11-06T00:01:00Z"))
	apply(t, s, "check_run", "c2", checkRunEvent(5, "test", "in_progress", "", "2026-11-06T00:00:10Z", ""))
	h, ok, _ := s.Head("o/r", "h1")
	run := h.Runs["9/test"]
	if !ok || run.Status != "completed" || run.Conclusion != "success" {
		t.Fatalf("stored run = %+v, want completed/success", run)
	}
	// A rerun is a new run id and replaces it; an older id arriving later does not.
	apply(t, s, "check_run", "c3", checkRunEvent(6, "test", "in_progress", "", "2026-11-06T00:02:00Z", ""))
	apply(t, s, "check_run", "c4", checkRunEvent(4, "test", "completed", "failure", "2026-11-06T00:00:01Z", "2026-11-06T00:00:30Z"))
	if h, _, _ := s.Head("o/r", "h1"); h.Runs["9/test"].ID != 6 || h.Runs["9/test"].Status != "in_progress" {
		t.Fatalf("rerun not kept: %+v", h.Runs["9/test"])
	}
}

func TestStatusAndSuiteAreKeptPerHead(t *testing.T) {
	s := Open(t.TempDir())
	apply(t, s, "status", "s2", []byte(`{"id":12,"sha":"h1","context":"ci/x","state":"success","target_url":"https://ci.example/12","updated_at":"2026-11-06T00:02:00Z"}`))
	apply(t, s, "status", "s1", []byte(`{"id":11,"sha":"h1","context":"ci/x","state":"failure","updated_at":"2026-11-06T00:01:00Z"}`))
	apply(t, s, "check_suite", "u1", []byte(`{"action":"completed","check_suite":{"id":9,"head_sha":"h1","status":"completed","conclusion":"success","updated_at":"2026-11-06T00:02:00Z","app":{"slug":"ci"}}}`))
	h, _, _ := s.Head("o/r", "h1")
	if h.Statuses["ci/x"].State != "success" || h.Statuses["ci/x"].TargetURL == "" || h.Suites[9].Conclusion != "success" {
		t.Fatalf("head = %+v", h)
	}
}

// (c) a review by the review login keeps its body marker intact, even when the body is large.
func TestReviewBodyMarkerSurvives(t *testing.T) {
	s := Open(t.TempDir())
	marker := "<!-- mergewake-review:c1 -->\n" + strings.Repeat("finding text ", 400) + "\n<!-- mergewake-pass:{\"pass\":1} -->"
	apply(t, s, "pull_request_review", "r1", reviewEvent("submitted", 100, "mergewake-bot", "commented", "c1", marker, "2026-11-06T00:05:00Z"))
	apply(t, s, "pull_request_review", "r2", reviewEvent("submitted", 101, "human", "approved", "c1", strings.Repeat("x", 5000), "2026-11-06T00:06:00Z"))
	got, ok, _ := s.Reviews("o/r", 7)
	if !ok || len(got) != 2 {
		t.Fatalf("reviews = %+v", got)
	}
	if got[0].Login != "mergewake-bot" || got[0].State != "COMMENTED" || got[0].CommitID != "c1" || got[0].Body != marker {
		t.Fatalf("marked review changed: %+v", got[0])
	}
	if len(got[1].Body) > plainReviewBody {
		t.Fatalf("an unmarked body was not bounded: %d bytes", len(got[1].Body))
	}
	// Dismissal changes the state; a later redelivery of "submitted" does not undo it.
	apply(t, s, "pull_request_review", "r3", reviewEvent("dismissed", 100, "mergewake-bot", "dismissed", "c1", marker, "2026-11-06T00:05:00Z"))
	apply(t, s, "pull_request_review", "r4", reviewEvent("submitted", 100, "mergewake-bot", "commented", "c1", marker, "2026-11-06T00:05:00Z"))
	if got, _, _ := s.Reviews("o/r", 7); got[0].State != "DISMISSED" {
		t.Fatalf("state = %q, want DISMISSED", got[0].State)
	}
}

// (g) the state survives a restart and is pruned when the PR closes.
func TestStateSurvivesRestartAndClosePrunes(t *testing.T) {
	dir := t.TempDir()
	s := Open(dir)
	apply(t, s, "pull_request", "d1", pullRequestEvent("opened", 7, "h1", "2026-11-06T00:01:00Z"))
	apply(t, s, "check_run", "c1", checkRunEvent(5, "test", "completed", "success", "2026-11-06T00:00:10Z", "2026-11-06T00:01:00Z"))
	apply(t, s, "pull_request_review", "r1", reviewEvent("submitted", 100, "mergewake-bot", "commented", "h1", "ok", "2026-11-06T00:05:00Z"))
	if e := s.Flush(); e != nil {
		t.Fatal(e)
	}

	again := Open(dir)
	if pr, ok := again.PR("o/r", 7); !ok || pr.HeadSHA != "h1" {
		t.Fatalf("PR after restart = %+v (found %v)", pr, ok)
	}
	if h, ok, _ := again.Head("o/r", "h1"); !ok || h.Runs["9/test"].Conclusion != "success" {
		t.Fatalf("checks after restart = %+v", h)
	}
	if rv, ok, _ := again.Reviews("o/r", 7); !ok || len(rv) != 1 {
		t.Fatalf("reviews after restart = %+v", rv)
	}
	// The delivery ids persist too: a redelivery after the restart is a no-op.
	apply(t, again, "pull_request", "d1", pullRequestEvent("synchronize", 7, "h9", "2026-11-06T00:09:00Z"))
	if pr, _ := again.PR("o/r", 7); pr.HeadSHA != "h1" {
		t.Fatalf("redelivery after restart applied: %q", pr.HeadSHA)
	}

	closed := strings.Replace(string(pullRequestEvent("closed", 7, "h1", "2026-11-06T00:10:00Z")), `"state":"open"`, `"state":"closed"`, 1)
	apply(t, again, "pull_request", "d2", []byte(closed))
	if _, ok := again.PR("o/r", 7); ok {
		t.Fatal("closed PR still stored")
	}
	if _, ok, _ := again.Head("o/r", "h1"); ok {
		t.Fatal("closed PR's checks still stored")
	}
	if _, ok, _ := again.Reviews("o/r", 7); ok {
		t.Fatal("closed PR's reviews still stored")
	}
	// A straggler older than the close must not bring it back.
	apply(t, again, "pull_request", "d3", pullRequestEvent("synchronize", 7, "h2", "2026-11-06T00:02:00Z"))
	if _, ok := again.PR("o/r", 7); ok {
		t.Fatal("a delivery older than the close resurrected the PR")
	}
	if e := again.Flush(); e != nil {
		t.Fatal(e)
	}
	if third := Open(dir); func() bool { _, ok := third.PR("o/r", 7); return ok }() {
		t.Fatal("pruned PR came back after a restart")
	}
}

// (h) malformed payloads and unknown events leave the state alone.
func TestMalformedPayloadsAreIgnored(t *testing.T) {
	s := Open(t.TempDir())
	apply(t, s, "pull_request", "d1", pullRequestEvent("opened", 7, "h1", "2026-11-06T00:01:00Z"))
	for _, tt := range []struct{ event, body string }{
		{"pull_request", `{not json`},
		{"pull_request", `{"pull_request":{"number":7,"updated_at":"yesterday"}}`},
		{"pull_request", `{}`},
		{"check_run", `{"check_run":{"id":1}}`},
		{"status", `[]`},
		{"pull_request_review", `{"review":{}}`},
	} {
		if e := s.Apply("o/r", tt.event, "bad-"+tt.event+tt.body, []byte(tt.body)); e == nil {
			t.Errorf("%s %q: want an error", tt.event, tt.body)
		}
	}
	if e := s.Apply("o/r", "deployment_status", "x", []byte(`{not json`)); e != nil {
		t.Errorf("an unknown event is ignored without error, got %v", e)
	}
	if pr, ok := s.PR("o/r", 7); !ok || pr.HeadSHA != "h1" {
		t.Fatalf("state changed: %+v", pr)
	}
}

func TestBoundsAndUsability(t *testing.T) {
	now := time.Date(2026, 11, 6, 1, 0, 0, 0, time.UTC)
	s := Open(t.TempDir())
	s.Now = func() time.Time { return now }
	apply(t, s, "pull_request", "d1", pullRequestEvent("opened", 7, "h1", "2026-11-06T00:01:00Z"))
	if _, ok := s.UsablePR("o/r", 7); !ok {
		t.Fatal("fresh PR not usable")
	}
	now = now.Add(MaxAge + time.Second)
	if _, ok := s.UsablePR("o/r", 7); ok {
		t.Fatal("an unconfirmed PR stayed usable past MaxAge")
	}
	// An API read confirms it again; a write by this process withdraws the confirmation.
	s.SeedPR("o/r", PR{Number: 7, State: "open", HeadSHA: "h1", UpdatedAt: time.Date(2026, 11, 6, 0, 1, 0, 0, time.UTC)})
	if _, ok := s.UsablePR("o/r", 7); !ok {
		t.Fatal("API read did not confirm the PR")
	}
	now = now.Add(time.Second)
	s.Distrust("o/r")
	if _, ok := s.UsablePR("o/r", 7); ok {
		t.Fatal("a write did not withdraw confirmation")
	}
	// A head that deliveries alone created is never complete.
	apply(t, s, "check_run", "c1", checkRunEvent(5, "test", "completed", "success", "2026-11-06T00:00:10Z", "2026-11-06T00:01:00Z"))
	if _, ok, usable := s.Head("o/r", "h1"); !ok || usable {
		t.Fatalf("webhook-only head: found %v usable %v", ok, usable)
	}
}
