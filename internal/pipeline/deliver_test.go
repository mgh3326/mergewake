package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// jobSink is an hk sink that knows task lanes and the job bound to a task.
type jobSink struct {
	fakeSink
	lanes map[string]string
	jobs  map[string]string
	recs  map[string]integrations.JobRecord
}

func (s *jobSink) Lane(_ context.Context, task string) (string, error) { return s.lanes[task], nil }
func (s *jobSink) TaskJob(_ context.Context, task string) (string, string, error) {
	return s.jobs[task], "", nil
}
func (s *jobSink) Job(_ context.Context, id string) (integrations.JobRecord, error) {
	rec, ok := s.recs[id]
	if !ok {
		return rec, integrations.ErrJobNotFound
	}
	return rec, nil
}

// ackNotifier records events and answers ACK reads from acked.
type ackNotifier struct {
	fakeNotifier
	acked  map[string]bool
	ackErr error
	reads  int
}

func (n *ackNotifier) AckedEvents(_ context.Context, lane string) (map[string]bool, error) {
	n.reads++
	if n.ackErr != nil {
		return nil, n.ackErr
	}
	out := map[string]bool{}
	for _, e := range n.events {
		if e.Lane == lane {
			out[e.ID] = n.acked[e.ID]
		}
	}
	return out, nil
}

// finding is an unresolved thread posted on shaA by author with body.
func finding(id, author, body string, line int, replies ...github.ReviewComment) github.ReviewThread {
	cs := append([]github.ReviewComment{{Author: author, Body: body, CommitSHA: shaA, OriginalCommitSHA: shaA, URL: "https://github.example/pr/7#discussion_" + id}}, replies...)
	return github.ReviewThread{ID: id, Path: "tool", Line: line, Comments: cs}
}

func ownFinding(id, severity string, line int, replies ...github.ReviewComment) github.ReviewThread {
	return finding(id, "review-bot", "<!-- mergewake-severity:"+severity+" -->\nfinding "+id, line, replies...)
}

// deliverSetup is a PR bound to task 2481, whose job w-7 is a live worker on
// lane w-7 owned by lane integrator; the head's review is complete.
func deliverSetup(t *testing.T) (*Service, *fakeGH, *ackNotifier, *jobSink, config.Repo, *time.Time) {
	t.Helper()
	s, g, _, _, r := setup(t)
	markReviewed(g)
	g.pull.Body = "Summary\n\nMergewake-Task: 2481\n"
	sink := &jobSink{
		lanes: map[string]string{"2481": "operator-desk"},
		jobs:  map[string]string{"2481": "w-7"},
		recs:  map[string]integrations.JobRecord{"w-7": {State: "spawned", Role: "worker", Label: "w-7", OwnerLane: "integrator"}},
	}
	n := &ackNotifier{acked: map[string]bool{}}
	s.Sink, s.Notifier = sink, n
	s.Config.Notifier.Lane, s.Config.Notifier.FallbackLane = "operator-desk", "operator-desk"
	s.Config.Daemon.PR.Findings.EscalateRounds = 3
	now := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	return s, g, n, sink, r, &now
}

func deliver(t *testing.T, s *Service, r config.Repo) {
	t.Helper()
	if e := s.DeliverFindings(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
}

// Acceptance 1: one review round with two findings produces exactly one event
// on the author's lane naming both, with what is required and how to ACK.
func TestFindingsRoundIsOneAuthorLaneEvent(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "medium", 3),
		ownFinding("T2", "high", 9),
		// Not delivered: a CodeRabbit Minor, an answered finding, a resolved one.
		finding("T3", "coderabbitai[bot]", crBody("amber", "Minor"), 4),
		ownFinding("T4", "high", 5, github.ReviewComment{Author: "dev", AuthorType: "User", Body: "Fixed in abc1234."}),
		func() github.ReviewThread { th := ownFinding("T5", "low", 6); th.IsResolved = true; return th }(),
	}
	deliver(t, s, r)
	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("want exactly one event, got %d: %+v", n.count, n.events)
	}
	e := n.events[0]
	if e.Lane != "w-7" || e.Type != "findings" || !strings.HasPrefix(e.ID, "mergewake-findings-example/project-7-") {
		t.Fatalf("event %+v", e)
	}
	for _, want := range []string{
		"[mergewake] FINDINGS example/project#7 head=aaaaaaa", "2 finding(s) for lane w-7",
		"1) high [blocks merge] tool:9 https://github.example/pr/7#discussion_T2", "2) medium tool:3 https://github.example/pr/7#discussion_T1",
		"every critical/high/Major finding needs a fix commit or a dismissal with evidence", "a command and its output", "the code line that disproves it",
		"panewire inbox list --lane w-7 --unread", "read each item, act on it or record its disposition", "panewire inbox ack <id>", "acknowledging does not resolve a finding",
		"Fixed in <sha>", "Dismiss: <evidence>", "Disposition: fixed <sha>", "Disposition: dismiss <evidence>", "Disposition: defer <hk task>", "defer only for nonblocking findings",
	} {
		if !strings.Contains(e.Text, want) {
			t.Fatalf("missing %q in %q", want, e.Text)
		}
	}
	if strings.Contains(e.Text, "T3") || strings.Contains(e.Text, "T4") || strings.Contains(e.Text, "T5") {
		t.Fatalf("delivered a finding that needs nothing: %q", e.Text)
	}
	// A later round's new finding is a new event; the first two are not repeated.
	g.feedback.Threads = append(g.feedback.Threads, finding("T6", "coderabbitai[bot]", crBody("major", "Major"), 12))
	deliver(t, s, r)
	if n.count != 2 || strings.Contains(n.events[1].Text, "T1") || !strings.Contains(n.events[1].Text, "1) major [blocks merge] tool:12") {
		t.Fatalf("second round %+v", n.events)
	}
}

// mergewake's findings on the current head wait for the head's -done marker,
// so a round still running is not split across events.
func TestFindingsWaitForTheRoundToFinish(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	g.refs = map[string]bool{}
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	if n.count != 0 {
		t.Fatal("delivered before the review round finished")
	}
	markReviewed(g)
	deliver(t, s, r)
	if n.count != 1 {
		t.Fatalf("events=%d", n.count)
	}
}

// Acceptance 1: no ACK within ack_after → exactly one re-send to the same
// lane; still none → one escalation to the owner lane; then nothing more.
func TestFindingsUnackedResendThenEscalate(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3), ownFinding("T2", "low", 4)}
	step := func(d time.Duration) { *now = now.Add(d); deliver(t, s, r) }
	step(0)
	step(14 * time.Minute)
	if n.count != 1 {
		t.Fatalf("re-sent early: %+v", n.events)
	}
	step(time.Minute)
	if n.count != 2 {
		t.Fatalf("no re-send after 15m: %+v", n.events)
	}
	re := n.events[1]
	if re.ID != n.events[0].ID+"-resend" || re.Lane != "w-7" || re.Type != "findings" || !strings.Contains(re.Text, "re-sent: no ack in 15m0s") {
		t.Fatalf("re-send %+v", re)
	}
	step(14 * time.Minute)
	if n.count != 2 {
		t.Fatal("escalated early")
	}
	step(91 * time.Minute)
	if n.count != 3 {
		t.Fatalf("no escalation: %+v", n.events)
	}
	esc := n.events[2]
	if esc.Lane != "operator-desk" || esc.Type != "decision" || !strings.Contains(esc.Text, "[mergewake] ESC example/project#7") || !strings.Contains(esc.Text, "findings still open") {
		t.Fatalf("escalation %+v", esc)
	}
	for i := 0; i < 4; i++ {
		step(time.Hour)
	}
	if n.count != 3 {
		t.Fatalf("followed up after escalating: %d events", n.count)
	}
}

// An inbox ACK, or a disposition on every named thread, closes a delivery:
// no re-send, no escalation.
func TestFindingsAckOrDispositionStopsFollowUp(t *testing.T) {
	for _, how := range []string{"inbox ack", "disposition"} {
		t.Run(how, func(t *testing.T) {
			s, g, n, _, r, now := deliverSetup(t)
			g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
			deliver(t, s, r)
			if how == "inbox ack" {
				n.acked[n.events[0].ID] = true
			} else {
				g.feedback.Threads[0].Comments = append(g.feedback.Threads[0].Comments, github.ReviewComment{Author: "review-bot", AuthorType: "User", Body: "Fixed in abc1234."})
			}
			for i := 0; i < 4; i++ {
				*now = now.Add(20 * time.Minute)
				deliver(t, s, r)
			}
			if n.count != 1 {
				t.Fatalf("followed up an acknowledged delivery: %+v", n.events)
			}
		})
	}
}

// Acceptance 1: a PR without a bound job falls back to the repo lane, then
// to the desk.
func TestFindingsWithoutBoundJobFallBack(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	delete(sink.jobs, "2481")
	r.Lane = "repo-lane"
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	if n.count != 1 || n.events[0].Lane != "operator-desk" || n.events[0].Type != "decision" {
		t.Fatalf("%+v", n.events)
	}

	s, g, n, _, r, _ = deliverSetup(t)
	g.pull.Body = "no trailer"
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	if n.count != 1 || n.events[0].Lane != "operator-desk" {
		t.Fatalf("no repo lane must reach the desk: %+v", n.events)
	}
}

// A job that is already terminal has no author attempt to fix anything: its
// findings go to the owner, and nothing is respawned.
func TestFindingsForATerminalJobKeepAuthorLane(t *testing.T) {
	s, g, n, sink, r, _ := deliverSetup(t)
	sink.recs["w-7"] = integrations.JobRecord{State: "completed", Role: "worker", Label: "w-7", OwnerLane: "integrator"}
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	if n.count != 1 || n.events[0].Lane != "w-7" {
		t.Fatalf("%+v", n.events)
	}
}

// Findings the reviewer marked outside the Acceptance contract go back to the
// owner once and are never re-sent or escalated.
func TestFindingsOutOfScopeGoToAuthorOnce(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{
		finding("T1", "review-bot", "<!-- mergewake-severity:medium -->\n<!-- mergewake-advisory -->\n**Advisory** outside the contract\nstyle", 3),
		ownFinding("T2", "high", 5),
	}
	for i := 0; i < 5; i++ {
		deliver(t, s, r)
		n.acked[n.events[0].ID] = true
		g.feedback.Threads[1].IsResolved = true
		*now = now.Add(time.Hour)
	}
	var author, owner int
	for _, e := range n.events {
		switch {
		case e.Lane == "w-7" && strings.Contains(e.Text, "T2") && !strings.Contains(e.Text, "T1"):
			author++
		case e.Lane == "w-7" && strings.Contains(e.Text, "OUT-OF-SCOPE") && strings.Contains(e.Text, "T1"):
			owner++
		default:
			t.Fatalf("unexpected event %+v", e)
		}
	}
	if author != 1 || owner != 1 {
		t.Fatalf("author=%d owner=%d: %+v", author, owner, n.events)
	}
}

// Many findings stay within panewire's lane event bound and say how many did
// not fit.
func TestFindingsTextFitsTheLaneEvent(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	for i := range 60 {
		g.feedback.Threads = append(g.feedback.Threads, ownFinding("T"+strings.Repeat("x", 30)+string(rune('A'+i%26))+string(rune('a'+i/26)), "medium", i+1))
	}
	deliver(t, s, r)
	if n.count != 1 || len(n.events[0].Text) > 2048 || !strings.Contains(n.events[0].Text, "more on the PR") || !strings.Contains(n.events[0].Text, "panewire inbox ack") {
		t.Fatalf("%d bytes: %q", len(n.events[0].Text), n.events[0].Text)
	}
}

// findings: {enabled: false} turns delivery off.
func TestFindingsDeliveryCanBeDisabled(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	no := false
	s.Config.Daemon.PR.Findings.Enabled = &no
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	if n.count != 0 {
		t.Fatal("disabled delivery sent an event")
	}
}

// auditRecords decodes the audit lines written to out for one command.
func auditRecords(t *testing.T, out []byte, command string) []map[string]any {
	t.Helper()
	var got []map[string]any
	dec := json.NewDecoder(bytes.NewReader(out))
	for dec.More() {
		var rec struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if e := dec.Decode(&rec); e != nil {
			t.Fatal(e)
		}
		if rec.Command == command {
			got = append(got, rec.Value)
		}
	}
	return got
}

// An unreadable ACK neither re-sends nor escalates on a guess, never holds a
// new round's findings, and does not fail the pass: it is audited once per
// lane per pass.
func TestFindingsUnreadableAckIsAuditedNotFailed(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	var out bytes.Buffer
	s.Out = &out
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	n.ackErr = errors.New("hub unreachable")
	*now = now.Add(time.Hour)
	g.feedback.Threads = append(g.feedback.Threads, finding("T2", "coderabbitai[bot]", crBody("major", "Major"), 8))
	if e := s.DeliverFindings(context.Background(), r, 7, Options{}); e != nil {
		t.Fatalf("unreadable ack failed the pass: %v", e)
	}
	if n.count != 2 || !strings.Contains(n.events[1].Text, "T2") || strings.HasSuffix(n.events[1].ID, "-resend") {
		t.Fatalf("new finding held or a guess re-sent: %+v", n.events)
	}
	unavailable := auditRecords(t, out.Bytes(), "findings-ack-unavailable")
	if len(unavailable) != 1 {
		t.Fatalf("want one findings-ack-unavailable record for the lane, got %+v", unavailable)
	}
	rec := unavailable[0]
	if rec["pr"] != float64(7) || rec["lane"] != n.events[0].Lane || !strings.Contains(fmt.Sprint(rec["error"]), "hub unreachable") {
		t.Fatalf("record %+v", rec)
	}
	if len(auditRecords(t, out.Bytes(), "findings-error")) != 0 {
		t.Fatal("findings-error recorded for an unreadable ack")
	}
}

// An aged finding is still escalated when its ACK cannot be read, because
// escalation does not depend on the read.
func TestFindingsUnreadableAckStillEscalatesAgedFindings(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	n.ackErr = errors.New("hub unreachable")
	*now = now.Add(2 * time.Hour)
	if e := s.DeliverFindings(context.Background(), r, 7, Options{}); e != nil {
		t.Fatalf("unreadable ack failed the pass: %v", e)
	}
	if n.count != 2 {
		t.Fatalf("aged finding not escalated: %+v", n.events)
	}
	esc := n.events[1]
	if esc.Lane != s.Config.Notifier.Lane || esc.Type != "decision" || !strings.Contains(esc.Text, "findings still open") {
		t.Fatalf("escalation %+v", esc)
	}
}

// A notifier delivery error is not an ACK read: it still fails the pass.
func TestFindingsNotifierErrorStillFailsDelivery(t *testing.T) {
	s, g, n, _, r, _ := deliverSetup(t)
	n.err = errors.New("notify down")
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	e := s.DeliverFindings(context.Background(), r, 7, Options{})
	if e == nil || !strings.Contains(e.Error(), "notify down") {
		t.Fatalf("delivery error not returned: %v", e)
	}
}

// A closed or merged PR gets no delivery and no follow-up.
func TestFindingsStopForAClosedPR(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3)}
	deliver(t, s, r)
	g.pull.State = "closed"
	g.feedback.Threads = append(g.feedback.Threads, ownFinding("T2", "high", 4))
	for range 4 {
		*now = now.Add(time.Hour)
		deliver(t, s, r)
	}
	if n.count != 1 {
		t.Fatalf("closed PR got events: %+v", n.events)
	}
}

// A reply that only acknowledges a blocking finding is not a disposition: the
// delivery is still followed up. Several deliveries on one lane read its
// inbox once per pass.
func TestFindingsAcknowledgementIsNotADisposition(t *testing.T) {
	s, g, n, _, r, now := deliverSetup(t)
	g.feedback.Threads = []github.ReviewThread{ownFinding("T1", "high", 3, github.ReviewComment{Author: "dev", AuthorType: "User", Body: "Looking into it."})}
	deliver(t, s, r)
	if n.count != 1 || !strings.Contains(n.events[0].Text, "high [blocks merge] tool:3") {
		t.Fatalf("an acknowledged high was not delivered: %+v", n.events)
	}
	*now = now.Add(time.Hour)
	g.feedback.Threads = append(g.feedback.Threads, finding("T2", "coderabbitai[bot]", crBody("major", "Major"), 8))
	deliver(t, s, r)
	*now = now.Add(time.Hour)
	n.reads = 0
	deliver(t, s, r)
	if n.reads != 1 {
		t.Fatalf("inbox read %d times for one lane", n.reads)
	}
	var resends int
	for _, e := range n.events {
		if strings.HasSuffix(e.ID, "-resend") {
			resends++
		}
	}
	if resends == 0 {
		t.Fatalf("acknowledged-only high was never followed up: %+v", n.events)
	}
}

// Without a bound job, findings go to an explicit author lane: the
// Notify-Lane trailer, then the task's lane unless it is the desk lane.
func TestFindingsRouteExplicitAuthorLane(t *testing.T) {
	for _, tt := range []struct {
		name, taskLane, notify, job, wantLane, wantVia string
	}{
		{"trailer matches task lane", "author-design", "author-design", "", "author-design", "notify-lane trailer"},
		{"task lane without trailer", "other-design", "", "", "other-design", "task 2481"},
		{"desk task lane with trailer", "operator-desk", "design-lane", "", "design-lane", "notify-lane trailer"},
		{"desk task lane without trailer", "operator-desk", "", "", "operator-desk", "unknown author; desk"},
		{"bound job wins", "author-design", "design-lane", "w-7", "w-7", "task 2481 job w-7"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, sink, r, _ := deliverSetup(t)
			sink.lanes["2481"] = tt.taskLane
			if tt.job == "" {
				delete(sink.jobs, "2481")
			}
			if tt.notify != "" {
				g.pull.Body += "Mergewake-Notify-Lane: " + tt.notify + "\n"
			}
			rt, e := s.findingsRoute(context.Background(), r, g.pull)
			if e != nil {
				t.Fatal(e)
			}
			if rt.lane != tt.wantLane || rt.via != tt.wantVia {
				t.Fatalf("route %q via %q, want %q via %q", rt.lane, rt.via, tt.wantLane, tt.wantVia)
			}
		})
	}
}
