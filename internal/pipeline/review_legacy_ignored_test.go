package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// The retired legacy findings sources are the attachment announced on the
// findings lane and findings.json in mergewake's own job directory. No review
// reads either. At every entry where a pass once took them (a resumed
// reviewer whose pane finished, a resumed reviewer hk cannot read, a
// completed record without a result, and any other record outcome) findings
// planted there are never posted, no findings inbox is listed or
// acknowledged, no attachment is fetched, and old lane events stay as they
// are (docs/config.md, review.panewire).

const legacyBody = `{"findings":[{"path":"tool","line":1,"body":"A legacy bug","severity":"high"}]}`

var legacySources = []string{"lane", "file"}

// plantLegacy configures the findings lane and puts legacyBody in one legacy
// source of label: an attachment announced on the lane (after a malformed
// announcement, and next to another lane's event), or the job-dir file.
func plantLegacy(t *testing.T, s *Service, runner *panewireRunner, label, source string) {
	t.Helper()
	s.Config.Review.Panewire.Lane = "review-desk"
	switch source {
	case "lane":
		sum := sha256.Sum256([]byte(legacyBody))
		sha := hex.EncodeToString(sum[:])
		runner.sink.files[sha] = []byte(legacyBody)
		runner.inbox = append(runner.inbox,
			`{"id":7,"kind":"lane.event","event":{"event_id":"other-findings","text":"[other] findings attachment=`+sha+`"}}`,
			`{"id":8,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings"}}`,
			`{"id":9,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings attachment=`+sha+`"}}`)
	case "file":
		dir := filepath.Join(s.Config.Review.Panewire.Jobs, label)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "findings.json"), []byte(legacyBody), 0o600); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown legacy source %q", source)
	}
}

// legacyPane is a fresh pane that leaves legacyBody only in source: it
// announces it on the findings lane, or writes the job-dir file, and never
// runs `panewire done --result`.
func legacyPane(source string) *panewireRunner {
	runner := &panewireRunner{findings: legacyBody, family: "openai", legacyOnly: true}
	if source == "lane" {
		runner.remote = &taskJobSink{}
	}
	return runner
}

// noLegacyRead checks that the run read no legacy source, acknowledged no
// lane event, audited no legacy use and posted none of the legacy findings.
func noLegacyRead(t *testing.T, s *Service, r config.Repo, runner *panewireRunner, g *fakeGH) {
	t.Helper()
	if runner.looks != 0 || len(runner.acks) != 0 || runner.sink.attachmentReads != 0 {
		t.Fatalf("legacy read: inbox looks=%d acks=%v attachment reads=%d", runner.looks, runner.acks, runner.sink.attachmentReads)
	}
	for _, f := range g.postedFindings {
		if f.Body == "A legacy bug" {
			t.Fatalf("legacy findings posted: %+v", g.postedFindings)
		}
	}
	for _, command := range []string{"review-inbox-fallback", "reviewer-lane-unreadable"} {
		if rows := auditRows(t, s, r, command); len(rows) != 0 {
			t.Fatalf("%s rows=%v", command, rows)
		}
	}
}

// standHub stands a fake hub as s's reap hub, as budgetPane does, so a
// reviewer node can answer job.result.
func standHub(t *testing.T, s *Service) *fakeReapHub {
	t.Helper()
	hub := newFakeReapHub()
	server := hub.serve(t)
	token := filepath.Join(t.TempDir(), "hub.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=svc-mergewake\nHUB_TOKEN=svc-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.Config.Reap.Transport = config.ReapTransportPanewire
	s.Config.Reap.Hub = config.ReapHub{URL: server.URL, TokenFile: token}
	return hub
}

// noJobSink is a task sink that reads no hk job records. Its attachment reads
// go to files, so a findings-lane read would still find the announced bytes.
type noJobSink struct {
	fakeLaneSink
	files *taskJobSink
}

func (noJobSink) TaskJob(context.Context, string) (string, string, error) {
	return "author-job", "Fix the thing (grade A, T2)", nil
}

func (s noJobSink) Attachment(ctx context.Context, sha string) ([]byte, error) {
	return s.files.Attachment(ctx, sha)
}

// (resumed-finished) A resumed reviewer whose pane is done, or still working
// past the budget, while its hk job stays claimed and its node has no result:
// the planted findings are not read. A done pane with budget left is nudged
// once and waited for; a pane working past the budget is replaced.
func TestLegacyIgnoredResumedFinished(t *testing.T) {
	for _, source := range legacySources {
		t.Run(source+"/idle pane", func(t *testing.T) {
			s, g, r, runner, _, old, _, n := nudgeSetup(t, "done")
			plantLegacy(t, s, runner, old.Label, source)
			reviewWaits(t, s, r)
			noLegacyRead(t, s, r, runner, g)
			if events := nudgeEvents(n); len(events) != 1 || events[0].Lane != old.Label {
				t.Fatalf("nudge events=%+v", events)
			}
			if got := readReviewerJob(t, s, r, 1); got.Label != old.Label || got.NudgedAt == nil {
				t.Fatalf("record=%+v", got)
			}
			if len(runner.spawns) != 1 || g.postCount != 0 || len(auditRows(t, s, r, "review-result-lost")) != 0 {
				t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
			}
		})
		t.Run(source+"/working past the budget", func(t *testing.T) {
			s, g, r, runner, c := interruptedReviewer(t)
			old := readReviewerJob(t, s, r, 1)
			budgetPane(t, s, runner, old, "working")
			plantLegacy(t, s, runner, old.Label, source)
			runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
			pastBudget(g, c)
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			noLegacyRead(t, s, r, runner, g)
			if len(runner.spawns) != 2 || arg(runner.spawns[1], "-l") == old.Label || g.postCount != 1 {
				t.Fatalf("spawns=%v posts=%d", runner.spawns, g.postCount)
			}
			if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 1 || rows[0]["collect_error"] != lostOverdue {
				t.Fatalf("lost rows=%v", rows)
			}
		})
	}
}

// (completed-no-result, resumed) A resumed reviewer whose record completed
// without a result and whose host has none lost its result although the
// legacy sources hold findings: it is replaced once.
func TestLegacyIgnoredCompletedNoResultResumed(t *testing.T) {
	for _, source := range legacySources {
		t.Run(source, func(t *testing.T) {
			f := completedWithoutResult(t, remoteNotFound)
			plantLegacy(t, f.s, f.runner.panewireRunner, f.old.Label, source)
			if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			noLegacyRead(t, f.s, f.r, f.runner.panewireRunner, f.g)
			if calls := f.hub.ops("job.result"); len(calls) != 1 {
				t.Fatalf("node reads=%v, want one", calls)
			}
			if len(f.runner.spawns) != 2 || arg(f.runner.spawns[1], "-l") == f.old.Label || f.g.postCount != 0 {
				t.Fatalf("spawns=%v posts=%d, want one replacement", f.runner.spawns, f.g.postCount)
			}
			if rows := auditRows(t, f.s, f.r, "review-result-lost"); len(rows) != 1 || rows[0]["label"] != f.old.Label {
				t.Fatalf("lost rows=%v", rows)
			}
		})
	}
}

// (completed-no-result, fresh) A fresh reviewer whose record completed
// without a result, with no host on the record or none that has the result,
// lost it: a typed lost result, its audit and its notice, and nothing posted
// from the legacy sources.
func TestLegacyIgnoredCompletedNoResultFresh(t *testing.T) {
	for _, source := range legacySources {
		for name, host := range map[string]string{"no host": "", "local node without it": "local", "remote node without it": "node-a"} {
			t.Run(source+"/"+name, func(t *testing.T) {
				runner := legacyPane(source)
				s, g, r := panewireSetup(t, runner)
				s.Config.Notifier.Lane = "desk"
				s.Config.Review.Panewire.Lane = "review-desk"
				var hub *fakeReapHub
				if host == "node-a" {
					hub = standHub(t, s)
				}
				runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Host: host}
				e := s.Review(context.Background(), r, 7, "", Options{})
				label := arg(runner.spawns[0], "-l")
				var lost *reviewResultLost
				if !errors.As(e, &lost) || lost.label != label || g.postCount != 0 || g.refs[doneRef(7, shaA)] {
					t.Fatalf("err=%v posts=%d", e, g.postCount)
				}
				noLegacyRead(t, s, r, runner, g)
				if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 1 || rows[0]["label"] != label {
					t.Fatalf("lost rows=%v", rows)
				}
				if n := s.Notifier.(*fakeNotifier); n.count != 1 || !strings.Contains(n.events[0].Text, "result was lost") {
					t.Fatalf("notices=%+v", n.events)
				}
				switch host {
				case "local":
					if len(runner.nodeReads) != 1 || runner.nodeReads[0] != label {
						t.Fatalf("local node reads=%v", runner.nodeReads)
					}
				case "node-a":
					if calls := hub.ops("job.result"); len(calls) != 1 || calls[0]["job"] != label {
						t.Fatalf("remote node reads=%v", calls)
					}
				}
			})
		}
	}
}

// (record-outcome) A fresh reviewer whose record answers without findings,
// whatever the reason, fails its pass with that reason: no record, an
// unreadable hk, a sink without job records, a state that is not terminal,
// or such a state on a host that has no result either. The legacy findings
// are not read, and the pass is a skip, not a lost result.
func TestLegacyIgnoredRecordOutcome(t *testing.T) {
	for _, source := range legacySources {
		for name, tc := range map[string]struct {
			record *integrations.JobRecord
			jobErr error
			noJob  bool
			reason string
			node   bool // the record names a host, which is read
		}{
			"no record":                   {reason: "no job record"},
			"hk unreadable":               {jobErr: errors.New("hk: HTTP 502 bad gateway"), reason: "hk-unavailable"},
			"sink without job records":    {noJob: true, reason: "hk-unavailable"},
			"not terminal":                {record: &integrations.JobRecord{State: "claimed"}, reason: "job record state claimed"},
			"not terminal, host has none": {record: &integrations.JobRecord{State: "claimed", Host: "local"}, reason: "job record state claimed", node: true},
		} {
			t.Run(source+"/"+name, func(t *testing.T) {
				runner := legacyPane(source)
				s, g, r := panewireSetup(t, runner)
				s.Config.Review.Panewire.Lane = "review-desk"
				runner.sink.missing = tc.record
				runner.sink.jobErr = tc.jobErr
				if tc.noJob {
					s.Sink = &noJobSink{files: runner.sink}
				}
				e := s.Review(context.Background(), r, 7, "", Options{})
				if !errors.Is(e, ErrReviewUnavailable) || !strings.Contains(e.Error(), "no findings: job record "+tc.reason) {
					t.Fatalf("err=%v, want the %q skip", e, tc.reason)
				}
				noLegacyRead(t, s, r, runner, g)
				if g.postCount != 0 || g.refs[doneRef(7, shaA)] || len(auditRows(t, s, r, "review-result-lost")) != 0 {
					t.Fatalf("posts=%d lost rows=%v", g.postCount, auditRows(t, s, r, "review-result-lost"))
				}
				if reads := len(runner.nodeReads); (reads == 1) != tc.node || reads > 1 {
					t.Fatalf("node reads=%v", runner.nodeReads)
				}
			})
		}
	}
}

// A record that answers without findings but names a host is answered from
// that host: the reviewer's node result is posted, and the job, still open in
// hk, is closed as completed.
func TestRecordOutcomeReadsTheNamedHost(t *testing.T) {
	const node = `{"found":true,"findings":{"findings":[{"path":"tool","line":1,"body":"A node bug","severity":"high"}]}}`
	for _, host := range []string{"local", "node-a"} {
		t.Run(host, func(t *testing.T) {
			runner := &panewireRunner{family: "openai"}
			s, g, r := panewireSetup(t, runner)
			var hub *fakeReapHub
			if host == "local" {
				runner.nodeResult = node
			} else {
				hub = standHub(t, s)
				hub.mu.Lock()
				hub.jobResult = json.RawMessage(node)
				hub.mu.Unlock()
			}
			runner.sink.missing = &integrations.JobRecord{State: "claimed", Host: host, OwnerLane: "owner"}
			if e := s.Review(context.Background(), r, 7, "", Options{}); e != nil {
				t.Fatal(e)
			}
			label := arg(runner.spawns[0], "-l")
			if len(g.postedFindings) != 1 || g.postedFindings[0].Body != "A node bug" || !g.refs[doneRef(7, shaA)] {
				t.Fatalf("posted=%+v", g.postedFindings)
			}
			if host == "local" {
				if len(runner.nodeReads) != 1 || runner.nodeReads[0] != label {
					t.Fatalf("local node reads=%v", runner.nodeReads)
				}
			} else if calls := hub.ops("job.result"); len(calls) != 1 || calls[0]["job"] != label {
				t.Fatalf("remote node reads=%v", calls)
			}
			collectedClosedOnce(t, runner, label)
			if rows := auditRows(t, s, r, "reviewer-remote-result"); len(rows) != 1 || rows[0]["outcome"] != "found" {
				t.Fatalf("node audit=%v", rows)
			}
		})
	}
}

// cancelOnNodeRead cancels the review while the reviewer's node is read for
// its result.
type cancelOnNodeRead struct {
	*panewireRunner
	cancel context.CancelFunc
}

func (r cancelOnNodeRead) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "node" && args[1] == "job-result" {
		r.cancel()
		return nil, ctx.Err()
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

// A review cancelled while the node of a record without a result is read
// returns the cancellation: the reviewer record stays for the next run, and
// nothing is lost, replaced, noticed or posted.
func TestNodeReadCancelledKeepsTheReviewer(t *testing.T) {
	for name, state := range map[string]string{"completed without a result": "completed", "not terminal": "claimed"} {
		t.Run(name, func(t *testing.T) {
			runner := &panewireRunner{family: "openai"}
			s, g, r := panewireSetup(t, runner)
			s.Config.Notifier.Lane = "desk"
			runner.sink.missing = &integrations.JobRecord{State: state, Host: "local", OwnerLane: "owner"}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.Runner = cancelOnNodeRead{runner, cancel}
			e := s.Review(ctx, r, 7, "", Options{})
			if !errors.Is(e, context.Canceled) {
				t.Fatalf("err=%v, want the cancellation", e)
			}
			if got := readReviewerJob(t, s, r, 1); got.Label != arg(runner.spawns[0], "-l") {
				t.Fatalf("record=%+v", got)
			}
			if g.postCount != 0 || len(runner.spawns) != 1 || len(auditRows(t, s, r, "review-result-lost")) != 0 || s.Notifier.(*fakeNotifier).count != 0 {
				t.Fatalf("posts=%d spawns=%d notices=%+v", g.postCount, len(runner.spawns), s.Notifier.(*fakeNotifier).events)
			}
		})
	}
}

// The same legacy-only pane keeps the two failure classes apart: with its
// record completed without a result the result is lost, a collection failure
// that backs off without counting; with its record still claimed the review
// is missing, a counted failure that escalates.
func TestLegacyIgnoredRetryClasses(t *testing.T) {
	for state, lostClass := range map[string]bool{"completed": true, "claimed": false} {
		t.Run(state, func(t *testing.T) {
			runner := legacyPane("file")
			s, g, r := panewireSetup(t, runner)
			s.Config.Notifier.Lane = "desk"
			c := &clock{t: time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)}
			s.Now = c.now
			s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "8m", Failures: 1}
			runner.sink.missing = &integrations.JobRecord{State: state}
			expireLease(g)
			if err := s.RetryReview(context.Background(), r, g.pull, Options{}); err == nil {
				t.Fatal("want a review failure")
			}
			noLegacyRead(t, s, r, runner, g)
			st := retryStateOf(t, s, r)
			lost, failed := resultLostNotices(s)
			lostRows := len(auditRows(t, s, r, "review-result-lost"))
			if lostClass {
				if st.Failures != 0 || st.Escalated || st.Backoffs != 1 || lost != 1 || failed != 0 || lostRows != 1 {
					t.Fatalf("state=%+v lost=%d failed=%d rows=%d", st, lost, failed, lostRows)
				}
				return
			}
			if st.Failures != 1 || !st.Escalated || lost != 0 || failed != 1 || lostRows != 0 {
				t.Fatalf("state=%+v lost=%d failed=%d rows=%d", st, lost, failed, lostRows)
			}
		})
	}
}

// cancelOnReviewerRead cancels the review when the resumed reviewer's hk
// record is read, and that read returns the cancellation.
type cancelOnReviewerRead struct {
	*taskJobSink
	label  string
	cancel context.CancelFunc
}

func (s cancelOnReviewerRead) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if id == s.label {
		s.cancel()
		return integrations.JobRecord{}, context.Canceled
	}
	return s.taskJobSink.Job(ctx, id)
}

// cancelOnHubJobResult puts a proxy in front of s's reap hub that cancels the
// review when a reviewer node is asked for job.result: that read never
// answers, and fails with the cancellation. Every other hub call goes through.
func cancelOnHubJobResult(t *testing.T, s *Service, cancel context.CancelFunc) {
	t.Helper()
	target, err := url.Parse(s.Config.Reap.Hub.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte(`"job.result"`)) {
			cancel()
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	s.Config.Reap.Hub.URL = server.URL
}

// cancellable gives f's next evaluations a context that cancel ends.
func cancellable(f *unreadableFixture) context.CancelFunc {
	ctx, cancel := context.WithCancel(f.ctx)
	f.ctx = ctx
	return cancel
}

// keptAfterCancel checks that a resumed evaluation cut short by its context
// returned the cancellation and decided nothing about the reviewer: its record
// stays, nothing is lost, noticed, closed, replaced or posted.
func keptAfterCancel(t *testing.T, f *unreadableFixture, err error, notices int) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want the cancellation", err)
	}
	if got := readReviewerJob(t, f.s, f.r, 1); got.Label != f.old.Label || got.Replacements != 0 {
		t.Fatalf("record=%+v, want %s kept", got, f.old.Label)
	}
	n := f.s.Notifier.(*fakeNotifier)
	if lost, _ := resultLostNotices(f.s); lost != 0 || n.count != notices {
		t.Fatalf("lost notices=%d notices=%d, want %d: %+v", lost, n.count, notices, n.events)
	}
	if rows := auditRows(t, f.s, f.r, "review-result-lost"); len(rows) != 0 {
		t.Fatalf("lost rows=%v", rows)
	}
	if len(f.runner.spawns) != 1 || len(f.runner.closes) != 0 || f.g.postCount != 0 {
		t.Fatalf("spawns=%d closes=%v posts=%d", len(f.runner.spawns), f.runner.closes, f.g.postCount)
	}
}

// A resumed review whose read of the reviewer's hk record is cancelled,
// within the review budget or past it, returns the cancellation: no unreadable
// hk is assumed, no wait is scheduled in its place, and the reviewer is not
// lost or replaced.
func TestResumedHkReadCancelledKeepsTheReviewer(t *testing.T) {
	for name, past := range map[string]bool{"within budget": false, "over budget": true} {
		t.Run(name, func(t *testing.T) {
			f := unreadableReviewer(t, remoteFound)
			f.runner.sink.jobErr = nil
			cancel := cancellable(f)
			defer cancel()
			f.s.Sink = cancelOnReviewerRead{f.runner.sink, f.old.Label, cancel}
			notices := f.s.Notifier.(*fakeNotifier).count
			now := f.old.Recorded.Add(time.Minute)
			if past {
				now = f.timer
			}
			err := f.again(t, now)
			keptAfterCancel(t, f, err, notices)
			if calls := f.hub.ops("job.result"); len(calls) != 0 {
				t.Fatalf("node read after the cancellation: %v", calls)
			}
		})
	}
}

// A resumed review cancelled while the reviewer's node is read for its result
// returns the cancellation, whether the node was read for a finished pane
// whose record is still claimed or for a record hk cannot read: the read is
// no missing result, so the reviewer is neither waited for in its place,
// nudged, lost nor replaced.
func TestResumedNodeReadCancelledKeepsTheReviewer(t *testing.T) {
	for name, tc := range map[string]struct {
		hkUnreadable, past bool
	}{
		"pane finished within budget": {},
		"pane finished over budget":   {past: true},
		"hk unreadable within budget": {hkUnreadable: true},
		"hk unreadable over budget":   {hkUnreadable: true, past: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := unreadableReviewer(t, remoteFound)
			if !tc.hkUnreadable {
				f.runner.sink.jobErr = nil
			}
			cancel := cancellable(f)
			defer cancel()
			cancelOnHubJobResult(t, f.s, cancel)
			notices := f.s.Notifier.(*fakeNotifier).count
			now := f.old.Recorded.Add(time.Minute)
			if tc.past {
				now = f.timer
			}
			err := f.again(t, now)
			keptAfterCancel(t, f, err, notices)
			if events := nudgeEvents(f.s.Notifier.(*fakeNotifier)); len(events) != 0 || len(auditRows(t, f.s, f.r, "reviewer-nudge")) != 0 {
				t.Fatalf("nudged after the cancellation: %+v", events)
			}
		})
	}
}
