package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// The legacy findings sources are the attachment announced on the findings
// lane and findings.json in the local job directory. Every pass whose result
// is taken from one of them writes exactly one review-inbox-fallback audit,
// naming the entry point that took the bytes and the source they came from;
// a pass whose result comes from hk or the reviewer's node writes none.

const legacyBody = `{"findings":[{"path":"tool","line":1,"body":"A legacy bug","severity":"high"}]}`

var legacySources = []string{"lane", "file"}

// plantLegacy configures the findings lane and puts legacyBody in one legacy
// source of label: an attachment announced on the lane, or the job-dir file.
func plantLegacy(t *testing.T, s *Service, runner *panewireRunner, label, source string) {
	t.Helper()
	s.Config.Review.Panewire.Lane = "review-desk"
	switch source {
	case "lane":
		sum := sha256.Sum256([]byte(legacyBody))
		sha := hex.EncodeToString(sum[:])
		runner.sink.files[sha] = []byte(legacyBody)
		runner.inbox = append(runner.inbox, `{"id":9,"kind":"lane.event","event":{"event_id":"`+label+`-findings","text":"[`+label+`] findings attachment=`+sha+`"}}`)
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

// oneLegacyAudit checks the single review-inbox-fallback row of the run: the
// fields it always carried, and the entry and source of the legacy use.
func oneLegacyAudit(t *testing.T, s *Service, r config.Repo, label, record, entry, source string) {
	t.Helper()
	rows := auditRows(t, s, r, "review-inbox-fallback")
	if len(rows) != 1 {
		t.Fatalf("review-inbox-fallback rows=%v, want one", rows)
	}
	want := map[string]any{"pr": float64(7), "pass": float64(1), "label": label, "record": record, "count": float64(1), "entry": entry, "source": source}
	for k, v := range want {
		if rows[0][k] != v {
			t.Errorf("%s=%v, want %v (row %v)", k, rows[0][k], v, rows[0])
		}
	}
}

func noLegacyAudit(t *testing.T, s *Service, r config.Repo) {
	t.Helper()
	if rows := auditRows(t, s, r, "review-inbox-fallback"); len(rows) != 0 {
		t.Fatalf("review-inbox-fallback rows=%v, want none", rows)
	}
}

// postedLegacy checks the outcome a legacy use has always had: the legacy
// findings are posted once, nothing is replaced and the reviewer record goes.
func postedLegacy(t *testing.T, f *asyncFixture) {
	t.Helper()
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "A legacy bug" || len(f.runner.spawns) != 1 {
		t.Fatalf("posted=%+v spawns=%d", f.g.postedFindings, len(f.runner.spawns))
	}
	if got := len(auditRows(t, f.s, f.r, "review-result-lost")); got != 0 {
		t.Fatalf("replaced: %d lost rows", got)
	}
	reviewerJobGone(t, f.s, f.r, 1)
}

// legacyNote checks that the counted use is noted on the pass receipt.
func legacyNote(t *testing.T, g *fakeGH) {
	t.Helper()
	if got := g.passes[shaA]; len(got) != 1 || got[0].Fallback != "findings returned through the legacy lane/file path (inbox fallback 1)" {
		t.Fatalf("receipts=%+v", got)
	}
}

// onLaneRead runs hook when the pass lists the findings lane: the first
// legacy read of a resumed, finished reviewer.
type onLaneRead struct {
	*longReviewerRunner
	hook func()
}

func (r onLaneRead) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "inbox" && args[1] == "list" && r.hook != nil {
		r.hook()
	}
	return r.longReviewerRunner.Run(ctx, command, args, input, dir)
}

// finishedReviewer is a resumed reviewer whose pane is done while its hk job
// is still claimed, and whose node has no result.
func finishedReviewer(t *testing.T) (*asyncFixture, reviewerJob) {
	t.Helper()
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatal(err)
	}
	old := readReviewerJob(t, f.s, f.r, 1)
	hub := budgetPane(t, f.s, f.runner.panewireRunner, old, "done")
	hub.jobResult = json.RawMessage(remoteNotFound)
	return f, old
}

// (resumed-finished) A finished reviewer's legacy findings are read before
// its record is collected; they are the result when the record then answers
// without one, still claimed or completed without a result.
func TestLegacyAuditResumedFinished(t *testing.T) {
	for _, source := range legacySources {
		for name, then := range map[string]*integrations.JobRecord{
			"still claimed":              nil,
			"completed without a result": {State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", OwnerLane: "owner"},
		} {
			t.Run(source+"/"+name, func(t *testing.T) {
				f, old := finishedReviewer(t)
				plantLegacy(t, f.s, f.runner.panewireRunner, old.Label, source)
				record := "job record state claimed"
				if then != nil {
					record = "completed without a result"
					f.s.Runner = onLaneRead{f.runner, func() { f.runner.sink.jobs[old.Label] = *then }}
				}
				f.c.t = old.Recorded.Add(time.Minute)
				f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
				if err := f.review(t); err != nil {
					t.Fatal(err)
				}
				postedLegacy(t, f)
				oneLegacyAudit(t, f.s, f.r, old.Label, record, "resumed-finished", source)
				legacyNote(t, f.g)
			})
		}
	}
}

// (resumed-hk-unreadable) A resumed reviewer whose hk job cannot be read
// takes its legacy findings as the result, before its node is asked.
func TestLegacyAuditResumedHkUnreadable(t *testing.T) {
	for _, source := range legacySources {
		t.Run(source, func(t *testing.T) {
			f := unreadableReviewer(t, remoteNotFound)
			plantLegacy(t, f.s, f.runner.panewireRunner, f.old.Label, source)
			if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			postedLegacy(t, f.asyncFixture)
			if calls := f.hub.ops("job.result"); len(calls) != 0 {
				t.Fatalf("node asked after a legacy result: %v", calls)
			}
			oneLegacyAudit(t, f.s, f.r, f.old.Label, "unreadable in hk", "resumed-hk-unreadable", source)
			legacyNote(t, f.g)
		})
	}
}

// (completed-no-result) A completed record without a result whose node has
// none either takes the legacy findings.
func TestLegacyAuditCompletedNoResult(t *testing.T) {
	for _, source := range legacySources {
		t.Run(source, func(t *testing.T) {
			f := completedWithoutResult(t, remoteNotFound)
			plantLegacy(t, f.s, f.runner.panewireRunner, f.old.Label, source)
			if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if calls := f.hub.ops("job.result"); len(calls) != 1 {
				t.Fatalf("node reads=%v, want one before the legacy sources", calls)
			}
			postedLegacy(t, f.asyncFixture)
			oneLegacyAudit(t, f.s, f.r, f.old.Label, "completed without a result", "completed-no-result", source)
			legacyNote(t, f.g)
		})
	}
}

// (record-outcome) A fresh pass whose job record answers without findings
// (here: no record at all) takes the legacy findings.
func TestLegacyAuditRecordOutcome(t *testing.T) {
	for _, source := range legacySources {
		t.Run(source, func(t *testing.T) {
			runner := &panewireRunner{findings: legacyBody, family: "openai"}
			if source == "lane" {
				runner.remote = &taskJobSink{}
			}
			s, g, r := panewireSetup(t, runner)
			s.Config.Review.Panewire.Lane = "review-desk"
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(g.postedFindings) != 1 || g.postedFindings[0].Body != "A legacy bug" || len(runner.spawns) != 1 {
				t.Fatalf("posted=%+v spawns=%d", g.postedFindings, len(runner.spawns))
			}
			oneLegacyAudit(t, s, r, arg(runner.spawns[0], "-l"), "no job record", "record-outcome", source)
			legacyNote(t, g)
		})
	}
}

// A completed hk result wins over planted legacy findings: no legacy audit,
// whether the legacy bytes were never read (a fresh pass) or read and then
// discarded (a finished reviewer whose record completes with a result while
// the legacy sources are read).
func TestLegacyAuditNoneWhenHkResultWins(t *testing.T) {
	hk := integrations.JobRecord{State: "completed", Family: "openai", Host: "node-a", PaneID: "p1", OwnerLane: "owner",
		Result: json.RawMessage(`{"findings":[{"path":"tool","line":2,"body":"An hk bug","severity":"high"}]}`)}
	for _, source := range legacySources {
		t.Run("fresh/"+source, func(t *testing.T) {
			runner := &panewireRunner{findings: legacyBody, family: "openai"}
			if source == "lane" {
				runner.remote = &taskJobSink{}
			}
			s, g, r := panewireSetup(t, runner)
			s.Config.Review.Panewire.Lane = "review-desk"
			runner.sink.missing = &hk
			if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
				t.Fatal(err)
			}
			if len(g.postedFindings) != 1 || g.postedFindings[0].Body != "An hk bug" {
				t.Fatalf("posted=%+v", g.postedFindings)
			}
			noLegacyAudit(t, s, r)
		})
		t.Run("read then discarded/"+source, func(t *testing.T) {
			f, old := finishedReviewer(t)
			plantLegacy(t, f.s, f.runner.panewireRunner, old.Label, source)
			f.s.Runner = onLaneRead{f.runner, func() { f.runner.sink.jobs[old.Label] = hk }}
			f.c.t = old.Recorded.Add(time.Minute)
			f.g.leaseTimes[claimRef(7, shaA, 1)] = time.Now()
			if err := f.review(t); err != nil {
				t.Fatal(err)
			}
			if f.runner.looks == 0 {
				t.Fatal("the legacy sources were never read")
			}
			if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "An hk bug" || len(f.runner.spawns) != 1 {
				t.Fatalf("posted=%+v spawns=%d", f.g.postedFindings, len(f.runner.spawns))
			}
			if got := f.g.passes[shaA]; len(got) != 1 || got[0].Fallback != "" {
				t.Fatalf("receipts=%+v", got)
			}
			noLegacyAudit(t, f.s, f.r)
		})
	}
}

// A node job.result wins over planted legacy findings: the legacy sources are
// not read and no legacy audit is written.
func TestLegacyAuditNoneWhenNodeResultWins(t *testing.T) {
	f := completedWithoutResult(t, remoteFound)
	for _, source := range legacySources {
		plantLegacy(t, f.s, f.runner.panewireRunner, f.old.Label, source)
	}
	if err := f.again(t, f.old.Recorded.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(f.g.postedFindings) != 1 || f.g.postedFindings[0].Body != "A concrete bug" || f.runner.looks != 0 {
		t.Fatalf("posted=%+v lane looks=%d", f.g.postedFindings, f.runner.looks)
	}
	noLegacyAudit(t, f.s, f.r)
}
