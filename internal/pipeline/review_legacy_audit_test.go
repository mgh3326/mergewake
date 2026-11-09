package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
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

// failFallbackOut fails every Out line that carries the review-inbox-fallback
// audit and takes the rest.
type failFallbackOut struct{}

func (failFallbackOut) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(`"command":"review-inbox-fallback"`)) {
		return 0, errors.New("stdout closed for ops@example.com")
	}
	return len(p), nil
}

// fallbackAppendFails takes every Out line, and while the review-inbox-fallback
// audit is written it turns decisions.jsonl into a directory: that record
// reaches Out and fails its append, and the next audit's Out line puts the
// file back before its own append, so only that one record is lost.
type fallbackAppendFails struct {
	t      *testing.T
	path   string
	broken bool
}

func (f *fallbackAppendFails) Write(p []byte) (int, error) {
	keep := f.path + ".keep"
	if f.broken {
		if err := os.Remove(f.path); err != nil {
			f.t.Error(err)
		}
		if err := os.Rename(keep, f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			f.t.Error(err)
		}
		f.broken = false
	}
	if bytes.Contains(p, []byte(`"command":"review-inbox-fallback"`)) {
		if err := os.Rename(f.path, keep); err != nil && !errors.Is(err, os.ErrNotExist) {
			f.t.Error(err)
		}
		if err := os.MkdirAll(f.path, 0o700); err != nil {
			f.t.Error(err)
		}
		f.broken = true
	}
	return len(p), nil
}

// legacyOutcome is what a legacy use does to the PR and its receipt.
type legacyOutcome struct {
	posted   []github.Finding
	fallback string
}

// captureAuditLost points fallbackAuditLog at a buffer for the test and
// returns a reader of its review-inbox-fallback-audit-lost lines.
func captureAuditLost(t *testing.T) func() []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	prev := fallbackAuditLog
	fallbackAuditLog = slog.New(slog.NewJSONHandler(&buf, nil))
	t.Cleanup(func() { fallbackAuditLog = prev })
	return func() []map[string]any {
		var lines []map[string]any
		sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
		for sc.Scan() {
			var line map[string]any
			if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
				t.Fatalf("log line %q: %v", sc.Text(), err)
			}
			if line["msg"] == "review-inbox-fallback-audit-lost" {
				lines = append(lines, line)
			}
		}
		return lines
	}
}

// A review-inbox-fallback audit that cannot be written, on Out or in
// decisions.jsonl, changes nothing about the review: the legacy findings are
// posted and noted exactly as when the audit is written. The lost audit is
// logged once under a fixed marker instead, so a zero count is only trusted
// next to zero such lines (docs/config.md, review-inbox-fallback).
func TestLegacyAuditLostIsLogged(t *testing.T) {
	run := func(t *testing.T, source string, breakAudit func(*testing.T, *Service, config.Repo)) (legacyOutcome, string, []map[string]any, *Service, config.Repo) {
		t.Helper()
		lost := captureAuditLost(t)
		runner := &panewireRunner{findings: legacyBody, family: "openai"}
		if source == "lane" {
			runner.remote = &taskJobSink{}
		}
		s, g, r := panewireSetup(t, runner)
		s.Config.Review.Panewire.Lane = "review-desk"
		if breakAudit != nil {
			breakAudit(t, s, r)
		}
		if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
			t.Fatal(err)
		}
		if len(runner.spawns) != 1 {
			t.Fatalf("spawns=%d", len(runner.spawns))
		}
		out := legacyOutcome{posted: g.postedFindings}
		if got := g.passes[shaA]; len(got) == 1 {
			out.fallback = got[0].Fallback
		}
		return out, arg(runner.spawns[0], "-l"), lost(), s, r
	}
	for _, source := range legacySources {
		t.Run(source, func(t *testing.T) {
			var want legacyOutcome
			t.Run("audit written", func(t *testing.T) {
				got, label, lost, s, r := run(t, source, nil)
				if len(got.posted) != 1 || got.posted[0].Body != "A legacy bug" || got.fallback == "" {
					t.Fatalf("outcome=%+v", got)
				}
				oneLegacyAudit(t, s, r, label, "no job record", "record-outcome", source)
				if len(lost) != 0 {
					t.Fatalf("lost lines=%v, want none", lost)
				}
				want = got
			})
			for name, c := range map[string]struct {
				breakAudit func(*testing.T, *Service, config.Repo)
				cause      string
			}{
				"out fails": {
					breakAudit: func(_ *testing.T, s *Service, _ config.Repo) { s.Out = failFallbackOut{} },
					cause:      "stdout closed for [redacted]",
				},
				"decisions append fails": {
					breakAudit: func(t *testing.T, s *Service, r config.Repo) {
						s.Out = &fallbackAppendFails{t: t, path: filepath.Join(s.state(r.Name), "decisions.jsonl")}
					},
					cause: "is a directory",
				},
			} {
				t.Run(name, func(t *testing.T) {
					if want.posted == nil {
						t.Skip("no baseline outcome: the audit-written case failed")
					}
					got, label, lost, s, r := run(t, source, c.breakAudit)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("outcome=%+v, want the audit-written outcome %+v", got, want)
					}
					if rows := auditRows(t, s, r, "review-inbox-fallback"); len(rows) != 0 {
						t.Fatalf("review-inbox-fallback rows=%v: the audit was not lost", rows)
					}
					if len(lost) != 1 {
						t.Fatalf("lost lines=%v, want one", lost)
					}
					line := lost[0]
					for k, v := range map[string]any{"level": "WARN", "pr": float64(7), "pass": float64(1), "label": label, "entry": "record-outcome", "source": source} {
						if line[k] != v {
							t.Errorf("%s=%v, want %v (line %v)", k, line[k], v, line)
						}
					}
					if e, _ := line["error"].(string); !strings.Contains(e, c.cause) || strings.Contains(e, "ops@example.com") {
						t.Errorf("error=%q, want the redacted cause %q", e, c.cause)
					}
				})
			}
		})
	}
}
