package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// jobRecordPR is a merged PR whose body carries the task whose job record names
// the worktree: no pane and no earlier reap row names it, so only the record
// can lead reap to it.
var jobRecordPR = github.Pull{
	Number: 12, State: "closed", Merged: true,
	Body:     "Reap through the job record.\n\nMergewake-Task: 4669\n",
	Head:     github.Ref{Ref: "wk/x", SHA: headSHA},
	Base:     github.Ref{Ref: "main"},
	MergedAt: rfc(time.Now().Add(-8 * 24 * time.Hour)), MergeCommitSHA: "abc123",
}

type jobCandGH struct{ reapGH }

func (jobCandGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return []github.Pull{jobRecordPR}, nil
}

// candSink is the hk sink of the job-record source: it answers the task's bound
// job and the job record, and can fail that record read.
type candSink struct {
	jobSink
	jobErr error
}

func (s *candSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if s.jobErr != nil {
		return integrations.JobRecord{}, s.jobErr
	}
	return s.jobSink.Job(ctx, id)
}

// jobCandFixture is a host with no pane and no reap row: the job record of the
// task the merged PR carries is the only thing that can name the worktree.
func jobCandFixture(t *testing.T, rec integrations.JobRecord) (*Service, *candSink, *scriptRunner, *bytes.Buffer) {
	t.Helper()
	outs := map[string]string{}
	for k, v := range worktreeOuts("/repo") {
		outs[k] = v
	}
	outs[key("herdr", "agent", "list")] = `{"result":{"agents":[]}}`
	outs[key("herdr", "pane", "list")] = `{"result":{"panes":[]}}`
	run := &scriptRunner{outs: outs}
	sink := &candSink{jobSink: jobSink{
		jobs: map[string]string{"4669": "job-a"},
		recs: map[string]integrations.JobRecord{"job-a": rec},
	}}
	s := svc(t, &jobCandGH{}, run)
	s.Sink = sink
	out := &bytes.Buffer{}
	s.Out = out
	return s, sink, run, out
}

// reapSummary is the pass summary the service printed: the last "reap" audit
// record's value.
func reapSummary(t *testing.T, out *bytes.Buffer) map[string]any {
	t.Helper()
	var last map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("summary line %q: %v", line, err)
		}
		if rec.Command == "reap" {
			last = rec.Value
		}
	}
	if last == nil {
		t.Fatalf("no reap summary in %q", out.String())
	}
	return last
}

// summaryCount is one numeric counter of the last pass summary.
func summaryCount(t *testing.T, out *bytes.Buffer, key string) int {
	t.Helper()
	n, _ := reapSummary(t, out)[key].(float64)
	return int(n)
}

// jobReach is the summary's job-record source counters.
func jobReach(t *testing.T, summary map[string]any) (worktrees int, misses map[string]int) {
	t.Helper()
	jr, ok := summary["job_records"].(map[string]any)
	if !ok {
		t.Fatalf("summary has no job_records: %v", summary)
	}
	n, _ := jr["worktrees"].(float64)
	misses = map[string]int{}
	raw, _ := jr["misses"].(map[string]any)
	for k, v := range raw {
		f, _ := v.(float64)
		misses[k] = int(f)
	}
	return int(n), misses
}

// Acceptance 1: a merged PR with no pane and no reap row, whose task's job
// record names host local and cwd /repo.wt, is planned and then removed.
func TestReapFindsWorktreeThroughJobRecord(t *testing.T) {
	s, _, run, out := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	row := leadRow(t, s, "remove-worktree")
	if row == nil || row["result"] != "would-remove" || row["host"] != "local" || row["worktree"] != "/repo.wt" || row["cwd"] != "/repo.wt" || row["source"] != "job-record" {
		t.Fatalf("plan row: %v", reapLogRows(t, s, "example/project"))
	}
	if run.called("git\x00-C\x00/repo\x00worktree\x00remove") {
		t.Fatal("shadow removed the worktree")
	}

	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	if !run.called("git\x00-C\x00/repo\x00worktree\x00remove\x00/repo.wt") {
		t.Fatalf("worktree never removed: %q", run.calls)
	}
	if row := leadRow(t, s, "remove-worktree"); row["result"] != "removed" || row["worktree"] != "/repo.wt" {
		// The plan's own receipt now names the worktree too, so this pass may
		// credit either source; the first pass is what proves the record did.
		t.Fatalf("removal row: %v", row)
	}
	worktrees, misses := jobReach(t, reapSummary(t, out))
	if worktrees != 1 || len(misses) != 0 {
		t.Fatalf("reach = %d %v", worktrees, misses)
	}
}

// Acceptance 2: a job record without a cwd is counted as job-without-cwd and
// nothing is removed.
func TestReapCountsJobWithoutCwd(t *testing.T) {
	s, _, run, out := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local"})
	if err := s.Reap(context.Background(), testRepo, 0, Options{}); err != nil {
		t.Fatal(err)
	}
	worktrees, misses := jobReach(t, reapSummary(t, out))
	if worktrees != 0 || misses["job-without-cwd"] != 1 {
		t.Fatalf("reach = %d %v", worktrees, misses)
	}
	if run.called("git\x00-C\x00/repo\x00worktree\x00remove") || run.called("git\x00-C\x00/repo\x00branch\x00-d") {
		t.Fatalf("removed without a cwd: %q", run.calls)
	}
	if row := leadRow(t, s, "remove-worktree"); row != nil {
		t.Fatalf("remove-worktree row without a cwd: %v", row)
	}
}

// Acceptance 3: an hk read error yields no candidate and is never counted as a
// missing job; the error is reported.
func TestReapJobRecordReadErrorIsNotAMiss(t *testing.T) {
	s, sink, run, out := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	sink.jobErr = errors.New("hk jobs unreachable")
	err := s.Reap(context.Background(), testRepo, 0, Options{})
	if err == nil || !strings.Contains(err.Error(), "hk jobs unreachable") {
		t.Fatalf("read error not reported: %v", err)
	}
	worktrees, misses := jobReach(t, reapSummary(t, out))
	if worktrees != 0 || misses["job-read-error"] != 1 || misses["no-bound-job"] != 0 || misses["no-job-record"] != 0 {
		t.Fatalf("reach = %d %v", worktrees, misses)
	}
	if run.called("git\x00-C\x00/repo\x00worktree\x00remove") {
		t.Fatalf("removed on a failed read: %q", run.calls)
	}
}

// siblingPR is a second merged PR of the same task, on its own branch: the job's
// worktree is on wk/x, so this PR must not be offered it.
var siblingPR = github.Pull{
	Number: 13, State: "closed", Merged: true,
	Body:     "Sibling of the same task.\n\nMergewake-Task: 4669\n",
	Head:     github.Ref{Ref: "wk/y", SHA: "feedfeed"},
	Base:     github.Ref{Ref: "main"},
	MergedAt: rfc(time.Now().Add(-8 * 24 * time.Hour)), MergeCommitSHA: "def456",
}

// siblingGH lists jobRecordPR and its sibling, and counts the revert checks each
// PR costs.
type siblingGH struct {
	jobCandGH
	reverts map[int]int
}

func (g *siblingGH) MergedPulls(context.Context, string, string, time.Time) ([]github.Pull, error) {
	return []github.Pull{jobRecordPR, siblingPR}, nil
}

func (g *siblingGH) WatchRevert(_ context.Context, _ string, n int, _ string) (*github.Pull, error) {
	g.reverts[n]++
	return nil, nil
}

// Acceptance 4: a job's worktree is offered only to the merged PR whose head
// branch it is on; a sibling PR of the same task makes no revert check for it and
// leaves no row.
func TestReapOffersJobWorktreeOnlyToItsBranchPR(t *testing.T) {
	s, _, _, out := jobCandFixture(t, integrations.JobRecord{State: "completed", Host: "local", Cwd: "/repo.wt"})
	gh := &siblingGH{reverts: map[int]int{}}
	s.GH = gh
	if err := s.Reap(context.Background(), testRepo, 0, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if gh.reverts[13] != 0 {
		t.Fatalf("sibling PR 13 ran the revert check %d times", gh.reverts[13])
	}
	if gh.reverts[12] != 1 {
		t.Fatalf("owner PR 12 revert checks = %d, want 1", gh.reverts[12])
	}
	for _, row := range reapLogRows(t, s, "example/project") {
		if row["pr"] == float64(13) && row["worktree"] == "/repo.wt" {
			t.Fatalf("sibling PR 13 got the job worktree: %v", row)
		}
	}
	worktrees, misses := jobReach(t, reapSummary(t, out))
	if worktrees != 1 || len(misses) != 0 {
		t.Fatalf("reach = %d %v", worktrees, misses)
	}
}
