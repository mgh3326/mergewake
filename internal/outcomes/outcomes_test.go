package outcomes

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeReps struct {
	fail   bool
	calls  int
	values []Outcome
}

func (f *fakeReps) Publish(_ context.Context, v Outcome) (string, error) {
	f.calls++
	if f.fail {
		return "", errors.New("offline")
	}
	f.values = append(f.values, v)
	return "srv:1", nil
}
func (f *fakeReps) Read(context.Context) ([]Outcome, error) { return f.values, nil }
func boolp(v bool) *bool                                    { return &v }
func complete(v *Outcome) {
	v.Head = "head"
	v.Profile = "example"
	v.Model = "model"
	v.Effort = "high"
	v.Grade = "A"
	v.Tier = "T1"
	v.Findings = map[string]int{}
	v.Final = true
	v.Decision = "landed"
	v.FirstCIGreen = boolp(true)
	v.Reverted = boolp(false)
	v.Reviews = map[string]map[string]int{"head": {}}
}
func TestOutboxRetryAndFollowup(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	reps := &fakeReps{fail: true}
	if ledger.Flush(context.Background(), reps, "") == nil {
		t.Fatal("lost failed publish")
	}
	reps.fail = false
	if err := ledger.Flush(context.Background(), reps, ""); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Flush(context.Background(), reps, ""); err != nil {
		t.Fatal(err)
	}
	if reps.calls != 2 || len(reps.values) != 1 {
		t.Fatal(reps.calls)
	}
	if err := ledger.Update("example/project", 1, func(v *Outcome) { v.Reverted = boolp(true) }); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Flush(context.Background(), reps, ""); err != nil {
		t.Fatal(err)
	}
	if len(reps.values) != 2 || reps.values[0].Evidence() != "clean_pass" || reps.values[1].Evidence() != "failure" {
		t.Fatal(reps.values)
	}
	report := Report(reps.values, time.Time{})
	if len(report) != 1 || report[0].Seen != 1 || report[0].Failure != 1 || report[0].CleanPass != 0 {
		t.Fatal(report)
	}
	data, _ := os.ReadFile(filepath.Join(ledger.Dir, "outcomes.jsonl"))
	if len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 4 {
		t.Fatal("not append only")
	}
	info, _ := os.Stat(filepath.Join(ledger.Dir, "outcomes.jsonl"))
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
}
func TestFlushScopesToRepo(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Update("other/repo", 2, complete); err != nil {
		t.Fatal(err)
	}
	reps := &fakeReps{fail: true}
	err := ledger.Flush(context.Background(), reps, "example/project")
	if err == nil || !strings.Contains(err.Error(), "example/project") || strings.Contains(err.Error(), "other/repo") {
		t.Fatalf("flush error leaked another repo: %v", err)
	}
	if reps.calls != 1 {
		t.Fatalf("publish calls=%d", reps.calls)
	}
	reps.fail = false
	if err = ledger.Flush(context.Background(), reps, "other/repo"); err != nil {
		t.Fatal(err)
	}
	if len(reps.values) != 1 || reps.values[0].Repo != "other/repo" {
		t.Fatal(reps.values)
	}
}
func TestConservativeEvidence(t *testing.T) {
	var base Outcome
	complete(&base)
	for _, tc := range []struct {
		name   string
		change func(*Outcome)
		want   string
	}{
		{"all three", func(*Outcome) {}, "clean_pass"},
		{"window incomplete", func(v *Outcome) { v.Reverted = nil }, "pending"},
		{"no CI", func(v *Outcome) { v.FirstCIGreen = nil }, "pending"},
		{"no review", func(v *Outcome) { v.Reviews = nil }, "pending"},
		{"new head unreviewed", func(v *Outcome) { v.Head = "new-head" }, "pending"},
		{"critical", func(v *Outcome) { v.Findings = map[string]int{"critical": 1}; v.Reverted = nil }, "failure"},
		{"high", func(v *Outcome) { v.Findings = map[string]int{"high": 1} }, "failure"},
		{"revert", func(v *Outcome) { v.Reverted = boolp(true) }, "failure"},
		{"initial red", func(v *Outcome) { v.FirstCIGreen = boolp(false) }, "with_fixes"},
		{"fast merge is insufficient", func(v *Outcome) { n := 1.0; v.TimeToMerge = &n; v.Reverted = nil }, "pending"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := base
			tc.change(&v)
			if v.Evidence() != tc.want {
				t.Fatal(v.Evidence())
			}
		})
	}
}
func TestConcurrentUpdatesAndNoDuplicateObservation(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if err := ledger.Update("example/project", n, complete); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	state, err := ledger.Read()
	if err != nil || len(state.Latest) != 16 {
		t.Fatal(state, err)
	}
	if err = ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	state, _ = ledger.Read()
	if state.Latest[Key("example/project", 1)].Revision != 1 {
		t.Fatal("duplicate observation appended")
	}
}

type cliRunner struct {
	calls    int
	listed   string
	args     []string
	argsSeen [][]string
	retryOut []byte
	retryErr error
}

func (r *cliRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.calls++
	r.args = args
	r.argsSeen = append(r.argsSeen, slices.Clone(args))
	if args[1] == "list" {
		return []byte(r.listed), nil
	}
	if args[1] == "pending" {
		return r.retryOut, r.retryErr
	}
	return []byte("recorded rep id=srv:42\n"), nil
}
func TestRepMappingAndSuccessBeforeAck(t *testing.T) {
	r := &cliRunner{}
	cli := CLI{Runner: r, Command: "scopefuel"}
	var v Outcome
	complete(&v)
	v.Task = Key("example/project", 1)
	v.ID = "stable"
	v.Revision = 3
	v.Findings = map[string]int{"critical": 2, "high": 3, "minor": 7}
	if _, err := cli.Publish(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(r.args, " ")
	for _, want := range []string{"--role impl", "--rounds 1", "--completed 1", "--blockers-found 5", `"evidence":"failure"`} {
		if !strings.Contains(args, want) {
			t.Fatal(args)
		}
	}
	notes := r.args[len(r.args)-1]
	r.listed = "id=srv:42 notes=" + notes
	r.calls = 0
	if _, err := cli.Publish(context.Background(), v); err != nil || r.calls != 1 {
		t.Fatal(err, r.calls)
	}
}

func TestLedgerFixedClockIsReproducible(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var ids []string
	for range 2 {
		ledger := Ledger{Dir: t.TempDir(), Now: func() time.Time { return at }}
		if err := ledger.Update("example/project", 1, complete); err != nil {
			t.Fatal(err)
		}
		state, err := ledger.Read()
		if err != nil {
			t.Fatal(err)
		}
		v := state.Latest[Key("example/project", 1)]
		if !v.UpdatedAt.Equal(at) {
			t.Fatalf("updated_at %v, want %v", v.UpdatedAt, at)
		}
		ids = append(ids, v.ID)
	}
	if ids[0] == "" || ids[0] != ids[1] {
		t.Fatalf("same input and clock gave different outcome IDs: %v", ids)
	}
}

type recordRunner struct{ adds [][]string }

func (r *recordRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	if args[1] == "list" {
		return nil, nil
	}
	r.adds = append(r.adds, args)
	return []byte("recorded rep id=srv:7\n"), nil
}

func inExperiment(v *Outcome) {
	complete(v)
	v.Experiment, v.Arm = "x1", "downshift"
}

func notesOf(t *testing.T, args []string) (top, nested map[string]any) {
	t.Helper()
	if err := json.Unmarshal([]byte(args[len(args)-1]), &top); err != nil {
		t.Fatal(err)
	}
	nested, _ = top["outcome"].(map[string]any)
	if nested == nil {
		t.Fatal("notes carry no outcome object")
	}
	return top, nested
}

func TestReconciliationMetadataDoesNotReviseOrPublish(t *testing.T) {
	at := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	ledger := Ledger{Dir: t.TempDir(), Now: func() time.Time { return at }}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	due := at.Add(time.Hour)
	if err := ledger.Update("example/project", 1, func(v *Outcome) {
		v.ReconcileFinished = true
		v.ReconcileDueAt = &due
	}); err != nil {
		t.Fatal(err)
	}
	state, err := ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	first := state.Latest[Key("example/project", 1)]
	if first.Revision != 1 || !first.UpdatedAt.Equal(at) || first.ReconcileDueAt == nil || !first.ReconcileDueAt.Equal(due) {
		t.Errorf("schedule update changed outcome revision or was not persisted: rev=%d updated=%s due=%v", first.Revision, first.UpdatedAt, first.ReconcileDueAt)
	}
	r := &recordRunner{}
	cli := CLI{Runner: r, Command: "panewire"}
	if err := ledger.Flush(context.Background(), cli, ""); err != nil {
		t.Fatal(err)
	}
	if len(r.adds) != 1 {
		t.Fatalf("initial publish calls=%d, want 1", len(r.adds))
	}
	_, nested := notesOf(t, r.adds[0])
	if _, ok := nested["reconcile_due_at"]; ok {
		t.Error("published outcome contains reconciliation due time")
	}
	if _, ok := nested["reconcile_finished"]; ok {
		t.Error("published outcome contains reconciliation completion marker")
	}
	due = due.Add(time.Hour)
	if err := ledger.Update("example/project", 1, func(v *Outcome) {
		v.ReconcileFinished = false
		v.ReconcileDueAt = &due
	}); err != nil {
		t.Fatal(err)
	}
	state, err = ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	second := state.Latest[Key("example/project", 1)]
	if second.ID != first.ID || second.Revision != first.Revision || second.ReconcileDueAt == nil || !second.ReconcileDueAt.Equal(due) {
		t.Errorf("next schedule update changed published identity: first=%+v second=%+v", first, second)
	}
	if err := ledger.Flush(context.Background(), cli, ""); err != nil {
		t.Fatal(err)
	}
	if len(r.adds) != 1 {
		t.Errorf("schedule-only update created %d rep publications, want 0 additional", len(r.adds)-1)
	}
}

func TestLedgerRevisionTracksInPlaceMapChanges(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, complete); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Update("example/project", 1, func(v *Outcome) {
		v.ReviewBlocked = map[string]string{"head": "blocked"}
	}); err != nil {
		t.Fatal(err)
	}
	state, err := ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	before := state.Latest[Key("example/project", 1)]
	if err := ledger.Update("example/project", 1, func(v *Outcome) {
		v.ReviewBlocked["head"] = "clear"
	}); err != nil {
		t.Fatal(err)
	}
	state, err = ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	after := state.Latest[Key("example/project", 1)]
	if after.ReviewBlocked["head"] != "clear" || after.ID == before.ID || after.Revision != before.Revision+1 {
		t.Fatalf("in-place map update did not create a revision: before=%+v after=%+v", before, after)
	}
}

func TestPublishWithoutEffortSendsNoExperiment(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, func(v *Outcome) { inExperiment(v); v.Effort = "" }); err != nil {
		t.Fatal(err)
	}
	r := &recordRunner{}
	if err := ledger.Flush(context.Background(), CLI{Runner: r, Command: "panewire"}, ""); err != nil {
		t.Fatal(err)
	}
	if len(r.adds) != 1 {
		t.Fatalf("reps add calls=%d", len(r.adds))
	}
	if slices.Contains(r.adds[0], "--effort") {
		t.Fatalf("effort-less row passed --effort: %q", r.adds[0])
	}
	top, nested := notesOf(t, r.adds[0])
	if top["experiment_id"] != "" || top["arm"] != "" || nested["experiment_id"] != "" || nested["arm"] != "" {
		t.Fatalf("experiment leaked: top=%v/%v nested=%v/%v", top["experiment_id"], top["arm"], nested["experiment_id"], nested["arm"])
	}
	s, err := ledger.Read()
	if err != nil {
		t.Fatal(err)
	}
	row := s.Latest[Key("example/project", 1)]
	if s.Published[row.ID] == "" {
		t.Fatal("no published event")
	}
	if row.Experiment != "x1" || row.Arm != "downshift" {
		t.Fatalf("ledger row changed: %q/%q", row.Experiment, row.Arm)
	}
}

func TestPublishWithEffortKeepsExperiment(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, inExperiment); err != nil {
		t.Fatal(err)
	}
	r := &recordRunner{}
	if err := ledger.Flush(context.Background(), CLI{Runner: r, Command: "panewire"}, ""); err != nil {
		t.Fatal(err)
	}
	if len(r.adds) != 1 {
		t.Fatalf("reps add calls=%d", len(r.adds))
	}
	if !strings.Contains(strings.Join(r.adds[0], " "), "--model model --effort high --task ") {
		t.Fatalf("args: %q", r.adds[0])
	}
	top, nested := notesOf(t, r.adds[0])
	if top["experiment_id"] != "x1" || top["arm"] != "downshift" || nested["experiment_id"] != "x1" || nested["arm"] != "downshift" {
		t.Fatalf("notes lost experiment: %v", top)
	}
}

func TestPublishRefusesMissingModel(t *testing.T) {
	ledger := Ledger{Dir: t.TempDir()}
	if err := ledger.Update("example/project", 1, func(v *Outcome) { inExperiment(v); v.Model = "" }); err != nil {
		t.Fatal(err)
	}
	r := &recordRunner{}
	if err := ledger.Flush(context.Background(), CLI{Runner: r, Command: "panewire"}, ""); err != nil {
		t.Fatal(err) // Flush skips an unattributed row without error
	}
	if len(r.adds) != 0 {
		t.Fatalf("published unattributed row: %q", r.adds)
	}
	var v Outcome
	inExperiment(&v)
	v.Model = ""
	v.ID = "m"
	if _, err := (CLI{Runner: r, Command: "panewire"}).Publish(context.Background(), v); err == nil || !strings.Contains(err.Error(), "outcome attribution incomplete") {
		t.Fatalf("publish err=%v", err)
	}
	s, _ := ledger.Read()
	if len(s.Published) != 0 {
		t.Fatalf("published=%v", s.Published)
	}
}

func TestReportGroupsEffortlessRowAsInferredOutsideArms(t *testing.T) {
	at := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	var noEffort, withEffort Outcome
	inExperiment(&noEffort)
	noEffort.Effort, noEffort.Task, noEffort.UpdatedAt = "", "t1", at
	inExperiment(&withEffort)
	withEffort.Task, withEffort.UpdatedAt = "t2", at
	got := map[string]Aggregate{}
	for _, g := range Report([]Outcome{noEffort, withEffort}, time.Time{}) {
		got[g.ProfileEffort] = g
	}
	if len(got) != 2 {
		t.Fatalf("groups=%v", got)
	}
	if g := got["example@inferred"]; g.Seen != 1 || g.Experiment != "" || g.Arm != "" || g.AttributionMissing != 0 {
		t.Fatalf("inferred group=%+v", g)
	}
	if g := got["example@high"]; g.Seen != 1 || g.Experiment != "x1" || g.Arm != "downshift" || g.AttributionMissing != 0 {
		t.Fatalf("high group=%+v", g)
	}
}

// A row written before reviewers_by_head and grade_unknown existed decodes and
// reports as it did.
func TestReportOldRowWithoutReviewers(t *testing.T) {
	old := `{"outcome_id":"x","revision":1,"task_ref":"t1","profile":"example","model":"m","effort":"high","grade":"A","tier":"T1","finalized":true,"decision":"landed","reviews_by_head":{"h":{}},"head":"h","updated_at":"2026-01-02T00:00:00Z"}`
	var v Outcome
	if err := json.Unmarshal([]byte(old), &v); err != nil {
		t.Fatal(err)
	}
	if v.ReviewersByHead != nil || !v.Attributed() {
		t.Fatalf("%+v", v)
	}
	b, _ := json.Marshal(v)
	if strings.Contains(string(b), "reviewers_by_head") {
		t.Fatal(string(b))
	}
	got := Report([]Outcome{v}, time.Time{})
	if len(got) != 1 || got[0].GradeUnknown != 0 || got[0].AttributionMissing != 0 || got[0].Seen != 1 {
		t.Fatalf("%+v", got)
	}
}

func TestPublishCarriesReviewersInOutcome(t *testing.T) {
	r := &cliRunner{}
	var v Outcome
	complete(&v)
	v.Task, v.ID = Key("example/project", 1), "stable"
	v.SetReviewer("head", "1", "panewire:seat-a")
	v.SetReviewer("head", "2", "claude")
	if _, err := (CLI{Runner: r, Command: "scopefuel"}).Publish(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	var notes struct {
		Outcome Outcome `json:"outcome"`
	}
	if err := json.Unmarshal([]byte(r.args[len(r.args)-1]), &notes); err != nil {
		t.Fatal(err)
	}
	if got := notes.Outcome.ReviewersByHead["head"]; got["1"] != "panewire:seat-a" || got["2"] != "claude" {
		t.Fatalf("%v", notes.Outcome.ReviewersByHead)
	}
}

func TestReportCountsUnknownGrade(t *testing.T) {
	var known, unknown Outcome
	complete(&known)
	complete(&unknown)
	known.Task, unknown.Task, unknown.Grade = "t1", "t2", ""
	got := Report([]Outcome{known, unknown}, time.Time{})
	if len(got) != 1 || got[0].GradeUnknown != 1 || got[0].AttributionMissing != 1 || got[0].Seen != 2 {
		t.Fatalf("%+v", got)
	}
}
