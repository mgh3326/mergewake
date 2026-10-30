package outcomes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	calls  int
	listed string
	args   []string
}

func (r *cliRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.calls++
	r.args = args
	if args[1] == "list" {
		return []byte(r.listed), nil
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
