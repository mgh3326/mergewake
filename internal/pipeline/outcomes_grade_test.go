package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

// titleSink serves the title of the hk task a PR names.
type titleSink struct {
	fakeSink
	title string
	err   error
}

func (s *titleSink) TaskJob(context.Context, string) (string, string, error) {
	return "", s.title, s.err
}

// repsRunner records the `reps` calls the outcome flush makes.
type repsRunner struct{ adds, notes []string }

func (r *repsRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	switch {
	case len(args) > 1 && args[0] == "reps" && args[1] == "list":
		return nil, nil
	case len(args) > 1 && args[0] == "reps" && args[1] == "add":
		r.adds = append(r.adds, strings.Join(args, " "))
		r.notes = append(r.notes, args[len(args)-1])
		return []byte("recorded rep id=srv:1\n"), nil
	}
	return nil, nil
}

const gradeTrailerBody = "Mergewake-Author-Profile: example@high\nMergewake-Author-Model: concrete-model\nMergewake-Task: 2981\n"

// observeGrade records one merged PR with the given body and task title, and
// returns the service and the row.
func observeGrade(t *testing.T, body string, sink *titleSink) (*Service, config.Repo, outcomes.Outcome, error) {
	t.Helper()
	s, g, _, _, repo := setup(t)
	merged := "2026-11-02T00:00:00Z"
	g.pull.CreatedAt = "2026-11-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = body
	s.GH = &outcomeGH{fakeGH: g, first: true}
	s.Sink = sink
	err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true})
	state, e := s.ledger().Read()
	if e != nil {
		t.Fatal(e)
	}
	return s, repo, state.Latest[outcomes.Key(repo.Name, g.pull.Number)], err
}

func TestOutcomeGradeTrailerBeatsTaskTitle(t *testing.T) {
	_, _, v, err := observeGrade(t, gradeTrailerBody+"Mergewake-Grade: C\n", &titleSink{title: "do it (grade B, T2)"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Grade != "C" || v.Tier != "T2" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
}

func TestOutcomeGradeAndTierFromTaskTitle(t *testing.T) {
	_, _, v, err := observeGrade(t, gradeTrailerBody, &titleSink{title: "do it (grade B, T2)"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Grade != "B" || v.Tier != "T2" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
}

func TestOutcomeGradeAndTierFromSSTaskTitle(t *testing.T) {
	_, _, v, err := observeGrade(t, gradeTrailerBody, &titleSink{title: "do it (grade SS, T2)"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Grade != "SS" || v.Tier != "T2" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
}

func TestOutcomeAcceptsSSTrailer(t *testing.T) {
	_, _, v, err := observeGrade(t, gradeTrailerBody+"Mergewake-Grade: SS\nMergewake-Tier: T2\n", &titleSink{})
	if err != nil {
		t.Fatal(err)
	}
	if v.Grade != "SS" || v.Tier != "T2" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
}

func TestTaskGradeMarkerScale(t *testing.T) {
	for _, grade := range []string{"C", "B", "A", "A+", "S", "S+", "SS"} {
		t.Run(grade, func(t *testing.T) {
			title := "task (grade " + grade + ", T2)"
			marker := taskMarkerMatch(title)
			if marker == nil || strings.ToUpper(marker[1]) != grade || strings.ToUpper(marker[2]) != "T2" {
				t.Fatalf("taskMarker(%q) = %v", title, marker)
			}
			scale, ok := taskScale(title)
			if !ok || scale != "T2" {
				t.Fatalf("taskScale(%q) = %q, %v", title, scale, ok)
			}
		})
	}
	for _, grade := range []string{"SSS", "S++", "D"} {
		t.Run(grade, func(t *testing.T) {
			title := "task (grade " + grade + ", T2)"
			if marker := taskMarkerMatch(title); marker != nil {
				t.Errorf("taskMarker(%q) = %v, want no match", title, marker)
			}
			if scale, ok := taskScale(title); ok {
				t.Errorf("taskScale(%q) = %q, want no match", title, scale)
			}
		})
	}
}

func TestTaskGradeMarkerIsCaseInsensitive(t *testing.T) {
	title := "task (gRaDe ss, t2)"
	marker := taskMarkerMatch(title)
	if marker == nil || strings.ToUpper(marker[1]) != "SS" || strings.ToUpper(marker[2]) != "T2" {
		t.Fatalf("taskMarker(%q) = %v", title, marker)
	}
	scale, ok := taskScale(title)
	if !ok || scale != "T2" {
		t.Fatalf("taskScale(%q) = %q, %v", title, scale, ok)
	}
}

func TestOutcomeGradeUnknownIsNotPublished(t *testing.T) {
	// A configured default must not stand in for the PR's own evidence.
	s, g, _, _, repo := setup(t)
	s.Config.Review.DefaultGrade = "A"
	s.Config.Outcomes.Tier = "T3"
	merged := "2026-11-02T00:00:00Z"
	g.pull.CreatedAt = "2026-11-01T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.Body = "Mergewake-Author-Profile: example@high\nMergewake-Author-Model: concrete-model\n"
	s.GH = &outcomeGH{fakeGH: g, first: true}
	runner := &repsRunner{}
	s.Runner = runner
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	state, err := s.ledger().Read()
	if err != nil {
		t.Fatal(err)
	}
	v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
	if v.Grade != "" || v.Tier != "" || v.Attributed() {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
	if len(runner.adds) != 0 {
		t.Fatalf("published an unknown-grade row: %v", runner.adds)
	}
	report := outcomes.Report([]outcomes.Outcome{v}, time.Time{})
	if len(report) != 1 || report[0].GradeUnknown != 1 || report[0].AttributionMissing != 1 {
		t.Fatalf("%+v", report)
	}
}

func TestOutcomeGradeTaskReadFailureLeavesGradeUnknown(t *testing.T) {
	_, _, v, err := observeGrade(t, gradeTrailerBody, &titleSink{err: errors.New("hk unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	if v.Grade != "" || v.Tier != "" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
}

// A later observation fills the grade in, and a failed read afterwards does
// not blank a grade an earlier observation recorded.
func TestOutcomeGradeFilledLaterAndKeptOnReadFailure(t *testing.T) {
	s, g, _, _, repo := setup(t)
	g.pull.Body = gradeTrailerBody
	s.GH = &outcomeGH{fakeGH: g, first: true}
	sink := &titleSink{err: errors.New("hk unavailable")}
	s.Sink = sink
	observe := func() outcomes.Outcome {
		if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
			t.Fatal(err)
		}
		state, _ := s.ledger().Read()
		return state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
	}
	if v := observe(); v.Grade != "" {
		t.Fatalf("grade=%q", v.Grade)
	}
	sink.err, sink.title = nil, "x (grade A+, T0)"
	if v := observe(); v.Grade != "A+" || v.Tier != "T0" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
	sink.err = errors.New("hk unavailable")
	if v := observe(); v.Grade != "A+" || v.Tier != "T0" {
		t.Fatalf("grade=%q tier=%q", v.Grade, v.Tier)
	}
}

func TestOutcomeRecordsReviewerPerPass(t *testing.T) {
	s, g, _, _, repo := setup(t)
	g.pull.Body = gradeTrailerBody + "Mergewake-Grade: B\nMergewake-Tier: T1\n"
	s.GH = &outcomeGH{fakeGH: g, first: true}
	s.Sink = &titleSink{}
	merged := "2026-11-02T00:00:00Z"
	g.pull.MergedAt = &merged
	g.pull.State = "closed"
	g.pull.CreatedAt = "2026-11-01T00:00:00Z"
	head := g.pull.Head.SHA
	for pass, reviewer := range map[int]string{1: "panewire:seat-a", 2: "claude"} {
		if err := s.recordReview(repo.Name, g.pull.Number, head, nil, github.PassReceipt{Pass: pass, Reviewer: reviewer}, false); err != nil {
			t.Fatal(err)
		}
	}
	runner := &repsRunner{}
	s.Runner = runner
	if err := s.ObservePR(context.Background(), repo, g.pull, Options{Shadow: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushOutcomes(context.Background(), repo.Name); err != nil {
		t.Fatal(err)
	}
	state, _ := s.ledger().Read()
	v := state.Latest[outcomes.Key(repo.Name, g.pull.Number)]
	got := v.ReviewersByHead[head]
	if len(got) != 2 || got["1"] != "panewire:seat-a" || got["2"] != "claude" {
		t.Fatalf("%v", v.ReviewersByHead)
	}
	if len(runner.notes) != 1 || !strings.Contains(runner.notes[0], `"reviewers_by_head":{"`+head+`":{"1":"panewire:seat-a","2":"claude"}}`) {
		t.Fatalf("%v", runner.notes)
	}
}
