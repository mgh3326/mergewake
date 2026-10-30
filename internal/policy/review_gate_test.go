package policy

import "testing"

func TestReviewGateDisposition(t *testing.T) {
	no := false
	for _, tt := range []struct {
		name, class            string
		f                      ReviewFinding
		gate                   ReviewGate
		block, triage, ignored int
	}{
		{name: "answered critical holds", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "critical", Answered: true}, block: 1},
		{name: "author resolved critical holds", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "critical", Resolved: true}, block: 1},
		{name: "reviewing bot resolved critical clears", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "critical", Resolved: true, ReviewerResolved: true}, ignored: 1},
		{name: "ignore bot resolved false holds", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "critical", Resolved: true, ReviewerResolved: true}, gate: ReviewGate{IgnoreBotResolved: &no}, block: 1},
		{name: "routine P1 holds", class: "routine", f: ReviewFinding{Author: "chatgpt-codex-connector", Severity: "p1"}, block: 1},
		{name: "outdated critical holds without recheck", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "critical", Outdated: true}, block: 1},
		{name: "outdated critical rechecked clears", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "critical", Outdated: true, Rechecked: true}, ignored: 1},
		{name: "outdated P1 holds without recheck", class: "routine", f: ReviewFinding{Author: "chatgpt-codex-connector", Severity: "p1", Outdated: true}, block: 1},
		{name: "outdated P2 needs reply", class: "routine", f: ReviewFinding{Author: "chatgpt-codex-connector", Severity: "p2", Outdated: true}, triage: 1},
		{name: "outdated routine major clears", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "major", Outdated: true}, ignored: 1},
		{name: "outdated blocking major clears", class: "blocking", f: ReviewFinding{Author: "coderabbitai", Severity: "major", Outdated: true}, ignored: 1},
		{name: "unknown severity holds blocking", class: "blocking", f: ReviewFinding{Author: "coderabbitai", Severity: "surprise"}, triage: 1},
		{name: "missing severity holds blocking", class: "blocking", f: ReviewFinding{Author: "coderabbitai"}, triage: 1},
		{name: "ghost author holds blocking", class: "blocking", f: ReviewFinding{Severity: "critical"}, triage: 1},
		{name: "known minor ignored", class: "blocking", f: ReviewFinding{Author: "coderabbitai", Severity: "minor"}, ignored: 1},
		{name: "answered triage clears", class: "routine", f: ReviewFinding{Author: "chatgpt-codex-connector", Severity: "p2", Answered: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.gate.Evaluate([]ReviewFinding{tt.f}, tt.class)
			if len(v.Blocking) != tt.block || len(v.Triage) != tt.triage || v.Ignored != tt.ignored {
				t.Fatalf("%+v", v)
			}
		})
	}
}

func TestReviewGateNoCrossBotAnswer(t *testing.T) {
	fs := []ReviewFinding{
		{Author: "coderabbitai", Severity: "major", Path: "tool", Line: 1, Answered: true},
		{Author: "chatgpt-codex-connector", Severity: "p1", Path: "tool", Line: 1},
		{Author: "chatgpt-codex-connector", Severity: "p2", Path: "tool", Line: 1},
	}
	v := (ReviewGate{}).Evaluate(fs, "routine")
	if len(v.Blocking) != 1 || len(v.Triage) != 1 {
		t.Fatalf("%+v", v)
	}
}

func TestRequiredBotsDefaultAndValidation(t *testing.T) {
	if missing := (ReviewGate{}).WithDefaults().MissingRequiredBots(nil, nil, "head"); len(missing) != 0 {
		t.Fatal(missing)
	}
	for _, tt := range []struct {
		name    string
		authors []string
		fail    bool
	}{
		{"empty", []string{}, false}, {"configured", []string{"coderabbitai"}, false},
		{"blank", []string{""}, true}, {"duplicate normalized", []string{"coderabbitai", "coderabbitai[bot]"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := Policy{Version: 1, RequiredChecks: []string{"CI"}, ReviewGate: &ReviewGate{RequiredBots: tt.authors}}
			if err := p.Validate(); (err != nil) != tt.fail {
				t.Fatal(err)
			}
		})
	}
}

func TestReviewGateGradeOwnLogin(t *testing.T) {
	g := (ReviewGate{}).WithOwnLogin("Review-Bot[bot]")
	for _, tt := range []struct {
		name, severity, class, want string
		own                         bool
	}{
		{"critical blocks", "critical", "routine", GradeBlock, true},
		{"high blocks", "high", "routine", GradeBlock, true},
		{"blocking alias blocks", "blocking", "routine", GradeBlock, true},
		{"medium triages", "medium", "routine", GradeTriage, true},
		{"major alias triages", "major", "routine", GradeTriage, true},
		{"low is advisory", "low", "routine", GradeAdvisory, true},
		{"minor alias is advisory", "minor", "blocking", GradeAdvisory, true},
		{"missing severity needs an answer", "", "routine", GradeTriage, true},
		{"unknown severity needs an answer", "surprise", "routine", GradeTriage, true},
		{"other author without rule is advisory", "high", "routine", GradeAdvisory, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			author := "review-bot"
			if !tt.own {
				author = "someone"
			}
			if got := g.Grade(ReviewFinding{Author: author, Severity: tt.severity, OwnReview: tt.own}, tt.class); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestReviewGateOwnLoginDefaultsYieldToPolicy(t *testing.T) {
	g := ReviewGate{Block: []ReviewGateRule{{Author: "review-bot", Severity: []string{"critical"}}}}.WithOwnLogin("review-bot")
	if got := g.Grade(ReviewFinding{Author: "review-bot", Severity: "high", OwnReview: true}, "routine"); got != GradeAdvisory {
		t.Fatalf("explicit rules for the login must win over defaults, got %s", got)
	}
	if got := (ReviewGate{}).Grade(ReviewFinding{Author: "coderabbitai", Severity: "critical"}, "routine"); got != GradeBlock {
		t.Fatalf("default third-party rule lost: %s", got)
	}
	if got := (ReviewGate{IgnoreAuthors: []string{"review-bot"}}).WithOwnLogin("review-bot").Grade(ReviewFinding{Author: "review-bot", Severity: "high"}, "routine"); got != GradeIgnored {
		t.Fatalf("ignored author graded %s", got)
	}
}

func TestFastLane(t *testing.T) {
	p := Policy{Version: 1, RequiredChecks: []string{"CI"}}
	if p.FastLaneEligible("routine", 1) {
		t.Fatal("absent fast_lane must never apply")
	}
	p.FastLane = &FastLane{MaxChangedLines: 50}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, tt := range []struct {
		class string
		lines int
		want  bool
	}{{"routine", 50, true}, {"routine", 51, false}, {"blocking", 1, false}, {"routine", 0, true}} {
		if got := p.FastLaneEligible(tt.class, tt.lines); got != tt.want {
			t.Fatalf("%+v: %v", tt, got)
		}
	}
	for _, n := range []int{0, -1} {
		p.FastLane = &FastLane{MaxChangedLines: n}
		if p.Validate() == nil {
			t.Fatalf("max_changed_lines %d accepted", n)
		}
	}
}
