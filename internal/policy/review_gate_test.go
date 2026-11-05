package policy

import (
	"gopkg.in/yaml.v3"
	"testing"
)

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
		{name: "Major ranks with critical: outdated routine major holds without recheck", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "major", Outdated: true}, block: 1},
		{name: "outdated blocking major holds without recheck", class: "blocking", f: ReviewFinding{Author: "coderabbitai", Severity: "major", Outdated: true}, block: 1},
		{name: "outdated major rechecked clears", class: "routine", f: ReviewFinding{Author: "coderabbitai", Severity: "major", Outdated: true, Rechecked: true}, ignored: 1},
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
		{Author: "chatgpt-codex-connector", Severity: "p2", Path: "tool", Line: 1, Answered: true},
		{Author: "chatgpt-codex-connector", Severity: "p1", Path: "tool", Line: 1},
		{Author: "coderabbitai", Severity: "major", Path: "tool", Line: 1},
	}
	v := (ReviewGate{}).Evaluate(fs, "routine")
	if len(v.Blocking) != 2 || len(v.Triage) != 0 {
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
		{"medium is advisory", "medium", "routine", GradeAdvisory, true},
		{"major alias is advisory", "major", "routine", GradeAdvisory, true},
		{"low is advisory", "low", "routine", GradeAdvisory, true},
		{"minor alias is advisory on a blocking path", "minor", "blocking", GradeAdvisory, true},
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

// Naming the login in the block list replaces its default block rule; a
// policy may still put the login's lower severities on its triage list.
func TestReviewGateOwnLoginDefaultsPerList(t *testing.T) {
	triageOnly := ReviewGate{Triage: []ReviewGateRule{{Author: "review-bot", Severity: []string{"low"}}}}.WithOwnLogin("review-bot")
	blockOnly := ReviewGate{Block: []ReviewGateRule{{Author: "review-bot", Severity: []string{"critical"}}}}.WithOwnLogin("review-bot")
	for _, tt := range []struct {
		name, severity, want string
		g                    ReviewGate
	}{
		{"triage-only policy keeps default block", "high", GradeBlock, triageOnly},
		{"triage-only policy replaces default triage", "medium", GradeAdvisory, triageOnly},
		{"triage-only policy uses its own triage rule", "low", GradeTriage, triageOnly},
		{"block-only policy leaves medium advisory", "medium", GradeAdvisory, blockOnly},
		{"block-only policy replaces default block", "high", GradeAdvisory, blockOnly},
		{"block-only policy uses its own block rule", "critical", GradeBlock, blockOnly},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.g.Grade(ReviewFinding{Author: "review-bot", Severity: tt.severity, OwnReview: true}, "routine"); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
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

func TestReviewGateOutdatedCleared(t *testing.T) {
	no := false
	for _, tt := range []struct {
		name  string
		g     ReviewGate
		f     ReviewFinding
		grade string
		want  bool
	}{
		{"block needs a recheck", ReviewGate{}, ReviewFinding{Outdated: true}, GradeBlock, false},
		{"rechecked block clears", ReviewGate{}, ReviewFinding{Outdated: true, Rechecked: true}, GradeBlock, true},
		{"own review never clears", ReviewGate{}, ReviewFinding{Outdated: true, Rechecked: true, OwnReview: true}, GradeBlock, false},
		{"current finding never clears", ReviewGate{}, ReviewFinding{Rechecked: true}, GradeBlock, false},
		{"major triage clears", ReviewGate{}, ReviewFinding{Outdated: true, Severity: "Major"}, GradeTriage, true},
		{"other triage stays", ReviewGate{}, ReviewFinding{Outdated: true, Severity: "p2"}, GradeTriage, false},
		{"ignore_outdated false keeps all", ReviewGate{IgnoreOutdated: &no}, ReviewFinding{Outdated: true, Rechecked: true}, GradeBlock, false},
		{"advisory is not cleared", ReviewGate{}, ReviewFinding{Outdated: true, Rechecked: true}, GradeAdvisory, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.g.OutdatedCleared(tt.f, tt.grade); got != tt.want {
				t.Fatalf("got %v", got)
			}
		})
	}
}

func TestReviewGateLandModeValidation(t *testing.T) {
	for mode, fail := range map[string]bool{"": false, "graded": false, "count": true, "Graded": true} {
		p := Policy{Version: 1, RequiredChecks: []string{"CI"}, ReviewGate: &ReviewGate{Land: mode}}
		if err := p.Validate(); (err != nil) != fail {
			t.Fatalf("land %q: %v", mode, err)
		}
	}
}

func TestOwnMediumTriageDefault(t *testing.T) {
	for _, tc := range []struct {
		name, class, severity string
		answered              bool
		want                  int
	}{
		{"blocking unanswered", "blocking", "medium", false, 1},
		{"blocking major unanswered", "blocking", "major", false, 1},
		{"blocking answered", "blocking", "medium", true, 0},
		{"routine unanswered", "routine", "medium", false, 0},
		{"blocking low", "blocking", "low", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := (ReviewGate{}).WithOwnLogin("review-bot")
			v := g.Evaluate([]ReviewFinding{{Author: "review-bot", Severity: tc.severity, OwnReview: true, Answered: tc.answered}}, tc.class)
			if len(v.Triage) != tc.want {
				t.Fatalf("triage = %d, want %d", len(v.Triage), tc.want)
			}
		})
	}
}

func TestOwnMediumTriageConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        int
	}{
		{"absent", "{}", 1}, {"enabled", "own_medium_triage: true", 1}, {"disabled", "own_medium_triage: false", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g ReviewGate
			if err := yaml.Unmarshal([]byte(tc.input), &g); err != nil {
				t.Fatal(err)
			}
			v := g.WithOwnLogin("review-bot").Evaluate([]ReviewFinding{{Author: "review-bot", Severity: "medium", OwnReview: true}}, "blocking")
			if len(v.Triage) != tc.want {
				t.Fatalf("triage = %d, want %d", len(v.Triage), tc.want)
			}
		})
	}
	g := (ReviewGate{Triage: []ReviewGateRule{{Author: "Review-Bot[bot]", Severity: []string{"low"}}}}).WithOwnLogin("review-bot")
	if len(g.Triage) != 1 || g.Triage[0].Severity[0] != "low" {
		t.Fatalf("triage rules: %+v", g.Triage)
	}
}
