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
