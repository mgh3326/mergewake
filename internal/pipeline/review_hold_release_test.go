package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// holdAtRound7 drives a 7-round PR with one open high finding into the round
// hold and returns the service, GH fake and repo once the gate is blocked.
func holdAtRound7(t *testing.T) (*Service, *roundRuleGH, config.Repo) {
	t.Helper()
	s, g, _, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	if !s.isRoundHoldActive(context.Background(), r, g.pull) {
		t.Fatalf("round 7 without a pass must hold the PR: %+v", v.Reasons)
	}
	return s, g, r
}

func gateAfterComments(t *testing.T, s *Service, g *roundRuleGH, r config.Repo) *gateVerdict {
	t.Helper()
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB)
	if err != nil {
		t.Fatalf("mergeGate err: %v", err)
	}
	return &v
}

func held(v *gateVerdict) bool {
	for _, reason := range v.Reasons {
		if reason == "round_hold" {
			return true
		}
	}
	return false
}

// (a) The review login is also the operator's account: a release comment from
// it that mergewake did not generate (no `<!-- mergewake` marker) releases.
func TestRoundHoldRelease_ReviewLoginWithoutMarkerReleases(t *testing.T) {
	s, g, r := holdAtRound7(t)
	g.issueComments = []github.IssueComment{
		{User: github.PullUser{Login: "review-bot", Type: "User"}, Body: "Mergewake-Round-Hold: release"},
	}
	if v := gateAfterComments(t, s, g, r); held(v) {
		t.Fatalf("release comment from the review login did not release the hold: %v", v.Reasons)
	}
	st := s.loadRoundRuleState(r.Name, 7)
	if !st.released() || st.HoldReleasedBy != "review-bot" {
		t.Fatalf("release not recorded with releaser: %+v", st)
	}
}

// (b) The round-hold decision text quotes the release phrase; mergewake marks
// it, so a comment carrying that text never releases the hold it describes.
func TestRoundHoldRelease_MergewakeDecisionCommentNeverReleases(t *testing.T) {
	s, g, r := holdAtRound7(t)
	text := round5DecisionText(r, g.pull, 7, []github.ReviewThread{ownFinding("T1", "high", 10)})
	if !strings.Contains(text, "<!-- mergewake") {
		t.Fatalf("round-hold decision text carries no mergewake marker: %q", text)
	}
	g.issueComments = []github.IssueComment{
		{User: github.PullUser{Login: "review-bot", Type: "User"}, Body: text},
	}
	if v := gateAfterComments(t, s, g, r); !held(v) {
		t.Fatalf("mergewake's own decision comment released the hold: %v", v.Reasons)
	}
}

// (c) A bot's release-looking comment never releases the hold.
func TestRoundHoldRelease_BotCommentNeverReleases(t *testing.T) {
	s, g, r := holdAtRound7(t)
	g.issueComments = []github.IssueComment{
		{User: github.PullUser{Login: "some-bot[bot]", Type: "Bot"}, Body: "Mergewake-Round-Hold: release"},
		{User: github.PullUser{Login: "review-bot", Type: "User"}, Body: "<!-- mergewake-review:abc -->\nMergewake-Round-Hold: release"},
	}
	if v := gateAfterComments(t, s, g, r); !held(v) {
		t.Fatalf("bot or mergewake-marked comment released the hold: %v", v.Reasons)
	}
}

// (d) A release comment older than the hold's baseline does not count.
func TestRoundHoldRelease_StaleCommentDoesNotCount(t *testing.T) {
	s, g, _, _, _, r := roundRuleSetup(t, 7)
	g.feedback.Threads = []github.ReviewThread{
		ownFinding("T1", "high", 10),
	}
	g.issueComments = []github.IssueComment{
		{User: github.PullUser{Login: "operator", Type: "User"}, Body: "Mergewake-Round-Hold: release"},
	}
	p := policy.Policy{ReviewGate: &policy.ReviewGate{Land: "graded"}}
	if v, err := s.mergeGate(context.Background(), r, g.pull, p, shaB); err != nil || !held(&v) {
		t.Fatalf("pre-hold release comment released the hold: v=%+v err=%v", v, err)
	}
	if v := gateAfterComments(t, s, g, r); !held(v) {
		t.Fatalf("stale release comment counted after the baseline: %v", v.Reasons)
	}
}

// (e) The audit line for a release names who released it.
func TestRoundHoldRelease_AuditNamesReleaser(t *testing.T) {
	s, g, r := holdAtRound7(t)
	var out bytes.Buffer
	s.Out = &out
	g.issueComments = []github.IssueComment{
		{User: github.PullUser{Login: "review-bot", Type: "User"}, Body: "Mergewake-Round-Hold: release"},
	}
	if v := gateAfterComments(t, s, g, r); held(v) {
		t.Fatalf("release comment did not release the hold: %v", v.Reasons)
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Command != "round-hold-release" {
			continue
		}
		found = true
		if rec.Value["by"] != "review-bot" || rec.Value["pr"] != float64(7) {
			t.Fatalf("round-hold-release audit does not name the releaser: %s", line)
		}
	}
	if !found {
		t.Fatalf("no round-hold-release audit line in output: %q", out.String())
	}
}
