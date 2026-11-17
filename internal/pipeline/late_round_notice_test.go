package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestLateRoundFindingsNoticeThresholdCountsAndDeduplication(t *testing.T) {
	s, g, _, n, r := reviewSetup(t)
	r.Lane = "design-lane"
	added := true
	disputes := true
	facts := []integrations.ReviewFindingRecord{
		{Path: "internal/a.go", Severity: "high", AddedAfterFirst: &added, DisputesEarlierReply: &disputes},
		{Path: "docs/guide.md", Severity: "high", AddedAfterFirst: &added},
		{Path: "internal/b.go", Severity: "high", AddedAfterFirst: &added},
	}

	if err := s.notifyLateRoundFindings(context.Background(), r, g.pull, 3, facts); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 {
		t.Fatalf("round 3 sent a notice: %+v", n.events)
	}
	if err := s.notifyLateRoundFindings(context.Background(), r, g.pull, 4, []integrations.ReviewFindingRecord{{Path: "a.go", Severity: "medium", AddedAfterFirst: &added}}); err != nil {
		t.Fatal(err)
	}
	if n.count != 0 {
		t.Fatalf("round with no new critical/high findings sent a notice: %+v", n.events)
	}
	if err := s.auditRoundFindingFacts(r.Name, g.pull.Number, g.pull.Head.SHA, 1, 4, facts[:2]); err != nil {
		t.Fatal(err)
	}
	if err := s.auditRoundFindingFacts(r.Name, g.pull.Number, g.pull.Head.SHA, 2, 4, facts[2:]); err != nil {
		t.Fatal(err)
	}
	posted := map[int]github.PassReceipt{1: {Pass: 1, Round: 4}, 2: {Pass: 2, Round: 4}}
	s.notifyCompletedLateRound(context.Background(), r, g.pull, 4, posted, nil)
	if n.count != 1 {
		t.Fatalf("notice count = %d, want one: %+v", n.count, n.events)
	}
	event := n.events[0]
	if event.Lane != "design-lane" || event.Type != "late-round-findings" {
		t.Fatalf("notice route/type = %+v", event)
	}
	for _, want := range []string{"round=4", "new critical/high=3", "lines added after first head=3", "earlier author reply=1", "documents=1"} {
		if !strings.Contains(event.Text, want) {
			t.Errorf("notice %q does not contain %q", event.Text, want)
		}
	}
	s.notifyCompletedLateRound(context.Background(), r, g.pull, 4, posted, nil)
	if n.count != 1 {
		t.Fatalf("re-evaluating the same PR round resent notice: %+v", n.events)
	}
}
