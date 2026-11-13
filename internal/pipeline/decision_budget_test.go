package pipeline

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

const budgetTestBase = "0123456789abcdef0123456789abcdef01234567"

// worstCase builds the input of hk 4560's worst case: five verified keys
// (256-byte keys, 5 KiB documents, 35-byte timestamps), an Operator decisions
// section and an Acceptance section each over 4 KiB, and many outside-diff,
// prior and answered findings.
func worstCase(t *testing.T, rp roundPlan) (string, github.ReviewFeedback, verifiedDecisions, []outsideFinding) {
	t.Helper()
	var body strings.Builder
	body.WriteString("## Operator decisions\n")
	verified := verifiedDecisions{}
	for i := range 5 {
		key := fmt.Sprintf("decision/%0247d", i)
		fmt.Fprintf(&body, "- decision %d hk:doc %s\n", i, key)
		verified[key] = integrations.DecisionDocument{
			Key:       key,
			UpdatedAt: "2026-11-10T00:00:00.000000000+00:00",
			// Words with spaces survive fleetevent.Redact.
			Body: strings.Repeat("the decision stands\n", 256),
		}
	}
	body.WriteString("- " + strings.Repeat("the unverified claim holds ", 170) + "\n")

	var fb github.ReviewFeedback
	for i := range 20 {
		fb.Threads = append(fb.Threads, thread(fmt.Sprintf("p%02d.go", i), i+1, false,
			say("review-bot", "prior finding "+strings.Repeat("x ", 150))))
		fb.Threads = append(fb.Threads, thread(fmt.Sprintf("a%02d.go", i), i+1, false,
			say("review-bot", strings.Repeat("finding ", 60)), say("pr-author", strings.Repeat("answer ", 60))))
	}
	var outside []outsideFinding
	for i := range 30 {
		outside = append(outside, outsideFinding{
			Ref: fmt.Sprintf("o%d", i), Path: fmt.Sprintf("out%02d.go", i), Line: i + 1,
			Severity: "high", Body: strings.Repeat("outside body ", 40), State: outsideOpen,
		})
	}
	return body.String(), fb, verified, outside
}

// (a) The worst case stays within contextBudget, and a findings section still
// shows an entry.
func TestWorstCaseContextFitsBudget(t *testing.T) {
	plans := map[string]roundPlan{
		"delta with budget note": {
			round: 2, base: budgetTestBase, diff: "d", quiet: true, budget: 3,
			contract:       clipContract(strings.Repeat("the contract must hold ", 200)),
			contractSource: "PR description",
		},
		"fallback note with budget note": {
			round: 2, note: strings.Repeat("the interdiff failed ", 60), quiet: true, budget: 3,
			contract:       clipContract(strings.Repeat("the contract must hold ", 200)),
			contractSource: "PR description",
		},
	}
	for name, rp := range plans {
		t.Run(name, func(t *testing.T) {
			body, fb, verified, outside := worstCase(t, rp)
			rc := buildRoundContext(body, fb, rp, "review-bot", verified, outside...)
			if len(rc.text) > contextBudget {
				t.Fatalf("context is %d bytes, over budget %d", len(rc.text), contextBudget)
			}
			if rc.prior+len(rc.outside) < 1 {
				t.Fatalf("no findings section entry shown: prior=%d outside=%d", rc.prior, len(rc.outside))
			}
			if !rc.truncated {
				t.Fatal("worst case did not report truncation")
			}
			for key := range verified {
				if !strings.Contains(rc.text, key+" (updated_at: ") {
					t.Fatalf("verified key %s has no label", key)
				}
			}
		})
	}
}

// (b) A budget-cut excerpt carries the cut label, ends on a UTF-8 boundary and
// never exceeds 600 bytes. An excerpt with no room carries the omitted line.
func TestBudgetCutExcerptIsLabelledAndOnABoundary(t *testing.T) {
	rp := roundPlan{
		round: 2, base: budgetTestBase, quiet: true, budget: 3,
		contract:       clipContract(strings.Repeat("the contract must hold ", 200)),
		contractSource: "PR description",
	}
	body, fb, verified, outside := worstCase(t, rp)
	for key, doc := range verified {
		doc.Body = strings.Repeat("€", 2000)
		verified[key] = doc
	}
	rc := buildRoundContext(body, fb, rp, "review-bot", verified, outside...)
	if len(rc.text) > contextBudget {
		t.Fatalf("context is %d bytes, over budget %d", len(rc.text), contextBudget)
	}
	const cutLabel = "cut at the context size bound):\n"
	cuts := strings.Count(rc.text, cutLabel)
	if cuts == 0 {
		t.Fatal("no budget-cut excerpt label in the worst case")
	}
	rest := rc.text
	for range cuts {
		at := strings.Index(rest, cutLabel)
		var shown, total int
		head := rest[:at+len(cutLabel)]
		start := strings.LastIndex(head, "document excerpt (")
		if _, err := fmt.Sscanf(head[start:], "document excerpt (%d of %d bytes, cut", &shown, &total); err != nil {
			t.Fatalf("label %q: %v", head[start:], err)
		}
		bodyStart := at + len(cutLabel)
		text := rest[bodyStart : bodyStart+shown]
		if shown > decisionExcerptLimit {
			t.Fatalf("budget-cut excerpt shows %d bytes, over %d", shown, decisionExcerptLimit)
		}
		if !utf8.ValidString(text) || shown%3 != 0 || rest[bodyStart+shown] != '\n' {
			t.Fatalf("excerpt cut mid-character at %d bytes: %q", shown, text)
		}
		rest = rest[bodyStart+shown:]
	}
	if !rc.truncated {
		t.Fatal("budget-cut excerpt did not set truncated")
	}
}

// (c) Five verified keys in round one with no findings and small sections:
// every excerpt is still the first 600 bytes of its document.
func TestRoundOneExcerptsKeepSixHundredBytes(t *testing.T) {
	var body strings.Builder
	body.WriteString("## Operator decisions\n")
	verified := verifiedDecisions{}
	for i := range 5 {
		key := fmt.Sprintf("decision/key%d", i)
		fmt.Fprintf(&body, "- decision %d hk:doc %s\n", i, key)
		verified[key] = integrations.DecisionDocument{
			Key: key, UpdatedAt: "2026-11-10T00:00:00Z",
			Body: strings.Repeat("the decision stands\n", 256),
		}
	}
	rc := buildRoundContext(body.String(), github.ReviewFeedback{}, roundPlan{}, "", verified)
	if len(rc.text) > contextBudget {
		t.Fatalf("context is %d bytes, over budget %d", len(rc.text), contextBudget)
	}
	if rc.truncated {
		t.Fatal("small round-one context was reported as truncated")
	}
	const excerpt = "document excerpt (600 of 5120 bytes):\n" + "the decision stands\n" // 30 repeats = 600 bytes
	for key := range verified {
		want := excerpt + strings.Repeat("the decision stands\n", 29) + "\n"
		if !strings.Contains(rc.text, key+" (updated_at: 2026-11-10T00:00:00Z):") ||
			!strings.Contains(rc.text, want) {
			t.Fatalf("key %s excerpt is not 600 bytes:\n%s", key, rc.text)
		}
	}
}
