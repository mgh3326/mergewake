package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// hkDoc builds a DecisionDocument the way the hk response decode does, so the
// fixtures compile against a DecisionDocument that does not yet carry Body.
func hkDoc(key, body string) integrations.DecisionDocument {
	raw, err := json.Marshal(map[string]string{
		"key":        key,
		"updated_at": "2026-11-10T00:00:00Z",
		"body":       body,
	})
	if err != nil {
		panic(err)
	}
	var doc integrations.DecisionDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		panic(err)
	}
	return doc
}

// (a) A verified key shows the PR's line, then the document excerpt line and
// the document text itself.
func TestDecisionExcerptShowsVerifiedDocumentText(t *testing.T) {
	body := "## Operator decisions\n- ship the queue hk:doc decision/queue-v2\n"
	verified := verifiedDecisions{
		"decision/queue-v2": hkDoc("decision/queue-v2", "use the durable queue\n"),
	}
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{}, "", verified)
	line := "- ship the queue hk:doc decision/queue-v2"
	label := "document excerpt (22 of 22 bytes):"
	text := "use the durable queue"
	for _, want := range []string{line, "decision/queue-v2 (updated_at: 2026-11-10T00:00:00Z):", label, text} {
		if !strings.Contains(rc.text, want) {
			t.Fatalf("missing %q in:\n%s", want, rc.text)
		}
	}
	at, excerptAt, docAt := strings.Index(rc.text, line), strings.Index(rc.text, label), strings.Index(rc.text, text)
	if !(at >= 0 && at < excerptAt && excerptAt < docAt) {
		t.Fatalf("excerpt out of order in:\n%s", rc.text)
	}
}

func TestDecisionExcerptEmptyDocument(t *testing.T) {
	body := "## Operator decisions\n- ship hk:doc decision/empty\n"
	verified := verifiedDecisions{
		"decision/empty": hkDoc("decision/empty", ""),
	}
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{}, "", verified)
	if !strings.Contains(rc.text, "document excerpt: (empty)") {
		t.Fatalf("missing empty marker in:\n%s", rc.text)
	}
}

// (b) The excerpt is the document's start, cut on a UTF-8 boundary to at most
// 600 bytes, labelled with the shown and total byte counts.
func TestDecisionExcerptIsCutOnABoundary(t *testing.T) {
	ascii := strings.Repeat("a", 5120)
	rc := buildRoundContext("## Operator decisions\n- big hk:doc decision/big\n", github.ReviewFeedback{}, roundPlan{}, "",
		verifiedDecisions{"decision/big": hkDoc("decision/big", ascii)})
	if !strings.Contains(rc.text, "document excerpt (600 of 5120 bytes):\n"+strings.Repeat("a", 600)) {
		t.Fatalf("missing bounded excerpt in:\n%s", rc.text)
	}
	if strings.Contains(rc.text, strings.Repeat("a", 601)) {
		t.Fatal("excerpt exceeded 600 bytes")
	}

	// 599 ASCII bytes then a 3-byte euro sign: a 600-byte cut would split the
	// rune, so the excerpt backs up to 599 bytes and shows no partial rune.
	mixed := strings.Repeat("a", 599) + strings.Repeat("€", 2000)
	rc = buildRoundContext("## Operator decisions\n- mixed hk:doc decision/mixed\n", github.ReviewFeedback{}, roundPlan{}, "",
		verifiedDecisions{"decision/mixed": hkDoc("decision/mixed", mixed)})
	if !strings.Contains(rc.text, "document excerpt (599 of 6599 bytes):\n"+strings.Repeat("a", 599)) {
		t.Fatalf("missing boundary-cut excerpt in:\n%s", rc.text)
	}
	if strings.Contains(rc.text, "€") {
		t.Fatal("partial rune leaked into the excerpt")
	}
}

// (c) Credential-shaped text in the document is redacted before it reaches
// the prompt.
func TestDecisionExcerptRedactsDocumentSecrets(t *testing.T) {
	body := "## Operator decisions\n- rotate hk:doc decision/secrets\n"
	docBody := "set api_key=sk-abcdef1234567890 and password=hunter2secretvalue before launch\n"
	verified := verifiedDecisions{
		"decision/secrets": hkDoc("decision/secrets", docBody),
	}
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{}, "", verified)
	if !strings.Contains(rc.text, "[redacted]") {
		t.Fatalf("no redaction in:\n%s", rc.text)
	}
	for _, bad := range []string{"sk-abcdef1234567890", "hunter2secretvalue"} {
		if strings.Contains(rc.text, bad) {
			t.Fatalf("secret %q leaked into:\n%s", bad, rc.text)
		}
	}
}

// (d) Five verified keys with long documents plus many answered findings:
// the excerpts are written before the findings, which fill what is left of
// contextBudget, so the whole context stays within it.
func TestDecisionExcerptsLeaveRoomWithinContextBudget(t *testing.T) {
	var body strings.Builder
	body.WriteString("## Operator decisions\n")
	verified := verifiedDecisions{}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("decision/key%d", i)
		fmt.Fprintf(&body, "- decision %d hk:doc %s\n", i, key)
		// Words with spaces survive fleetevent.Redact; a single repeated
		// letter would collapse to "[redacted]" as a long base64 blob.
		verified[key] = hkDoc(key, strings.Repeat("the decision stands\n", 200))
	}
	var fb github.ReviewFeedback
	for i := 0; i < 60; i++ {
		fb.Threads = append(fb.Threads, thread(fmt.Sprintf("f%02d.go", i), i+1, false,
			say("review-bot", strings.Repeat("finding ", 100)), say("pr-author", strings.Repeat("answer ", 100))))
	}
	rc := buildRoundContext(body.String(), fb, roundPlan{}, "", verified)
	if len(rc.text) > contextBudget {
		t.Fatalf("context is %d bytes, over budget %d", len(rc.text), contextBudget)
	}
	if !rc.truncated || rc.answered == 0 || rc.answered >= 60 {
		t.Fatal(rc.answered, rc.truncated)
	}
}

// (f) A claimed line with an unavailable hk renders exactly the CLAIMED block:
// no VERIFIED section and no excerpt marker.
func TestClaimedLineAndUnavailableHkRenderNoExcerpt(t *testing.T) {
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	body := "## Operator decisions\n- plain claim hk:doc decision/one\n"
	verified := resolveDecisions(context.Background(), body, integrations.Handoffkeep{})
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{}, "", verified)
	for _, bad := range []string{"VERIFIED OPERATOR DECISIONS", "document excerpt"} {
		if strings.Contains(rc.text, bad) {
			t.Fatalf("unexpected %q in:\n%s", bad, rc.text)
		}
	}
	if !strings.Contains(rc.text, "CLAIMED DECISIONS") || !strings.Contains(rc.text, "- plain claim hk:doc decision/one") {
		t.Fatalf("claim missing in:\n%s", rc.text)
	}
}
