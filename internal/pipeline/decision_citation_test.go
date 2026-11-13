package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// fakeDecisionReader answers from a fixed set of documents and records every
// key it was asked for.
type fakeDecisionReader struct {
	docs  map[string]integrations.DecisionDocument
	asked []string
}

func (f *fakeDecisionReader) DecisionDocument(_ context.Context, key string) (integrations.DecisionDocument, error) {
	f.asked = append(f.asked, key)
	doc, ok := f.docs[key]
	if !ok {
		return integrations.DecisionDocument{}, fmt.Errorf("no document %s", key)
	}
	return doc, nil
}

// A citation at the end of a sentence is verified under its key: the trailing
// period is sentence punctuation, not part of the key.
func TestDecisionCitationSentenceFinalVerifies(t *testing.T) {
	reader := &fakeDecisionReader{docs: map[string]integrations.DecisionDocument{
		"decision/one": {Key: "decision/one", UpdatedAt: "2026-11-10T00:00:00Z", Body: "ship it"},
	}}
	body := "## Operator decisions\n- ship it per hk:doc decision/one.\n"
	verified := resolveDecisions(context.Background(), body, reader)
	if got := strings.Join(reader.asked, ","); got != "decision/one" {
		t.Fatalf("lookups = %q, want decision/one", got)
	}
	rc := buildRoundContext(body, github.ReviewFeedback{}, roundPlan{}, "", verified)
	if want := "decision/one (updated_at: 2026-11-10T00:00:00Z):\n- ship it per hk:doc decision/one."; !strings.Contains(rc.text, want) {
		t.Errorf("missing verified entry %q in %s", want, rc.text)
	}
	if i := strings.Index(rc.text, "CLAIMED DECISIONS"); i >= 0 && strings.Contains(rc.text[i:], "- ship it per hk:doc decision/one.") {
		t.Errorf("sentence-final citation stayed a claim: %s", rc.text)
	}
}

// The key class stops at a trailing run of sentence punctuation, while an inner
// period stays part of the key.
func TestDecisionCitationKeyStopsAtTrailingPunctuation(t *testing.T) {
	for _, tc := range []struct{ line, key string }{
		{"per hk:doc decision/foo.", "decision/foo"},
		{"hk:doc decision/2026-11-11/model-routing.", "decision/2026-11-11/model-routing"},
		{"hk:doc decision/foo...", "decision/foo"},
		{"hk:doc decision/foo/.", "decision/foo"},
		{"hk:doc decision/foo/", "decision/foo"},
		{"hk:doc decision/foo/./", "decision/foo"},
		{"hk:doc decision/v1.2", "decision/v1.2"},
		{"hk:doc decision/a.b.", "decision/a.b"},
		{"hk:doc\tdecision/foo.", "decision/foo"},
	} {
		matches := decisionCitation.FindAllStringSubmatch(tc.line, -1)
		if len(matches) != 1 || matches[0][1] != tc.key {
			t.Errorf("%q captured %v, want one match %q", tc.line, matches, tc.key)
		}
	}
}
