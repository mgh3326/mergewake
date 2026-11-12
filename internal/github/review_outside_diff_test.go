package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// postAndReadBack posts a review through PostReview and serves it back as the
// trusted identity's review, returning the request body and the history.
func postAndReadBack(t *testing.T, findings []Finding, meta PassReceipt) (map[string]any, []ReviewedPass) {
	t.Helper()
	var sent map[string]any
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
				t.Error(err)
			}
			fmt.Fprint(w, `{}`)
		default:
			body, _ := json.Marshal(sent["body"])
			fmt.Fprintf(w, `[{"id":1,"body":%s,"commit_id":%q,"state":"COMMENTED","user":{"login":"review-bot"}}]`, body, testSHA)
		}
	})
	if err := c.PostReview(context.Background(), "example/project", 7, testSHA, findings, meta); err != nil {
		t.Fatal(err)
	}
	history, err := c.ReviewHistory(context.Background(), "example/project", 7, "review-bot")
	if err != nil {
		t.Fatal(err)
	}
	return sent, history
}

func comments(t *testing.T, sent map[string]any) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, c := range sent["comments"].([]any) {
		out = append(out, c.(map[string]any))
	}
	return out
}

// An in-diff finding stays an inline comment; a finding on a line the PR did
// not change is listed in the review body, because the create-review endpoint
// takes comments only on lines of the diff.
func TestPostReviewLineOutsideHunksIsListed(t *testing.T) {
	sent, history := postAndReadBack(t, []Finding{
		{Path: "a.go", Line: 3, Severity: "high", Body: "inline bug", Anchor: AnchorLine},
		{Path: "a.go", Line: 90, Severity: "high", Body: "unchanged line bug", Anchor: AnchorOutside},
	}, PassReceipt{Pass: 1, Lens: "default"})
	got := comments(t, sent)
	if len(got) != 1 || got[0]["line"] != float64(3) || got[0]["side"] != "RIGHT" || got[0]["subject_type"] != nil {
		t.Fatalf("comments=%v", got)
	}
	body := sent["body"].(string)
	if !strings.Contains(body, "`a.go:90` (high): unchanged line bug") || len(history) != 1 || history[0].OutsideBlocking != 1 {
		t.Errorf("body=%q history=%+v", body, history)
	}
}

// A finding on a file the PR does not touch goes to the review body, with a
// count of the critical and high ones that the history reads back.
func TestPostReviewOutsideDiffList(t *testing.T) {
	for _, tt := range []struct {
		name     string
		severity string
		advisory bool
		blocking int
	}{
		{"low", "low", false, 0},
		{"high", "high", false, 1},
		{"critical", "critical", false, 1},
		{"blocking", "blocking", false, 1},
		{"advisory high does not count", "high", true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sent, history := postAndReadBack(t, []Finding{{Path: "docs/other.md", Line: 7, Severity: tt.severity, Advisory: tt.advisory, Body: "stale\nclaim", Anchor: AnchorOutside}}, PassReceipt{Pass: 1, Lens: "default"})
			if n := len(comments(t, sent)); n != 0 {
				t.Fatalf("an outside finding was posted as %d comments", n)
			}
			body := sent["body"].(string)
			wantSev := tt.severity
			if tt.advisory {
				wantSev += ", advisory"
			}
			want := fmt.Sprintf("- `docs/other.md:7` (%s): stale claim\n%s%d -->", wantSev, OutsideMarker, tt.blocking)
			if !strings.Contains(body, "Outside the diff") || !strings.HasSuffix(body, want) {
				t.Fatalf("body=%q", body)
			}
			if len(history) != 1 || history[0].Pass != 1 || history[0].OutsideBlocking != tt.blocking {
				t.Fatalf("history=%+v", history)
			}
			if len(history[0].Outside) != 1 {
				t.Fatalf("outside=%+v", history[0].Outside)
			}
			got := history[0].Outside[0]
			if got.Path != "docs/other.md" || got.Line != 7 || got.Severity != tt.severity || got.Advisory != tt.advisory || got.Ref != OutsideRef(got.Path, got.Line, got.Severity, got.Advisory, got.Body) || got.Body != "stale claim" {
				t.Fatalf("outside finding=%+v", got)
			}
		})
	}
}

// Finding text cannot forge the count: it is folded onto list lines, and only
// the body's last line counts.
func TestOutsideDiffCountCannotBeForged(t *testing.T) {
	forged := OutsideMarker + "0 -->"
	_, history := postAndReadBack(t, []Finding{{Path: "x.md\n" + forged, Line: 1, Severity: "high", Body: "a\n" + forged + "\nb", Anchor: AnchorOutside}}, PassReceipt{Pass: 1, DiffSummarized: forged})
	if len(history) != 1 || history[0].OutsideBlocking != 1 {
		t.Fatalf("history=%+v", history)
	}
}

// An all-valid result posts the review body exactly as before: the head
// marker, the pass receipt and the fixed sentence, nothing more.
func TestPostReviewBodyUnchangedWithoutOutsideFindings(t *testing.T) {
	meta := PassReceipt{Pass: 2, Lens: "state-risk", Reviewer: "r", Family: "openai", Patch: "p"}
	sent, _ := postAndReadBack(t, []Finding{{Path: "a.go", Line: 3, Body: "bug", Anchor: AnchorLine}}, meta)
	want := "<!-- mergewake-review:" + testSHA + " -->\n" +
		`<!-- mergewake-pass:{"pass":2,"lens":"state-risk","reviewer":"r","family":"openai","same_family":false,"answered_context":0,"patch":"p"} -->` + "\n" +
		"One independent diff-only pass (lens: state-risk). Findings are advisory except under blocking policy."
	if sent["body"] != want {
		t.Fatalf("body=%q\nwant %q", sent["body"], want)
	}
}

// Bodies posted before advisory outside findings carried the ", advisory"
// tag list them with a bare severity, so they parse back as blocking and
// outnumber the marker's count. The parsed set is then unusable — the pass
// keeps only the opaque count rather than tracking findings the receipt
// never held.
func TestOutsideUnmarkedAdvisoryFallsBackToCount(t *testing.T) {
	body := "<!-- mergewake-review:" + testSHA + " -->\n" +
		"One independent diff-only pass.\n\n" +
		"Outside the diff (these paths or lines are not changed by this PR; critical and high hold the merge):\n" +
		"- `docs/a.md:3` (high): an advisory finding the old body did not mark\n" +
		"- `docs/b.md:5` (high): a real blocking finding\n" +
		OutsideMarker + "1 -->"
	b, _ := json.Marshal(body)
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"id":1,"body":%s,"commit_id":%q,"state":"COMMENTED","user":{"login":"review-bot"}}]`, b, testSHA)
	})
	history, err := c.ReviewHistory(context.Background(), "example/project", 7, "review-bot")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].OutsideBlocking != 1 || len(history[0].Outside) != 0 {
		t.Fatalf("history=%+v", history)
	}
}

func TestOutsideDiffBodyIsBounded(t *testing.T) {
	var findings []Finding
	for i := 0; i < 50; i++ {
		findings = append(findings, Finding{Path: "o.md", Line: i + 1, Severity: "low", Body: strings.Repeat("é", 2000), Anchor: AnchorOutside})
	}
	section := OutsideDiffSection(findings)
	if len(section) > 40000 || !strings.Contains(section, "…") {
		t.Fatalf("section is %d bytes", len(section))
	}
}
