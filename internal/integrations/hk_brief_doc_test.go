package integrations

import (
	"context"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/hktest"
)

// briefPutter is the sink capability the panewire executor reads a review
// brief's hk PUT through.
type briefPutter interface {
	PutBrief(ctx context.Context, key, job string, task *int64, body []byte) error
}

func putBrief(t *testing.T, h Handoffkeep) briefPutter {
	t.Helper()
	p, ok := any(h).(briefPutter)
	if !ok {
		t.Fatal("Handoffkeep has no PutBrief")
	}
	return p
}

// PutBrief stores the review brief over HTTP as a kind-brief document with
// its job, task and origin class job; no hk CLI subprocess runs.
func TestHandoffkeepPutBriefStoresBriefDocument(t *testing.T) {
	hk := hktest.New(t)
	r := noCLI(t)
	task := int64(42)
	if e := putBrief(t, Handoffkeep{Runner: r, Command: "handoffkeep"}).PutBrief(context.Background(), "review/mwr-7-abc1234-p1-9", "mwr-7-abc1234-p1-9", &task, []byte("# Review\n")); e != nil {
		t.Fatal(e)
	}
	reqs := hk.Requests()
	if len(reqs) != 1 || reqs[0].Route() != "PUT /v1/documents/review%2Fmwr-7-abc1234-p1-9" {
		t.Fatalf("requests %v", reqs)
	}
	d, ok := hk.Documents()["review/mwr-7-abc1234-p1-9"]
	if !ok || d.Kind != "brief" || d.Job != "mwr-7-abc1234-p1-9" || d.Body != "# Review\n" || d.TaskID == nil || *d.TaskID != 42 || d.OriginClass != "job" || d.Session != "" {
		t.Fatalf("document %+v %t", d, ok)
	}
	if len(r.calls) != 0 {
		t.Fatalf("subprocesses run: %q", r.calls)
	}
	// Without a task the document carries no task link.
	if e := putBrief(t, Handoffkeep{Runner: r}).PutBrief(context.Background(), "review/x", "x", nil, []byte("b")); e != nil {
		t.Fatal(e)
	}
	if d := hk.Documents()["review/x"]; d.TaskID != nil || d.Kind != "brief" {
		t.Fatalf("document %+v", d)
	}
}

// A refused PUT and an unconfigured credential are errors, never a silent
// success.
func TestHandoffkeepPutBriefErrors(t *testing.T) {
	hk := hktest.New(t)
	hk.Fail("PUT /v1/documents/{key}", 500, "internal")
	if e := putBrief(t, Handoffkeep{Runner: noCLI(t)}).PutBrief(context.Background(), "review/x", "x", nil, []byte("b")); e == nil {
		t.Fatal("refused PUT returned nil")
	}
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
	if e := putBrief(t, Handoffkeep{Runner: noCLI(t)}).PutBrief(context.Background(), "review/x", "x", nil, []byte("b")); e == nil {
		t.Fatal("unconfigured PUT returned nil")
	}
}

// ackCases are hk acknowledgements of a brief PUT that do not prove the
// document holds the brief: each answers 200, yet names no document, another
// key or another sha256, or cannot be read at all.
var ackCases = map[string]func(key, sha string) string{
	"empty":  func(key, sha string) string { return `{}` },
	"null":   func(key, sha string) string { return `null` },
	"no sha": func(key, sha string) string { return `{"document":{"key":"` + key + `"},"changed":true}` },
	"wrong key": func(key, sha string) string {
		return `{"document":{"key":"` + key + `-other","sha256":"` + sha + `"},"changed":true}`
	},
	"wrong sha": func(key, sha string) string {
		return `{"document":{"key":"` + key + `","sha256":"` + strings.Repeat("0", 64) + `"},"changed":true}`
	},
	"unreadable": func(key, sha string) string { return `{"document":` },
}

// A PUT counts as stored only when hk acknowledges the key sent and the
// sha256 of the brief sent; any other 200 answer is an error.
func TestHandoffkeepPutBriefRequiresMatchingAcknowledgement(t *testing.T) {
	for name, answer := range ackCases {
		t.Run(name, func(t *testing.T) {
			hk := hktest.New(t)
			hk.AnswerDocumentPut(answer)
			if e := putBrief(t, Handoffkeep{Runner: noCLI(t)}).PutBrief(context.Background(), "review/x", "x", nil, []byte("# Review\n")); e == nil {
				t.Fatal("an unverified acknowledgement returned nil")
			}
		})
	}
	// The acknowledgement hk gives for the key and body sent is a success.
	hk := hktest.New(t)
	hk.AnswerDocumentPut(func(key, sha string) string {
		return `{"document":{"key":"` + key + `","sha256":"` + sha + `"},"changed":false}`
	})
	if e := putBrief(t, Handoffkeep{Runner: noCLI(t)}).PutBrief(context.Background(), "review/x", "x", nil, []byte("# Review\n")); e != nil {
		t.Fatal(e)
	}
}
