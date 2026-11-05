package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/policy"
)

// GitHub's REST pull head and the GraphQL head disagree for a few seconds
// after update-branch: the gate waits instead of erroring.
func TestGateMovingHeadIsPending(t *testing.T) {
	s, g, _, _, _ := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedbackHead = shaB
	v := gateAt(t, s, g, shaB)
	if v.State != "pending" || reasons(v) != "head_moving" {
		t.Fatalf("verdict %+v, want pending head_moving", v)
	}
}

// changed_files and the files list disagree just after a push.
func TestGateSettlingFilesIsPending(t *testing.T) {
	s, g, _, _, _ := setup(t)
	g.enabled = true
	markReviewed(g)
	g.files = []policy.File{{Path: "a", Kind: "modified"}, {Path: "b", Kind: "modified"}}
	g.pull.ChangedFiles = 3
	v := gateAt(t, s, g, shaB)
	if v.State != "pending" || reasons(v) != "files_settling" {
		t.Fatalf("verdict %+v, want pending files_settling", v)
	}
}

func TestLandMovingHeadAuditsPendingNotError(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedbackHead = shaB
	var out bytes.Buffer
	s.Out = &out
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 0 {
		t.Fatalf("merges = %d, want 0", g.mergeCount)
	}
	var lands int
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec struct {
			Command string `json:"command"`
			Value   struct {
				State   string   `json:"state"`
				Reasons []string `json:"reasons"`
			} `json:"value"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			t.Fatalf("audit line %q", line)
		}
		if rec.Command == "land-error" {
			t.Fatalf("land-error audited: %s", line)
		}
		if rec.Command == "land" {
			lands++
			if rec.Value.State != "pending" || strings.Join(rec.Value.Reasons, ",") != "head_moving" {
				t.Fatalf("land record %s", line)
			}
		}
	}
	if lands != 1 {
		t.Fatalf("land records = %d in %s", lands, out.String())
	}
}

// A settling file list in the review paths skips the pass without a failure.
func TestRetryReviewStatusSettlingFilesIsPending(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.files = []policy.File{{Path: "a", Kind: "modified"}, {Path: "b", Kind: "modified"}}
	g.pull.ChangedFiles = 3
	complete, pending, e := s.retryReviewStatus(context.Background(), r, g.pull)
	if e != nil || complete || !pending {
		t.Fatalf("complete=%v pending=%v err=%v", complete, pending, e)
	}
}

func init() { headSettleWait = 0 }

// A feedback read that still carries the old head right after update-branch
// settles in the same pass: the gate re-reads past the cache and decides.
func TestGateMovingHeadSettlesInSamePass(t *testing.T) {
	s, g, _, _, _ := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedbackHeads = []string{shaB, shaB}
	v := gateAt(t, s, g, shaB)
	if v.State == "pending" && reasons(v) == "head_moving" {
		t.Fatalf("verdict %+v: the head settled on the third read", v)
	}
	if g.feedbackReads != 3 {
		t.Fatalf("feedback reads = %d, want 3", g.feedbackReads)
	}
}

// A head that never settles is still deferred, after the bounded re-reads.
func TestGateMovingHeadIsBounded(t *testing.T) {
	s, g, _, _, _ := setup(t)
	g.enabled = true
	markReviewed(g)
	g.feedbackHead = shaB
	v := gateAt(t, s, g, shaB)
	if v.State != "pending" || reasons(v) != "head_moving" || g.feedbackReads != 1+headSettleTries {
		t.Fatalf("verdict %+v reads=%d", v, g.feedbackReads)
	}
}
