package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// A reviewer job records the grade decision it was spawned under. When its
// result is collected after the policy changed, the review audit row names the
// decision of that job, not the policy of the collecting run (hk 5672).
func TestAuditRowNamesTheReviewerJobsDecision(t *testing.T) {
	f := asyncReview(t)
	// Placed in round one under the raise: the PR grade B is asked at A.
	f.s.Config.Review.FirstRoundGrade = map[string]string{"B": "A"}
	if err := f.review(t); err != nil {
		t.Fatalf("place the reviewer: %v", err)
	}
	// The hub accepted the request and no wait followed, as recordedRequest leaves it.
	rj := readReviewerJob(t, f.s, f.r, 1)
	rj.Spawned = acceptedReceipt(rj.Label, asyncRequestID)
	if err := fsutil.WriteJSON(f.s.reviewerJobPath(f.r.Name, 7, 1), rj); err != nil {
		t.Fatalf("save the receipt: %v", err)
	}
	// The policy changes before the result is collected: no raise any more.
	f.s.Config.Review.FirstRoundGrade = map[string]string{}
	f.runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Profile: "codex-sol", Effort: "medium", Result: json.RawMessage(`{"findings":[]}`)}
	f.c.t = f.c.t.Add(2 * time.Minute)
	expireLease(f.g)
	if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
		t.Fatalf("review: %v", err)
	}
	rows := auditRows(t, f.s, f.r, "review")
	if len(rows) != 1 {
		t.Fatalf("review rows = %v", rows)
	}
	if rows[0]["pr_grade"] != "B" || rows[0]["grade_asked"] != "A" || rows[0]["first_round"] != true {
		t.Fatalf("audit row = %v, want the job's B asked at A in round one", rows[0])
	}
}
