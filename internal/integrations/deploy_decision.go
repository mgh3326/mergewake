package integrations

import (
	"context"
	"errors"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// deployDecisionRecord is the hk decision request mergewake asked, or
// adopted, for one deploy task and question. It lives in the rollout's
// durable state (the file the caller names), so the answer is looked up by
// request id after a restart.
type deployDecisionRecord struct {
	Task      string `json:"task"`
	Question  string `json:"question"`
	RequestID string `json:"request_id"`
}

// DeployDecision only consumes an operator answer to this exact immutable
// host/range/release question, read and asked over hk's HTTP API as
// svc-mergewake. The task's current request counts only when its id is the
// one recorded in the file record for this task and question, or a request
// that supersedes it, and its question text is still question.
//
// While record does not exist (the first decision after the upgrade from
// question-text lookup), a human-only request whose question text is
// question is adopted once, as the old lookup found it: its id is recorded
// and matched from then on. Once record exists, a request with the same text
// but another id is never adopted.
//
// now decides whether a request's due time has passed. An unconfigured hk
// credential is errEndpointUnconfigured, so the deploy is held.
func (h Handoffkeep) DeployDecision(ctx context.Context, task, question, record string, now time.Time) (bool, error) {
	if !TaskID.MatchString(task) {
		return false, errors.New("deploy_task must name a dedicated handoffkeep task")
	}
	if record == "" {
		return false, errors.New("deploy decision record path required")
	}
	c, id, e := taskClient(task)
	if e != nil {
		return false, e
	}
	var prior deployDecisionRecord
	recorded, e := fsutil.ReadJSON(record, &prior)
	if e != nil {
		return false, e
	}
	current, e := getTask(ctx, c, id)
	if e != nil {
		return false, e
	}
	var r client.DecisionRequest
	if current.Refs.DecisionRequest != nil {
		r = *current.Refs.DecisionRequest
	}
	expired := r.DueAt != nil && !now.Before(*r.DueAt)

	known := ""
	if recorded && prior.Task == task && prior.Question == question {
		known = prior.RequestID
	}
	ours := !recorded || (known != "" && (r.ID == known || r.Supersedes == known))
	if ours && r.ID != "" && r.HumanOnly && r.Question == question {
		if r.ID != known {
			if e = fsutil.WriteJSON(record, deployDecisionRecord{task, question, r.ID}); e != nil {
				return false, e
			}
		}
		if expired {
			return false, nil
		}
		return r.Status == "answered" && r.Resolution != nil && r.Resolution.Kind == "answered" && r.Resolution.Option == "A", nil
	}
	// Refuse to supersede another open request. A dedicated task per target/host
	// prevents one rollout from replacing another rollout's pending question.
	if r.ID != "" && r.Status == "open" && !expired {
		return false, errors.New("another deployment decision is open")
	}

	in := client.DecisionAskInput{
		Question: question,
		Options: client.DecisionOptions{AllowFree: true, Options: []client.DecisionOption{
			{Key: "A", Label: "Approve this deployment"}, {Key: "B", Label: "Keep the current version"}}},
		DefaultAction: "Hold; keep the current version",
		HumanOnly:     true,
	}
	if expired && r.ID != "" && r.Status == "open" {
		in.Supersedes = r.ID
	}
	asked, e := c.AskDecision(ctx, id, in)
	if e != nil {
		return false, e
	}
	if asked.Request.ID == "" {
		return false, errors.New("handoffkeep decision ask returned no request id")
	}
	return false, fsutil.WriteJSON(record, deployDecisionRecord{task, question, asked.Request.ID})
}
