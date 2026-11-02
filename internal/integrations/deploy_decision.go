package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// DeployDecision only consumes an operator answer to this exact immutable
// host/range/release question. The hk CLI records and mirrors the request.
func (h Handoffkeep) DeployDecision(ctx context.Context, task, question string) (bool, error) {
	if !TaskID.MatchString(task) {
		return false, errors.New("deploy_task must name a dedicated handoffkeep task")
	}
	shown, e := h.Runner.Run(ctx, h.Command, []string{"tasks", "show", task}, "", "")
	if e != nil {
		return false, e
	}
	var current struct {
		Refs struct {
			Request struct {
				ID         string     `json:"id"`
				Status     string     `json:"status"`
				Question   string     `json:"question"`
				HumanOnly  bool       `json:"human_only"`
				Due        *time.Time `json:"due_at"`
				Resolution *struct {
					Kind   string `json:"kind"`
					Option string `json:"option"`
				} `json:"resolution"`
			} `json:"decision_request"`
		} `json:"refs"`
	}
	if e = json.Unmarshal(shown, &current); e != nil {
		return false, e
	}
	r := current.Refs.Request
	if r.Question == question && r.ID != "" && r.HumanOnly {
		if r.Due != nil && !time.Now().Before(*r.Due) {
			return false, nil
		}
		return r.Status == "answered" && r.Resolution != nil && r.Resolution.Kind == "answered" && r.Resolution.Option == "A", nil
	}
	// Refuse to supersede another open request. A dedicated task per target/host
	// prevents one rollout from replacing another rollout's pending question.
	if r.ID != "" && r.Status == "open" {
		return false, errors.New("another deployment decision is open")
	}
	_, e = h.Runner.Run(ctx, h.Command, []string{"decisions", "ask", task, "--question", question, "--option", "A|Approve this deployment", "--option", "B|Keep the current version", "--default-action", "Hold; keep the current version", "--human-only"}, "", "")
	return false, e
}
