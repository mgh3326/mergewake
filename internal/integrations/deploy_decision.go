package integrations

import (
	"context"
	"errors"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// deployDecisionEntry is the hk decision request mergewake asked, or
// adopted, for one deploy task's question.
type deployDecisionEntry struct {
	Question  string `json:"question"`
	RequestID string `json:"request_id"`
}

// deployDecisionRecord is the rollout's durable record of deploy decision
// requests, one entry per deploy task: one repository can have a task per
// target host, and one task's request must neither replace nor block
// another's. It lives in the file the caller names, so an answer is looked
// up by request id after a restart.
type deployDecisionRecord struct {
	Tasks map[string]deployDecisionEntry `json:"by_task"`
}

// readDeployDecisionRecord reads the record at path; a missing file is an
// empty record. The single-record shape {task, question, request_id}, which
// only an unreleased build wrote, is read as a one-entry map.
func readDeployDecisionRecord(path string) (deployDecisionRecord, error) {
	var raw struct {
		Tasks     map[string]deployDecisionEntry `json:"by_task"`
		Task      string                         `json:"task"`
		Question  string                         `json:"question"`
		RequestID string                         `json:"request_id"`
	}
	if _, e := fsutil.ReadJSON(path, &raw); e != nil {
		return deployDecisionRecord{}, e
	}
	out := deployDecisionRecord{Tasks: raw.Tasks}
	if out.Tasks == nil {
		out.Tasks = map[string]deployDecisionEntry{}
		if raw.Task != "" {
			out.Tasks[raw.Task] = deployDecisionEntry{raw.Question, raw.RequestID}
		}
	}
	return out, nil
}

// DeployDecision only consumes an operator answer to this exact immutable
// host/range/release question, read and recorded over hk's HTTP API as
// svc-mergewake. It asks on the record-only route (the hk CLI's `decisions
// ask` without --linear), which opens no Linear issue. The task's current
// request counts only when its id is the one recorded in the file record
// for this task and question, or a request that supersedes it, and its
// question text is still question.
//
// While record has no entry for task (the first decision after the upgrade
// from question-text lookup, or a task new to this repository), a
// human-only request whose question text is question is adopted once, as
// the old lookup found it: its id is recorded and matched from then on.
// Once task has an entry, a request with the same text but another id is
// never adopted. Entries of other tasks are left as they are.
//
// The read-modify-write of record is not locked here: its one caller,
// pipeline.Service.Rollout, holds the repository's rollout decision-lock,
// and the record lives in that repository's rollout state.
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
	records, e := readDeployDecisionRecord(record)
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
	save := func(requestID string) error {
		records.Tasks[task] = deployDecisionEntry{question, requestID}
		return fsutil.WriteJSON(record, records)
	}

	prior, recorded := records.Tasks[task]
	known := ""
	if recorded && prior.Question == question {
		known = prior.RequestID
	}
	ours := !recorded || (known != "" && (r.ID == known || r.Supersedes == known))
	if ours && r.ID != "" && r.HumanOnly && r.Question == question {
		if r.ID != known {
			if e = save(r.ID); e != nil {
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

	// The body is the one `decisions ask` sends for these flags: options A
	// and B with a free answer allowed (no --no-free-answer), none
	// recommended, human_only, and supersedes only for an expired open
	// request.
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
	asked, e := c.RecordDecisionRequest(ctx, id, in)
	if e != nil {
		return false, e
	}
	if asked.Request.ID == "" {
		return false, errors.New("handoffkeep decision request returned no request id")
	}
	return false, save(asked.Request.ID)
}
