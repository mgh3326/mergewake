package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// deployDecisionEntry is the hk decision request mergewake asked, or
// adopted, for one deploy task's question. Root is the root hk gave that
// request (the first revision of its chain); an entry written before roots
// were recorded has none.
type deployDecisionEntry struct {
	Question  string `json:"question"`
	RequestID string `json:"request_id"`
	Root      string `json:"root,omitempty"`
}

// deployDecisionRecord is the rollout's durable record of deploy decision
// requests, one entry per deploy task: one repository can have a task per
// target host, and one task's request must neither replace nor block
// another's. It lives in the file the caller names, so an answer is looked
// up by request id after a restart.
type deployDecisionRecord struct {
	Tasks map[string]deployDecisionEntry `json:"by_task"`
}

// readDeployDecisionRecord reads the record at path. Only a missing file is
// an empty record. A present file must be the map shape {"by_task": {...}}
// with a question and a request id in every entry, or the complete
// single-record shape {task, question, request_id} that only an unreleased
// build wrote, read as a one-entry map. Anything else (null, {}, a null
// map, an empty entry, a wrong type or an unknown field) is corrupt: an
// error, so the rollout holds instead of reopening question-text adoption.
func readDeployDecisionRecord(path string) (deployDecisionRecord, error) {
	b, e := os.ReadFile(path)
	if errors.Is(e, fs.ErrNotExist) {
		return deployDecisionRecord{Tasks: map[string]deployDecisionEntry{}}, nil
	}
	if e != nil {
		return deployDecisionRecord{}, e
	}
	out, why := parseDeployDecisionRecord(b)
	if why != "" {
		return deployDecisionRecord{}, fmt.Errorf("deploy decision record %s is corrupt (%s); hold", path, why)
	}
	return out, nil
}

func parseDeployDecisionRecord(b []byte) (deployDecisionRecord, string) {
	var top map[string]json.RawMessage
	if e := strictJSON(b, &top); e != nil {
		return deployDecisionRecord{}, e.Error()
	}
	if top == nil {
		return deployDecisionRecord{}, "not an object"
	}
	if raw, ok := top["by_task"]; ok {
		if len(top) != 1 {
			return deployDecisionRecord{}, "by_task beside other fields"
		}
		var tasks map[string]json.RawMessage
		if e := strictJSON(raw, &tasks); e != nil {
			return deployDecisionRecord{}, "by_task: " + e.Error()
		}
		if tasks == nil {
			return deployDecisionRecord{}, "by_task is not an object"
		}
		out := deployDecisionRecord{Tasks: make(map[string]deployDecisionEntry, len(tasks))}
		for task, raw := range tasks {
			var x deployDecisionEntry
			if e := strictJSON(raw, &x); e != nil {
				return deployDecisionRecord{}, fmt.Sprintf("task %q: %v", task, e)
			}
			if x.Question == "" || x.RequestID == "" {
				return deployDecisionRecord{}, fmt.Sprintf("task %q: question and request_id required", task)
			}
			out.Tasks[task] = x
		}
		return out, ""
	}
	var v1 struct {
		Task      string `json:"task"`
		Question  string `json:"question"`
		RequestID string `json:"request_id"`
	}
	if e := strictJSON(b, &v1); e != nil {
		return deployDecisionRecord{}, e.Error()
	}
	if v1.Task == "" || v1.Question == "" || v1.RequestID == "" {
		return deployDecisionRecord{}, "neither by_task nor a complete task, question and request_id"
	}
	return deployDecisionRecord{Tasks: map[string]deployDecisionEntry{v1.Task: {Question: v1.Question, RequestID: v1.RequestID}}}, ""
}

// strictJSON decodes one JSON value from b into v, refusing unknown fields
// and anything but whitespace after that value. The end is proven by a
// second Decode reaching io.EOF: Decoder.More reports false before a stray
// ] or }, so it would accept {"by_task":{}}].
func strictJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	if e := d.Decode(&json.RawMessage{}); e != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// DeployDecision only consumes an operator answer to this exact immutable
// host/range/release question, read and recorded over hk's HTTP API as
// svc-mergewake. It asks on the record-only route (the hk CLI's `decisions
// ask` without --linear), which opens no Linear issue. The task's current
// request counts only when it is human-only, its question text is still
// question, and it belongs to the request recorded in the file record for
// this task and question: it is that request, it supersedes it, or hk gives
// it the recorded request's chain root (so a request superseded any number
// of times is still followed).
//
// While record has no entry for task (the first decision after the upgrade
// from question-text lookup, or a task new to this repository), a
// human-only request whose question text is question is adopted once, as
// the old lookup found it: its id is recorded and matched from then on.
// Once task has an entry, a request with the same text but another id is
// never adopted, including one hk returns as a duplicate of the ask (hk
// compares neither status nor requester): the rollout then holds with an
// error. Entries of other tasks are left as they are. A record file that is
// present but not a valid record holds the rollout before hk is read.
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

	prior, recorded := records.Tasks[task]
	var known deployDecisionEntry
	if recorded && prior.Question == question {
		known = prior
	}
	// counts is whether x is this rollout's request for question: in the
	// recorded request's chain or, while task has no entry, the one-time
	// adoption by question text.
	counts := func(x client.DecisionRequest) bool {
		inChain := known.RequestID != "" && (x.ID == known.RequestID || x.Supersedes == known.RequestID ||
			x.Root != "" && (x.Root == known.RequestID || x.Root == known.Root))
		return x.ID != "" && x.HumanOnly && x.Question == question && (!recorded || inChain)
	}
	save := func(x client.DecisionRequest) error {
		entry := deployDecisionEntry{question, x.ID, x.Root}
		if recorded && entry == prior {
			return nil
		}
		records.Tasks[task] = entry
		return fsutil.WriteJSON(record, records)
	}

	if counts(r) {
		if e = save(r); e != nil {
			return false, e
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
	// hk answers a re-send of the task's current request with that request
	// and duplicate set, whatever its status or requester. Only this
	// rollout's own request (or, before the task has an entry, one the old
	// question-text rule adopts) is recorded from a duplicate.
	if asked.Duplicate && !counts(asked.Request) {
		return false, fmt.Errorf("deployment decision: hk returned an existing request this rollout did not ask (duplicate %s); hold", asked.Request.ID)
	}
	return false, save(asked.Request)
}
