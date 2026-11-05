package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Task event sources hk accepts on POST /v1/task-events (handoffkeep#136,
// docs/linear-internalize.md "Event-driven transitions").
const (
	SourcePR     = "mergewake.pr"
	SourceReview = "mergewake.review"
	SourceMerge  = "mergewake.merge"
	SourceRevert = "mergewake.revert"
)

// TaskEvent is one mergewake event addressed to one hk task. SourceRef is the
// PR URL, or for a revert the revert PR URL; hk deduplicates on (task,
// source, source_ref), so a resend changes nothing.
type TaskEvent struct {
	Source     string     `json:"source"`
	SourceRef  string     `json:"source_ref"`
	TaskID     int64      `json:"task_id"`
	PR         string     `json:"pr,omitempty"`
	RevertedPR string     `json:"reverted_pr,omitempty"`
	RevertPR   string     `json:"revert_pr,omitempty"`
	At         *time.Time `json:"at,omitempty"`
	// The fields below are optional extensions (hk 3037): head and base of the
	// PR on any source, the merge commit on mergewake.merge, and one record per
	// posted review pass on mergewake.review. An hk that does not know them
	// answers 400; PostTaskEvent then resends the event without them.
	HeadSHA  string         `json:"head_sha,omitempty"`
	BaseSHA  string         `json:"base_sha,omitempty"`
	MergeSHA string         `json:"merge_sha,omitempty"`
	Reviews  []ReviewRecord `json:"reviews,omitempty"`
}

// MaxReviewRecords is how many reviews[] items hk takes in one event.
const MaxReviewRecords = 16

// ReviewRecord is one review pass posted for a head, in hk's `reviews[]`
// shape. Every field but ReviewedHead is left out when mergewake does not know
// it. Tokens (Lens, Family, ...) match [a-z0-9_.-]; use Token to build them.
type ReviewRecord struct {
	ReviewedHead   string     `json:"reviewed_head"`
	ReviewedBase   string     `json:"reviewed_base,omitempty"`
	PatchID        string     `json:"patch_id,omitempty"`
	Round          int        `json:"round,omitempty"`
	Pass           int        `json:"pass,omitempty"`
	Lens           string     `json:"lens,omitempty"`
	ReviewerJobID  string     `json:"reviewer_job_id,omitempty"`
	Family         string     `json:"family,omitempty"`
	Profile        string     `json:"profile,omitempty"`
	Model          string     `json:"model,omitempty"`
	Harness        string     `json:"harness,omitempty"`
	Effort         string     `json:"effort,omitempty"`
	Executor       string     `json:"executor,omitempty"`
	Fallback       bool       `json:"fallback,omitempty"`
	PolicyRevision string     `json:"policy_revision,omitempty"`
	ContractSource string     `json:"contract_source,omitempty"`
	Verdict        string     `json:"verdict,omitempty"`
	Critical       int        `json:"critical"`
	High           int        `json:"high"`
	Medium         int        `json:"medium"`
	Low            int        `json:"low"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
}

var (
	shaRE        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	tokenInvalid = regexp.MustCompile(`[^a-z0-9_.-]+`)
)

// SHA returns s when it is a 40-hex commit ID, else "" (omitted).
func SHA(s string) string {
	if shaRE.MatchString(s) {
		return s
	}
	return ""
}

// Token normalises s to a short [a-z0-9_.-] token: lower-cased, every other
// run of characters one "-", edges trimmed. "" when nothing is left.
func Token(s string) string {
	t := strings.Trim(tokenInvalid.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), "-"), "-")
	if len(t) > 64 {
		t = strings.Trim(t[:64], "-")
	}
	return t
}

// extended reports whether ev carries any optional field, and plain returns
// ev without them: the form every hk accepts.
func (ev TaskEvent) extended() bool {
	return ev.HeadSHA != "" || ev.BaseSHA != "" || ev.MergeSHA != "" || len(ev.Reviews) > 0
}

func (ev TaskEvent) plain() TaskEvent {
	ev.HeadSHA, ev.BaseSHA, ev.MergeSHA, ev.Reviews = "", "", "", nil
	return ev
}

// TaskEventResult is what hk did with an event: applied moved the task,
// redelivered means hk had already recorded it.
type TaskEventResult struct {
	Applied     bool   `json:"applied"`
	Redelivered bool   `json:"redelivered"`
	State       string `json:"state,omitempty"`
	Reason      string `json:"reason,omitempty"`
	// FieldsRefused is set by PostTaskEvent, never by hk: hk answered 400 to the
	// extended payload (this is its error code, or "invalid" when it sent none)
	// and the event was accepted only when resent without the optional fields.
	FieldsRefused string `json:"-"`
}

// HKError is a non-2xx answer from hk. Code is hk's error code
// (`{"error": "<code>"}`) when it has the documented shape, else empty; the
// rest of the body is withheld.
type HKError struct {
	Status int
	Code   string
}

func (e *HKError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("handoffkeep HTTP %d", e.Status)
	}
	return fmt.Sprintf("handoffkeep HTTP %d %s", e.Status, e.Code)
}

// Retryable is a 5xx: hk or its proxy failed, and the same event may succeed
// later. Every 4xx, 404 included, is a final answer.
func (e *HKError) Retryable() bool { return e.Status >= 500 }

// TaskEventPoster posts one task event to hk.
type TaskEventPoster interface {
	PostTaskEvent(context.Context, TaskEvent) (TaskEventResult, error)
}

// Defaults for task-event delivery; the Handoffkeep fields override them.
const (
	DefaultEventAttempts = 3
	DefaultEventBackoff  = 500 * time.Millisecond
	DefaultEventTimeout  = 5 * time.Second
)

var hkErrorCode = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// PostTaskEvent posts ev through the same handoffkeep endpoint and
// credentials Job reads. A network error or 5xx is retried, with doubling
// backoff, up to EventAttempts tries in total while ctx lasts; a 4xx is
// returned at once as *HKError.
//
// An extended payload (see TaskEvent) that hk answers with 400 is resent once
// without the optional fields, so the event is never lost to them; the result
// then carries FieldsRefused.
func (h Handoffkeep) PostTaskEvent(ctx context.Context, ev TaskEvent) (TaskEventResult, error) {
	res, e := h.postTaskEvent(ctx, ev)
	var hk *HKError
	if e != nil && ev.extended() && errors.As(e, &hk) && hk.Status == http.StatusBadRequest {
		code := hk.Code
		if code == "" {
			code = "invalid"
		}
		if res, e = h.postTaskEvent(ctx, ev.plain()); e == nil {
			res.FieldsRefused = code
		}
	}
	return res, e
}

func (h Handoffkeep) postTaskEvent(ctx context.Context, ev TaskEvent) (TaskEventResult, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		return TaskEventResult{}, e
	}
	attempts, backoff, timeout := h.EventAttempts, h.EventBackoff, h.EventTimeout
	if attempts < 1 {
		attempts = DefaultEventAttempts
	}
	if backoff <= 0 {
		backoff = DefaultEventBackoff
	}
	if timeout <= 0 {
		timeout = DefaultEventTimeout
	}
	body, e := json.Marshal(ev)
	if e != nil {
		return TaskEventResult{}, e
	}
	client := c.client(timeout)
	for try := 1; ; try++ {
		res, e := c.postEvent(ctx, client, body)
		var hk *HKError
		if e == nil || (errors.As(e, &hk) && !hk.Retryable()) || try >= attempts || ctx.Err() != nil {
			if e != nil && try > 1 {
				e = fmt.Errorf("%w (after %d attempts)", e, try)
			}
			return res, e
		}
		select {
		case <-ctx.Done():
			return res, fmt.Errorf("%w (after %d attempts)", e, try)
		case <-time.After(backoff << (try - 1)):
		}
	}
}

func (c *hkJobs) postEvent(ctx context.Context, client *http.Client, body []byte) (TaskEventResult, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/task-events", bytes.NewReader(body))
	if e != nil {
		return TaskEventResult{}, errors.New("invalid task event request")
	}
	c.authorize(req)
	req.Header.Set("Content-Type", "application/json")
	resp, e := client.Do(req)
	if e != nil {
		return TaskEventResult{}, errors.New("handoffkeep request failed")
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, 1<<20)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var answer struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(limited).Decode(&answer)
		if !hkErrorCode.MatchString(answer.Error) {
			answer.Error = ""
		}
		return TaskEventResult{}, &HKError{Status: resp.StatusCode, Code: answer.Error}
	}
	var res TaskEventResult
	if e := json.NewDecoder(limited).Decode(&res); e != nil {
		return TaskEventResult{}, errors.New("handoffkeep response is not valid JSON")
	}
	return res, nil
}
