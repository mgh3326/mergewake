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
}

// TaskEventResult is what hk did with an event: applied moved the task,
// redelivered means hk had already recorded it.
type TaskEventResult struct {
	Applied     bool   `json:"applied"`
	Redelivered bool   `json:"redelivered"`
	State       string `json:"state,omitempty"`
	Reason      string `json:"reason,omitempty"`
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
func (h Handoffkeep) PostTaskEvent(ctx context.Context, ev TaskEvent) (TaskEventResult, error) {
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
