package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/mgh3326/handoffkeep/client"
)

// TaskBody reads the body of the document named by a task's body_doc field.
// The public hk client intentionally omits that field from client.Task, so the
// task answer is decoded here over the same configured HTTP client.
func (Handoffkeep) TaskBody(ctx context.Context, task string) (string, error) {
	c, id, err := taskClient(task)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, hkClientTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/tasks/%d", c.URL, id), nil)
	if err != nil {
		return "", errors.New("invalid handoffkeep task request")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", client.UserAgent)
	if c.CFAccessClientID != "" {
		req.Header.Set("CF-Access-Client-Id", c.CFAccessClientID)
		req.Header.Set("CF-Access-Client-Secret", c.CFAccessClientSecret)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", errors.New("handoffkeep task request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			var answer struct {
				Error string `json:"error"`
			}
			if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&answer) == nil && answer.Error == "not_found" {
				return "", fmt.Errorf("%w: %d", errTaskNotFound, id)
			}
		}
		return "", errors.New("handoffkeep task unavailable")
	}
	var answer struct {
		client.Task
		BodyDoc string `json:"body_doc"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&answer) != nil || answer.ID != id {
		return "", errors.New("invalid handoffkeep task response")
	}
	if answer.BodyDoc == "" {
		return "", nil
	}
	doc, found, err := c.GetDocument(ctx, answer.BodyDoc)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return doc.Body, nil
}

// Reopen reopens tasks settled by hk, including icebox handoffs, while active
// and already-unstarted tasks only receive the recovery comment.
func reopenableState(state string) bool {
	switch state {
	case "merged", "deployed", "completed", "dropped", "duplicate", "icebox":
		return true
	default:
		return false
	}
}
