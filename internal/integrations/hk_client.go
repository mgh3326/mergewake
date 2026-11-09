package integrations

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

// hkClientTimeout bounds one hk public-client call: a task read, comment or
// reopen, an attachment upload, or a document write.
const hkClientTimeout = 30 * time.Second

// hkClient returns mergewake's hk public client, built from hkJobEndpoint:
// the svc-mergewake credential from the environment only, with the same
// validation and HTTP client as the job reads (no redirect followed, no proxy
// over plain HTTP). An unset credential is errEndpointUnconfigured; there is
// no fallback to the hk CLI or its config.env (hk 3526 decision §4).
func hkClient() (client.Client, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		return client.Client{}, e
	}
	return client.Client{URL: c.base, Token: c.token, CFAccessClientID: c.cfID, CFAccessClientSecret: c.cfSecret, HTTP: c.client(hkClientTimeout)}, nil
}

// errTaskNotFound is hk's not_found answer for a task: the CLI's `tasks show`
// failed on it, and the sink keeps that an error.
var errTaskNotFound = errors.New("handoffkeep task not_found")

// taskClient validates a task ID and returns it with the hk client.
func taskClient(task string) (client.Client, int64, error) {
	id, e := strconv.ParseInt(task, 10, 64)
	if !TaskID.MatchString(task) || e != nil {
		return client.Client{}, 0, errors.New("task ID must be a positive integer")
	}
	c, e := hkClient()
	return c, id, e
}

// getTask reads one task; an unknown task is errTaskNotFound.
func getTask(ctx context.Context, c client.Client, id int64) (client.Task, error) {
	x, found, e := c.GetTask(ctx, id)
	if e != nil {
		return client.Task{}, e
	}
	if !found {
		return client.Task{}, fmt.Errorf("%w: %d", errTaskNotFound, id)
	}
	return x, nil
}

// task reads one task by its ID string.
func (Handoffkeep) task(ctx context.Context, task string) (client.Task, error) {
	c, id, e := taskClient(task)
	if e != nil {
		return client.Task{}, e
	}
	return getTask(ctx, c, id)
}

// Comment appends body to the task's comments as svc-mergewake.
func (Handoffkeep) Comment(ctx context.Context, task, body string) error {
	c, id, e := taskClient(task)
	if e != nil {
		return e
	}
	_, e = c.CommentTask(ctx, id, body)
	return e
}

// UploadAttachment stores body in hk as an attachment named name and returns
// its sha256. The MIME type is chosen as the hk CLI's `attach put` chooses
// it, because hk's attachment allowlist judges it. mergewake sets no size
// cap of its own; hk refuses a body over its limit as attachment_too_large.
func (Handoffkeep) UploadAttachment(ctx context.Context, name string, body []byte) (string, error) {
	c, e := hkClient()
	if e != nil {
		return "", e
	}
	x, _, e := c.PutAttachment(ctx, name, attachmentMIME(name, body), "", body)
	if e != nil {
		return "", e
	}
	if !attachmentID.MatchString(x.SHA256) {
		return "", errors.New("attachment upload returned an invalid sha256")
	}
	return x.SHA256, nil
}

// attachmentMIME is the hk CLI's `attach put` type for name: its extension
// table (cmd/handoffkeep attachmentMIME), else the sniffed type of body.
func attachmentMIME(name string, body []byte) string {
	if m := map[string]string{".json": "application/json", ".md": "text/markdown", ".csv": "text/csv", ".ndjson": "application/x-ndjson",
		".log": "text/x-log", ".txt": "text/plain", ".html": "text/html", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
		".gif": "image/gif", ".pdf": "application/pdf"}[strings.ToLower(filepath.Ext(name))]; m != "" {
		return m
	}
	return http.DetectContentType(body)
}
