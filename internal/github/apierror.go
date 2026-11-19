package github

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/fleetevent"
)

// apiMessageLimit bounds the GitHub message an APIError carries, in bytes.
const apiMessageLimit = 300

// apiBodyLimit bounds how much of an error response is read for its message.
// GitHub's error bodies are a few hundred bytes; the rest is never kept.
const apiBodyLimit = 8 << 10

// readAPIMessage reads the bounded error body of a refused request and returns
// its GitHub message. A body that is not the documented JSON gives "".
func readAPIMessage(body io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(body, apiBodyLimit))
	return apiMessage(b)
}

// apiMessage pulls GitHub's error text out of a response body: the top-level
// message, then each entry of errors. GitHub sends errors as plain strings on
// some routes and as objects with message and code on others, so each entry
// is decoded on its own and a shape it does not know is skipped, not fatal.
func apiMessage(body []byte) string {
	var doc struct {
		Message string            `json:"message"`
		Errors  []json.RawMessage `json:"errors"`
	}
	if json.Unmarshal(body, &doc) != nil {
		return ""
	}
	parts := []string{}
	if doc.Message != "" {
		parts = append(parts, doc.Message)
	}
	for _, raw := range doc.Errors {
		var text string
		if json.Unmarshal(raw, &text) == nil {
			parts = append(parts, text)
			continue
		}
		var entry struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		}
		if json.Unmarshal(raw, &entry) != nil {
			continue
		}
		if entry.Message != "" {
			parts = append(parts, entry.Message)
		} else if entry.Code != "" {
			parts = append(parts, entry.Code)
		}
	}
	return boundedText(strings.Join(parts, "; "))
}

// boundedText collapses whitespace, redacts credential-shaped text, and clips
// to apiMessageLimit bytes on a rune boundary. Redaction runs first, so a token
// cut by the clip cannot escape the patterns.
func boundedText(s string) string {
	s = fleetevent.Redact(strings.Join(strings.Fields(s), " "))
	if len(s) <= apiMessageLimit {
		return s
	}
	cut := apiMessageLimit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// PendingReviewConflict reports whether GitHub refused a review because the
// posting user already holds a pending review on the pull request. GitHub
// allows one pending review per user per pull request, and only that
// review's owner can submit or delete it: a retry cannot get past it.
func PendingReviewConflict(err error) bool {
	var a *APIError
	return errors.As(err, &a) &&
		a.Code == http.StatusUnprocessableEntity &&
		a.Method == http.MethodPost &&
		strings.HasSuffix(a.Path, "/reviews") &&
		strings.Contains(strings.ToLower(a.Message), "pending review")
}
