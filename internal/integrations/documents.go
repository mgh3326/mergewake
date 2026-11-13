package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// DecisionDocument is the metadata needed to establish a decision's provenance,
// plus the document text the review prompt shows as a bounded excerpt.
type DecisionDocument struct {
	Key       string `json:"key"`
	UpdatedAt string `json:"updated_at"`
	Body      string `json:"body"`
}

var decisionKey = regexp.MustCompile(`^decision/[A-Za-z0-9_-][A-Za-z0-9._-]*(/[A-Za-z0-9_-][A-Za-z0-9._-]*)*$`)

// DecisionDocument reads a decision using mergewake's own hk credential.
func (h Handoffkeep) DecisionDocument(ctx context.Context, key string) (DecisionDocument, error) {
	if len(key) > 256 || !decisionKey.MatchString(key) {
		return DecisionDocument{}, errors.New("invalid decision key")
	}
	c, err := hkJobEndpoint()
	if err != nil {
		return DecisionDocument{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/documents/"+strings.Join(parts, "/"), nil)
	if err != nil {
		return DecisionDocument{}, errors.New("invalid document request")
	}
	c.authorize(req)
	resp, err := c.client(3 * time.Second).Do(req)
	if err != nil {
		return DecisionDocument{}, errors.New("handoffkeep document request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return DecisionDocument{}, errors.New("handoffkeep document unavailable")
	}
	var doc DecisionDocument
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&doc) != nil || doc.Key != key || doc.UpdatedAt == "" {
		return DecisionDocument{}, errors.New("invalid handoffkeep document response")
	}
	if _, err := time.Parse(time.RFC3339Nano, doc.UpdatedAt); err != nil {
		return DecisionDocument{}, errors.New("invalid handoffkeep document timestamp")
	}
	return doc, nil
}
