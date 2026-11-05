package promotion

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Record is the immutable promotion record, stored as the message of the
// annotated tag promotion/<seq> whose target is SHA.
type Record struct {
	Schema     int         `json:"schema"`
	Repo       string      `json:"repo"`
	Seq        int         `json:"seq"`
	SHA        string      `json:"sha"`
	Previous   *Previous   `json:"previous"`
	Artifact   ArtifactRef `json:"artifact"`
	Suite      SuiteRef    `json:"suite"`
	Evidence   []Evidence  `json:"evidence"`
	Policy     string      `json:"policy"`
	Controller Controller  `json:"controller"`
	Created    string      `json:"created"`
}

type Previous struct {
	Seq int    `json:"seq"`
	SHA string `json:"sha"`
}

type ArtifactRef struct {
	Image    string `json:"image"`
	Platform string `json:"platform"`
	Tag      string `json:"tag"`
	Digest   string `json:"digest"`
}

type SuiteRef struct {
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

type Evidence struct {
	WorkflowPath string        `json:"workflow_path"`
	WorkflowID   int64         `json:"workflow_id"`
	RunID        int64         `json:"run_id"`
	RunAttempt   int           `json:"run_attempt"`
	Event        string        `json:"event"`
	HeadBranch   string        `json:"head_branch"`
	HeadSHA      string        `json:"head_sha"`
	Jobs         []JobEvidence `json:"jobs"`
}

type JobEvidence struct {
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

type Controller struct {
	Identity   string `json:"identity"`
	Generation int64  `json:"generation"`
}

// Canonical is the record's one accepted encoding: compact JSON in field order.
func (r Record) Canonical() []byte {
	b, _ := json.Marshal(r)
	return b
}

// ParseRecord accepts only the canonical encoding of a well-formed record.
func ParseRecord(msg string) (Record, error) {
	raw := []byte(strings.TrimSpace(msg))
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var r Record
	if e := d.Decode(&r); e != nil {
		return Record{}, deny("record does not parse: %v", e)
	}
	if !bytes.Equal(r.Canonical(), raw) {
		return Record{}, deny("record is not canonical JSON")
	}
	if r.Schema != RecordSchema {
		return Record{}, deny("record schema %d is not %d", r.Schema, RecordSchema)
	}
	if !fullSHA.MatchString(r.SHA) || r.Seq < 1 || r.Repo == "" {
		return Record{}, deny("record lacks a repo, a positive seq or a full sha")
	}
	if !digestRE.MatchString(r.Artifact.Digest) || r.Artifact.Image == "" {
		return Record{}, deny("record has no image digest")
	}
	if r.Previous != nil && (r.Previous.Seq >= r.Seq || !fullSHA.MatchString(r.Previous.SHA)) {
		return Record{}, deny("record previous promotion is malformed")
	}
	if _, e := time.Parse(time.RFC3339, r.Created); e != nil {
		return Record{}, deny("record created time is malformed")
	}
	return r, nil
}

// SeqOf parses the sequence number of a promotion tag name; 0 if not one.
func SeqOf(tag string) int {
	n, e := strconv.Atoi(strings.TrimPrefix(tag, TagPrefix))
	if !strings.HasPrefix(tag, TagPrefix) || e != nil || n < 1 || strconv.Itoa(n) != strings.TrimPrefix(tag, TagPrefix) {
		return 0
	}
	return n
}

// TagName is the promotion tag for seq.
func TagName(seq int) string { return TagPrefix + strconv.Itoa(seq) }
