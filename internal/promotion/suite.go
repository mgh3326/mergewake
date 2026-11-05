// Package promotion certifies an exact main commit and its image digest,
// records the certification as an immutable annotated tag, advances a
// forward-only deployable branch, and admits only what that branch and its
// record agree on. See docs/promotion.md.
package promotion

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// PolicyVersion names the certification rules this code enforces. A record
// carries it so a later reader knows which rules admitted the commit.
const PolicyVersion = "promotion-policy-1"

// RecordSchema is the version of the record JSON.
const RecordSchema = 1

// TagPrefix is the ref namespace of promotion records.
const TagPrefix = "promotion/"

var (
	fullSHA  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestRE = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

//go:embed suites.json
var suitesJSON []byte

// Workflow is one required post-merge workflow, matched by its file path,
// never by its display name, and the job names it must report.
type Workflow struct {
	Path string   `json:"path"`
	Jobs []string `json:"jobs"`
}

// Artifact says which image certifies a commit and how its provenance is read.
type Artifact struct {
	Image         string `json:"image"`
	Tag           string `json:"tag"` // "{sha}" and "{short7}" expand to the commit
	Platform      string `json:"platform"`
	RevisionLabel string `json:"revision_label"`
}

// Suite is the checked-in, versioned definition of what certifies one repo.
// It lives in mergewake, not in the candidate repo, so a candidate cannot
// weaken its own certification policy.
type Suite struct {
	Version   string     `json:"version"`
	Main      string     `json:"main"`
	Pointer   string     `json:"pointer"`
	Workflows []Workflow `json:"workflows"`
	Artifact  Artifact   `json:"artifact"`
}

// Digest is the sha256 of the suite's canonical JSON.
func (s Suite) Digest() string {
	b, _ := json.Marshal(s)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// TagFor expands the artifact tag template for a full commit SHA.
func (a Artifact) TagFor(sha string) string {
	return strings.NewReplacer("{sha}", sha, "{short7}", sha[:7]).Replace(a.Tag)
}

// Suites returns the checked-in suite definitions keyed by owner/name.
func Suites() (map[string]Suite, error) { return parseSuites(suitesJSON) }

func parseSuites(b []byte) (map[string]Suite, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var m map[string]Suite
	if e := d.Decode(&m); e != nil {
		return nil, fmt.Errorf("suite definitions: %w", e)
	}
	for repo, s := range m {
		if s.Version == "" || s.Main == "" || s.Pointer == "" || len(s.Workflows) == 0 {
			return nil, fmt.Errorf("suite %s: version, main, pointer and workflows are required", repo)
		}
		for _, w := range s.Workflows {
			if !strings.HasPrefix(w.Path, ".github/workflows/") || len(w.Jobs) == 0 {
				return nil, fmt.Errorf("suite %s: workflow %q needs a .github/workflows path and jobs", repo, w.Path)
			}
		}
		a := s.Artifact
		if a.Image == "" || a.Tag == "" || a.RevisionLabel == "" || strings.Count(a.Platform, "/") != 1 {
			return nil, fmt.Errorf("suite %s: artifact image, tag, platform os/arch and revision_label are required", repo)
		}
	}
	return m, nil
}

// Denied is a refusal: the input is missing, unreadable or does not match.
// Every error from Admit and Promote is a refusal; this type carries the reason.
type Denied struct{ Reason string }

func (d *Denied) Error() string { return "denied: " + d.Reason }

func deny(format string, a ...any) error { return &Denied{fmt.Sprintf(format, a...)} }

// IsDenied reports whether e is an explicit refusal rather than an I/O failure.
// Both refuse; the distinction is only for messages.
func IsDenied(e error) bool { var d *Denied; return errors.As(e, &d) }
