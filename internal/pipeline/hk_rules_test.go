package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode"
)

// hkRulesCheck is a copy of hk's validation of a mergewake task event's change
// fields: checkEventChangeFields and checkEventReview in
// handoffkeep internal/store/change_payload.go at commit 11ccc30. It returns
// the first violation, or "" when hk would accept the event. The copy is the
// contract mergewake's records are tested against; when hk's rules change, this
// file changes with them.

var (
	hkGitSHARE        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hkPatchIDRE       = regexp.MustCompile(`^[0-9a-f]{7,64}$`)
	hkReviewTokenRE   = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)
	hkReviewProfileRE = regexp.MustCompile(`^[a-z0-9_.@-]{1,64}$`)
	hkPatchDigestRE   = regexp.MustCompile(`^[0-9a-f]{12,128}$`)
)

const (
	hkMaxEventReviews = 16
	hkMaxReviewText   = 200
	hkMaxReviewCount  = 1e6
	hkMaxReviewRound  = 1000
)

type hkTaskEvent struct {
	Source   string     `json:"source"`
	HeadSHA  string     `json:"head_sha"`
	BaseSHA  string     `json:"base_sha"`
	MergeSHA string     `json:"merge_sha"`
	Reviews  []hkReview `json:"reviews"`
}

// hkReview is store.TaskEventReview: round, pass and lens carry no omitempty
// on hk's side, so an absent one reads as 0 or "".
type hkReview struct {
	ReviewedHead   string     `json:"reviewed_head"`
	ReviewedBase   string     `json:"reviewed_base"`
	PatchID        string     `json:"patch_id"`
	Round          int        `json:"round"`
	Pass           int        `json:"pass"`
	Lens           string     `json:"lens"`
	ReviewerJobID  string     `json:"reviewer_job_id"`
	Family         string     `json:"family"`
	Profile        string     `json:"profile"`
	Model          string     `json:"model"`
	Harness        string     `json:"harness"`
	Effort         string     `json:"effort"`
	Executor       string     `json:"executor"`
	PolicyRevision string     `json:"policy_revision"`
	ContractSource string     `json:"contract_source"`
	Verdict        string     `json:"verdict"`
	Critical       *int       `json:"critical"`
	High           *int       `json:"high"`
	Medium         *int       `json:"medium"`
	Low            *int       `json:"low"`
	StartedAt      *time.Time `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at"`
	// The review-history fields (hk 3208, ReviewHistory in change_payload.go).
	Reviewer            *string `json:"reviewer"`
	SameFamily          *bool   `json:"same_family"`
	Delta               *bool   `json:"delta"`
	Quiet               *bool   `json:"quiet"`
	OutsideDiffBlocking *int    `json:"outside_diff_blocking"`
	PatchDigest         *string `json:"patch_digest"`
}

func hkRulesCheck(raw []byte) string {
	var in hkTaskEvent
	if e := json.Unmarshal(raw, &in); e != nil {
		return "undecodable: " + e.Error()
	}
	for name, v := range map[string]string{"head_sha": in.HeadSHA, "base_sha": in.BaseSHA, "merge_sha": in.MergeSHA} {
		if v != "" && !hkGitSHARE.MatchString(v) {
			return fmt.Sprintf("%s must be 40 lower-case hex digits", name)
		}
	}
	if len(in.Reviews) > 0 && in.Source != "mergewake.review" {
		return "reviews belong to mergewake.review"
	}
	if len(in.Reviews) > hkMaxEventReviews {
		return fmt.Sprintf("reviews has more than %d items", hkMaxEventReviews)
	}
	for i, rv := range in.Reviews {
		if e := hkCheckReview(rv); e != nil {
			return fmt.Sprintf("reviews[%d]: %v", i, e)
		}
	}
	return ""
}

func hkCheckReview(rv hkReview) error {
	if !hkGitSHARE.MatchString(rv.ReviewedHead) {
		return errors.New("reviewed_head must be 40 lower-case hex digits")
	}
	if rv.ReviewedBase != "" && !hkGitSHARE.MatchString(rv.ReviewedBase) {
		return errors.New("reviewed_base must be 40 lower-case hex digits")
	}
	if rv.PatchID != "" && !hkPatchIDRE.MatchString(rv.PatchID) {
		return errors.New("patch_id must be 7 to 64 lower-case hex digits")
	}
	if rv.Round < 1 || rv.Round > hkMaxReviewRound || rv.Pass < 1 || rv.Pass > hkMaxReviewRound {
		return fmt.Errorf("round and pass must be 1 to %d", hkMaxReviewRound)
	}
	if !hkReviewTokenRE.MatchString(rv.Lens) {
		return errors.New("lens must be a short [a-z0-9_.-] token")
	}
	for name, v := range map[string]string{"verdict": rv.Verdict, "family": rv.Family} {
		if v != "" && !hkReviewTokenRE.MatchString(v) {
			return fmt.Errorf("%s must be a short [a-z0-9_.-] token", name)
		}
	}
	if rv.Profile != "" && !hkReviewProfileRE.MatchString(rv.Profile) {
		return errors.New("profile must be a short [a-z0-9_.@-] token")
	}
	for name, v := range map[string]string{"reviewer_job_id": rv.ReviewerJobID, "model": rv.Model, "harness": rv.Harness, "effort": rv.Effort, "executor": rv.Executor, "policy_revision": rv.PolicyRevision, "contract_source": rv.ContractSource} {
		if !hkValidReviewText(v) {
			return fmt.Errorf("%s must be printable text of at most %d bytes", name, hkMaxReviewText)
		}
	}
	for name, v := range map[string]*int{"critical": rv.Critical, "high": rv.High, "medium": rv.Medium, "low": rv.Low} {
		if v != nil && (*v < 0 || *v > hkMaxReviewCount) {
			return fmt.Errorf("%s must be 0 to %d", name, int(hkMaxReviewCount))
		}
	}
	if rv.Reviewer != nil && (*rv.Reviewer == "" || !hkValidReviewText(*rv.Reviewer)) {
		return fmt.Errorf("reviewer must be non-empty printable text of at most %d bytes", hkMaxReviewText)
	}
	if rv.PatchDigest != nil && !hkPatchDigestRE.MatchString(*rv.PatchDigest) {
		return errors.New("patch_digest must be 12 to 128 lower-case hex digits")
	}
	if rv.OutsideDiffBlocking != nil && (*rv.OutsideDiffBlocking < 0 || *rv.OutsideDiffBlocking > hkMaxReviewCount) {
		return fmt.Errorf("outside_diff_blocking must be 0 to %d", int(hkMaxReviewCount))
	}
	if rv.StartedAt != nil && rv.FinishedAt != nil && rv.FinishedAt.Before(*rv.StartedAt) {
		return errors.New("finished_at is before started_at")
	}
	return nil
}

func hkValidReviewText(v string) bool {
	if len(v) > hkMaxReviewText {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
