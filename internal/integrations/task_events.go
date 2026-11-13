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
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/fleetevent"
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
	// Review-history fields (hk 3296). A nil pointer is "unknown" and is left
	// out of the JSON: hk reads a missing field as unknown and an explicit
	// false or 0 as a fact, so mergewake sends one only for a value it knows.
	Reviewer            *string                `json:"reviewer,omitempty"`
	SameFamily          *bool                  `json:"same_family,omitempty"`
	Delta               *bool                  `json:"delta,omitempty"`
	Quiet               *bool                  `json:"quiet,omitempty"`
	OutsideDiffBlocking *int                   `json:"outside_diff_blocking,omitempty"`
	PatchDigest         *string                `json:"patch_digest,omitempty"`
	FindingFacts        *[]ReviewFindingRecord `json:"finding_facts,omitempty"`
}

// ReviewFindingRecord records each finding's relationship to new code and
// earlier threads for the round that posted it.
type ReviewFindingRecord struct {
	Path            string `json:"path"`
	Line            int    `json:"line"`
	Title           string `json:"title"`
	AddedAfterFirst *bool  `json:"added_after_first_head,omitempty"`
	RepeatsEarlier  *bool  `json:"repeats_earlier_thread,omitempty"`
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

// PatchID returns s when it is a hex patch id hk takes (7 to 64 lower-case hex
// digits), else "" (omitted).
func PatchID(s string) string {
	if hkPatchID.MatchString(s) {
		return s
	}
	return ""
}

// PatchDigest returns s when it is a digest hk takes for patch_digest (12 to
// 128 lower-case hex digits), else "" (omitted).
func PatchDigest(s string) string {
	if hkPatchDigest.MatchString(s) {
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

var (
	hkPatchID = regexp.MustCompile(`^[0-9a-f]{7,64}$`)
	hkToken   = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)
	hkProfile = regexp.MustCompile(`^[a-z0-9_.@-]{1,64}$`)
	// hkPatchDigest is hk's rule for patch_digest, the digest half of a patch receipt.
	hkPatchDigest = regexp.MustCompile(`^[0-9a-f]{12,128}$`)
)

// hk's bounds on a review record (handoffkeep internal/store/change_payload.go).
const (
	maxReviewText  = 200
	maxReviewCount = 1_000_000
	maxReviewRound = 1000
)

func validReviewText(v string) bool {
	return len(v) <= maxReviewText && !strings.ContainsFunc(v, unicode.IsControl)
}

// ForHK fits rv to hk's review rules, field by field, so one bad field never
// costs the record and one bad record never costs the event. It returns the
// record with every field hk would refuse cleared (named in cleared), or
// drop != "" when the record has no usable identity left: a head that is not a
// commit, or a lens, pass or round hk requires and cannot be guessed here.
func (rv ReviewRecord) ForHK() (out ReviewRecord, cleared []string, drop string) {
	switch {
	case !shaRE.MatchString(rv.ReviewedHead):
		return rv, nil, "reviewed_head is not a 40-hex commit"
	case rv.Pass < 1 || rv.Pass > maxReviewRound:
		return rv, nil, fmt.Sprintf("pass %d is outside 1..%d", rv.Pass, maxReviewRound)
	case rv.Round < 1 || rv.Round > maxReviewRound:
		return rv, nil, fmt.Sprintf("round %d is outside 1..%d", rv.Round, maxReviewRound)
	case !hkToken.MatchString(rv.Lens):
		return rv, nil, "lens is not a [a-z0-9_.-] token"
	}
	clear := func(name string, bad bool, field *string) {
		if bad {
			*field = ""
			cleared = append(cleared, name)
		}
	}
	clear("reviewed_base", rv.ReviewedBase != "" && !shaRE.MatchString(rv.ReviewedBase), &rv.ReviewedBase)
	clear("patch_id", rv.PatchID != "" && !hkPatchID.MatchString(rv.PatchID), &rv.PatchID)
	clear("family", rv.Family != "" && !hkToken.MatchString(rv.Family), &rv.Family)
	clear("verdict", rv.Verdict != "" && !hkToken.MatchString(rv.Verdict), &rv.Verdict)
	clear("profile", rv.Profile != "" && !hkProfile.MatchString(rv.Profile), &rv.Profile)
	clear("reviewer_job_id", !validReviewText(rv.ReviewerJobID), &rv.ReviewerJobID)
	clear("model", !validReviewText(rv.Model), &rv.Model)
	clear("harness", !validReviewText(rv.Harness), &rv.Harness)
	clear("effort", !validReviewText(rv.Effort), &rv.Effort)
	clear("executor", !validReviewText(rv.Executor), &rv.Executor)
	clear("policy_revision", !validReviewText(rv.PolicyRevision), &rv.PolicyRevision)
	clear("contract_source", !validReviewText(rv.ContractSource), &rv.ContractSource)
	if rv.Reviewer != nil && (*rv.Reviewer == "" || !validReviewText(*rv.Reviewer)) {
		rv.Reviewer = nil
		cleared = append(cleared, "reviewer")
	}
	if rv.PatchDigest != nil && !hkPatchDigest.MatchString(*rv.PatchDigest) {
		rv.PatchDigest = nil
		cleared = append(cleared, "patch_digest")
	}
	if n := rv.OutsideDiffBlocking; n != nil && (*n < 0 || *n > maxReviewCount) {
		v := min(max(*n, 0), maxReviewCount)
		rv.OutsideDiffBlocking = &v
		cleared = append(cleared, "outside_diff_blocking")
	}
	for name, n := range map[string]*int{"critical": &rv.Critical, "high": &rv.High, "medium": &rv.Medium, "low": &rv.Low} {
		if *n < 0 || *n > maxReviewCount {
			*n = min(max(*n, 0), maxReviewCount)
			cleared = append(cleared, name)
		}
	}
	if rv.StartedAt != nil && rv.FinishedAt != nil && rv.FinishedAt.Before(*rv.StartedAt) {
		rv.FinishedAt = nil
		cleared = append(cleared, "finished_at")
	}
	slices.Sort(cleared)
	return rv, cleared, ""
}

// extended reports whether ev carries any optional field, and plain returns
// ev without them: the form every hk accepts.
func (ev TaskEvent) extended() bool {
	return ev.HeadSHA != "" || ev.BaseSHA != "" || ev.MergeSHA != "" || len(ev.Reviews) > 0
}

func (ev TaskEvent) withoutReviews() TaskEvent {
	ev.Reviews = nil
	return ev
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
	// and the event was accepted only when resent with fewer optional fields.
	FieldsRefused string `json:"-"`
	// FieldsRefusedMessage is hk's message on that first 400, bounded and
	// redacted; empty when hk sent none. FieldsResend names the form that was
	// accepted: ResendWithoutReviews keeps head, base and merge, ResendPlain
	// drops every optional field.
	FieldsRefusedMessage string `json:"-"`
	FieldsResend         string `json:"-"`
}

// The resend forms of a refused extended event, most complete first.
const (
	ResendWithoutReviews = "without-reviews"
	ResendPlain          = "plain"
)

// HKError is a non-2xx answer from hk. Code is hk's error code
// (`{"error": "<code>"}`) when it has the documented shape, else empty.
// Message is the body's "message" (else "reason") text, redacted, printable and
// at most maxErrorMessage bytes; the rest of the body is withheld.
type HKError struct {
	Status  int
	Code    string
	Message string
}

// maxErrorMessage bounds the hk message kept in an error and in the audit.
const maxErrorMessage = 200

func (e *HKError) Error() string {
	msg := ""
	if e.Message != "" {
		msg = ": " + e.Message
	}
	if e.Code == "" {
		return fmt.Sprintf("handoffkeep HTTP %d%s", e.Status, msg)
	}
	return fmt.Sprintf("handoffkeep HTTP %d %s%s", e.Status, e.Code, msg)
}

// hexRun matches a run of 32 or more hex digits. hk text redacts each one
// whole: a hex credential is kept by neither fleetevent.Redact's exemption for
// pure-hex runs nor its 48-character floor, and a commit SHA is redacted too.
var hexRun = regexp.MustCompile(`[0-9a-fA-F]{32,}`)

// errorMessage redacts s (hex runs first, then fleetevent.Redact), drops
// control characters and cuts it to maxErrorMessage bytes on a rune boundary.
func errorMessage(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, fleetevent.Redact(hexRun.ReplaceAllString(s, "[redacted]")))
	s = strings.TrimSpace(s)
	if len(s) <= maxErrorMessage {
		return s
	}
	cut := maxErrorMessage
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
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

// resendForm is one reduced form of a refused extended event.
type resendForm struct {
	name string
	ev   TaskEvent
}

// PostTaskEvent posts ev through the same handoffkeep endpoint and
// credentials Job reads. A network error or 5xx is retried, with doubling
// backoff, up to EventAttempts tries in total while ctx lasts; a 4xx is
// returned at once as *HKError.
//
// An extended payload (see TaskEvent) that hk answers with 400 is resent with
// fewer optional fields, so the event is never lost to them. A payload with
// reviews and head, base or merge is first resent with only the reviews dropped
// (one bad review record never strips head and base); a 400 to that, or a
// payload with nothing else to keep, is resent plain. The result then carries
// FieldsRefused, hk's message on the first 400 and the form that was accepted.
func (h Handoffkeep) PostTaskEvent(ctx context.Context, ev TaskEvent) (TaskEventResult, error) {
	res, used, e := h.postTaskEvent(ctx, ev, 0)
	var first *HKError
	if e == nil || !ev.extended() || !errors.As(e, &first) || first.Status != http.StatusBadRequest {
		return res, e
	}
	code := first.Code
	if code == "" {
		code = "invalid"
	}
	forms := []resendForm{{ResendPlain, ev.plain()}}
	if keep := ev.withoutReviews(); len(ev.Reviews) > 0 && keep.extended() {
		forms = slices.Insert(forms, 0, resendForm{ResendWithoutReviews, keep})
	}
	for i, form := range forms {
		// Each resend spends what is left of the EventAttempts budget, and at
		// least one try: the event is never lost to the new fields.
		var n int
		res, n, e = h.postTaskEvent(ctx, form.ev, max(1, h.attempts()-used))
		used += n
		if e == nil {
			res.FieldsRefused, res.FieldsRefusedMessage, res.FieldsResend = code, first.Message, form.name
			return res, nil
		}
		var again *HKError
		if i == len(forms)-1 || !errors.As(e, &again) || again.Status != http.StatusBadRequest {
			break
		}
	}
	return res, e
}

func (h Handoffkeep) attempts() int {
	if h.EventAttempts < 1 {
		return DefaultEventAttempts
	}
	return h.EventAttempts
}

// postTaskEvent is one send of ev with up to attempts tries (0: the full
// EventAttempts budget); it returns the tries used.
func (h Handoffkeep) postTaskEvent(ctx context.Context, ev TaskEvent, attempts int) (TaskEventResult, int, error) {
	c, e := hkJobEndpoint()
	if e != nil {
		return TaskEventResult{}, 0, e
	}
	if attempts < 1 {
		attempts = h.attempts()
	}
	backoff, timeout := h.EventBackoff, h.EventTimeout
	if backoff <= 0 {
		backoff = DefaultEventBackoff
	}
	if timeout <= 0 {
		timeout = DefaultEventTimeout
	}
	body, e := json.Marshal(ev)
	if e != nil {
		return TaskEventResult{}, 0, e
	}
	client := c.client(timeout)
	for try := 1; ; try++ {
		res, e := c.postEvent(ctx, client, body)
		var hk *HKError
		if e == nil || (errors.As(e, &hk) && !hk.Retryable()) || try >= attempts || ctx.Err() != nil {
			if e != nil && try > 1 {
				e = fmt.Errorf("%w (after %d attempts)", e, try)
			}
			return res, try, e
		}
		select {
		case <-ctx.Done():
			return res, try, fmt.Errorf("%w (after %d attempts)", e, try)
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
			Error   string `json:"error"`
			Message string `json:"message"`
			Reason  string `json:"reason"`
		}
		_ = json.NewDecoder(limited).Decode(&answer)
		if !hkErrorCode.MatchString(answer.Error) || hexRun.MatchString(answer.Error) {
			answer.Error = ""
		}
		message := answer.Message
		if message == "" {
			message = answer.Reason
		}
		return TaskEventResult{}, &HKError{Status: resp.StatusCode, Code: answer.Error, Message: errorMessage(message)}
	}
	var res TaskEventResult
	if e := json.NewDecoder(limited).Decode(&res); e != nil {
		return TaskEventResult{}, errors.New("handoffkeep response is not valid JSON")
	}
	return res, nil
}
