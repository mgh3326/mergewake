package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

func (s *Service) ledger() outcomes.Ledger {
	return outcomes.Ledger{Dir: s.Config.StateDir, Now: s.Now}
}
func trailerValues(body, key string) []string {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `: ([^\r\n]+?)[\t ]*$`)
	matches := re.FindAllStringSubmatch(body, -1)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		values = append(values, match[1])
	}
	return values
}

func trailer(body, key string) string {
	values := trailerValues(body, key)
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

var profileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

// authorProfile parses a Mergewake-Author-Profile value. Workers write either
// `profile` (the rstack wake brief) or `profile@effort`. An effort outside the
// known levels keeps the profile and leaves effort empty; a name that fails
// profileName or a value with more than one `@` records nothing.
func authorProfile(value string) (profile, effort string) {
	name, level, _ := strings.Cut(value, "@")
	if strings.Contains(level, "@") || !profileName.MatchString(name) {
		return "", ""
	}
	switch level {
	case "low", "medium", "high", "xhigh", "max":
		return name, level
	}
	return name, ""
}

func titleGradePattern() string {
	grades := config.Grades()
	for i, grade := range grades {
		grades[i] = regexp.QuoteMeta(grade)
	}
	return strings.Join(grades, "|")
}

// taskMarker reads both halves of the `(grade X, Tn)` routing marker in an hk
// task title. Its grade alternatives come from the config grade scale.
var taskMarker = regexp.MustCompile(`(?i)(?:\(|\b)grade (` + titleGradePattern() + `),\s*(T[0-3])\b`)

func taskMarkerMatch(title string) []string {
	m := taskMarker.FindStringSubmatch(title)
	if m == nil || !config.ValidGrade(strings.ToUpper(m[1])) {
		return nil
	}
	return m
}

// taskScale extracts the tier from a valid task title marker for panewire.
func taskScale(title string) (string, bool) {
	m := taskMarkerMatch(title)
	if m == nil {
		return "", false
	}
	return strings.ToUpper(m[2]), true
}

// taskTitleScale reads the grade and tier from the title of the hk task the PR
// names. found is false when the PR names no task or the sink cannot read one;
// err is an hk read failure. A title without a marker is found, with empty
// grade and tier.
func (s *Service) taskTitleScale(ctx context.Context, r config.Repo, p github.Pull) (grade, tier string, found bool, err error) {
	jobs, ok := s.Sink.(interface {
		TaskJob(context.Context, string) (string, string, error)
	})
	if !ok {
		return "", "", false, nil
	}
	task, err := s.prTask(ctx, r, p)
	if err != nil || task == "" {
		return "", "", false, err
	}
	_, title, err := jobs.TaskJob(ctx, task)
	if err != nil {
		return "", "", false, err
	}
	if m := taskMarkerMatch(title); m != nil {
		return strings.ToUpper(m[1]), strings.ToUpper(m[2]), true, nil
	}
	return "", "", true, nil
}
func validTier(t string) bool {
	switch t {
	case "T0", "T1", "T2", "T3":
		return true
	}
	return false
}
func parsedTime(value string) *time.Time {
	v, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	return &v
}
func (s *Service) ObservePR(ctx context.Context, r config.Repo, p github.Pull, o Options) error {
	if o.DryRun {
		return nil
	}
	// Every trailer comes from the PR body, else the head commit. A failed
	// commit read leaves the trailer-derived fields as an earlier observation
	// of the row recorded them, like a failed hk read below.
	trailersUnread := false
	read := func(key string, valid func(string) bool) string {
		v, e := s.prTrailer(ctx, r, p, key, valid)
		if e != nil {
			trailersUnread = true
		}
		return v
	}
	grade, tier := read("Mergewake-Grade", config.ValidGrade), read("Mergewake-Tier", validTier)
	family := read("Mergewake-Author-Family", nil)
	profile := read("Mergewake-Author-Profile", nil)
	experiment := strings.SplitN(read("Mergewake-Experiment", nil), "/", 2)
	model := read("Mergewake-Author-Model", nil)
	// The grade and tier come from the PR's own evidence: the trailer, else the
	// hk task title. Never from config. hk is read before the ledger lock is
	// taken, and only when a trailer leaves a gap.
	titleUnread := false
	if grade == "" || tier == "" {
		g, t, _, e := s.taskTitleScale(ctx, r, p)
		titleUnread = e != nil
		if grade == "" {
			grade = g
		}
		if tier == "" {
			tier = t
		}
	}
	err := s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) {
		v.Head = p.Head.SHA
		v.Shadow = o.Shadow
		if opened := parsedTime(p.CreatedAt); opened != nil {
			v.OpenedAt = *opened
		}
		if !trailersUnread {
			v.Family = config.CanonicalFamily(family)
			v.Profile = ""
			v.Effort = ""
			v.Experiment = ""
			v.Arm = ""
			v.Profile, v.Effort = authorProfile(profile)
			if len(experiment) == 2 {
				v.Experiment = experiment[0]
				v.Arm = experiment[1]
			}
			v.Model = model
			if reviewer, ok := s.Config.Review.Profiles[v.Profile]; ok && v.Model == "" {
				for i := 0; i+1 < len(reviewer.Args); i += 2 {
					if reviewer.Args[i] == "--model" {
						v.Model = reviewer.Args[i+1]
					}
				}
			}
		}
		if grade != "" || !(titleUnread || trailersUnread) {
			v.Grade = grade
		}
		if tier != "" || !(titleUnread || trailersUnread) {
			v.Tier = tier
		}
		if p.ClosedAt != nil {
			v.ClosedAt = parsedTime(*p.ClosedAt)
		}
		if p.MergedAt != nil {
			v.MergedAt = parsedTime(*p.MergedAt)
			v.Final = true
			v.Decision = "landed"
			if v.MergedAt != nil && !v.OpenedAt.IsZero() {
				seconds := v.MergedAt.Sub(v.OpenedAt).Seconds()
				if seconds >= 0 {
					v.TimeToMerge = &seconds
				}
			}
		} else if p.State == "closed" {
			v.Final = true
			v.Decision = "blocked"
		} else {
			v.Final = false
			v.ReconcileFinished = false
			v.ReconcileDueAt = nil
		}
	})
	if err != nil {
		return err
	}
	s.observePRTaskEvent(ctx, r, p, o)
	state, err := s.ledger().Read()
	if err != nil {
		return err
	}
	v := state.Latest[outcomes.Key(r.Name, p.Number)]
	if _, known := v.Reviews[p.Head.SHA]; !known {
		if source, ok := s.GH.(interface {
			ReviewOutcomes(context.Context, string, int, string) (map[string]map[string]int, error)
		}); ok {
			reviews, e := source.ReviewOutcomes(ctx, r.Name, p.Number, s.Config.Review.Login)
			if e != nil {
				return e
			}
			if len(reviews) > 0 {
				// The review order comes from the review history, never from
				// the map above, which has none.
				var order []string
				if lister, ok := s.GH.(headLister); ok {
					heads, e := lister.ReviewedHeads(ctx, r.Name, p.Number, s.Config.Review.Login)
					if e != nil {
						return e
					}
					for _, h := range heads {
						order = append(order, h.SHA)
					}
				}
				sameFamily := false
				reviewers := map[string]map[string]string{}
				for _, head := range slices.Sorted(maps.Keys(reviews)) {
					passes, e := s.GH.ReviewPasses(ctx, r.Name, p.Number, head, s.Config.Review.Login)
					if e != nil {
						return e
					}
					for _, receipt := range passes {
						sameFamily = sameFamily || receipt.SameFamily
						if receipt.Reviewer != "" {
							if reviewers[head] == nil {
								reviewers[head] = map[string]string{}
							}
							reviewers[head][strconv.Itoa(receipt.Pass)] = receipt.Reviewer
						}
					}
				}
				if e = s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) {
					if v.Reviews == nil {
						v.Reviews = map[string]map[string]int{}
					}
					for head, counts := range reviews {
						v.Reviews[head] = counts
					}
					v.SetReviewedHeadOrder(order)
					v.Findings = map[string]int{}
					for _, counts := range v.Reviews {
						for severity, n := range counts {
							v.Findings[severity] += n
						}
					}
					v.SameFamily = v.SameFamily || sameFamily
					for head, passes := range reviewers {
						for pass, reviewer := range passes {
							v.SetReviewer(head, pass, reviewer)
						}
					}
				}); e != nil {
					return e
				}
			}
		}
	}
	if v.FirstCIGreen == nil {
		if source, ok := s.GH.(interface {
			FirstPRCI(context.Context, string, github.Pull, []string) (*bool, error)
		}); ok {
			head, e := s.GH.Head(ctx, r.Name, r.Branch)
			if e != nil {
				return e
			}
			policy, e := s.Policy(ctx, r, head)
			if e == nil {
				first, e := source.FirstPRCI(ctx, r.Name, p, policy.RequiredChecks)
				if e != nil {
					return e
				}
				if first != nil {
					if e = s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) { v.FirstCIGreen = first }); e != nil {
						return e
					}
				}
			} else if !github.NotFound(e) {
				return e
			}
		}
	}
	if v.MergedAt != nil && v.Reverted == nil {
		if v.RevertPR == 0 && p.MergeCommitSHA != "" {
			if source, ok := s.GH.(interface {
				WatchRevert(context.Context, string, int, string) (*github.Pull, error)
			}); ok {
				recovery, e := source.WatchRevert(ctx, r.Name, p.Number, p.MergeCommitSHA)
				if e != nil {
					return e
				}
				if recovery != nil {
					v.RevertPR = recovery.Number
					v.RevertURL = recovery.HTMLURL
					if e = s.recordRevert(r.Name, p.Number, v.RevertURL); e != nil {
						return e
					}
				}
			}
		}

		var reverted *bool
		if v.RevertPR > 0 {
			recovery, e := s.GH.Pull(ctx, r.Name, v.RevertPR)
			if e != nil {
				return e
			}
			if recovery.MergedAt != nil {
				at := parsedTime(*recovery.MergedAt)
				if at != nil && !at.Before(*v.MergedAt) && !at.After(v.MergedAt.Add(7*24*time.Hour)) {
					yes := true
					reverted = &yes
				}
			}
		}
		if reverted == nil && s.now().After(v.MergedAt.Add(7*24*time.Hour)) {
			no := false
			reverted = &no
		}
		if reverted != nil {
			if e := s.ledger().Update(r.Name, p.Number, func(v *outcomes.Outcome) { v.Reverted = reverted }); e != nil {
				return e
			}
		}
	}
	// The revert event follows the revert PR, not the seven-day verdict.
	s.observeRevertTaskEvent(ctx, r, p, v.MergedAt, v.RevertPR, o)
	return nil
}
func (s *Service) recordReview(repo string, pr int, head string, findings []github.Finding, receipt github.PassReceipt, dry bool) error {
	if dry {
		return nil
	}
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		counts := map[string]int{}
		for _, f := range findings {
			severity := f.Severity
			if severity == "" {
				severity = "unspecified"
			}
			counts[severity]++
		}
		if v.Reviews == nil {
			v.Reviews = map[string]map[string]int{}
		}
		v.Reviews[head] = counts
		v.AddReviewedHead(head)
		v.Findings = map[string]int{}
		for _, review := range v.Reviews {
			for k, n := range review {
				v.Findings[k] += n
			}
		}
		v.SameFamily = v.SameFamily || receipt.SameFamily
		if receipt.Reviewer != "" {
			v.SetReviewer(head, strconv.Itoa(receipt.Pass), receipt.Reviewer)
		}
	})
}
func (s *Service) recordDecision(repo string, pr int, action string, shadow, sameFamily, dry bool) error {
	if dry {
		return nil
	}
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		v.Action = action
		v.Shadow = shadow
		v.SameFamily = v.SameFamily || sameFamily
		if !v.Final {
			if action == "merge" {
				v.Decision = "eligible"
			} else {
				v.Decision = "blocked"
			}
		}
	})
}
func (s *Service) recordRevert(repo string, pr int, url string) error {
	return s.ledger().Update(repo, pr, func(v *outcomes.Outcome) {
		v.RevertURL = url
		n, _ := strconv.Atoi(url[strings.LastIndex(url, "/")+1:])
		v.RevertPR = n
	})
}

// closedUnmergedInterval is how often a PR closed without merging is re-read
// while its ClosedAt is known; closedUnmergedWindow bounds that re-reading to
// a week after closing, the same span as the revert verdict.
const (
	closedUnmergedInterval = 24 * time.Hour
	closedUnmergedWindow   = 7 * 24 * time.Hour
)

// inClosedUnmergedWindow reports whether v is a final, unmerged row whose
// ClosedAt is still inside closedUnmergedWindow: such a row is read daily.
func inClosedUnmergedWindow(v outcomes.Outcome, now time.Time) bool {
	return v.Final && v.MergedAt == nil && v.ClosedAt != nil && now.Before(v.ClosedAt.Add(closedUnmergedWindow))
}

// writeReconcileFlags records the flags the reconcile computed from seen,
// unless the ledger row's Final, MergedAt or ClosedAt changed since seen was
// read — an evaluation that observed the PR reopen, merge or reclose in
// between owns those fields — in which case the row is left unfinished and
// undated so the next reconcile reads it again.
func (s *Service) writeReconcileFlags(repo string, pr int, seen outcomes.Outcome, finished bool, due *time.Time) error {
	return s.ledger().Update(repo, pr, func(current *outcomes.Outcome) {
		if current.Final != seen.Final || !sameTimePtr(current.MergedAt, seen.MergedAt) || !sameTimePtr(current.ClosedAt, seen.ClosedAt) {
			current.ReconcileFinished = false
			current.ReconcileDueAt = nil
			return
		}
		current.ReconcileFinished = finished
		current.ReconcileDueAt = due
	})
}

func sameTimePtr(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// Revisit PRs no longer in the open list: this observes external desk merges,
// closures and recovery merges, even while the daemon itself is shadow-only.
// isOpen is asked for each row as it is reached, not once up front, so a PR
// that opened after the ledger was read is not taken for a closed one.
// It publishes nothing: it tells the outcome publisher what it recorded.
func (s *Service) ReconcileOutcomes(ctx context.Context, r config.Repo, isOpen func(pr int) bool, o Options) error {
	if o.DryRun {
		return nil
	}
	state, err := s.ledger().Read()
	if err != nil {
		return err
	}
	var errs []error
	var closedRowsRead, closedRowsSkippedNotDue int
	// A row is revisited at its due time while its revert verdict or event is unsettled.
	sent, _ := s.taskEventKeys(r.Name)
	for _, key := range slices.Sorted(maps.Keys(state.Latest)) {
		v := state.Latest[key]
		if v.Repo != r.Name || isOpen(v.PR) {
			continue
		}
		pendingEvent := s.revertEventPending(r, v, o, sent)
		// A row the pre-window logic finished with no due time is still inside
		// its ClosedAt week: read it once more so a reopen it missed is recorded.
		// That read puts it on the daily schedule, or finishes it after the window.
		legacyInWindow := v.ReconcileFinished && v.ReconcileDueAt == nil && inClosedUnmergedWindow(v, s.now())
		if v.ReconcileFinished && !pendingEvent && !legacyInWindow {
			closedRowsSkippedNotDue++
			continue
		}
		// A closed-unmerged row with a known ClosedAt can still hide a reopen
		// that was closed or merged before any evaluation saw it open, so it is
		// re-read daily until a week after closing — the revert window — then
		// finished without another read.
		if v.Final && v.MergedAt == nil && v.ClosedAt != nil && !s.now().Before(v.ClosedAt.Add(closedUnmergedWindow)) {
			if err := s.writeReconcileFlags(r.Name, v.PR, v, true, nil); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if v.ReconcileDueAt != nil && s.now().Before(*v.ReconcileDueAt) {
			closedRowsSkippedNotDue++
			continue
		}
		closedRowsRead++
		p, e := s.GH.Pull(ctx, r.Name, v.PR)
		if e == nil {
			s.finalizeStatus(r, p)
			e = s.ObservePR(ctx, r, p, o)
		}
		if e != nil {
			errs = append(errs, e)
			continue
		}
		latest, readErr := s.ledger().Read()
		if readErr != nil {
			errs = append(errs, readErr)
			continue
		}
		v = latest.Latest[key]
		finished, daily := false, false
		switch {
		case !v.Final:
		case v.MergedAt == nil && v.Reverted == nil:
			daily = inClosedUnmergedWindow(v, s.now())
			finished = !daily
		case v.MergedAt == nil || v.Reverted != nil:
			finished = true
		}
		// Re-read sent keys because ObservePR can settle a revert event during
		// this visit. A pending event remains eligible at the same hourly cadence.
		sent, _ = s.taskEventKeys(r.Name)
		var due *time.Time
		switch {
		case finished && !s.revertEventPending(r, v, o, sent):
		case daily:
			d := s.now().Add(closedUnmergedInterval)
			due = &d
		default:
			d := s.now().Add(time.Hour)
			due = &d
		}
		if err := s.writeReconcileFlags(r.Name, v.PR, v, finished, due); err != nil {
			errs = append(errs, err)
		}
	}
	slog.Info("closed PR outcome reconciliation", "repo", r.Name, "closed_rows_read", closedRowsRead, "closed_rows_skipped_not_due", closedRowsSkippedNotDue)
	if closedRowsRead > 0 {
		s.nudgeOutcomes()
	}
	return errors.Join(errs...)
}

// outcomePublishErrorMax bounds the error an outcome-publish-error audit
// keeps, in bytes.
const outcomePublishErrorMax = 512

// outcomePublishDetail is the error text an outcome-publish-error audit
// keeps: invalid UTF-8 replaced, then cut on a character boundary to
// outcomePublishErrorMax bytes.
func outcomePublishDetail(msg string) string {
	return clip(strings.ToValidUTF8(msg, "\uFFFD"), outcomePublishErrorMax)
}

// nudgeOutcomes tells the outcome publisher, when there is one, that the ledger
// has new outcomes.
func (s *Service) nudgeOutcomes() {
	if s.OutcomesChanged != nil {
		s.OutcomesChanged()
	}
}

// FlushOutcomes is the automatic flush of one repo's outcomes, run by the
// outcome publisher (never by Land or the reconcile). A publication failure does not fail the pass: it is audited as
// outcome-publish-error and logged, and the outcomes stay in the outbox for
// the next flush. A ledger or IO failure, or an audit row that cannot be
// written, is still returned. The operator's `mergewake outcomes sync` calls
// Ledger.Flush directly and stays strict.
func (s *Service) FlushOutcomes(ctx context.Context, repo string) error {
	err := s.ledger().Flush(ctx, outcomes.CLI{Runner: s.Runner, Command: s.Config.Outcomes.Command}, repo)
	var failed *outcomes.PublishError
	if !errors.As(err, &failed) {
		return err
	}
	detail := outcomePublishDetail(failed.Error())
	slog.Warn("outcome publish failed; retained in outbox", "repo", repo, "count", failed.Count, "error", detail)
	if e := s.audit(repo, "outcome-publish-error", map[string]any{"repo": repo, "count": failed.Count, "error": detail}, false); e != nil {
		slog.Warn("outcome-publish-error audit not written", "repo", repo, "error", e)
		return fmt.Errorf("outcome-publish-error audit for %s: %w", repo, e)
	}
	return nil
}
