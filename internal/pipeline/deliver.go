package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// Review findings go to the job-bound author lane. An unresolved delivery
// reaches the desk only after its configured age or number of distinct heads.
// Inbox acknowledgment stops reminders, while a disposition stops aging.

// findingsLaneText bounds the event text: panewire refuses a lane.event over
// 2048 bytes.
const findingsLaneText = 2000

// findingsDelivery is one delivered lane event and how it stands.
type findingsDelivery struct {
	ID   string `json:"id"`
	PR   int    `json:"pr"`
	Head string `json:"head"`
	// Scope is "author" for in-scope findings (ACK tracked) or "owner" for
	// out-of-scope ones handed back once.
	Scope       string    `json:"scope"`
	Threads     []string  `json:"threads"`
	Lane        string    `json:"lane"`
	Via         string    `json:"via"`
	Owner       string    `json:"owner"`
	Text        string    `json:"text"`
	SentAt      time.Time `json:"sent_at"`
	ResentAt    time.Time `json:"resent_at,omitzero"`
	EscalatedAt time.Time `json:"escalated_at,omitzero"`
	AckedAt     time.Time `json:"acked_at,omitzero"`
	// AckBy is "inbox" for a `panewire inbox ack`, "disposition" when every
	// named thread was answered or resolved first.
	AckBy string   `json:"ack_by,omitempty"`
	Heads []string `json:"heads,omitempty"`
}

// pendingFinding is one thread a delivery names.
type pendingFinding struct {
	thread   string
	severity string
	path     string
	line     int
	link     string
	blocking bool
}

// findingsRoute is where a PR's findings go: the author lane and the owning
// lane escalations and out-of-scope findings go to.
type findingsRoute struct {
	lane, via   string
	owner, oVia string
}

// DeliverFindings sends the PR's undelivered review findings to its author
// lane and follows up deliveries nobody acknowledged. Dry-run records the
// decisions without sending or writing state; shadow still delivers, since
// telling a lane is not merging.
func (s *Service) DeliverFindings(ctx context.Context, r config.Repo, n int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "findings-error", map[string]any{"pr": n, "error": integrations.Detail(retErr)}, o.DryRun))
		}
	}()
	if !s.Config.Daemon.PR.Findings.On() {
		return nil
	}
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if pr.State != "open" || pr.Draft || pr.Base.Ref != r.Branch {
		return nil
	}
	fb, e := s.GH.ReviewFeedback(ctx, r.Name, n)
	if e != nil {
		return e
	}
	done, e := s.GH.ReviewClaimed(ctx, r.Name, n, pr.Head.SHA+"-done")
	if errors.Is(e, github.ErrPollSkipped) {
		done, e = false, nil
	}
	if e != nil {
		return e
	}
	ledger, e := s.loadDeliveries(r.Name)
	if e != nil {
		return e
	}
	// A follow-up that cannot read an ACK or send is reported after this
	// pass's new deliveries, so an unreachable hub never holds new findings.
	// A fix claim GitHub could not check reads as not yet disposed: the author
	// still gets (or keeps being followed up on) that finding.
	verify := s.fixVerifier(ctx, r.Name, pr.Head.SHA).verified
	changed, followErr := s.followUpDeliveries(ctx, r, pr, fb, ledger, o, verify)
	inScope, outScope := s.undeliveredFindings(pr, fb, ledger, done, verify)
	if len(inScope)+len(outScope) > 0 {
		route, e := s.findingsRoute(ctx, r, pr)
		if e != nil {
			return e
		}
		for _, batch := range []struct {
			scope    string
			findings []pendingFinding
		}{{"author", inScope}, {"owner", outScope}} {
			if len(batch.findings) == 0 {
				continue
			}
			d, e := s.sendFindings(ctx, r, pr, route, batch.scope, batch.findings, o)
			if e != nil {
				// Keep what was already delivered; a retry of the failed batch
				// reuses its event ID, which panewire dedupes.
				if changed && !o.DryRun {
					e = errors.Join(e, s.saveDeliveries(r.Name, ledger))
				}
				return errors.Join(followErr, e)
			}
			ledger = append(ledger, d)
			changed = true
		}
	}
	if changed && !o.DryRun {
		followErr = errors.Join(followErr, s.saveDeliveries(r.Name, ledger))
	}
	return followErr
}

// undeliveredFindings lists the unresolved, unanswered findings no delivery
// has named yet. mergewake's own findings on the current head wait for the
// head's -done marker, so one review round goes out as one event.
func (s *Service) undeliveredFindings(pr github.Pull, fb github.ReviewFeedback, ledger []findingsDelivery, headDone bool, verify fixCheck) (inScope, outScope []pendingFinding) {
	named := map[string]bool{}
	for _, d := range ledger {
		if d.PR == pr.Number {
			for _, t := range d.Threads {
				named[t] = true
			}
		}
	}
	own := policy.NormalizeAuthor(s.Config.Review.Login)
	for _, t := range fb.Threads {
		if len(t.Comments) == 0 || t.IsResolved || t.ID == "" || named[t.ID] {
			continue
		}
		first := t.Comments[0]
		author := policy.NormalizeAuthor(first.Author)
		severity := github.ThreadSeverity(first.Author, first.Body)
		isOwn := own != "" && author == own && strings.Contains(first.Body, "<!-- mergewake-severity:")
		f := pendingFinding{thread: t.ID, severity: severity, path: t.Path, line: t.Line, link: first.URL}
		switch {
		case isOwn:
			if postedOn(first) == pr.Head.SHA && !headDone {
				continue
			}
			f.blocking = severity == "critical" || severity == "high" || severity == "blocking"
		case author == "coderabbitai" && (severity == "critical" || severity == "major"):
			f.blocking = true
		default:
			continue
		}
		if disposed(t, own, verify, s.Config.Review.FleetAuthors) {
			continue
		}
		if severity == "advisory" {
			outScope = append(outScope, f)
		} else {
			inScope = append(inScope, f)
		}
	}
	return inScope, outScope
}

// disposed reports whether finding thread t no longer needs the author: it
// is resolved, or answered by the gate's own rule — a blocking finding
// (mergewake critical/high, CodeRabbit Critical/Major) only by a verified fix
// commit or a reasoned dismissal, any other by any human answer.
func disposed(t github.ReviewThread, own string, verify fixCheck, fleet ...[]string) bool {
	if t.IsResolved {
		return true
	}
	if len(t.Comments) == 0 {
		return false
	}
	first := t.Comments[0]
	isOwn := own != "" && policy.NormalizeAuthor(first.Author) == own
	if mustFix(github.ThreadSeverity(first.Author, first.Body)) {
		return blockDisposition(t, isOwn, verify, fleet...) != ""
	}
	return answered(t, isOwn, fleet...)
}

// mustFix is a severity the author must fix or dismiss with evidence
// (mergewake critical/high, CodeRabbit Critical/Major).
func mustFix(s string) bool {
	return s == "critical" || s == "high" || s == "blocking" || s == "major"
}

// prTask reads the Mergewake-Task trailer: PR body first, then the head commit.
func (s *Service) prTask(ctx context.Context, r config.Repo, pr github.Pull) (string, error) {
	if t := trailer(pr.Body, "Mergewake-Task"); integrations.TaskID.MatchString(t) {
		return t, nil
	}
	c, e := s.GH.Commit(ctx, r.Name, pr.Head.SHA)
	if e != nil {
		return "", e
	}
	if t := trailer(c.Commit.Message, "Mergewake-Task"); integrations.TaskID.MatchString(t) {
		return t, nil
	}
	return "", nil
}

// terminalJob reports the hk job states after which the author attempt no
// longer writes to the branch.
func terminalJob(state string) bool {
	return state == "completed" || state == "escalated" || state == "lost"
}

// findingsRoute uses the same task/job binding as reap, without caching an
// open PR's binding. A worker or lead keeps its author lane after completion.
// Missing ownership routes to the desk; a read error retries without guessing.
func (s *Service) findingsRoute(ctx context.Context, r config.Repo, pr github.Pull) (findingsRoute, error) {
	desk := s.Config.Notifier.FallbackLane
	if desk == "" {
		desk = s.Config.Notifier.Lane
	}
	rt := findingsRoute{owner: desk, oVia: "desk"}
	// Open PR bindings may change, so share the reap binding interface without
	// its merged-PR cache.
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		return rt, e
	}
	if jobs, ok := s.Sink.(reapJobs); task != "" && ok {
		job, _, e := jobs.TaskJob(ctx, task)
		if e != nil {
			return rt, e
		}
		if job != "" {
			rec, e := jobs.Job(ctx, job)
			if e != nil && !errors.Is(e, integrations.ErrJobNotFound) {
				return rt, e
			}
			if e == nil {
				for _, lane := range []string{rec.HubLane, rec.Lane, rec.Label} {
					if laneName.MatchString(lane) {
						rt.lane, rt.via = lane, "task "+task+" job "+job
						return rt, nil
					}
				}
			}
		}
	}
	// Explicit PR routing is evidence of ownership; a repo/desk default is not.
	lane, via, e := s.notifyLane(ctx, r, pr)
	if e != nil {
		return rt, e
	}
	if via == "notify-lane trailer" {
		rt.lane, rt.via = lane, via
		return rt, nil
	}
	rt.lane, rt.via = desk, "unknown author; desk"
	return rt, nil
}

// severityRank orders findings most severe first.
func severityRank(s string) int {
	switch s {
	case "critical":
		return 0
	case "high", "blocking":
		return 1
	case "major":
		return 2
	case "medium":
		return 3
	case "low":
		return 4
	}
	return 5
}

// findingsText renders one delivery: what was found, what is required and how
// to acknowledge, within the lane event bound. Findings that do not fit are
// counted, not dropped silently.
func findingsText(r config.Repo, pr github.Pull, scope, lane string, fs []pendingFinding) string {
	tag := "FINDINGS"
	required := `required: fix every critical/high/Major finding; accepted replies: "Fixed in <sha>", "Dismiss: <evidence>", "Disposition: fixed <sha>", "Disposition: dismiss <evidence>", or "Disposition: defer <hk task>" (defer only for nonblocking findings); fixes require a commit in the PR after the finding; reply to every other with a disposition`
	if scope == "owner" {
		tag = "OUT-OF-SCOPE"
		required = "the reviewer marked these outside the PR's Acceptance contract: the owner decides (dismiss, defer to an hk task, or widen the contract); they do not block"
	}
	head := fmt.Sprintf("[mergewake] %s %s#%d head=%s | %d finding(s) for lane %s: ", tag, r.Name, pr.Number, short7(pr.Head.SHA), len(fs), lane)
	tail := " | " + required
	if scope == "author" {
		tail += fmt.Sprintf(" | ack: panewire inbox list --lane %s --unread, then panewire inbox ack <id>", lane)
	}
	var b strings.Builder
	b.WriteString(head)
	for i, f := range fs {
		sev := f.severity
		if sev == "" {
			sev = "unrated"
		}
		ref := f.link
		if ref == "" {
			ref = f.thread
		}
		if f.blocking {
			sev += " [blocks merge]"
		}
		item := fmt.Sprintf("%d) %s %s:%d %s; ", i+1, sev, f.path, f.line, ref)
		more := fmt.Sprintf("+%d more on the PR; ", len(fs)-i)
		if b.Len()+len(item)+len(more)+len(tail) > findingsLaneText {
			b.WriteString(more)
			break
		}
		b.WriteString(item)
	}
	text := strings.TrimSuffix(b.String(), "; ") + tail
	for len(text) > findingsLaneText || !utf8.ValidString(text) {
		text = clip(text, findingsLaneText)
	}
	return text
}

// sendFindings emits one delivery and returns its ledger record. Dry-run
// records the decision only.
func (s *Service) sendFindings(ctx context.Context, r config.Repo, pr github.Pull, rt findingsRoute, scope string, fs []pendingFinding, o Options) (findingsDelivery, error) {
	slices.SortStableFunc(fs, func(a, b pendingFinding) int {
		if d := severityRank(a.severity) - severityRank(b.severity); d != 0 {
			return d
		}
		if c := strings.Compare(a.path, b.path); c != 0 {
			return c
		}
		return a.line - b.line
	})
	ids := make([]string, len(fs))
	for i, f := range fs {
		ids[i] = f.thread
	}
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	lane, via := rt.lane, rt.via
	d := findingsDelivery{
		ID:      fmt.Sprintf("mergewake-%s-%s-%d-%s", map[string]string{"author": "findings", "owner": "out-of-scope"}[scope], r.Name, pr.Number, hex.EncodeToString(sum[:6])),
		PR:      pr.Number,
		Head:    pr.Head.SHA,
		Scope:   scope,
		Threads: ids,
		Lane:    lane,
		Via:     via,
		Owner:   rt.owner,
		Text:    findingsText(r, pr, scope, lane, fs),
		SentAt:  s.now().UTC(),
		Heads:   []string{pr.Head.SHA},
	}
	typ := "findings"
	if scope == "owner" {
		typ = "info"
	}
	if rt.via == "unknown author; desk" {
		typ = "decision"
	}
	if !o.DryRun {
		reportCtx, cancel := reportContext(ctx)
		defer cancel()
		if e := s.Notifier.Notify(reportCtx, integrations.Event{ID: d.ID, Text: d.Text, Lane: lane, Type: typ}); e != nil {
			return d, fmt.Errorf("findings delivery to %s: %w", lane, e)
		}
	}
	return d, s.audit(r.Name, "findings-delivered", d, o.DryRun)
}

// ackReader is the notifier side that can tell which events a lane
// acknowledged (integrations.Panewire).
type ackReader interface {
	AckedEvents(ctx context.Context, lane string) (map[string]bool, error)
}

// followUpDeliveries resends an unacknowledged delivery once. Every unresolved
// delivery keeps aging, including one acknowledged in the inbox.
func (s *Service) followUpDeliveries(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, ledger []findingsDelivery, o Options, verify fixCheck) (bool, error) {
	reader, canRead := s.Notifier.(ackReader)
	window := s.Config.Daemon.PR.Findings.AckWindow()
	now := s.now().UTC()
	threads := map[string]github.ReviewThread{}
	for _, t := range fb.Threads {
		threads[t.ID] = t
	}
	own := policy.NormalizeAuthor(s.Config.Review.Login)
	changed := false
	var errs []error
	// One inbox read per lane per pass, however many deliveries it holds.
	inboxes := map[string]map[string]bool{}
	inboxErr := map[string]error{}
	for i := range ledger {
		d := &ledger[i]
		if d.PR != pr.Number || d.Scope != "author" || d.AckBy == "disposition" || !d.EscalatedAt.IsZero() {
			continue
		}
		done := true
		for _, id := range d.Threads {
			if t, ok := threads[id]; ok && !disposed(t, own, verify, s.Config.Review.FleetAuthors) {
				done = false
				break
			}
		}
		if done {
			d.AckedAt, d.AckBy, changed = now, "disposition", true
			continue
		}
		desk := s.Config.Notifier.FallbackLane
		if desk == "" {
			desk = s.Config.Notifier.Lane
		}
		if d.Owner != desk {
			d.Owner = desk
			changed = true
		}
		if len(d.Heads) == 0 {
			d.Heads = []string{d.Head}
			changed = true
		}
		if !slices.Contains(d.Heads, pr.Head.SHA) {
			d.Heads = append(d.Heads, pr.Head.SHA)
			changed = true
		}
		aged := now.Sub(d.SentAt) >= s.Config.Daemon.PR.Findings.EscalationWindow() || (s.Config.Daemon.PR.Findings.EscalationRounds() > 0 && len(d.Heads) >= s.Config.Daemon.PR.Findings.EscalationRounds())
		if aged {
			text := fmt.Sprintf("[mergewake] ESC %s#%d | findings still open after %d head rounds and %s | %s", r.Name, pr.Number, len(d.Heads), now.Sub(d.SentAt).Round(time.Minute), d.Text)
			if !o.DryRun && d.Owner != "" && d.Owner != d.Lane {
				nctx, cancel := reportContext(ctx)
				e := s.Notifier.Notify(nctx, integrations.Event{ID: d.ID + "-escalated", Text: clip(text, findingsLaneText), Lane: d.Owner, Type: "decision"})
				cancel()
				if e != nil {
					errs = append(errs, e)
					continue
				}
			}
			d.EscalatedAt, changed = now, true
			errs = append(errs, s.audit(r.Name, "findings-escalated", map[string]any{"pr": d.PR, "id": d.ID, "lane": d.Lane, "owner": d.Owner}, o.DryRun))
			continue
		}
		if !d.AckedAt.IsZero() || !canRead {
			continue
		}
		inbox, read := inboxes[d.Lane]
		if !read && inboxErr[d.Lane] == nil {
			var e error
			if inbox, e = reader.AckedEvents(ctx, d.Lane); e != nil {
				inboxErr[d.Lane] = e
				errs = append(errs, s.audit(r.Name, "findings-ack-unavailable", map[string]any{"pr": d.PR, "lane": d.Lane, "error": integrations.Detail(e)}, o.DryRun))
			} else {
				inboxes[d.Lane] = inbox
			}
		}
		if inboxErr[d.Lane] != nil {
			// Without an ACK reading, re-sending or escalating would guess.
			continue
		}
		acked := inbox[d.ID] || inbox[d.ID+"-resend"]
		switch {
		case acked:
			d.AckedAt, d.AckBy, changed = now, "inbox", true
			errs = append(errs, s.audit(r.Name, "findings-acked", map[string]any{"pr": d.PR, "id": d.ID, "lane": d.Lane}, o.DryRun))
		case d.ResentAt.IsZero() && now.Sub(d.SentAt) >= window:
			if !o.DryRun {
				reportCtx, cancel := reportContext(ctx)
				e := s.Notifier.Notify(reportCtx, integrations.Event{ID: d.ID + "-resend", Text: resendText(d.Text, window), Lane: d.Lane, Type: "findings"})
				cancel()
				if e != nil {
					errs = append(errs, fmt.Errorf("findings re-send to %s: %w", d.Lane, e))
					continue
				}
			}
			d.ResentAt, changed = now, true
			errs = append(errs, s.audit(r.Name, "findings-resent", map[string]any{"pr": d.PR, "id": d.ID, "lane": d.Lane}, o.DryRun))
		}
	}
	return changed, errors.Join(errs...)
}

// resendText marks a re-send in the event text, within the lane event bound.
func resendText(text string, window time.Duration) string {
	marked := strings.Replace(text, "[mergewake] FINDINGS", fmt.Sprintf("[mergewake] FINDINGS (re-sent: no ack in %s)", window), 1)
	if len(marked) > findingsLaneText {
		return text
	}
	return marked
}

func (s *Service) deliveriesFile(repo string) string {
	return filepath.Join(s.state(repo), "findings-deliveries.json")
}

func (s *Service) loadDeliveries(repo string) ([]findingsDelivery, error) {
	b, e := os.ReadFile(s.deliveriesFile(repo))
	if errors.Is(e, fs.ErrNotExist) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var ds []findingsDelivery
	if e = json.Unmarshal(b, &ds); e != nil {
		return nil, fmt.Errorf("%s: %w", s.deliveriesFile(repo), e)
	}
	return ds, nil
}

// saveDeliveries keeps the deliveries of the last 30 days, newest 1000 at most.
func (s *Service) saveDeliveries(repo string, ds []findingsDelivery) error {
	cutoff := s.now().Add(-30 * 24 * time.Hour)
	ds = slices.DeleteFunc(ds, func(d findingsDelivery) bool { return d.SentAt.Before(cutoff) })
	if len(ds) > 1000 {
		ds = ds[len(ds)-1000:]
	}
	b, e := json.Marshal(ds)
	if e != nil {
		return e
	}
	return fsutil.Atomic(s.deliveriesFile(repo), b, 0600)
}
