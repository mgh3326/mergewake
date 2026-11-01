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

// Review findings go straight to the PR's author, not through the desk. Each
// pass of the pr loop collects the PR's findings no delivery has named yet —
// mergewake's own once the head's review is complete, and CodeRabbit's
// Critical and Major threads — and sends them as one lane event to the author
// lane: the PR's Mergewake-Task → hk task → bound job → that job's lane. A PR
// with no bound job falls back to the repo lane, then the desk. The author
// acknowledges with `panewire inbox ack`; with no ACK (and no disposition on
// the named threads) within daemon.pr.findings.ack_after the event is re-sent
// once, and after another window it is escalated to the owning lane. Findings
// the reviewer marked outside the PR's Acceptance contract go to the owner
// once. mergewake never respawns a worker: a job that is already terminal has
// its findings sent to the owner, who decides.

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
	AckBy string `json:"ack_by,omitempty"`
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
		if disposed(t, own, verify) {
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
func disposed(t github.ReviewThread, own string, verify fixCheck) bool {
	if t.IsResolved {
		return true
	}
	if len(t.Comments) == 0 {
		return false
	}
	first := t.Comments[0]
	isOwn := own != "" && policy.NormalizeAuthor(first.Author) == own
	if mustFix(github.ThreadSeverity(first.Author, first.Body)) {
		return blockDisposition(t, isOwn, verify) != ""
	}
	return answered(t, isOwn)
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

// findingsRoute resolves the author lane without guessing: Mergewake-Task →
// hk task → its bound job (refs.job_id) → the job's hub lane (a worker's lane
// is its label). The owner is the job's owner_lane, else the lane READY and
// BLOCKED go to. A terminal job sends its findings to the owner; a PR with no
// bound job, or a job whose lane is not recorded, falls back to the repo lane,
// then the desk. A read error retries next pass rather than rerouting.
func (s *Service) findingsRoute(ctx context.Context, r config.Repo, pr github.Pull) (findingsRoute, error) {
	var rt findingsRoute
	owner, oVia, e := s.notifyLane(ctx, r, pr)
	if e != nil {
		return rt, e
	}
	rt.owner, rt.oVia = owner, oVia
	task, e := s.prTask(ctx, r, pr)
	if e != nil {
		return rt, e
	}
	jobs, ok := s.Sink.(interface {
		TaskJob(context.Context, string) (string, string, error)
		Job(context.Context, string) (integrations.JobRecord, error)
	})
	if task != "" && ok {
		job, _, e := jobs.TaskJob(ctx, task)
		if e != nil {
			return rt, e
		}
		if job != "" {
			rec, e := jobs.Job(ctx, job)
			switch {
			case errors.Is(e, integrations.ErrJobNotFound):
			case e != nil:
				return rt, e
			default:
				if laneName.MatchString(rec.OwnerLane) {
					rt.owner, rt.oVia = rec.OwnerLane, "job "+job+" owner"
				}
				switch {
				case terminalJob(rec.State):
					rt.lane, rt.via = rt.owner, fmt.Sprintf("job %s is %s; owner (%s)", job, rec.State, rt.oVia)
					return rt, nil
				case rec.Role != "builder" && laneName.MatchString(rec.Label):
					rt.lane, rt.via = rec.Label, "task "+task+" job "+job
					return rt, nil
				}
			}
		}
	}
	if laneName.MatchString(r.Lane) {
		rt.lane, rt.via = r.Lane, "repo lane"
		return rt, nil
	}
	desk := s.Config.Notifier.FallbackLane
	if desk == "" {
		desk = s.Config.Notifier.Lane
	}
	rt.lane, rt.via = desk, "desk"
	if rt.owner == "" {
		rt.owner, rt.oVia = desk, "desk"
	}
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
	required := `required: fix every critical/high/Major finding (reply "Fixed in <sha>" or "Dismiss: <evidence>"); reply to every other with a disposition (fix, dismiss with a reason, or defer to an hk task)`
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
	if scope == "owner" {
		lane, via = rt.owner, rt.oVia
	}
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
	}
	typ := "findings"
	if scope == "owner" {
		typ = "info"
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

// followUpDeliveries advances this PR's unacknowledged author deliveries: an
// ACK or a disposition on every named thread closes one; past ack_after it is
// re-sent once; past another window it is escalated to the owner once. A
// notifier that cannot read ACKs leaves deliveries as sent.
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
		if d.PR != pr.Number || d.Scope != "author" || !d.AckedAt.IsZero() || !d.EscalatedAt.IsZero() {
			continue
		}
		done := true
		for _, id := range d.Threads {
			if t, ok := threads[id]; ok && !disposed(t, own, verify) {
				done = false
				break
			}
		}
		if done {
			d.AckedAt, d.AckBy, changed = now, "disposition", true
			continue
		}
		if !canRead {
			continue
		}
		inbox, read := inboxes[d.Lane]
		if !read && inboxErr[d.Lane] == nil {
			var e error
			if inbox, e = reader.AckedEvents(ctx, d.Lane); e != nil {
				inboxErr[d.Lane] = e
				errs = append(errs, fmt.Errorf("findings ack on %s: %w", d.Lane, e))
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
		case !d.ResentAt.IsZero() && now.Sub(d.ResentAt) >= window:
			text := fmt.Sprintf("[mergewake] ESC %s#%d head=%s | findings event %s to lane %s has no ACK (sent %s, re-sent %s) | the owner decides: relay, hand off or re-scope; mergewake never respawns a worker",
				r.Name, pr.Number, short7(d.Head), d.ID, d.Lane, d.SentAt.Format(time.RFC3339), d.ResentAt.Format(time.RFC3339))
			if !o.DryRun && d.Owner != "" {
				reportCtx, cancel := reportContext(ctx)
				e := s.Notifier.Notify(reportCtx, integrations.Event{ID: d.ID + "-escalated", Text: text, Lane: d.Owner, Type: "decision"})
				cancel()
				if e != nil {
					errs = append(errs, fmt.Errorf("findings escalation to %s: %w", d.Owner, e))
					continue
				}
			}
			d.EscalatedAt, changed = now, true
			errs = append(errs, s.audit(r.Name, "findings-escalated", map[string]any{"pr": d.PR, "id": d.ID, "lane": d.Lane, "owner": d.Owner}, o.DryRun))
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
