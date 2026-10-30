package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

var laneName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

// NotifyPR emits one lane event when the land decision for a PR head becomes
// ready or blocked: READY once required checks, review passes, the review gate
// and mergeability all pass at that head, BLOCKED once a required check fails
// or completed checks still leave the head held. Pending required checks emit
// nothing. Each (PR, head, state) notifies at most once across restarts.
//
// The pr role notifies under shadow too — telling a lane is not merging — while
// dry-run only prints the decision and writes no keys or events.
func (s *Service) NotifyPR(ctx context.Context, r config.Repo, n int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "pr-notify-error", map[string]any{"pr": n, "error": retErr.Error()}, o.DryRun))
		}
	}()
	if !s.Config.Daemon.PR.Notify.On() {
		return nil
	}
	pr, e := s.GH.Pull(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if pr.State != "open" || pr.Draft || pr.Base.Ref != r.Branch {
		return nil
	}
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return e
	}
	files, e := s.GH.Files(ctx, r.Name, n)
	if e != nil {
		return e
	}
	if len(files) != pr.ChangedFiles {
		return errors.New("incomplete PR file list")
	}
	d := p.ClassifyGuarded(files, r.Policy)
	states, e := s.GH.CheckStates(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
	if e != nil {
		return e
	}
	var failing []string
	pending := false
	for _, name := range p.RequiredChecks {
		switch states[name] {
		case github.Red:
			failing = append(failing, name)
		case github.Green:
		default:
			pending = true
		}
	}
	record := struct {
		PR      int      `json:"pr"`
		Head    string   `json:"head"`
		State   string   `json:"state"`
		Reasons []string `json:"reasons,omitempty"`
		Lane    string   `json:"lane,omitempty"`
		Via     string   `json:"via,omitempty"`
		Sent    bool     `json:"sent"`
		Deduped bool     `json:"deduped,omitempty"`
	}{PR: n, Head: pr.Head.SHA}
	state := ""
	var reasons []string
	switch {
	case len(failing) > 0:
		state, reasons = "blocked", []string{"failing checks: " + strings.Join(failing, ",")}
	case pending:
		record.State = "pending"
		return s.audit(r.Name, "pr-notify", record, o.DryRun)
	default:
		recovery := pr.Head.Ref == github.RecoveryBranch && pr.Head.Repo.FullName == r.Name
		watchChecks := p.RequiredChecks
		if len(p.WatchChecks) > 0 {
			watchChecks = p.WatchChecks
		}
		mainCI, e := s.GH.Check(ctx, r.Name, head, watchChecks)
		if e != nil {
			return e
		}
		unknownMain := false
		if !recovery {
			switch mainCI {
			case github.Red:
				reasons = append(reasons, "main CI red")
			case github.Green:
			default:
				unknownMain = true
			}
		}
		needed := 1
		if d.Class == "blocking" {
			needed = 2
		}
		receipts, e := s.GH.ReviewPasses(ctx, r.Name, n, pr.Head.SHA, s.Config.Review.Login)
		if e != nil {
			return e
		}
		count, complete := distinctPasses(receipts, needed)
		if !complete {
			switch want := needed - count; {
			case want == 1 && needed == 2:
				reasons = append(reasons, "needs 2nd review")
			case want == 1:
				reasons = append(reasons, "needs review")
			default:
				reasons = append(reasons, fmt.Sprintf("needs %d reviews", want))
			}
		} else {
			_, verdict, missing, e := s.reviewGate(ctx, r, n, d.Class, p, pr.Head.SHA)
			if e != nil {
				return e
			}
			if len(verdict.Blocking) > 0 {
				reasons = append(reasons, fmt.Sprintf("open blocking findings: %d", len(verdict.Blocking)))
			}
			if len(verdict.Triage) > 0 {
				reasons = append(reasons, fmt.Sprintf("unanswered findings: %d", len(verdict.Triage)))
			}
			if len(missing) > 0 {
				reasons = append(reasons, "missing required reviews: "+strings.Join(missing, ","))
			}
		}
		if (pr.Mergeable != nil && !*pr.Mergeable) || pr.MergeableState == "dirty" {
			reasons = append(reasons, "conflicts")
		}
		unknownMerge := pr.Mergeable == nil || pr.MergeableState == "unknown"
		switch {
		case len(reasons) > 0:
			state = "blocked"
		case unknownMain || unknownMerge:
			record.State = "pending"
			return s.audit(r.Name, "pr-notify", record, o.DryRun)
		default:
			state = "ready"
		}
	}
	lane, via, e := s.notifyLane(ctx, r, pr)
	if e != nil {
		return e
	}
	record.State, record.Reasons, record.Lane, record.Via = state, reasons, lane, via
	sha7 := pr.Head.SHA
	if len(sha7) > 7 {
		sha7 = sha7[:7]
	}
	title := pr.Title
	if state == "blocked" {
		title = strings.Join(reasons, "; ")
	}
	shown := lane
	if shown == "" {
		shown = "-"
	}
	text := fmt.Sprintf("[mergewake] %s %s#%d head=%s | %s | lane=%s via %s",
		strings.ToUpper(state), r.Name, n, sha7, title, shown, via)
	if o.DryRun {
		return s.audit(r.Name, "pr-notify", record, true)
	}
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "pr-notify-sent"), 255)
	if e != nil {
		return e
	}
	key := fmt.Sprintf("%s %d %s", state, n, pr.Head.SHA)
	if sent.has(key) {
		record.Deduped = true
		return s.audit(r.Name, "pr-notify", record, false)
	}
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	if e = s.Notifier.Notify(reportCtx, integrations.Event{
		ID:   fmt.Sprintf("mergewake-%s-%s-%d-%s", state, r.Name, n, pr.Head.SHA),
		Text: text,
		Lane: lane,
	}); e != nil {
		return e
	}
	record.Sent = true
	if e = sent.add(key); e != nil {
		return e
	}
	return s.audit(r.Name, "pr-notify", record, false)
}

// notifyLane resolves the destination lane without guessing: the handoffkeep
// task's lane via a Mergewake-Task trailer (PR body, then head commit message),
// an explicit Mergewake-Notify-Lane trailer in the same order, then the
// configured notifier lane. The returned rule names the choosing source and is
// recorded in the event text; a sink read error retries rather than reroutes.
func (s *Service) notifyLane(ctx context.Context, r config.Repo, pr github.Pull) (string, string, error) {
	message := ""
	fetched := false
	headMessage := func() (string, error) {
		if !fetched {
			fetched = true
			c, e := s.GH.Commit(ctx, r.Name, pr.Head.SHA)
			if e != nil {
				return "", e
			}
			message = c.Commit.Message
		}
		return message, nil
	}
	task := ""
	if t := trailer(pr.Body, "Mergewake-Task"); integrations.TaskID.MatchString(t) {
		task = t
	}
	if task == "" {
		msg, e := headMessage()
		if e != nil {
			return "", "", e
		}
		if t := trailer(msg, "Mergewake-Task"); integrations.TaskID.MatchString(t) {
			task = t
		}
	}
	if task != "" {
		if l, ok := s.Sink.(interface {
			Lane(context.Context, string) (string, error)
		}); ok {
			lane, e := l.Lane(ctx, task)
			if e != nil {
				return "", "", e
			}
			if laneName.MatchString(lane) {
				return lane, "task " + task, nil
			}
		}
	}
	notify := trailer(pr.Body, "Mergewake-Notify-Lane")
	if notify == "" {
		msg, e := headMessage()
		if e != nil {
			return "", "", e
		}
		notify = trailer(msg, "Mergewake-Notify-Lane")
	}
	if laneName.MatchString(notify) {
		return notify, "notify-lane trailer", nil
	}
	return s.Config.Notifier.Lane, "config", nil
}
