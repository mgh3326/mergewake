package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// Round3Directive is sent to the author when round 3 ends without a pass.
const Round3Directive = "stop patching; ask a consult one grade above your seat in another family, give it every finding so far, ask for the root cause"

var roundHoldReleaseRegex = regexp.MustCompile(`(?i)\bMergewake-Round-Hold:\s*release\b`)

// roundRuleState persists whether round 3 directive and escalation have run,
// and whether round 5 hold and decision event are active or released.
type roundRuleState struct {
	// Round 3
	Round3DirectiveSent bool      `json:"round3_directive_sent,omitempty"`
	Round3Escalated     bool      `json:"round3_escalated,omitempty"`
	Round3Output        string    `json:"round3_output,omitempty"`
	Round3At            time.Time `json:"round3_at,omitzero"`

	// Round 5
	Round5Held         bool      `json:"round5_held,omitempty"`
	Round5EventSent    bool      `json:"round5_event_sent,omitempty"`
	Round5Released     bool      `json:"round5_released,omitempty"`
	Round5ReleasedBy   string    `json:"round5_released_by,omitempty"`
	Round5At           time.Time `json:"round5_at,omitzero"`
	Round5CommentCount int       `json:"round5_comment_count,omitempty"`
}

func (s *Service) roundRuleFile(repo string, pr int) string {
	return filepath.Join(s.state(repo), "round-rules", strconv.Itoa(pr)+".json")
}

func (s *Service) loadRoundRuleState(repo string, pr int) roundRuleState {
	var st roundRuleState
	_, _ = fsutil.ReadJSON(s.roundRuleFile(repo, pr), &st)
	return st
}

func (s *Service) saveRoundRuleState(repo string, pr int, st roundRuleState) error {
	return fsutil.WriteJSON(s.roundRuleFile(repo, pr), st)
}

// roundEndsWithoutPass reports whether the PR has at least one critical or high
// finding still open after the verdict.
func (s *Service) roundEndsWithoutPass(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback) (bool, []github.ReviewThread) {
	verify := s.fixVerifier(ctx, r.Name, pr.Head.SHA).verified
	own := policy.NormalizeAuthor(s.Config.Review.Login)
	var open []github.ReviewThread
	for _, t := range fb.Threads {
		if t.IsResolved || len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		sev := github.ThreadSeverity(first.Author, first.Body)
		if !mustFix(sev) {
			continue
		}
		if disposed(t, own, verify, s.Config.Review.FleetAuthors) {
			continue
		}
		open = append(open, t)
	}
	return len(open) > 0, open
}

type escalateFinding struct {
	Ref      string `json:"ref"`
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Title    string `json:"title"`
	Body     string `json:"body"`
}

func buildFindingsFile(fb github.ReviewFeedback) ([]byte, error) {
	list := make([]escalateFinding, 0, len(fb.Threads))
	for _, t := range fb.Threads {
		if len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		ref := t.ID
		if ref == "" {
			ref = first.URL
		}
		sev := github.ThreadSeverity(first.Author, first.Body)
		if sev == "" {
			sev = "unrated"
		}
		title := firstLine(first.Body)
		if title == "" {
			title = t.Path + ":" + strconv.Itoa(t.Line)
		}
		list = append(list, escalateFinding{
			Ref:      ref,
			Severity: sev,
			Path:     t.Path,
			Line:     t.Line,
			Title:    title,
			Body:     first.Body,
		})
	}
	return json.Marshal(list)
}

// escalateAuthorRound3 executes `panewire escalate-author` once per (task, PR, round 3).
func (s *Service) escalateAuthorRound3(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, o Options, st *roundRuleState) {
	if o.DryRun || st.Round3Escalated {
		return
	}
	st.Round3Escalated = true
	st.Round3At = s.now().UTC()
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)

	task, _ := s.prTask(ctx, r, pr)
	job, _, _, _ := s.boundJob(ctx, task)

	data, err := buildFindingsFile(fb)
	if err != nil {
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{"pr": pr.Number, "error": err.Error()}, o.DryRun)
		return
	}

	cleanRepo := strings.ReplaceAll(r.Name, "/", "-")
	tmp, err := os.CreateTemp("", fmt.Sprintf("mw-findings-%s-%d-*.json", cleanRepo, pr.Number))
	if err != nil {
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{"pr": pr.Number, "error": err.Error()}, o.DryRun)
		return
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = s.audit(r.Name, "escalate-author-error", map[string]any{"pr": pr.Number, "error": err.Error()}, o.DryRun)
		return
	}
	tmp.Close()

	args := []string{
		"escalate-author",
		"--task", task,
		"--job", job,
		"--pr", fmt.Sprintf("%s#%d", r.Name, pr.Number),
		"--round", "3",
		"--findings-file", tmp.Name(),
	}

	cmdCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	out, err := s.Runner.Run(cmdCtx, "panewire", args, "", "")
	outputStr := strings.TrimSpace(string(out))
	st.Round3Output = outputStr
	_ = s.saveRoundRuleState(r.Name, pr.Number, *st)

	if err == nil {
		_ = s.audit(r.Name, "escalate-author", map[string]any{
			"pr":     pr.Number,
			"round":  3,
			"rc":     0,
			"output": outputStr,
		}, o.DryRun)
		if s.Log != nil {
			s.Log.Info("escalate-author succeeded", "pr", pr.Number, "output", outputStr)
		}
		return
	}

	exitCode := -1
	var ce *integrations.CommandError
	if errors.As(err, &ce) {
		if ee, ok := ce.Err.(interface{ ExitCode() int }); ok {
			exitCode = ee.ExitCode()
		} else if strings.Contains(ce.Err.Error(), "exit status ") {
			fmt.Sscanf(ce.Err.Error(), "exit status %d", &exitCode)
		}
		if outputStr == "" && ce.Stderr != "" {
			outputStr = strings.TrimSpace(ce.Stderr)
		}
	} else if ee, ok := err.(interface{ ExitCode() int }); ok {
		exitCode = ee.ExitCode()
	} else if strings.Contains(err.Error(), "exit status ") {
		fmt.Sscanf(err.Error(), "exit status %d", &exitCode)
	}

	if exitCode == 2 {
		_ = s.audit(r.Name, "escalate-author", map[string]any{
			"pr":     pr.Number,
			"round":  3,
			"rc":     2,
			"output": outputStr,
		}, o.DryRun)
		if s.Log != nil {
			s.Log.Info("escalate-author refused", "pr", pr.Number, "output", outputStr)
		}
		return
	}

	_ = s.audit(r.Name, "escalate-author-error", map[string]any{
		"pr":     pr.Number,
		"round":  3,
		"rc":     exitCode,
		"error":  err.Error(),
		"output": outputStr,
	}, o.DryRun)
	if s.Log != nil {
		s.Log.Warn("escalate-author error", "pr", pr.Number, "error", err, "output", outputStr)
	}
}

func checkRoundHoldRelease(comments []github.IssueComment, reviewLogin string) (bool, string) {
	for _, c := range comments {
		if c.User.Type == "Bot" || strings.HasSuffix(c.User.Login, "[bot]") || (reviewLogin != "" && c.User.Login == reviewLogin) {
			continue
		}
		if roundHoldReleaseRegex.MatchString(c.Body) {
			return true, c.User.Login
		}
	}
	return false, ""
}

// designLane returns the task's lane, falling back to desk when the lane is
// the desk or unknown.
func (s *Service) designLane(ctx context.Context, r config.Repo, pr github.Pull) string {
	desk := s.deskLane()
	task, err := s.prTask(ctx, r, pr)
	if err != nil || task == "" {
		return desk
	}
	l, ok := s.Sink.(interface {
		Lane(context.Context, string) (string, error)
	})
	if !ok {
		return desk
	}
	lane, err := l.Lane(ctx, task)
	if err != nil || lane == "" || lane == desk {
		return desk
	}
	return lane
}

func round5DecisionText(r config.Repo, pr github.Pull, rounds int, open []github.ReviewThread) string {
	head := fmt.Sprintf("[mergewake] ESC %s#%d | round %d ended without a pass (%d open finding(s))", r.Name, pr.Number, rounds, len(open))
	choices := " | choices: 1) cut the scope (the part that keeps drawing findings goes to a follow-up task, the rest merges) or 2) one SS consult on the design | reply with PR comment: Mergewake-Round-Hold: release"

	var details strings.Builder
	for i, t := range open {
		if len(t.Comments) == 0 {
			continue
		}
		first := t.Comments[0]
		sev := github.ThreadSeverity(first.Author, first.Body)
		item := fmt.Sprintf("; finding %d: %s %s:%d %s", i+1, sev, t.Path, t.Line, t.ID)
		if len(head)+details.Len()+len(item)+len(choices) > findingsLaneText {
			break
		}
		details.WriteString(item)
	}
	text := head + details.String() + choices
	return clip(text, findingsLaneText)
}

func (s *Service) issueComments(ctx context.Context, repo string, n int) (comments []github.IssueComment, err error) {
	if s.GH == nil {
		return nil, nil
	}
	defer func() {
		if r := recover(); r != nil {
			comments, err = nil, nil
		}
	}()
	return s.GH.IssueComments(ctx, repo, n)
}

// isRoundHoldActive checks whether PR is held under round 5 hold and not released.
func (s *Service) isRoundHoldActive(ctx context.Context, r config.Repo, pr github.Pull) bool {
	st := s.loadRoundRuleState(r.Name, pr.Number)
	if !st.Round5Held || st.Round5Released {
		return false
	}
	comments, err := s.issueComments(ctx, r.Name, pr.Number)
	if err == nil && len(comments) > st.Round5CommentCount {
		if released, by := checkRoundHoldRelease(comments[st.Round5CommentCount:], s.Config.Review.Login); released {
			st.Round5Released = true
			st.Round5ReleasedBy = by
			_ = s.saveRoundRuleState(r.Name, pr.Number, st)
			return false
		}
	}
	return true
}

// checkRound5Gate evaluates the round 5 hold condition during gate evaluation.
func (s *Service) checkRound5Gate(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, rounds int, reviewed bool, v *gateVerdict) bool {
	st := s.loadRoundRuleState(r.Name, pr.Number)
	open, _ := s.roundEndsWithoutPass(ctx, r, pr, fb)

	if st.Round5Held {
		if st.Round5Released {
			return false
		}
		comments, err := s.issueComments(ctx, r.Name, pr.Number)
		if err == nil && len(comments) > st.Round5CommentCount {
			if released, by := checkRoundHoldRelease(comments[st.Round5CommentCount:], s.Config.Review.Login); released {
				st.Round5Released = true
				st.Round5ReleasedBy = by
				_ = s.saveRoundRuleState(r.Name, pr.Number, st)
				return false
			}
		}
		v.Reasons = append(v.Reasons, "round_hold")
		v.Review.Block = "round_hold"
		v.Review.Hold = "round_hold"
		v.State = "blocked"
		return true
	}

	if reviewed && rounds >= 5 && open {
		comments, _ := s.issueComments(ctx, r.Name, pr.Number)
		st.Round5Held = true
		st.Round5At = s.now().UTC()
		st.Round5CommentCount = len(comments)
		_ = s.saveRoundRuleState(r.Name, pr.Number, st)

		v.Reasons = append(v.Reasons, "round_hold")
		v.Review.Block = "round_hold"
		v.Review.Hold = "round_hold"
		v.State = "blocked"
		return true
	}

	return false
}

// sendRound5Decision sends exactly one decision event to the design lane if round 5 ends without a pass.
func (s *Service) sendRound5Decision(ctx context.Context, r config.Repo, pr github.Pull, fb github.ReviewFeedback, rounds int, o Options) error {
	st := s.loadRoundRuleState(r.Name, pr.Number)
	if st.Round5EventSent || st.Round5Released {
		return nil
	}
	open, openThreads := s.roundEndsWithoutPass(ctx, r, pr, fb)
	if !st.Round5Held && !(rounds >= 5 && open) {
		return nil
	}

	lane := s.designLane(ctx, r, pr)
	text := round5DecisionText(r, pr, rounds, openThreads)
	eventID := fmt.Sprintf("mergewake-round-hold-%s-%d-5", r.Name, pr.Number)

	if o.Reporting() {
		reportCtx, cancel := reportContext(ctx)
		defer cancel()
		if err := s.Notifier.Notify(reportCtx, integrations.Event{
			ID:   eventID,
			Lane: lane,
			Type: "decision",
			Text: text,
		}); err != nil {
			return err
		}
		st.Round5EventSent = true
		return s.saveRoundRuleState(r.Name, pr.Number, st)
	}
	return nil
}
