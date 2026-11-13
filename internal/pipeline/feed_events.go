package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/discord"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// feedClosedWindow is how far back a closed PR still posts: long enough to span
// a daemon restart or a weekend outage, short enough that a comment on an old
// PR, which puts it back in the recently-updated list, cannot post it again.
const feedClosedWindow = 72 * time.Hour

// Embed colors, the same ones the per-repo workflows use.
const (
	colorBlue   = 3447003
	colorGreen  = 5763719
	colorGrey   = 9807270
	colorYellow = 15844367
	colorRed    = 16711680
)

// closedLister is an optional GitHub capability: the most recently updated
// closed PRs against a branch. Without it merged and closed events do not post.
type closedLister interface {
	ClosedPulls(ctx context.Context, repo, branch string) ([]github.Pull, error)
}

// feedOn gates every hook on the channel it posts to, so a repo that cannot
// post spends no API calls. A dry run posts nothing; shadow mode still posts,
// like the pr role's lane events: reporting is not acting.
func (s *Service) feedOn(repo, channel string, o Options) bool {
	return s.Feed != nil && o.Reporting() && s.Feed.wants(repo, channel)
}

// feedError records a feed problem without returning it: a daemon role backs
// off on a returned error, and chat must not slow reviews or merges down.
func (s *Service) feedError(repo string, e error) {
	if e != nil {
		_ = s.audit(repo, "discord-feed-error", map[string]any{"error": e.Error()}, false)
	}
}

// feedBaseline is when the feed first ran for repo, kept across restarts.
// Older PRs do not post, so turning the feed on does not replay a repo's
// history into the channel.
func (s *Service) feedBaseline(repo string) (time.Time, error) {
	file := filepath.Join(s.state(repo), "discord-baseline")
	if b, e := os.ReadFile(file); e == nil {
		if t, pe := time.Parse(time.RFC3339, strings.TrimSpace(string(b))); pe == nil {
			return t, nil
		}
	} else if !os.IsNotExist(e) {
		return time.Time{}, e
	}
	now := s.now().UTC().Truncate(time.Second)
	return now, fsutil.Atomic(file, []byte(now.Format(time.RFC3339)+"\n"), 0600)
}

// FeedPRs posts this pass's PR events: a PR opened (a draft counts once it is
// ready for review), merged or closed, and a required check that failed on an
// open PR's head. It reads closed PRs itself, so a PR merged or closed outside
// the daemon, or while it was down, still posts. It takes the PR list after
// daemon.pr.skip_bots, so skipped bot PRs stay out of the feed too.
func (s *Service) FeedPRs(ctx context.Context, r config.Repo, open []github.Pull, o Options) {
	if s.feedOn(r.Name, "pr", o) {
		s.feedError(r.Name, s.feedPRState(ctx, r, open))
	}
	if s.feedOn(r.Name, "ci", o) {
		s.feedError(r.Name, s.feedPRChecks(ctx, r, open))
	}
}

// feedRead drops a read skipped inside a GitHub poll window: nothing is claimed
// for it, so the next pass asks again.
func feedRead(e error) error {
	if errors.Is(e, github.ErrPollSkipped) {
		return nil
	}
	return e
}

func (s *Service) feedPRState(ctx context.Context, r config.Repo, open []github.Pull) error {
	baseline, e := s.feedBaseline(r.Name)
	if e != nil {
		return e
	}
	seen, e := s.Feed.seen(r.Name)
	if e != nil {
		return e
	}
	var errs []error
	emit := func(pr github.Pull, kind, head string) {
		if seen(feedKey(kind, pr.Number, head)) {
			return
		}
		// The list endpoints leave out the change size; only the first sighting
		// pays for the read.
		full, e := s.GH.Pull(ctx, r.Name, pr.Number)
		if e == nil {
			e = s.Feed.Emit(feedEvent{Repo: r.Name, PR: pr.Number, Head: head, Kind: kind, Message: prMessage(r, full, kind, s.now())})
		}
		if e = feedRead(e); e != nil {
			errs = append(errs, fmt.Errorf("PR %d: %w", pr.Number, e))
		}
	}
	for _, pr := range open {
		created := parsedTime(pr.CreatedAt)
		if pr.Draft || created == nil || created.Before(baseline) {
			continue
		}
		// A force push is not a new PR, so this key has no head.
		emit(pr, "pr.opened", "-")
	}
	lister, ok := s.GH.(closedLister)
	if !ok {
		return errors.Join(errs...)
	}
	closed, e := lister.ClosedPulls(ctx, r.Name, r.Branch)
	if e != nil {
		return errors.Join(append(errs, feedRead(e))...)
	}
	since := s.now().Add(-feedClosedWindow)
	if baseline.After(since) {
		since = baseline
	}
	for _, pr := range closed {
		if s.Config.Daemon.PR.SkipBots && pr.User.Type == "Bot" {
			continue
		}
		kind, at := "pr.closed", pr.ClosedAt
		if pr.MergedAt != nil {
			kind, at = "pr.merged", pr.MergedAt
		}
		if at == nil {
			continue
		}
		if t := parsedTime(*at); t == nil || t.Before(since) {
			continue
		}
		emit(pr, kind, pr.Head.SHA)
	}
	return errors.Join(errs...)
}

func (s *Service) feedPRChecks(ctx context.Context, r config.Repo, open []github.Pull) error {
	seen, e := s.Feed.seen(r.Name)
	if e != nil {
		return e
	}
	var due []github.Pull
	for _, pr := range open {
		if !pr.Draft && !seen(feedKey("ci", pr.Number, pr.Head.SHA)) {
			due = append(due, pr)
		}
	}
	if len(due) == 0 {
		return nil
	}
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return feedRead(e)
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return feedRead(e)
	}
	var errs []error
	for _, pr := range due {
		failed, e := s.failedChecks(ctx, r.Name, pr.Head.SHA, p.RequiredChecks)
		if e == nil && len(failed) > 0 {
			var full github.Pull
			if full, e = s.GH.Pull(ctx, r.Name, pr.Number); e == nil {
				e = s.Feed.Emit(feedEvent{Repo: r.Name, PR: pr.Number, Head: pr.Head.SHA, Kind: "ci", Message: prCheckMessage(r, full, pr.Head.SHA, failed, s.now())})
			}
		}
		if e = feedRead(e); e != nil {
			errs = append(errs, fmt.Errorf("PR %d checks: %w", pr.Number, e))
		}
	}
	return errors.Join(errs...)
}

func (s *Service) failedChecks(ctx context.Context, repo, sha string, required []string) ([]string, error) {
	states, e := s.GH.CheckStates(ctx, repo, sha, required)
	if e != nil {
		return nil, e
	}
	var failed []string
	for _, name := range required {
		if states[name] == github.Red {
			failed = append(failed, name)
		}
	}
	return failed, nil
}

// feedBranchCI posts a red default-branch head once. The watch role calls it.
func (s *Service) feedBranchCI(ctx context.Context, r config.Repo, head string, checks []string, o Options) {
	if !s.feedOn(r.Name, "ci", o) {
		return
	}
	seen, e := s.Feed.seen(r.Name)
	if e != nil {
		s.feedError(r.Name, e)
		return
	}
	if seen(feedKey("ci", 0, head)) {
		return
	}
	failed, e := s.failedChecks(ctx, r.Name, head, checks)
	if e == nil && len(failed) > 0 {
		var c github.Commit
		if c, e = s.GH.Commit(ctx, r.Name, head); e == nil {
			e = s.Feed.Emit(feedEvent{Repo: r.Name, Head: head, Kind: "ci", Message: branchCheckMessage(r, c, head, failed, s.now())})
		}
	}
	s.feedError(r.Name, feedRead(e))
}

// feedWatch posts a watch outcome that took effect: a revert PR opened, a
// culprit reported or a hold. Callers skip shadow runs, where no revert opens.
func (s *Service) feedWatch(r config.Repo, result watchResult, o Options) {
	if !s.feedOn(r.Name, "revert", o) {
		return
	}
	s.feedError(r.Name, s.Feed.Emit(feedEvent{
		Repo: r.Name, PR: result.PR, Head: result.Head, Kind: "revert." + result.Action,
		Message: watchMessage(r, result, s.now()),
	}))
}

func prMessage(r config.Repo, pr github.Pull, kind string, at time.Time) discord.Message {
	title, color := "🆕 PR opened", colorBlue
	switch kind {
	case "pr.merged":
		title, color = "✅ PR merged", colorGreen
	case "pr.closed":
		title, color = "🗑️ PR closed", colorGrey
	}
	return discord.New(discord.Embed{
		Title:     fmt.Sprintf("%s: %s#%d %s", title, r.Name, pr.Number, pr.Title),
		URL:       pr.HTMLURL,
		Color:     color,
		Fields:    prFields(pr),
		Footer:    &discord.Footer{Text: r.Name},
		Timestamp: stamp(at),
	})
}

func prCheckMessage(r config.Repo, pr github.Pull, head string, failed []string, at time.Time) discord.Message {
	url := ""
	if pr.HTMLURL != "" {
		url = pr.HTMLURL + "/checks"
	}
	return discord.New(discord.Embed{
		Title: fmt.Sprintf("❌ CI failed: %s#%d %s", r.Name, pr.Number, pr.Title),
		URL:   url,
		Color: colorRed,
		Fields: append(prFields(pr),
			discord.Field{Name: "Failed", Value: codeList(failed), Inline: true},
			discord.Field{Name: "Head", Value: code(short7(head)), Inline: true}),
		Footer:    &discord.Footer{Text: r.Name},
		Timestamp: stamp(at),
	})
}

func branchCheckMessage(r config.Repo, c github.Commit, head string, failed []string, at time.Time) discord.Message {
	subject, _, _ := strings.Cut(c.Commit.Message, "\n")
	return discord.New(discord.Embed{
		Title:       fmt.Sprintf("❌ CI failed: %s@%s on %s", r.Name, short7(head), r.Branch),
		URL:         commitURL(r.Name, head),
		Description: subject,
		Color:       colorRed,
		Fields: []discord.Field{
			{Name: "Author", Value: c.Commit.Author.Name, Inline: true},
			{Name: "Failed", Value: codeList(failed), Inline: true},
		},
		Footer:    &discord.Footer{Text: r.Name},
		Timestamp: stamp(at),
	})
}

func watchMessage(r config.Repo, w watchResult, at time.Time) discord.Message {
	title, color, link := "⏸️ Watch hold", colorYellow, commitURL(r.Name, w.Head)
	switch w.Action {
	case "opened-revert-pr":
		title, color, link = "⏪ Revert PR opened", colorRed, w.URL
	case "report-culprit":
		title, color = "🎯 Culprit reported", colorRed
	}
	fields := []discord.Field{{Name: "Head", Value: code(short7(w.Head)), Inline: true}}
	if w.Culprit != "" {
		fields = append(fields, discord.Field{Name: "Culprit", Value: code(short7(w.Culprit)), Inline: true})
	}
	if w.PR != 0 {
		fields = append(fields, discord.Field{Name: "PR", Value: fmt.Sprintf("#%d", w.PR), Inline: true})
	}
	if len(w.Candidates) > 0 {
		candidates := make([]string, len(w.Candidates))
		for i, n := range w.Candidates {
			candidates[i] = "#" + strconv.Itoa(n)
		}
		fields = append(fields, discord.Field{Name: "Candidates", Value: strings.Join(candidates, ", "), Inline: true})
	}
	return discord.New(discord.Embed{
		Title:       fmt.Sprintf("%s: %s@%s", title, r.Name, short7(w.Head)),
		URL:         link,
		Description: w.Reason,
		Color:       color,
		Fields:      fields,
		Footer:      &discord.Footer{Text: r.Name},
		Timestamp:   stamp(at),
	})
}

func prFields(pr github.Pull) []discord.Field {
	return []discord.Field{
		{Name: "Author", Value: pr.User.Login, Inline: true},
		{Name: "Branch", Value: code(pr.Head.Ref) + " → " + code(pr.Base.Ref), Inline: true},
		{Name: "Changes", Value: fmt.Sprintf("%d files  |  +%d / -%d", pr.ChangedFiles, pr.Additions, pr.Deletions)},
	}
}

func commitURL(repo, sha string) string { return "https://github.com/" + repo + "/commit/" + sha }

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func code(s string) string { return "`" + strings.ReplaceAll(s, "`", "") + "`" }

func codeList(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = code(n)
	}
	return strings.Join(out, ", ")
}
