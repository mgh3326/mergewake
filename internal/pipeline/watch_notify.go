package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/policy"
)

// notifyCheck is one notify_checks streak on the default branch: the last
// head the check was green on, the head its current red streak began on, and
// whether the design lane was told about that streak. A streak ends only on
// green; pending, cancelled, skipped or absent evidence changes nothing.
type notifyCheck struct {
	LastGreen string `json:"last_green,omitempty"`
	FirstRed  string `json:"first_red,omitempty"`
	Notified  bool   `json:"notified,omitempty"`
}

// watchNotifyChecks reads the repo's notify_checks on the main head once per
// watch pass. A red streak sends the design lane one notice with the merges
// between the last green SHA and the first red SHA; it never reverts, takes
// the recovery latch or bisects. Like ciPolicy, a failed read never changes
// the watch pass: the failure is only audited, and an unsent notice is retried
// by the next pass because Notified stays false.
func (s *Service) watchNotifyChecks(ctx context.Context, r config.Repo, p policy.Policy, head string, o Options) {
	if len(p.NotifyChecks) == 0 || o.DryRun {
		return
	}
	if e := s.notifyChecksPass(ctx, r, p, head, o); e != nil {
		_ = s.audit(r.Name, "notify-check-error", map[string]any{"head": head, "error": integrations.Detail(e)}, false)
	}
}

func (s *Service) notifyChecksPass(ctx context.Context, r config.Repo, p policy.Policy, head string, o Options) error {
	file := filepath.Join(s.state(r.Name), "notify-checks.json")
	streaks := map[string]notifyCheck{}
	found, e := fsutil.ReadJSON(file, &streaks)
	if e != nil {
		return e
	}
	if !found {
		streaks = map[string]notifyCheck{}
	}
	states, e := s.GH.CheckStates(ctx, r.Name, head, p.NotifyChecks)
	if e != nil {
		return e
	}
	changed := false
	var errs []error
	// Streak state is observation, like the green checkpoint: it is recorded
	// in shadow too, while the notice itself waits for an acting pass.
	report := p.Enabled && !o.Shadow
	for _, name := range p.NotifyChecks {
		st := streaks[name]
		switch states[name] {
		case github.Green:
			if st.LastGreen != head || st.FirstRed != "" {
				st.LastGreen, st.FirstRed, st.Notified = head, "", false
				streaks[name] = st
				changed = true
			}
		case github.Red:
			if st.FirstRed == "" {
				st.FirstRed = head
			}
			if report && !st.Notified {
				if e := s.notifyCheckRed(ctx, r, name, head, st); e != nil {
					errs = append(errs, e)
				} else {
					st.Notified = true
				}
			}
			if streaks[name] != st {
				streaks[name] = st
				changed = true
			}
		}
	}
	if changed {
		if e := fsutil.WriteJSON(file, streaks); e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}

// notifyCheckRed sends the one design-lane notice for a red streak: the check
// name, the first red SHA and the merged PRs between the last green SHA and
// it.
func (s *Service) notifyCheckRed(ctx context.Context, r config.Repo, name, head string, st notifyCheck) error {
	pulls, bounded, e := s.notifyMerges(ctx, r, st.FirstRed, st.LastGreen)
	if e != nil {
		return e
	}
	var merges string
	if len(pulls) == 0 {
		merges = "no merged PRs in the interval"
	} else {
		const maxListed = 30
		parts := make([]string, 0, len(pulls))
		for _, pr := range pulls {
			parts = append(parts, fmt.Sprintf("#%d %s", pr.Number, pr.Title))
		}
		if len(parts) > maxListed {
			parts = append(parts[:maxListed], fmt.Sprintf("and %d more", len(pulls)-maxListed))
		}
		merges = strings.Join(parts, "; ")
	}
	text := fmt.Sprintf("mergewake: watch notify-check on %s %s: check %q first red %s", r.Name, short7(head), name, short7(st.FirstRed))
	if st.LastGreen != "" {
		text += ", last green " + short7(st.LastGreen)
	} else {
		text += ", no green baseline seen"
	}
	if !bounded {
		text += " (interval cut at watch_limit)"
	}
	text += "; merges: " + merges
	sum := sha256.Sum256([]byte(name))
	reportCtx, cancel := reportContext(ctx)
	defer cancel()
	var prs []int
	for _, pr := range pulls {
		prs = append(prs, pr.Number)
	}
	return errors.Join(
		s.Notifier.Notify(reportCtx, integrations.Event{
			ID:   fmt.Sprintf("mergewake-notify-check-%s-%s-%s", r.Name, short7(st.FirstRed), hex.EncodeToString(sum[:4])),
			Text: clip(text, findingsLaneText),
			Lane: s.ownerLane(r),
			Type: "info",
		}),
		s.audit(r.Name, "notify-check", map[string]any{
			"check": name, "head": head, "first_red": st.FirstRed, "last_green": st.LastGreen, "merges": prs,
		}, false),
	)
}

// notifyMerges lists the merged PRs landing in (lastGreen, firstRed]: the
// first-parent commits after the last green head up to and including the
// first red head. bounded is false when the walk hit watch_limit before
// reaching lastGreen, so the list may miss older merges. An empty lastGreen
// bounds the walk by watch_limit alone.
func (s *Service) notifyMerges(ctx context.Context, r config.Repo, firstRed, lastGreen string) ([]github.Pull, bool, error) {
	chain := []string{}
	current := firstRed
	reachedRoot := false
	for len(chain) < s.Config.WatchLimit && current != lastGreen {
		chain = append(chain, current)
		c, e := s.GH.Commit(ctx, r.Name, current)
		if e != nil {
			return nil, false, e
		}
		if len(c.Parents) == 0 {
			reachedRoot = true
			break
		}
		current = c.Parents[0].SHA
	}
	// bounded reports whether the walk saw the whole interval: it reached
	// lastGreen, hit the history root, or — with no green baseline — covered
	// the whole watch_limit window.
	bounded := current == lastGreen || reachedRoot || lastGreen == ""
	inBracket := map[string]bool{}
	for _, sha := range chain {
		inBracket[sha] = true
	}
	seen := map[int]bool{}
	var pulls []github.Pull
	for _, sha := range chain {
		prs, e := s.GH.PRsForCommit(ctx, r.Name, sha)
		if e != nil {
			return nil, false, e
		}
		for _, pr := range prs {
			if (pr.Merged || pr.MergedAt != nil) && pr.Base.Ref == r.Branch && inBracket[pr.MergeCommitSHA] && !seen[pr.Number] {
				seen[pr.Number] = true
				pulls = append(pulls, pr)
			}
		}
	}
	sort.Slice(pulls, func(i, j int) bool { return pulls[i].Number < pulls[j].Number })
	return pulls, bounded, nil
}
