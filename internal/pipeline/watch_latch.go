package pipeline

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// recoveryLatchGH is the part of the GitHub client that clears a latch; a
// client without it leaves the latch to a person.
type recoveryLatchGH interface {
	RecoveryRevert(context.Context, string) (*github.Pull, error)
	ClearRecovery(context.Context, string) error
}

type latchCleared struct {
	Culprit  string `json:"culprit,omitempty"`
	RevertPR int    `json:"revert_pr"`
	Head     string `json:"head"`
}

// clearRecoveryLatch ends a recovery watch started. Called on a green main
// head, it deletes the latch only when the revert PR the latch belongs to is
// merged and head holds its merge commit. A latch with no such PR (a person's
// tag, a report-only culprit) and a PR still open or closed unmerged stay for a
// person. The notice and audit row go out once per revert PR, and deleting refs
// that are already gone succeeds, so a pass that died partway finishes cleanly.
func (s *Service) clearRecoveryLatch(ctx context.Context, r config.Repo, head string) error {
	active, e := s.GH.RecoveryActive(ctx, r.Name)
	if e != nil || !active {
		return e
	}
	gh, ok := s.GH.(recoveryLatchGH)
	if !ok {
		return nil
	}
	pr, e := gh.RecoveryRevert(ctx, r.Name)
	if e != nil || pr == nil || pr.MergedAt == nil || pr.MergeCommitSHA == "" {
		return e
	}
	if head != pr.MergeCommitSHA {
		contains, e := s.GH.Ancestor(ctx, r.Name, pr.MergeCommitSHA, head)
		if e != nil || !contains {
			return e
		}
	}
	key := "latch-cleared:" + strconv.Itoa(pr.Number) + ":" + pr.MergeCommitSHA
	sent, e := loadSentKeys(filepath.Join(s.state(r.Name), "watch-reported"), 127)
	if e != nil {
		return e
	}
	if !sent.has(key) {
		culprit := ""
		if f := strings.Fields(pr.Body); len(f) > 1 && f[0] == "Reverts" {
			culprit = f[1]
		}
		reportCtx, cancel := reportContext(ctx)
		defer cancel()
		note := "mergewake: recovery latch cleared on " + r.Name + ": revert PR #" + strconv.Itoa(pr.Number) + " merged and main " + short7(head) + " is green"
		if e = s.Notifier.Notify(reportCtx, integrations.Event{ID: "mergewake-latch-cleared-" + r.Name + "-" + strconv.Itoa(pr.Number), Type: "info", Text: note}); e != nil {
			return e
		}
		if e = s.audit(r.Name, "recovery-latch-cleared", latchCleared{Culprit: culprit, RevertPR: pr.Number, Head: head}, false); e != nil {
			return e
		}
		if e = sent.add(key); e != nil {
			return e
		}
	}
	return gh.ClearRecovery(ctx, r.Name)
}
