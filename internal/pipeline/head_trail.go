package pipeline

import (
	"context"
	"path/filepath"
	"strconv"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

// headTrail is the last head of a PR whose review ended, and how it ended. The
// head-after-clean row of the head that follows it reads this record, because
// the open findings and check states of an old head change after its review.
type headTrail struct {
	Head string `json:"head"`
	// Clean: no open critical/high finding when the review ended.
	Clean bool `json:"clean"`
	// MediumLow: an open medium or low finding when the review ended.
	MediumLow bool `json:"medium_low,omitempty"`
}

func (s *Service) headTrailFile(repo string, pr int) string {
	return filepath.Join(s.state(repo), "head-trail", strconv.Itoa(pr)+".json")
}

// trackHead records that head's review ended. When the previous recorded head
// was clean, it writes that head's head-after-clean row, once: the record then
// moves to head, so later passes find head already recorded. A dry run writes
// no state. Only a review that ended is recorded, so a head is never judged
// while its review still runs.
func (s *Service) trackHead(ctx context.Context, r config.Repo, pr github.Pull, clean, mediumLow bool, o Options) error {
	file := s.headTrailFile(r.Name, pr.Number)
	var prev headTrail
	if _, e := fsutil.ReadJSON(file, &prev); e != nil {
		return e
	}
	if prev.Head == pr.Head.SHA {
		return nil
	}
	if prev.Head != "" && prev.Clean {
		cause, e := s.headCause(ctx, r, pr.Head.SHA, prev)
		if e != nil {
			return e
		}
		fields := map[string]any{"pr": pr.Number, "head": pr.Head.SHA, "previous_head": prev.Head, "cause": cause}
		if e := s.audit(r.Name, "head-after-clean", fields, o.DryRun); e != nil {
			return e
		}
	}
	if !o.Reporting() {
		return nil
	}
	return fsutil.WriteJSON(file, headTrail{Head: pr.Head.SHA, Clean: clean, MediumLow: mediumLow})
}

// headCause names why head followed the clean head prev. The first cause that
// holds wins, in this order: merge-only (a merge commit whose first parent is
// prev), failed-check (prev had a failed required check), medium-low (prev had
// an open medium or low finding), other. A commit or check that cannot be read
// is an error, so the row waits for a readable answer instead of a guess.
func (s *Service) headCause(ctx context.Context, r config.Repo, head string, prev headTrail) (string, error) {
	c, e := s.GH.Commit(ctx, r.Name, head)
	if e != nil {
		return "", e
	}
	if len(c.Parents) == 2 && c.Parents[0].SHA == prev.Head {
		return "merge-only", nil
	}
	p, e := s.Policy(ctx, r, prev.Head)
	if e != nil {
		return "", e
	}
	failed, e := s.failedChecks(ctx, r.Name, prev.Head, p.RequiredChecks)
	if e != nil {
		return "", e
	}
	switch {
	case len(failed) > 0:
		return "failed-check", nil
	case prev.MediumLow:
		return "medium-low", nil
	}
	return "other", nil
}

// openLowerFindings reports whether an unresolved medium or low thread on the
// head is still open: the gate does not dispose it. It is the lower-severity
// half of the open set openFindings reads for critical and high.
func (s *Service) openLowerFindings(fb github.ReviewFeedback, verify fixCheck) bool {
	own := policy.NormalizeAuthor(s.Config.Review.Login)
	for _, t := range fb.Threads {
		if len(t.Comments) == 0 || disposed(t, own, verify, s.Config.Review.FleetAuthors) {
			continue
		}
		switch github.ThreadSeverity(t.Comments[0].Author, t.Comments[0].Body) {
		case "medium", "low":
			return true
		}
	}
	return false
}
