package pipeline

import (
	"fmt"
	"path/filepath"
	"slices"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// replacedReviewersRecord is the reviewers a pass replaced for a lost result,
// kept beside the pass's reviewer record so it outlives that record. A record
// is dropped whenever its spawn ends before a pane exists, and the next spawn
// of the pass starts from a fresh plan: this file is what still says which
// pools the lost reviewers ran on.
type replacedReviewersRecord struct {
	Replaced []replacedReviewer `json:"replaced"`
}

func (s *Service) replacedReviewersPath(repo string, pr, pass int) string {
	return filepath.Join(s.state(repo), "replaced-reviewers", fmt.Sprintf("%d-p%d.json", pr, pass))
}

// saveReplacedReviewers writes the pass's replaced list, the whole of it.
func (s *Service) saveReplacedReviewers(repo string, pr, pass int, replaced []replacedReviewer) error {
	return fsutil.WriteJSON(s.replacedReviewersPath(repo, pr, pass), replacedReviewersRecord{Replaced: replaced})
}

// loadReplacedReviewers reads the pass's replaced list; none when absent.
func (s *Service) loadReplacedReviewers(repo string, pr, pass int) ([]replacedReviewer, error) {
	var rec replacedReviewersRecord
	ok, err := fsutil.ReadJSON(s.replacedReviewersPath(repo, pr, pass), &rec)
	if err != nil || !ok {
		return nil, err
	}
	return rec.Replaced, nil
}

// mergeReplaced returns replaced with each reviewer of more that it does not
// hold yet, matched by label. It never edits either slice.
func mergeReplaced(replaced, more []replacedReviewer) []replacedReviewer {
	out := slices.Clone(replaced)
	for _, m := range more {
		if !slices.ContainsFunc(out, func(o replacedReviewer) bool { return o.Label == m.Label }) {
			out = append(out, m)
		}
	}
	return out
}
