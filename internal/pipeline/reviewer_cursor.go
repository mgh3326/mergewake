package pipeline

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// The reviewer sweep visits its candidates in a fixed order (host rank, then
// label name) and the repo budget can end the pass between two of them. Labels
// that stay open (a claimed job, a pane in use) are visited again on every pass,
// so a budget that covers only some of them would starve the rest for good.
// A full sweep therefore records the candidate its budget stopped at, and the
// next full sweep starts there and wraps round. The record lives in the repo's
// state, so a daemon restart does not restart the order.

// reviewerCursor is the candidate a full sweep stopped before. The zero value
// means the last full sweep visited every candidate it found.
type reviewerCursor struct {
	Host  string `json:"host"`
	Label string `json:"label"`
}

func (s *Service) reviewerCursorPath(repo string) string {
	return filepath.Join(s.state(repo), "reviewer-sweep-cursor.json")
}

func (s *Service) reviewerSweepCursor(repo string) (reviewerCursor, error) {
	var c reviewerCursor
	_, err := fsutil.ReadJSON(s.reviewerCursorPath(repo), &c)
	return c, err
}

func (s *Service) saveReviewerSweepCursor(repo string, c reviewerCursor) error {
	defer lockPath(s.reviewerCursorPath(repo))()
	return fsutil.WriteJSON(s.reviewerCursorPath(repo), c)
}

// resume rotates cands, which are in visit order, so the pass starts at the
// first candidate at or after from and wraps round to the front. cands must be
// sorted by host rank, then label name, as candidates leaves them.
func (w *reviewerSweep) resume(cands []reviewerCandidate, from reviewerCursor) []reviewerCandidate {
	if from.Label == "" {
		return cands
	}
	fr := w.rank(from.Host)
	start := sort.Search(len(cands), func(i int) bool {
		ri := w.rank(cands[i].host)
		if ri != fr {
			return ri > fr
		}
		return cands[i].label.name >= from.Label
	})
	out := make([]reviewerCandidate, 0, len(cands))
	out = append(out, cands[start:]...)
	return append(out, cands[:start]...)
}

// stopAt records that the budget ran out before candidate c was visited, so
// the next full sweep starts with c. Only a durable full sweep writes it: a
// dry run and a single-PR sweep leave the order alone.
func (w *reviewerSweep) stopAt(c reviewerCandidate) {
	if !w.resumable {
		return
	}
	cur := reviewerCursor{Host: c.host, Label: c.label.name}
	if e := w.s.saveReviewerSweepCursor(w.r.Name, cur); e != nil {
		w.errs = append(w.errs, fmt.Errorf("reviewer reap: sweep cursor: %w", e))
	}
}

// finished clears a cursor the sweep has now passed, so the next full sweep
// starts from the front of the order again.
func (w *reviewerSweep) finished(from reviewerCursor) {
	if !w.resumable || from.Label == "" {
		return
	}
	if e := w.s.saveReviewerSweepCursor(w.r.Name, reviewerCursor{}); e != nil {
		w.errs = append(w.errs, fmt.Errorf("reviewer reap: sweep cursor: %w", e))
	}
}
