package pipeline

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// The worker-worktree stage visits its candidates host by host, in host order
// and then by path, and the repo budget can end the stage between two of them.
// A worker checkout that stays in use (its job still claimed) is visited again
// on every pass, so a budget that covers only the first few would starve the
// rest for good. A full stage therefore records the candidate its budget
// stopped before, and the next full stage starts there and wraps round. The
// record lives in the repo's state, so a daemon restart does not restart the
// order.

// workerCursor is the candidate a full worker stage stopped before. The zero
// value means the last full stage visited every candidate it found.
type workerCursor struct {
	Host string `json:"host"`
	Path string `json:"path"`
}

// workerVisit is one candidate in the stage's visit order.
type workerVisit struct {
	hs   *reapHost
	cand workerCand
}

func (s *Service) workerCursorPath(repo string) string {
	return filepath.Join(s.state(repo), "worker-worktree-cursor.json")
}

func (s *Service) workerStageCursor(repo string) (workerCursor, error) {
	var c workerCursor
	_, err := fsutil.ReadJSON(s.workerCursorPath(repo), &c)
	return c, err
}

func (s *Service) saveWorkerStageCursor(repo string, c workerCursor) error {
	defer lockPath(s.workerCursorPath(repo))()
	return fsutil.WriteJSON(s.workerCursorPath(repo), c)
}

// resumeWorkerVisits rotates visits, which are in visit order, so the stage
// starts at the first candidate at or after from and wraps round to the front.
// A cursor on a host that is no longer in order resumes nothing.
func resumeWorkerVisits(visits []workerVisit, order []*reapHost, from workerCursor) []workerVisit {
	if from.Path == "" {
		return visits
	}
	rank := make(map[string]int, len(order))
	for i, hs := range order {
		rank[hs.host.Name] = i
	}
	fr, ok := rank[from.Host]
	if !ok {
		return visits
	}
	start := sort.Search(len(visits), func(i int) bool {
		ri := rank[visits[i].hs.host.Name]
		if ri != fr {
			return ri > fr
		}
		return visits[i].cand.path >= from.Path
	})
	out := make([]workerVisit, 0, len(visits))
	out = append(out, visits[start:]...)
	return append(out, visits[:start]...)
}

// stopAt records that the budget ran out before candidate v was visited, so the
// next full stage starts with v. Only a durable stage writes it: a dry run
// leaves the order alone.
func (w *workerPass) stopAt(v workerVisit) error {
	if !w.resumable {
		return nil
	}
	cur := workerCursor{Host: v.hs.host.Name, Path: v.cand.path}
	if e := w.s.saveWorkerStageCursor(w.r.Name, cur); e != nil {
		return fmt.Errorf("worker worktrees: resume cursor: %w", e)
	}
	return nil
}

// finished clears a cursor the stage has now passed, so the next full stage
// starts from the front of the order again.
func (w *workerPass) finished(from workerCursor) error {
	if !w.resumable || from.Path == "" {
		return nil
	}
	if e := w.s.saveWorkerStageCursor(w.r.Name, workerCursor{}); e != nil {
		return fmt.Errorf("worker worktrees: resume cursor: %w", e)
	}
	return nil
}
