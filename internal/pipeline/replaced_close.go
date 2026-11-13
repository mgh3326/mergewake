package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// replacedCloseAttempts bounds how many times one forced pane close reads the
// pane again after the hub reports it changed under the read.
const replacedCloseAttempts = 3

// replacedClose is a replaced reviewer's job close and pane close that have
// not landed yet. It names the job and pane from the reviewer's own job
// record, so a later review run can finish them without the record.
// It is removed once both are closed or gone.
type replacedClose struct {
	PR       int    `json:"pr"`
	Pass     int    `json:"pass"`
	Label    string `json:"label"`
	Head     string `json:"head"`
	Reason   string `json:"reason"`
	Owner    string `json:"owner,omitempty"`
	Host     string `json:"host,omitempty"`
	PaneID   string `json:"pane_id,omitempty"`
	JobOpen  bool   `json:"job_open"`
	PaneOpen bool   `json:"pane_open"`
}

// replacedResult says which closes landed in one settle.
type replacedResult struct {
	JobClosed  bool
	PaneClosed bool
}

func (s *Service) replacedClosePath(repo string, pr int, label string) string {
	return filepath.Join(s.state(repo), "replaced-closes", fmt.Sprintf("%d-%s.json", pr, label))
}

// closeReplaced closes a replaced reviewer's job as abandoned and force-closes
// its pane in the same step, whatever the cause that replaced it. A close that
// does not land stays owed in a marker for the next review run of the PR.
func (x *panewireExecutor) closeReplaced(ctx context.Context, rj reviewerJob, job integrations.JobRecord, reason string) (replacedResult, error) {
	m := replacedClose{
		PR: x.pr.Number, Pass: rj.Pass, Label: rj.Label, Head: rj.Head, Reason: reason,
		Owner: reviewerLane(job, rj), Host: job.Host, PaneID: job.PaneID,
		JobOpen:  job.State != "",
		PaneOpen: job.Host != "" && job.PaneID != "",
	}
	if !m.JobOpen && !m.PaneOpen {
		return replacedResult{JobClosed: true}, nil
	}
	if err := x.keepReplaced(m); err != nil {
		return replacedResult{}, err
	}
	return x.settleReplaced(ctx, m)
}

// settleReplaced attempts the owed closes once. Both are attempted even when the
// job close fails, so a failed job close never blocks the pane close.
func (x *panewireExecutor) settleReplaced(ctx context.Context, m replacedClose) (replacedResult, error) {
	res := replacedResult{JobClosed: !m.JobOpen}
	var jobErr error
	if m.JobOpen {
		closed, err := x.closeReviewerJob(ctx, reviewerJob{PR: m.PR, Pass: m.Pass, Label: m.Label, Head: m.Head}, integrations.JobRecord{OwnerLane: m.Owner}, "abandoned", m.Reason)
		jobErr = err
		m.JobOpen = !closed
		res.JobClosed = closed
	}
	if m.PaneOpen {
		pane := reviewerPane{
			label: m.Label, head: m.Head, pass: m.Pass, confirmed: true, force: true,
			placement: integrations.JobRecord{Host: m.Host, PaneID: m.PaneID},
		}
		closed := x.closeReviewerPane(ctx, pane)
		m.PaneOpen = !closed
		res.PaneClosed = closed
	}
	if err := x.keepReplaced(m); err != nil {
		jobErr = errors.Join(jobErr, err)
	}
	audit := map[string]any{
		"pr": m.PR, "pass": m.Pass, "label": m.Label, "head": m.Head, "reason": m.Reason,
		"host": m.Host, "pane": m.PaneID, "job": closedWord(res.JobClosed), "pane_close": closedWord(res.PaneClosed),
	}
	if jobErr != nil {
		audit["error"] = jobErr.Error()
	}
	if err := x.s.audit(x.repo.Name, "reviewer-replaced-close", audit, false); err != nil {
		jobErr = errors.Join(jobErr, err)
	}
	return res, jobErr
}

func closedWord(closed bool) string {
	if closed {
		return "closed"
	}
	return "open"
}

// keepReplaced writes the owed closes, or removes the marker once none are owed.
func (x *panewireExecutor) keepReplaced(m replacedClose) error {
	path := x.s.replacedClosePath(x.repo.Name, m.PR, m.Label)
	if !m.JobOpen && !m.PaneOpen {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	return fsutil.WriteJSON(path, m)
}

// retryReplacedCloses finishes the closes owed by this PR's replaced reviewers.
// A failure is audited by settleReplaced and does not stop the review run.
func (x *panewireExecutor) retryReplacedCloses(ctx context.Context) error {
	owed, err := x.replacedCloses()
	if err != nil {
		return err
	}
	for _, m := range owed {
		if _, e := x.settleReplaced(ctx, m); e != nil && ctx.Err() != nil {
			return e
		}
	}
	return nil
}

// replacedCloses reads the owed closes of this PR in label order.
func (x *panewireExecutor) replacedCloses() ([]replacedClose, error) {
	dir := filepath.Dir(x.s.replacedClosePath(x.repo.Name, x.pr.Number, "_"))
	paths, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%d-*.json", x.pr.Number)))
	if err != nil {
		return nil, err
	}
	var owed []replacedClose
	for _, path := range paths {
		var m replacedClose
		ok, err := fsutil.ReadJSON(path, &m)
		if err != nil {
			return nil, err
		}
		if ok && m.PR == x.pr.Number {
			owed = append(owed, m)
		}
	}
	return owed, nil
}
