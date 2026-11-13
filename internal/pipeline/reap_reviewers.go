package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// The reviewer sweep closes the leftover pane of a finished reviewer job and
// removes its reviewer worktrees. A review pass already cleans up after itself
// (panewire_cleanup.go); this sweep picks up what a pass had to keep: a pane
// whose job was still running, a spawn whose wait timed out, an unconfirmed
// placement, or a refused removal. It runs whatever the PR's state, because a
// reviewer's job ends on its own clock, not on the merge.
//
// Every action needs all of: a reviewer label, a completed reviewer job, a
// pass that is over, a pane that is the one the job names, and an empty
// composer. A remote worktree goes only through the hub's guarded removal; a
// local checkout is removed only when it is clean, detached at the reviewed
// head and no local pane runs in it.
//
// Two reviewer-only exceptions keep finished panes from piling up. An omp pane
// has a composer herdr cannot read, so a reviewer pane that stayed idle with an
// unreadable composer for reviewerUnreadableIdle closes without the read. And a
// reviewer job that never left spawned, whose pane is idle and whose PR is
// closed or past the reviewed head, is closed abandoned first (abandonSpawned).
// A worker pane keeps the composer read: a person may type in it.

// reviewerLabelPattern is the label a review pass gives its pane, job and
// checkout: mwr-<pr>-<sha7>-p<pass>-<unix> (panewire.go). The hub's
// replacement for a quota-stopped reviewer adds -q<n> (n >= 1) to the old job id.
// reviewerUnreadableIdle is how long a reviewer pane must have sat idle with a
// composer the hub could not read before the sweep closes it unread. The clock
// is the pane stream's kept composer-unknown row: the receipts dedupe repeats,
// so that row's time is when the sweep first saw this state, and a pane that
// worked in between wrote a different row and started the clock again.
const reviewerUnreadableIdle = 2 * time.Hour

var reviewerLabelPattern = regexp.MustCompile(`^mwr-(\d+)-([0-9a-f]{7})-p(\d+)-(\d+)(?:-q([1-9]\d*))?$`)

type reviewerLabel struct {
	name    string
	pr      int
	sha7    string
	pass    int
	started time.Time
	// replacement is n of the -q<n> suffix, 0 for an original label.
	replacement int
}

func parseReviewerLabel(name string) (reviewerLabel, bool) {
	m := reviewerLabelPattern.FindStringSubmatch(name)
	if m == nil {
		return reviewerLabel{}, false
	}
	pr, e1 := strconv.Atoi(m[1])
	pass, e2 := strconv.Atoi(m[3])
	unix, e3 := strconv.ParseInt(m[4], 10, 64)
	repl := 0
	var e4 error
	if m[5] != "" {
		repl, e4 = strconv.Atoi(m[5])
	}
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || pr <= 0 {
		return reviewerLabel{}, false
	}
	return reviewerLabel{name: name, pr: pr, sha7: m[2], pass: pass, started: time.Unix(unix, 0), replacement: repl}, true
}

// reviewerStream names one receipt stream of a label: the pane stream (gate
// and close rows, no worktree) or one worktree on one host. rowKey dedupes
// reviewer-reap rows by it.
func reviewerStream(v reapReceipt) string {
	if v.Worktree == "" {
		return "pane"
	}
	return v.Host + "\x00" + v.Worktree
}

// reviewerCheckoutTTL: a checkout no pane uses is revisited until its label is
// this old, then released even if its job never reached a terminal state.
const reviewerCheckoutTTL = 24 * time.Hour

const (
	// reviewerPathUnknown is the terminal reason of a remote worktree whose path
	// no record names: the host's own sweep owns it from here.
	reviewerPathUnknown = "remote worktree path unknown; left for the host's own sweep"
	// reviewerRefused starts the reason of a removal the hub's guard refused.
	reviewerRefused = "hub guarded removal refused: "
	// reviewerRefusalRetry is how long a refused removal waits before the hub is
	// asked again; reviewerRefusalRetries is how many times it is asked again
	// before the refusal is final.
	reviewerRefusalRetry   = 24 * time.Hour
	reviewerRefusalRetries = 3
)

// reviewerTerminal reports whether a row ends its stream: nothing is left to
// do for that pane or worktree. A kept row is terminal only with the reason of
// an unknown remote path or a final hub refusal; any other kept row is waiting
// for something to change.
func reviewerTerminal(v reapReceipt) bool {
	switch v.Result {
	case "closed", "removed":
		return true
	case "skipped":
		return strings.Contains(v.Reason, "already gone")
	case "kept":
		return v.Worktree != "" && (v.Reason == reviewerPathUnknown || strings.HasPrefix(v.Reason, reviewerRefused))
	}
	return false
}

// refusalRetries reports how many retries a pending refusal row records: the
// first refusal carries none, each later one ends its reason with
// " (retry n of 3)". ok is false for a row that is not a pending refusal.
func refusalRetries(v reapReceipt) (n int, ok bool) {
	if v.Result != "pending-cleanup" || !strings.HasPrefix(v.Reason, reviewerRefused) {
		return 0, false
	}
	if i := strings.LastIndex(v.Reason, " (retry "); i >= 0 {
		n, _ = strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(v.Reason[i:], " (retry "), fmt.Sprintf(" of %d)", reviewerRefusalRetries)))
	}
	return n, true
}

// reviewerWaiting reports whether a pending refusal row is younger than the
// retry interval: the hub is not asked again, and the label is not visited.
func reviewerWaiting(v reapReceipt, now time.Time) bool {
	_, ok := refusalRetries(v)
	return ok && now.Sub(v.Time) < reviewerRefusalRetry
}

// reviewerLabelOf names the label a reviewer receipt is about: reviewer-reap
// rows carry it, reviewer-cleanup rows name the checkout or remote worktree
// whose directory base is the label.
func reviewerLabelOf(v reapReceipt) string {
	switch v.Action {
	case "reviewer-reap":
		return v.Label
	case "reviewer-cleanup":
		return filepath.Base(filepath.Clean(v.Worktree))
	}
	return ""
}

type reviewerSweep struct {
	s      *Service
	r      config.Repo
	hosts  []*reapHost
	acting bool
	now    time.Time
	settle time.Duration // review.timeout + 1m: a younger label's pass may still run
	jobs   reapJobs
	note   func(reapReceipt)
	errs   []error
	// streams is the newest reviewer-reap row per label and stream, as the
	// receipts stood when the pass started.
	streams map[string]map[string]reapReceipt
	local   *reapHost
	// remote is the newest remote worktree path a receipt recorded per label
	// and host, the fallback when the pane is gone and the job names no cwd.
	remote map[string]map[string]string
	// abandoned names labels an earlier pass closed abandoned. Their job record
	// still reads spawned until panewire's sweep ends the job (then lost), so
	// this receipt, not the record, stops a second abandon; the pane still needs reaping.
	abandoned map[string]bool
	// pulls memoises the PR reads of one pass; a failed read is not kept.
	pulls map[int]github.Pull
}

// reviewerCandidate is one label to visit, and the host and pane it was seen
// on when a pane drove it.
type reviewerCandidate struct {
	label reviewerLabel
	seen  *reapHost
	pane  *reap.Pane
}

// reapReviewers runs the sweep on every scanned host. prNum > 0 limits it to
// that PR's labels. note writes one reviewer-reap row and its audit record.
func (s *Service) reapReviewers(ctx context.Context, r config.Repo, prNum int, receipts []reapReceipt, hosts []*reapHost, acting bool, note func(reapReceipt)) []error {
	timeout := s.Config.Review.Timeout
	if timeout == "" {
		timeout = "10m" // config's default when the key is unset
	}
	d, e := config.Duration(timeout)
	if e != nil {
		return []error{fmt.Errorf("reviewer reap: review.timeout: %w", e)}
	}
	w := &reviewerSweep{s: s, r: r, hosts: hosts, acting: acting, now: s.now(), settle: d + time.Minute, note: note, streams: map[string]map[string]reapReceipt{}, remote: map[string]map[string]string{}, abandoned: map[string]bool{}, pulls: map[int]github.Pull{}}
	w.jobs, _ = s.Sink.(reapJobs)
	for _, hs := range hosts {
		if hs.host.Local() && w.local == nil {
			w.local = hs
		}
	}

	lastCleanup := map[string]reapReceipt{}
	for _, rec := range receipts {
		switch rec.Action {
		case "reviewer-reap":
			if rec.Result == "abandoned" {
				w.abandoned[rec.Label] = true
			}
			if w.streams[rec.Label] == nil {
				w.streams[rec.Label] = map[string]reapReceipt{}
			}
			w.streams[rec.Label][reviewerStream(rec)] = rec
			if rec.Worktree != "" && rec.Worktree != rec.Label {
				w.remember(rec.Label, rec.Host, rec.Cwd)
			}
		case "reviewer-cleanup":
			lastCleanup[reviewerLabelOf(rec)] = rec
			// Cwd holds the remote pane's cwd or the remote worktree; the
			// local checkout is only ever in Worktree.
			w.remember(reviewerLabelOf(rec), rec.Host, rec.Cwd)
		}
	}
	finished := func(label string) bool {
		// A reviewer-cleanup "closed" row only says the pane closed: the pass
		// removes its local checkout afterwards and may fail. A checkout still
		// on disk keeps the label open until its own removal is confirmed.
		streams := w.streams[label]
		waiting := false
		for _, v := range streams {
			switch {
			case reviewerTerminal(v):
			case reviewerWaiting(v, w.now):
				waiting = true
			default:
				return false
			}
		}
		// A refused removal that still waits keeps the local checkout too
		// (L2): the label is not visited until the hub is due to be asked.
		if waiting {
			return true
		}
		if w.localCheckout(label) != "" {
			return false
		}
		if len(streams) > 0 {
			return true
		}
		v, ok := lastCleanup[label]
		return !ok || reviewerTerminal(v)
	}
	keep := func(name string) (reviewerLabel, bool) {
		l, ok := parseReviewerLabel(name)
		return l, ok && (prNum == 0 || l.pr == prNum)
	}

	var cands []reviewerCandidate
	paned := map[string]bool{}
	for _, hs := range hosts {
		if hs.failed {
			continue
		}
		// The agent list names panes and is what CloseExact re-reads; the
		// full pane list only serves the in-use check.
		for i := range hs.panes {
			pane := hs.panes[i]
			l, ok := keep(pane.Name)
			if !ok || pane.ID == "" {
				continue
			}
			// A label names a PR number, not a repo: every repo's sweep sees
			// every reviewer pane on the host. Only a pane standing in a
			// worktree of this repo is this sweep's; an unknown worktree is
			// not, so another repo's reviewer is never closed or receipted here.
			if tree := hs.trees[pane.Cwd]; tree == nil || !strings.EqualFold(tree.Repo, w.r.Name) {
				continue
			}
			paned[l.name] = true
			cands = append(cands, reviewerCandidate{label: l, seen: hs, pane: &pane})
		}
	}
	labels := map[string]bool{}
	for label := range w.streams {
		labels[label] = true
	}
	for label := range lastCleanup {
		labels[label] = true
	}
	for label := range labels {
		if paned[label] || finished(label) {
			continue
		}
		if l, ok := keep(label); ok {
			cands = append(cands, reviewerCandidate{label: l})
		}
	}
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].label.name < cands[j].label.name })
	for _, c := range cands {
		if e := ctx.Err(); e != nil {
			return append(w.errs, e)
		}
		w.visit(ctx, c)
	}
	if acting {
		if e := s.reviewerDailySummary(r.Name, receipts, hosts, s.now()); e != nil {
			w.errs = append(w.errs, e)
		}
	}
	return w.errs
}

// remember records a remote path a receipt named for a label on a host; only
// an absolute path whose base is the label counts.
func (w *reviewerSweep) remember(label, host, path string) {
	if label == "" || host == "" || !filepath.IsAbs(path) || filepath.Base(filepath.Clean(path)) != label {
		return
	}
	if w.remote[label] == nil {
		w.remote[label] = map[string]string{}
	}
	w.remote[label][host] = path
}

// localCheckout is the label's local reviewer checkout
// <review.panewire.checkouts>/<label>, or "" when it is not on disk.
func (w *reviewerSweep) localCheckout(label string) string {
	dir := config.Expand(w.s.Config.Review.Panewire.Checkouts)
	if dir == "" {
		return ""
	}
	path := filepath.Join(dir, label)
	if _, e := os.Lstat(path); e != nil {
		return ""
	}
	return path
}

// row starts a reviewer-reap receipt for a label.
func (w *reviewerSweep) row(l reviewerLabel) reapReceipt {
	return reapReceipt{Action: "reviewer-reap", Repo: w.r.Name, PR: l.pr, Label: l.name}
}

func (w *reviewerSweep) record(v reapReceipt, result, reason string) {
	v.Result, v.Reason = result, fleetevent.Redact(reason)
	w.note(v)
}

// visit applies the sweep to one label: the gates (I2–I4), the pane close
// (I5), the remote worktree (I6) and the local checkout (I7), in that order.
// Any step that does not finish stops the later ones.
func (w *reviewerSweep) visit(ctx context.Context, c reviewerCandidate) {
	l := c.label
	// I3: the pass that spawned the pane may still be waiting on it.
	if !l.started.Add(w.settle).Before(w.now) {
		return
	}
	gate := w.row(l)
	if c.seen != nil {
		gate.Host, gate.Pane, gate.Cwd = c.seen.host.Name, c.pane.ID, c.pane.Cwd
	}
	// I2: only a completed reviewer job (or the worker seat a refused reviewer
	// role retries as) is ever acted on.
	if w.jobs == nil {
		w.record(gate, "kept", "job records unavailable")
		return
	}
	job, e := w.jobs.Job(ctx, l.name)
	// A label no pane uses is revisited on its own: an ended job, or a label
	// older than reviewerCheckoutTTL, releases the checkout it holds.
	unused := c.seen == nil
	aged := unused && !l.started.Add(reviewerCheckoutTTL).After(w.now)
	switch {
	case errors.Is(e, integrations.ErrJobNotFound) && aged:
		w.removeLocal(ctx, l)
		return
	case errors.Is(e, integrations.ErrJobNotFound):
		w.record(gate, "kept", "job-record-missing")
		return
	case e != nil:
		w.record(gate, "error", "job record: "+integrations.Detail(e))
		return
	}
	// A reviewer job stuck in spawned is closed abandoned when its review can
	// no longer matter; the pane then goes through the normal reap below.
	if job.State == "spawned" && job.Role == "reviewer" && c.pane != nil && !w.abandoned[l.name] {
		if !w.abandonSpawned(ctx, c, gate, job) {
			return
		}
		job.State = "completed"
	}
	held := job.State != "completed" && !w.abandoned[l.name]
	if held && unused && (job.State == "escalated" || job.State == "lost" || aged) {
		held = false
	}
	switch {
	case held:
		st := job.State
		if st == "" {
			st = "unknown"
		}
		w.record(gate, "kept", "job:"+st)
		return
	case job.Role != "reviewer" && job.Role != "worker":
		w.record(gate, "kept", "role:"+job.Role)
		return
	}
	// I4: the pane must be on the host the job names.
	var hs *reapHost
	if job.Host == "" && c.seen != nil {
		w.record(gate, "kept", "job record and pane disagree")
		return
	}
	if job.Host != "" {
		for _, h := range w.hosts {
			if h.host.Name == job.Host || h.host.MachineID() == job.Host {
				hs = h
				break
			}
		}
		switch {
		case c.seen != nil && c.seen != hs:
			w.record(gate, "kept", "job record and pane disagree")
			return
		case hs == nil:
			gate.Host = job.Host
			w.record(gate, "kept", "job-host-unconfigured")
			return
		case hs.failed:
			// The host's scan already reported its error.
			return
		}
		gate.Host = hs.host.Name
	}
	cwd := job.Cwd
	if cwd == "" && hs != nil {
		cwd = w.remote[l.name][hs.host.Name]
		if cwd == "" {
			cwd = w.remote[l.name][hs.host.MachineID()]
		}
	}
	if hs != nil {
		pane, ok := w.closePane(ctx, gate, hs, job, l)
		if !ok {
			return
		}
		if pane != nil && pane.Cwd != "" {
			cwd = pane.Cwd
		}
		// L1: an unknown remote path is recorded as pending-cleanup and does not
		// block the local checkout. L2: a refused or failed removal does.
		if !hs.host.Local() && w.removeRemote(ctx, hs, l, cwd) == remoteStop {
			return
		}
	}
	w.removeLocal(ctx, l)
}

// closePane finds the label's pane on hs and closes it under I4 and I5. It
// returns true once the pane is closed (or would be, in shadow) or gone.
func (w *reviewerSweep) closePane(ctx context.Context, rec reapReceipt, hs *reapHost, job integrations.JobRecord, l reviewerLabel) (*reap.Pane, bool) {
	var found []reap.Pane
	for _, p := range hs.panes {
		if p.Name == l.name && p.ID != "" {
			found = append(found, p)
		}
	}
	switch len(found) {
	case 0:
		// Gone. A pane this sweep closed before needs no second row.
		if prior, ok := w.streams[l.name]["pane"]; !ok || prior.Result != "closed" {
			rec.Pane = job.PaneID
			w.record(rec, "skipped", "pane already gone")
		}
		return nil, true
	case 1:
	default:
		w.record(rec, "kept", "pane-ambiguous")
		return nil, false
	}
	pane := found[0]
	rec.Pane, rec.Cwd = pane.ID, pane.Cwd
	switch {
	case job.PaneID == "" || pane.ID != job.PaneID || pane.Cwd == "" || filepath.Base(filepath.Clean(pane.Cwd)) != l.name,
		job.Cwd != "" && filepath.Clean(job.Cwd) != filepath.Clean(pane.Cwd):
		// The job record, when it names a cwd, must name this pane's.
		w.record(rec, "kept", "job record and pane disagree")
		return nil, false
	case pane.Focused:
		w.record(rec, "kept", "operator-owned")
		return nil, false
	}
	// I5: an unsent draft, or a composer that cannot be read, keeps the pane. A
	// reviewer pane whose composer the hub reports unreadable (not a failed
	// read) closes once it has been idle long enough.
	switch composer, e := hs.ops.Composer(ctx, pane); {
	case e != nil:
		w.record(rec, "kept", reap.ComposerUnknown)
		return nil, false
	case composer == reap.ComposerUnknown && job.Role == "reviewer":
		if reason := w.unreadableKeep(rec, pane); reason != "" {
			w.record(rec, "kept", reason)
			return nil, false
		}
	case composer != reap.ComposerEmpty:
		w.record(rec, "kept", composer)
		return nil, false
	}
	if !w.acting {
		hs.closed[pane.ID] = true
		w.record(rec, "would-close", "")
		return &pane, true
	}
	outcome, e := hs.ops.CloseExact(ctx, pane)
	switch {
	case e != nil:
		w.record(rec, "error", "pane close: "+e.Error())
		return nil, false
	case outcome == reap.CloseClosed:
		hs.closed[pane.ID] = true
		w.record(rec, "closed", "")
		return &pane, true
	case outcome == reap.CapturePaneGone:
		// Not marked closed: the in-use check must still see any pane that
		// remains in the worktree.
		w.record(rec, "skipped", "pane already gone")
		return &pane, true
	}
	w.record(rec, "kept", "pane close recheck: "+outcome)
	return nil, false
}

// unreadableKeep is why a reviewer pane with an unreadable composer stays, or
// "" once it may close: idle or done, and in the same unreadable state since a
// kept composer-unknown row at least reviewerUnreadableIdle old. The first
// sighting writes that row and keeps the pane.
func (w *reviewerSweep) unreadableKeep(rec reapReceipt, pane reap.Pane) string {
	if pane.Status != "idle" && pane.Status != "done" {
		st := pane.Status
		if st == "" {
			st = "unknown"
		}
		return "status:" + st
	}
	prior, ok := w.streams[rec.Label]["pane"]
	if !ok || prior.Result != "kept" || prior.Reason != reap.ComposerUnknown || w.now.Sub(prior.Time) < reviewerUnreadableIdle {
		return reap.ComposerUnknown
	}
	return ""
}

// abandonSpawned closes a reviewer job that is still spawned when its pane is
// idle or done and its review is over: the PR merged or closed, or the head
// the label reviewed is no longer the PR head. It reports whether the sweep may
// go on to the pane. The close is panewire's job close, the path a lost
// reviewer's replacement takes; a PR still open at the reviewed head, an
// unreadable PR or a busy pane leaves the job spawned.
func (w *reviewerSweep) abandonSpawned(ctx context.Context, c reviewerCandidate, gate reapReceipt, job integrations.JobRecord) bool {
	keep := func() bool {
		w.record(gate, "kept", "job:spawned")
		return false
	}
	if c.pane.Status != "idle" && c.pane.Status != "done" {
		return keep()
	}
	l := c.label
	pr, ok := w.pulls[l.pr]
	if !ok {
		var e error
		if pr, e = w.s.GH.Pull(ctx, w.r.Name, l.pr); e != nil {
			return keep()
		}
		w.pulls[l.pr] = pr
	}
	var reason string
	switch {
	case pr.Merged || pr.MergedAt != nil:
		reason = fmt.Sprintf("PR #%d merged", l.pr)
	case pr.State != "open":
		reason = fmt.Sprintf("PR #%d closed", l.pr)
	case pr.Head.SHA != "" && !strings.HasPrefix(pr.Head.SHA, l.sha7):
		reason = fmt.Sprintf("PR #%d head moved past reviewed %s", l.pr, l.sha7)
	default:
		return keep()
	}
	if !w.acting {
		w.record(gate, "would-abandon", reason)
		return true
	}
	if job.OwnerLane == "" {
		w.record(gate, "error", "abandon job: owner lane unavailable")
		return false
	}
	cmd := w.s.Config.Review.Panewire.Command
	argv := integrations.HubArgv(w.s.Config.Reap.Hub, "job", "close", l.name, "--lane", job.OwnerLane, "--outcome", "abandoned", "--reason", reason)
	if _, e := w.s.Runner.Run(ctx, cmd, argv, "", ""); e != nil {
		w.record(gate, "error", "abandon job: "+e.Error())
		return false
	}
	w.abandoned[l.name] = true
	w.record(gate, "abandoned", reason)
	return true
}

// remoteStep is how the remote worktree step ended, as the local step needs it.
type remoteStep int

const (
	// remoteStop: removal was refused or failed for a known path; the visit ends.
	remoteStop remoteStep = iota
	// remoteDone: the worktree is removed, would be, or is gone.
	remoteDone
	// remotePathUnknown: no remote path is known, so nothing remote was tried.
	// The remote side stays pending-cleanup; the local checkout is independent.
	remotePathUnknown
)

// removeRemote removes the reviewer's detached worktree on a remote host,
// only through the hub's guarded removal (I6).
func (w *reviewerSweep) removeRemote(ctx context.Context, hs *reapHost, l reviewerLabel, cwd string) remoteStep {
	rec := w.row(l)
	rec.Host, rec.Cwd, rec.Worktree = hs.host.Name, cwd, cwd
	if cwd == "" || filepath.Base(filepath.Clean(cwd)) != l.name {
		// Its own stream: the pane stream must keep its close. The path is not
		// going to become known, so the outcome is final once the sweep acts;
		// the host's own sweep owns the worktree. A shadow run stays pending.
		rec.Worktree = l.name
		if w.acting {
			w.record(rec, "kept", reviewerPathUnknown)
		} else {
			w.record(rec, "pending-cleanup", "remote cwd unavailable or mismatched")
		}
		return remotePathUnknown
	}
	// The stream's newest row says whether the hub was asked already: a final
	// refusal is not asked again, and a pending one waits out its interval.
	prior := w.streams[l.name][reviewerStream(rec)]
	switch {
	case reviewerTerminal(prior) && prior.Result == "kept":
		return remoteDone
	case reviewerWaiting(prior, w.now):
		return remoteStop
	}
	remover, ok := hs.ops.(interface {
		WorktreeRemove(context.Context, string, string, string, []string, bool) (reap.WorktreeRemoveResult, error)
	})
	if !hs.viaHub || !ok {
		w.record(rec, "pending-cleanup", "no hub for guarded removal")
		return remoteStop
	}
	if hs.usageUnknown {
		w.record(rec, "kept", "pane-scan-failed")
		return remoteStop
	}
	if w.acting {
		// A pane may have moved into the worktree since discovery.
		if e := hs.refreshPanes(ctx); e != nil {
			w.record(rec, "kept", "pane-scan-failed")
			return remoteStop
		}
	}
	if hs.paneInUse(cwd) {
		w.record(rec, "kept", "pane-in-use")
		return remoteStop
	}
	tree, e := hs.ops.Inspect(ctx, cwd)
	switch {
	case e != nil:
		w.record(rec, "error", "worktree inspect: "+e.Error())
		return remoteStop
	case tree == nil:
		w.record(rec, "skipped", "worktree already gone")
		return remoteDone
	case !w.reviewedTree(tree, cwd, l):
		w.record(rec, "kept", "worktree identity changed")
		return remoteStop
	case !w.acting:
		w.record(rec, "would-remove", "")
		return remoteDone
	}
	removed, e := remover.WorktreeRemove(ctx, tree.Path, tree.Head, w.r.Branch, nil, true)
	if e != nil {
		w.record(rec, "error", "worktree remove: "+e.Error())
		return remoteStop
	}
	result, reason := worktreeRemoveVerdict(removed)
	switch {
	case result == "removed":
		w.record(rec, "removed", "")
		return remoteDone
	case reason == "worktree-gone":
		w.record(rec, "skipped", "worktree already gone")
		return remoteDone
	}
	rec.Files = removed.Files
	// The hub's refusal is never overridden. It is asked again once a day, and
	// the refusal after the last retry is final.
	retries, refused := refusalRetries(prior)
	switch {
	case !refused:
		w.record(rec, "pending-cleanup", reviewerRefused+reason)
	case retries >= reviewerRefusalRetries-1:
		w.record(rec, "kept", reviewerRefused+reason)
	default:
		w.record(rec, "pending-cleanup", fmt.Sprintf("%s%s (retry %d of %d)", reviewerRefused, reason, retries+1, reviewerRefusalRetries))
	}
	return remoteStop
}

// reviewedTree reports whether a worktree is the reviewer's own: at path, in
// this repo, detached at a head with the label's sha7.
func (w *reviewerSweep) reviewedTree(tree *reap.Worktree, path string, l reviewerLabel) bool {
	return filepath.Clean(tree.Path) == filepath.Clean(path) && strings.EqualFold(tree.Repo, w.r.Name) && tree.Branch == "" && strings.HasPrefix(tree.Head, l.sha7)
}

// localHost is the configured local host, or a scan of this machine's panes
// when none is configured, so the in-use check always reads real panes.
func (w *reviewerSweep) localHost(ctx context.Context) *reapHost {
	if w.local != nil {
		return w.local
	}
	ex := reap.Exec{Runner: w.s.Runner, Host: reap.Host{Name: "local"}}
	w.local = &reapHost{host: ex.Host, exec: ex, ops: ex, trees: map[string]*reap.Worktree{}, closed: map[string]bool{}}
	if e := w.local.refreshPanes(ctx); e != nil {
		w.local.usageUnknown = true
	}
	return w.local
}

// removeLocal removes the reviewer's checkout <review.panewire.checkouts>/<label>
// on this machine (I7): clean, detached at the label's head, and no local
// pane in it. A checkout that is not there writes nothing.
func (w *reviewerSweep) removeLocal(ctx context.Context, l reviewerLabel) {
	path := w.localCheckout(l.name)
	if path == "" {
		return
	}
	hs := w.localHost(ctx)
	rec := w.row(l)
	rec.Host, rec.Cwd, rec.Worktree = hs.host.Name, path, path
	if hs.failed || hs.usageUnknown {
		w.record(rec, "kept", "pane-scan-failed")
		return
	}
	if hs.paneInUse(path) {
		w.record(rec, "kept", "pane-in-use")
		return
	}
	tree, e := hs.exec.Inspect(ctx, path)
	switch {
	case e != nil:
		w.record(rec, "error", "checkout inspect: "+e.Error())
		return
	case tree == nil:
		w.record(rec, "kept", "checkout is not a linked worktree")
		return
	case !w.reviewedTree(tree, path, l):
		w.record(rec, "kept", "checkout identity changed")
		return
	}
	clean, e := hs.exec.Clean(ctx, path)
	var ignored []string
	if e == nil {
		ignored, e = hs.exec.Ignored(ctx, path)
	}
	var unsafe []string
	for _, f := range ignored {
		if !reap.Regenerable(w.s.Config.Reap.Regenerable, f) {
			unsafe = append(unsafe, f)
		}
	}
	switch {
	case e != nil:
		w.record(rec, "kept", "status-check-failed")
		return
	case !clean:
		w.record(rec, "kept", "dirty")
		return
	case len(unsafe) > 0:
		rec.Files = unsafe
		w.record(rec, "kept", "ignored-files:"+filesSig(unsafe))
		return
	case !w.acting:
		w.record(rec, "would-remove", "")
		return
	}
	// The discovery scan is stale: re-read the panes before removing.
	if e := hs.refreshPanes(ctx); e != nil {
		w.record(rec, "kept", "pane-scan-failed")
		return
	}
	if hs.paneInUse(path) {
		w.record(rec, "kept", "pane-in-use")
		return
	}
	if e := hs.exec.RemoveWorktree(ctx, filepath.Dir(tree.Common), path); e != nil {
		w.record(rec, "error", "checkout remove: "+e.Error())
		return
	}
	w.record(rec, "removed", "")
}

// reviewerRow reports whether a receipt is about a reviewer, not the PR's
// author: the PR scan and its worktree candidates ignore these rows.
func reviewerRow(v reapReceipt) bool {
	return v.Action == "reviewer-cleanup" || v.Action == "reviewer-reap"
}
