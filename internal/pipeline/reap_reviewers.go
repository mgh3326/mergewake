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

// The reviewer sweep removes the reviewer worktrees of finished reviewer jobs
// once no pane uses them. A review pass already cleans up after itself
// (panewire_cleanup.go); this sweep picks up what a pass had to keep: a
// checkout whose pane was still running, a spawn whose wait timed out, an
// unconfirmed placement, or a refused removal. It runs whatever the PR's
// state, because a reviewer's job ends on its own clock, not on the merge.
// Panes are never closed here: panewire's node closes an ended job's pane, so
// a checkout still in use simply waits.
//
// Every action needs a reviewer label, a finished reviewer job and a pass
// that is over. A remote worktree goes only through the hub's guarded
// removal; a local checkout is removed only when it is clean, detached at
// the reviewed head and no local pane runs in it.
//
// A reviewer-only exception keeps finished checkouts from piling up; it
// applies to reviewer jobs only. A reviewer job that never left spawned, whose
// pane is idle and whose PR is closed or past the reviewed head, still has its
// job closed abandoned first (abandonSpawned); its pane is left for the node.
// A job still claimed or spawned with no result past review.panewire.stuck_after
// is treated the same way once its pane is proven gone (abandonStuck): panewire's
// node ends only finished records, so without the fallback its worktrees and its
// hk record would wait forever. While its pane is still present the label is
// marked stuck-reviewer once and everything waits.

// reviewerLabelPattern matches the original label form,
// mwr-<pr>-<sha7>-p<pass>-<unix seconds>, still found on hosts and in
// journals. reviewerLabelPatternT matches the compact form new reviewers get,
// mwr-<pr>-<sha7>p<pass>t<base36 seconds since reviewerLabelEpoch> (hk 5960).
// The original pattern rejects the compact form, so a release without it
// ignores compact labels instead of reading their stamp as a 1970s start.
// Both accept panewire's quota-replace chain: one -q<n> per replacement,
// appended to the label it replaces (-q1-q2).
var (
	reviewerLabelPattern  = regexp.MustCompile(`^mwr-(\d+)-([0-9a-f]{7})-p(\d+)-(\d+)((?:-q[1-9]\d*)*)$`)
	reviewerLabelPatternT = regexp.MustCompile(`^mwr-(\d+)-([0-9a-f]{7})p(\d+)t([0-9a-z]+)((?:-q[1-9]\d*)*)$`)
	reviewerQuotaSuffix   = regexp.MustCompile(`-q([1-9]\d*)`)
)

// reviewerLabelEpoch is the origin of a compact label's stamp. A label with a
// Unix-seconds stamp was 30 characters at a 4-digit PR, so panewire's -q<n>
// successors went past herdr's 32-character agent name. From this origin the
// base36 stamp has 6 digits from 2020-01-23 to 2089, so the compact form is 25
// characters at a 4-digit PR and 26 at a 5-digit PR, and two quota successors
// (-q1-q2, six characters) still fit in 32.
const reviewerLabelEpoch int64 = 1577836800 // 2020-01-01T00:00:00Z

// reviewerLabelName is the job id of a new reviewer for pr, head sha7 and
// pass, started at unix seconds.
func reviewerLabelName(pr int, sha7 string, pass int, unix int64) string {
	return fmt.Sprintf("mwr-%d-%sp%dt%s", pr, sha7, pass, strconv.FormatInt(max(unix-reviewerLabelEpoch, 0), 36))
}

type reviewerLabel struct {
	name    string
	pr      int
	sha7    string
	pass    int
	started time.Time
	// replacement is n of the last -q<n> suffix, 0 for an original label.
	replacement int
}

func parseReviewerLabel(name string) (reviewerLabel, bool) {
	var unix int64
	var e3 error
	m := reviewerLabelPattern.FindStringSubmatch(name)
	if m != nil {
		unix, e3 = strconv.ParseInt(m[4], 10, 64)
	} else if m = reviewerLabelPatternT.FindStringSubmatch(name); m != nil {
		var off int64
		off, e3 = strconv.ParseInt(m[4], 36, 64)
		unix = reviewerLabelEpoch + off
	} else {
		return reviewerLabel{}, false
	}
	pr, e1 := strconv.Atoi(m[1])
	pass, e2 := strconv.Atoi(m[3])
	repl := 0
	var e4 error
	if q := reviewerQuotaSuffix.FindAllStringSubmatch(m[5], -1); len(q) > 0 {
		repl, e4 = strconv.Atoi(q[len(q)-1][1])
	}
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || pr <= 0 {
		return reviewerLabel{}, false
	}
	return reviewerLabel{name: name, pr: pr, sha7: m[2], pass: pass, started: time.Unix(unix, 0), replacement: repl}, true
}

// reviewerStream names one receipt stream of a label: the pane stream (the
// gate row, no worktree) or one worktree on one host. rowKey dedupes
// reviewer-reap rows by it.
func reviewerStream(v reapReceipt) string {
	if v.Worktree == "" {
		return "pane"
	}
	return v.Host + "\x00" + v.Worktree
}

// reviewerCheckoutSuffix ends the directory a hub node prepares for a
// spec-placed reviewer (panewire names it <job>-checkout). A local reviewer
// checkout is named for the label alone.
const reviewerCheckoutSuffix = "-checkout"

// reviewerCheckoutDir reports whether a checkout directory's base name is
// label's own: <label> locally, <label>-checkout on the node.
func reviewerCheckoutDir(base, label string) bool {
	return base == label || base == label+reviewerCheckoutSuffix
}

// reviewerCheckoutTTL: a checkout no pane uses is revisited until its label is
// this old, then released even if its job never reached a terminal state.
const reviewerCheckoutTTL = 24 * time.Hour

// reviewerAuditVisitMax bounds one pass's visits of labels found only in the
// audit journal. The first pass after an upgrade reads the whole journal and
// can name a large backlog; each label costs a job-record read, so the rest
// waits for the next pass, which reads the same window again.
const reviewerAuditVisitMax = 200

const (
	// reviewerPathUnknown formats the pending reason of a remote worktree whose
	// path no record names because the host names no work root. It is retried
	// every pass, so adding the setting releases the label.
	reviewerPathUnknown = "remote worktree path unknown: hosts[%s].work_root is not configured"
	// reviewerRefused starts the reason of a removal the hub's guard refused.
	reviewerRefused = "hub guarded removal refused: "
	// reviewerHostUnknown is the terminal reason of an aged label whose job
	// record is gone and whose receipts and audit rows name no host: nothing
	// can build a remote path for it.
	reviewerHostUnknown = "no record names a host for this reviewer label"
	// reviewerAlreadyClosed is the reason of a closed row: panewire refused the
	// job close because the job already has a terminal record, so the job has
	// ended and nothing is owed for it.
	reviewerAlreadyClosed = "already closed"
	// reviewerNotClosable is the reason of a kept row: panewire refused the job
	// close as job_unknown and no terminal kind is known yet. The close is backed
	// off and retried, so the row is not final.
	reviewerNotClosable = "hub does not know the job; not closable via hub"
	// reviewerOutOfScope is the reason of a kept row: the hub refused the close
	// as scope_forbidden, so the job belongs to a lane this credential may not
	// close (an old reviewer another lane already closed, typically).
	reviewerOutOfScope = "job is outside this credential's close scope; not closable via hub"
	// reviewerOtherRepo starts the reason of a skipped row: a journal-only label
	// whose job record names another repository is not this sweep's to close.
	reviewerOtherRepo = "job names another repository: "
	// reviewerDrainHold is the pending reason of a removal held back by
	// reap.reviewer_drain.per_host: the next pass takes it.
	reviewerDrainHold = "drain cap reached for this host; the next pass continues"
	// reviewerBudgetHold is the pending reason of a reviewer step the repo
	// budget cut: the checkout is kept for a later sweep, and the cut is not an
	// error row.
	reviewerBudgetHold = "repo budget ended this step; checkout kept for a later sweep"
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
		return strings.Contains(v.Reason, "already gone") || strings.HasPrefix(v.Reason, reviewerOtherRepo)
	case "kept":
		return v.Worktree != "" && (v.Reason == reviewerHostUnknown || strings.HasPrefix(v.Reason, reviewerRefused))
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

// reviewerLabelOf names the label a reviewer receipt is about. A
// reviewer-cleanup row carries it since the sweep stopped depending on a pane;
// an older row names the checkout or remote worktree whose directory base is
// the label — with the node's -checkout suffix for a spec-placed reviewer.
func reviewerLabelOf(v reapReceipt) string {
	if v.Label != "" {
		return v.Label
	}
	if v.Action == "reviewer-cleanup" {
		base := strings.TrimSuffix(filepath.Base(filepath.Clean(v.Worktree)), reviewerCheckoutSuffix)
		if base == "." || base == "/" || base == "" {
			return ""
		}
		return base
	}
	return ""
}

type reviewerSweep struct {
	s          *Service
	r          config.Repo
	hosts      []*reapHost
	acting     bool
	now        time.Time
	settle     time.Duration // review.timeout + 1m: a younger label's pass may still run
	stuckAfter time.Duration // review.panewire.stuck_after: a claimed/spawned job this old with no result is treated as ended once its pane is gone
	jobs       reapJobs
	note       func(reapReceipt) error
	errs       []error
	// streams is the newest reviewer-reap row per label and stream, as the
	// receipts stood when the pass started.
	streams map[string]map[string]reapReceipt
	local   *reapHost
	// remote is the newest remote worktree path a receipt recorded per label
	// and host, the fallback when the pane is gone and the job names no cwd.
	remote map[string]map[string]string
	// abandoned names labels an earlier pass closed abandoned. Their job record
	// still reads spawned until panewire's sweep ends the job (then lost), so
	// this receipt, not the record, stops a second abandon.
	abandoned map[string]bool
	// stuck names labels already marked stuck-reviewer: a reviewer job past
	// the stuck bound whose pane is still present. The mark is written once;
	// the label keeps being revisited for the pane or the fallback.
	stuck map[string]bool
	// pulls memoises the PR reads of one pass; a failed read is not kept.
	pulls map[int]github.Pull
	// audits names labels the repo's reviewer audit rows named, with the host
	// and pane each row recorded when it recorded them.
	audits map[string]reviewerAudit
	// auditOnly marks labels whose only source is those audit rows: no reap
	// receipt and no live pane. A label that is not visited this pass must be
	// read again, so the audit offset is not saved.
	auditOnly map[string]bool
	// pendingAudit reports that an audit-only label was left unvisited: the
	// scan offset stays put so the next pass sees it again.
	pendingAudit bool
	// drain is the guarded removals one host still takes this pass, and host
	// the names in host_order.
	drain    map[string]int
	hostRank map[string]int
	// paneHost maps a PR and pane id to the host a receipt recorded for it,
	// the fallback when a leaked label's audit row names no host. A key two
	// hosts claim is not used: the pane id alone would be a guess.
	paneHost map[paneKey]string
	// paneConflict marks a PR and pane a receipt recorded on two hosts.
	paneConflict map[paneKey]bool
	// labelHost is the newest host a receipt recorded for a label.
	labelHost map[string]string
	// rowed names labels this pass wrote a durable row for.
	rowed map[string]bool
	// writeFailed reports that a row could not be written, so the journal
	// checkpoint must not pass the labels whose rows were lost.
	writeFailed bool
	// scopeForbidden counts the closes this pass the hub refused as
	// scope_forbidden, and scopeForbiddenSample names one of them. Three or
	// more mean the hub credential may have lost its close grant (hk 6010).
	scopeForbidden       int
	scopeForbiddenSample string
	// resumable marks a durable full sweep: only it resumes the visit order
	// from, and records where its budget stopped (reviewer_cursor.go).
	resumable bool
	// unreadable marks a sweep cursor the sweep could not read: the sweep
	// starts from the front and its next save overwrites the file.
	unreadable bool
}

// reviewerCandidate is one label to visit, the host it is known to run on and,
// when a pane drove it, the host and pane it was seen on.
type reviewerCandidate struct {
	label reviewerLabel
	host  string
	seen  *reapHost
	pane  *reap.Pane
}

// reapReviewers runs the sweep on every scanned host. prNum > 0 limits it to
// that PR's labels. durable reports that this pass writes its rows: only then
// is the audit scan's offset saved, so a dry run reads the same rows again.
// note writes one reviewer-reap row and its audit record.
func (s *Service) reapReviewers(ctx context.Context, r config.Repo, prNum int, receipts []reapReceipt, snapshot time.Time, hosts []*reapHost, acting, durable bool, note func(reapReceipt) error) []error {
	timeout := s.Config.Review.Timeout
	if timeout == "" {
		timeout = "10m" // config's default when the key is unset
	}
	d, e := config.Duration(timeout)
	if e != nil {
		return []error{fmt.Errorf("reviewer reap: review.timeout: %w", e)}
	}
	stuckAfter, e := reviewerStuckAfter(s.Config)
	if e != nil {
		return []error{fmt.Errorf("reviewer reap: review.panewire.stuck_after: %w", e)}
	}
	w := &reviewerSweep{s: s, r: r, hosts: hosts, acting: acting, now: s.now(), settle: d + time.Minute, stuckAfter: stuckAfter, note: note, streams: map[string]map[string]reapReceipt{}, remote: map[string]map[string]string{}, abandoned: map[string]bool{}, stuck: map[string]bool{}, pulls: map[int]github.Pull{}, audits: map[string]reviewerAudit{}, auditOnly: map[string]bool{}, rowed: map[string]bool{}, resumable: prNum == 0 && durable}
	if jobs, ok := s.Sink.(reapJobs); ok {
		w.jobs = reapTimedJobs{base: jobs, telemetry: telemetryFrom(ctx)}
	}
	for _, hs := range hosts {
		if hs.host.Local() && w.local == nil {
			w.local = hs
		}
	}
	w.initDrain()
	// A credential-wide close failure: several scope_forbidden refusals in one
	// pass mean the hub credential may have lost its close grant. One ERROR
	// line per pass names the count and a sample label (hk 6010). It is logged
	// on every exit once the sweep has started, an early return included (hk 6014).
	defer func() {
		if w.scopeForbidden >= 3 && s.Log != nil {
			s.Log.Error("reap: the hub credential may have lost its close grant: scope_forbidden closes refused", "repo", r.Name, "count", w.scopeForbidden, "sample", w.scopeForbiddenSample)
		}
	}()

	lastCleanup := w.collect(receipts)
	// The audit journal names labels whose rows predate the label field or
	// whose pass wrote no receipt the sweep can read. A single-PR sweep covers
	// only its own rows, so it does not consume the journal.
	var auditOffset int64
	auditRead := false
	scanAudit := prNum == 0
	if scanAudit {
		labels, off, err := s.reviewerAuditLabels(r.Name)
		if err != nil {
			w.errs = append(w.errs, fmt.Errorf("reviewer reap: audit journal: %w", err))
		} else {
			w.audits, auditOffset, auditRead = labels, off, true
		}
	}

	cands := w.candidates(lastCleanup, prNum)
	// A full sweep resumes after the candidate its last budget stopped at, so
	// a label behind a run of open ones is reached on a later pass.
	var from reviewerCursor
	if w.resumable {
		c, e := s.reviewerSweepCursor(r.Name)
		if e != nil {
			w.errs = append(w.errs, fmt.Errorf("reviewer reap: sweep cursor: %w", e))
			w.unreadable = true
		} else {
			from = c
		}
		cands = w.resume(cands, from)
	}
	auditVisits := 0
	for i, c := range cands {
		if e := reapBudgetCheck(ctx, "reviewer labels and checkout cleanup"); e != nil {
			w.stopAt(cands[i])
			return append(w.errs, e)
		}
		if e := ctx.Err(); e != nil {
			w.stopAt(cands[i])
			return append(w.errs, e)
		}
		if w.auditOnly[c.label.name] {
			// A host whose scan failed can answer nothing this pass: its
			// labels must not spend the visit budget the healthy hosts need.
			if w.hostScanFailed(c.host) {
				w.pendingAudit = true
				continue
			}
			// Only a durable row spends a visit, so a label that is waiting
			// (settle, a spent drain cap) cannot starve the rest.
			if auditVisits >= reviewerAuditVisitMax {
				w.pendingAudit = true
				continue
			}
		}
		w.visit(ctx, c)
		// An audit-only label with no row of its own is read again next pass:
		// nothing else would name it.
		if w.auditOnly[c.label.name] {
			if !w.rowed[c.label.name] {
				w.pendingAudit = true
			} else {
				auditVisits++
			}
		}
	}
	w.finished(from)
	if acting {
		if e := s.reviewerDailySummary(r.Name, receipts, hosts, snapshot); e != nil {
			w.errs = append(w.errs, e)
		}
	}
	// Only a pass that wrote its rows, and that visited every audit-only
	// label it found, moves the journal offset: anything else is read again.
	if durable && auditRead && !w.pendingAudit && !w.writeFailed {
		if e := s.saveReviewerAuditOffset(r.Name, auditOffset); e != nil {
			w.errs = append(w.errs, e)
		}
	}
	return w.errs
}

// collect folds the repo's receipts into the sweep's state and returns the
// newest reviewer-cleanup row per label: the source a pass end leaves even when
// no pane outlives it.
func (w *reviewerSweep) collect(receipts []reapReceipt) map[string]reapReceipt {
	lastCleanup := map[string]reapReceipt{}
	for _, rec := range receipts {
		switch rec.Action {
		case "reviewer-reap":
			// An abandoned row, or a closed row for a job panewire already closed,
			// means the job close is not owed again: the restart keeps it so.
			if rec.Result == "abandoned" || (rec.Result == "closed" && strings.HasPrefix(rec.Reason, reviewerAlreadyClosed)) {
				w.abandoned[rec.Label] = true
			}
			if rec.Result == "stuck-reviewer" {
				w.stuck[rec.Label] = true
			}
			if w.streams[rec.Label] == nil {
				w.streams[rec.Label] = map[string]reapReceipt{}
			}
			w.streams[rec.Label][reviewerStream(rec)] = rec
			if rec.Worktree != "" && rec.Worktree != rec.Label {
				w.remember(rec.Label, rec.Host, rec.Cwd)
			}
			w.rememberHost(rec.Label, rec.Host)
			w.rememberPane(rec.PR, rec.Pane, rec.Host)
		case "reviewer-cleanup":
			// An orphan-sweep row (no PR) names a checkout no job record owns: the
			// orphan sweep's guards decide it, so it is no reviewer candidate here.
			if rec.PR == 0 {
				continue
			}
			// The pane-to-host record outlives the label: an older receipt
			// names the pane and host of a row whose label the journal keeps.
			w.rememberPane(rec.PR, rec.Pane, rec.Host)
			label := reviewerLabelOf(rec)
			if label == "" {
				continue
			}
			lastCleanup[label] = rec
			// Cwd holds the remote pane's cwd or the remote worktree. A row
			// written since the label field exists can name the node's
			// worktree in Worktree while its pane is still present (Cwd
			// empty); that path is remembered too. An older row's Worktree is
			// this machine's checkout, which is no remote path, and so is
			// <review.panewire.checkouts>/<label>.
			path := rec.Cwd
			if path == "" && rec.Label != "" && !w.isLocalCheckout(rec.Worktree, label) {
				path = rec.Worktree
			}
			w.remember(label, rec.Host, path)
			w.rememberHost(label, rec.Host)
		}
	}
	return lastCleanup
}

// initDrain bounds the pass's guarded removals per host and fixes the order
// the labels are visited in: reap.reviewer_drain.host_order first, then the
// configured hosts in config order.
func (w *reviewerSweep) initDrain() {
	w.drain = map[string]int{}
	w.hostRank = map[string]int{}
	w.paneHost = map[paneKey]string{}
	w.paneConflict = map[paneKey]bool{}
	w.labelHost = map[string]string{}
	per := w.s.Config.Reap.ReviewerDrain.PerHost
	if per == 0 {
		per = config.DefaultReviewerDrainPerHost
	}
	rank := 0
	for _, name := range w.s.Config.Reap.ReviewerDrain.HostOrder {
		if _, ok := w.hostRank[name]; !ok {
			w.hostRank[name] = rank
			rank++
		}
	}
	for _, hs := range w.hosts {
		if _, ok := w.hostRank[hs.host.Name]; !ok {
			w.hostRank[hs.host.Name] = rank
			rank++
		}
		w.drain[hs.host.Name] = per
	}
}

// paneKey identifies a pane a reviewer pass ran in; a PR and pane pair is
// what a receipt and an audit row can both name.
type paneKey struct {
	pr   int
	pane string
}

// rememberHost records the host a reviewer receipt named for a label, so an
// audit row without a host still resolves one.
func (w *reviewerSweep) rememberHost(label, host string) {
	if label == "" || host == "" {
		return
	}
	w.labelHost[label] = w.hostName(host)
}

// rememberPane records the host a receipt recorded for a PR's pane. Two hosts
// claiming one pane leave the key unresolved rather than guessed.
func (w *reviewerSweep) rememberPane(pr int, pane, host string) {
	if pane == "" || host == "" {
		return
	}
	k := paneKey{pr: pr, pane: pane}
	name := w.hostName(host)
	if prev, ok := w.paneHost[k]; ok && prev != name {
		w.paneConflict[k] = true
		return
	}
	w.paneHost[k] = name
}

// hostName resolves a recorded host name or machine id to the configured host
// name; a name no configured host answers to is kept as it is.
func (w *reviewerSweep) hostName(name string) string {
	for _, hs := range w.hosts {
		if hs.host.Name == name || hs.host.MachineID() == name {
			return hs.host.Name
		}
	}
	return name
}

// rank orders hosts for the visit: the configured drain order, then the rest.
func (w *reviewerSweep) rank(host string) int {
	if r, ok := w.hostRank[host]; ok {
		return r
	}
	return len(w.hostRank)
}

// drained reports whether host has no guarded removal left this pass.
func (w *reviewerSweep) drained(host string) bool {
	n, ok := w.drain[host]
	return ok && n <= 0
}

// takeDrain spends one of host's guarded removals for this pass.
func (w *reviewerSweep) takeDrain(host string) bool {
	n, ok := w.drain[host]
	if !ok || n <= 0 {
		return false
	}
	w.drain[host] = n - 1
	return true
}

// hostScanFailed reports whether a host named by a candidate could not be read
// this pass, so nothing about it can be proven.
func (w *reviewerSweep) hostScanFailed(name string) bool {
	hs := w.scannedHost(name)
	return hs != nil && hs.failed
}

// hostUnknownTerminal reports whether every stream of a label ended and one of
// them ended because no host was named for it.
func (w *reviewerSweep) hostUnknownTerminal(label string) bool {
	found := false
	for _, v := range w.streams[label] {
		if !reviewerTerminal(v) {
			return false
		}
		if v.Result == "kept" && v.Reason == reviewerHostUnknown {
			found = true
		}
	}
	return found
}

// hostIsLocal reports whether a candidate host is this machine, whose
// checkouts are removed without the hub's guard and so without the cap.
func (w *reviewerSweep) hostIsLocal(name string) bool {
	for _, hs := range w.hosts {
		if hs.host.Name == name {
			return hs.host.Local()
		}
	}
	return false
}

// finishedLabel reports whether every receipt stream of a label ended, and no
// checkout is left on disk to confirm: the label needs no visit.
func (w *reviewerSweep) finishedLabel(label string, lastCleanup map[string]reapReceipt) bool {
	// A checkout still on disk keeps the label open until its own removal
	// is confirmed.
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

// candidates names every label this pass visits, in host order then label
// order: a pane named <label> on a host, a receipt stream or cleanup row, and
// a reviewer audit row. A label with a live pane is visited through that pane;
// a label whose streams all ended is not visited at all.
func (w *reviewerSweep) candidates(lastCleanup map[string]reapReceipt, prNum int) []reviewerCandidate {
	keep := func(name string) (reviewerLabel, bool) {
		l, ok := parseReviewerLabel(name)
		return l, ok && (prNum == 0 || l.pr == prNum)
	}
	var cands []reviewerCandidate
	paned := map[string]bool{}
	for _, hs := range w.hosts {
		if hs.failed {
			continue
		}
		// The agent list names panes; the full pane list serves the in-use
		// check that keeps a checkout its pane still uses.
		for i := range hs.panes {
			pane := hs.panes[i]
			l, ok := keep(pane.Name)
			if !ok || pane.ID == "" {
				continue
			}
			// A label names a PR number, not a repo: every repo's sweep sees
			// every reviewer pane on the host. Only a pane standing in a
			// worktree of this repo is this sweep's; an unknown worktree is
			// not, so another repo's reviewer is never receipted here.
			if tree := hs.trees[pane.Cwd]; tree == nil || !strings.EqualFold(tree.Repo, w.r.Name) {
				continue
			}
			paned[l.name] = true
			cands = append(cands, reviewerCandidate{label: l, host: hs.host.Name, seen: hs, pane: &pane})
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
		if paned[label] || w.finishedLabel(label, lastCleanup) {
			continue
		}
		if l, ok := keep(label); ok {
			cands = append(cands, reviewerCandidate{label: l, host: w.labelHost[label]})
		}
	}
	// The journal names labels whose receipts predate the label field, and a
	// later row can name the host an older row did not: merge what any row
	// recorded before the receipts order and open the candidates below.
	for name, a := range w.audits {
		l, ok := keep(name)
		if !ok {
			continue
		}
		host := w.hostName(a.host)
		if host == "" && a.pane != "" {
			k := paneKey{pr: l.pr, pane: a.pane}
			if !w.paneConflict[k] {
				host = w.paneHost[k]
			}
		}
		if host == "" {
			host = w.labelHost[name]
		}
		if host != "" {
			w.labelHost[name] = host
		}
		switch {
		case w.streams[name] == nil:
			if _, ok := lastCleanup[name]; ok {
				continue // the receipts own it
			}
			// The label is named by the journal and has no receipt yet: even
			// a live pane's candidate must not consume the window until a row
			// of its own exists, or a crash could take the pane away unseen.
			w.auditOnly[name] = true
			if !paned[name] {
				cands = append(cands, reviewerCandidate{label: l, host: host})
			}
		case host != "" && !paned[name] && w.hostUnknownTerminal(name):
			// The label once ended because no host was named; a later journal
			// row named one, so it is a candidate again.
			cands = append(cands, reviewerCandidate{label: l, host: host})
		}
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if ri, rj := w.rank(cands[i].host), w.rank(cands[j].host); ri != rj {
			return ri < rj
		}
		return cands[i].label.name < cands[j].label.name
	})
	return cands
}

// reviewerStuckAfter is review.panewire.stuck_after: how old a claimed or
// spawned reviewer job with no result may get before it counts as stuck. Six
// hours when unset.
func reviewerStuckAfter(c config.Config) (time.Duration, error) {
	if v := c.Review.Panewire.StuckAfter; v != "" {
		return config.Duration(v)
	}
	return 6 * time.Hour, nil
}

// remember records a remote path a receipt named for a label on a host; only
// an absolute path whose base is the label — or the node's <label>-checkout —
// counts.
func (w *reviewerSweep) remember(label, host, path string) {
	if label == "" || host == "" || !filepath.IsAbs(path) || !reviewerCheckoutDir(filepath.Base(filepath.Clean(path)), label) {
		return
	}
	if w.remote[label] == nil {
		w.remote[label] = map[string]string{}
	}
	w.remote[label][host] = path
}

// isLocalCheckout reports whether path is the label's local reviewer checkout,
// the one thing a receipt's Worktree names that is not on a remote host.
func (w *reviewerSweep) isLocalCheckout(path, label string) bool {
	dir := config.Expand(w.s.Config.Review.Panewire.Checkouts)
	if path == "" || dir == "" {
		return false
	}
	return filepath.Clean(path) == filepath.Clean(filepath.Join(dir, label))
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
	if e := w.note(v); e != nil {
		w.errs = append(w.errs, e)
		w.writeFailed = true
		return
	}
	if v.Label != "" {
		w.rowed[v.Label] = true
	}
}

// visit applies the sweep to one label: the gates (I2–I4), the remote
// worktree (I6) and the local checkout (I7), in that order. The pane itself
// is never closed here: panewire's node closes an ended job's pane, and a
// pane still using a worktree keeps it through the pane-in-use guards below.
// Any step that does not finish stops the later ones.
func (w *reviewerSweep) visit(ctx context.Context, c reviewerCandidate) {
	l := c.label
	// I3: the pass that spawned the pane may still be waiting on it.
	if !l.started.Add(w.settle).Before(w.now) {
		return
	}
	// A host whose guarded removals for this pass are spent waits for the next
	// one: its labels keep their streams or their audit rows until then.
	if c.host != "" && !w.hostIsLocal(c.host) && w.drained(c.host) {
		return
	}
	gate := w.row(l)
	gate.Host = c.host
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
		// No record names the host any more: the receipts and audit rows still
		// name it, and a host's work root builds the node's path.
		w.removeUnrecorded(ctx, l, c)
		return
	case errors.Is(e, integrations.ErrJobNotFound):
		w.record(gate, "kept", "job-record-missing")
		return
	case e != nil:
		w.record(gate, "error", "job record: "+integrations.Detail(e))
		return
	}
	// A journal-only label (no pane in this repo's worktree) whose job record
	// names another repository is that repository's reviewer: nothing here is
	// owed for it, so it gets no close and no removal, only a terminal row.
	if c.seen == nil {
		if other := jobOtherRepo(job, w.r.Name); other != "" {
			w.record(gate, "skipped", reviewerOtherRepo+other)
			return
		}
	}
	// A reviewer job stuck in spawned is closed abandoned when its review can
	// no longer matter; its pane is left for panewire's node to close, and the
	// worktree cleanup below runs once no pane uses it.
	reaped := job.State == "completed"
	stuck := w.stuckJob(job, l)
	if job.State == "spawned" && job.Role == "reviewer" && c.pane != nil && !w.abandoned[l.name] {
		if !w.abandonSpawned(ctx, c, gate, job) {
			if stuck {
				w.noteStuck(gate, l, job)
			}
			return
		}
		reaped = true
	}
	// stuckEnded marks a stuck job whose pane is proven gone: it is treated
	// as ended below, and its record is closed abandoned once every keep-gate
	// has passed, just before the worktrees go.
	stuckEnded := false
	if stuck && !reaped {
		switch {
		case !unused:
			// The pane is still there: panewire's node owns its close and
			// the worktrees wait for it. The label is marked once so the
			// owner sees it.
			w.noteStuck(gate, l, job)
			return
		case !w.abandoned[l.name] && !w.stuckPaneGone(ctx, l, job):
			// No pane carries the label, but the job's own pane still
			// lists — or no host could prove it absent: same mark, same
			// wait.
			w.noteStuck(gate, l, job)
			return
		default:
			stuckEnded = true
		}
	}
	held := !reaped && !stuckEnded && !w.abandoned[l.name]
	// A lost reviewer job cannot be holding anyone's work either: its pane is
	// left for panewire's node to close. Only reviewer jobs get this; a lost
	// worker or implementer keeps its hold.
	if held && job.Role == "reviewer" && job.State == "lost" && c.pane != nil && paneFinished(c.pane.Status) {
		held = false
	}
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
	case !reviewerReapRole(job.Role):
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
	} else if c.host != "" {
		// The record names no host; the receipts, the audit row or the pane
		// does, and the visit goes on only with a scanned host.
		hs = w.scannedHost(c.host)
		if hs != nil {
			gate.Host = hs.host.Name
			if hs.failed {
				// The host's scan already reported its error.
				return
			}
		}
	}
	cwd := job.Cwd
	if cwd == "" && hs != nil {
		cwd = w.remote[l.name][hs.host.Name]
		if cwd == "" {
			cwd = w.remote[l.name][hs.host.MachineID()]
		}
	}
	if cwd == "" && c.pane != nil {
		// A pane still around names the worktree it uses; the pane-in-use
		// guards below keep it until panewire's node closes the pane.
		cwd = c.pane.Cwd
	}
	if stuckEnded && !w.abandoned[l.name] && !w.abandonStuck(ctx, gate, job, l, hs) {
		return
	}
	if hs != nil {
		// L1: an unknown remote path is recorded as pending-cleanup and does not
		// block the local checkout. L2: a refused or failed removal does.
		if !hs.host.Local() && w.removeRemote(ctx, hs, l, cwd) == remoteStop {
			return
		}
	}
	w.removeLocal(ctx, l)
}

func reviewerReapRole(role string) bool {
	switch role {
	case "reviewer", "worker", "implementer", "tester", "counsel", "consult":
		return true
	default:
		return false
	}
}

// paneFinished reports whether a pane is idle or done: it is not running work
// and is not waiting at a prompt, so a person is not typing in it.
func paneFinished(status string) bool {
	return status == "idle" || status == "done"
}

// abandonSpawned closes a reviewer job that is still spawned when its pane is
// idle or done and its review is over: the PR merged or closed, or the head
// the label reviewed is no longer the PR head. It reports whether the sweep may
// go on to the pane. The close is panewire's job close, the path a lost
// reviewer's replacement takes; a PR still open at the reviewed head, an
// unreadable PR or a busy pane leaves the job spawned.
func (w *reviewerSweep) abandonSpawned(ctx context.Context, c reviewerCandidate, gate reapReceipt, job integrations.JobRecord) bool {
	hs := c.seen
	keep := func() bool {
		w.record(gate, "kept", "job:spawned")
		return false
	}
	if !paneFinished(c.pane.Status) {
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
	// The pane's status came from a host scan that an earlier repository of the
	// pass may have taken. Read the pane again before the close, so a pane that
	// started work since the scan keeps its job.
	if fresh, e := freshPaneFinished(ctx, c); e != nil {
		w.record(gate, "error", "abandon job: pane read: "+e.Error())
		return false
	} else if !fresh {
		return keep()
	}
	if w.closeRetryBlocked(l.name, reapPass(ctx)) {
		return false
	}
	if job.OwnerLane == "" {
		w.record(gate, "error", "abandon job: owner lane unavailable")
		return false
	}
	cmd := w.s.Config.Review.Panewire.Command
	argv := integrations.HubArgv(w.s.Config.Reap.Hub, "job", "close", l.name, "--lane", job.OwnerLane, "--outcome", "abandoned", "--reason", reason)
	if _, e := reapRunner(ctx, w.s.Runner).Run(ctx, cmd, argv, "", ""); e != nil {
		if alreadyClosedRefusal(e) {
			return w.alreadyClosed(gate, l)
		}
		if unknownJobRefusal(e) {
			return w.unknownJob(ctx, gate, l, hs)
		}
		if scopeForbiddenRefusal(e) {
			return w.settleScopeForbidden(ctx, gate, l, hs, job, e)
		}
		// The retry key is the failure id, not the reason: the reason keeps
		// stderr, which can change per call. The row carries the id so a
		// restart restores the same backoff (closeRetryBlocked).
		key := abandonRetryKey(e)
		w.s.reapRetryFailed(w.r.Name, l.name, key, reapPass(ctx))
		gate.RetryKey = key
		w.record(gate, "error", "abandon job: "+fleetevent.Redact(integrations.Detail(e)))
		return false
	}
	w.s.reapRetrySucceeded(w.r.Name, l.name)
	w.abandoned[l.name] = true
	w.record(gate, "abandoned", reason)
	return true
}

// alreadyClosed settles a close panewire refused because the job already has a
// terminal record: the job has ended, so the label gets a closed row, its retry
// state clears and no later pass closes it again. The sweep goes on to cleanup
// as after a close.
func (w *reviewerSweep) alreadyClosed(gate reapReceipt, l reviewerLabel) bool {
	w.s.reapRetrySucceeded(w.r.Name, l.name)
	w.abandoned[l.name] = true
	w.record(gate, "closed", reviewerAlreadyClosed)
	return true
}

// unknownJob settles a close panewire refused as job_unknown: the hub does not
// know the job. A terminal event in the job's node dir means the job has ended:
// the label gets a closed row that names the event, as alreadyClosed does, and
// the sweep goes on to cleanup. Without one the close is backed off like a failed
// close, with no error row, and the label gets one kept row. Nothing is
// recorded as permanent: a later pass retries once the backoff passes, so the
// label settles when the node reports a terminal kind.
func (w *reviewerSweep) unknownJob(ctx context.Context, gate reapReceipt, l reviewerLabel, hs *reapHost) bool {
	return w.notClosable(ctx, gate, l, hs, "job_unknown", reviewerNotClosable)
}

// settleScopeForbidden settles a close the hub refused as scope_forbidden: the
// credential may not close jobs of that lane. A terminal event in the job's
// node dir means the job has ended and the label is closed, whatever lane
// owns it. Otherwise a job owned by the lane mergewake closes under (the
// configured review.panewire.owner) is an error, as before hk 6009, plus one
// notice a day; any other lane's job takes the benign kept path (hk 6010).
// Every refusal counts toward the pass's credential-wide failure.
func (w *reviewerSweep) settleScopeForbidden(ctx context.Context, gate reapReceipt, l reviewerLabel, hs *reapHost, job integrations.JobRecord, closeErr error) bool {
	w.scopeForbidden++
	if w.scopeForbiddenSample == "" {
		w.scopeForbiddenSample = l.name
	}
	if kind := w.terminalEvent(ctx, hs, l.name); kind != "" {
		w.s.reapRetrySucceeded(w.r.Name, l.name)
		w.abandoned[l.name] = true
		w.record(gate, "closed", "already closed (scope_forbidden, terminal event "+kind+")")
		return true
	}
	if w.ownsJob(job) {
		// The retry key is the failure id (abandonRetryKey), kept on the row.
		key := abandonRetryKey(closeErr)
		w.s.reapRetryFailed(w.r.Name, l.name, key, reapPass(ctx))
		gate.RetryKey = key
		w.record(gate, "error", "abandon job: "+fleetevent.Redact(integrations.Detail(closeErr)))
		w.noteScopeForbidden(ctx, l, job, closeErr)
		return false
	}
	gate.RetryAfter = w.s.reapRetryFailed(w.r.Name, l.name, reviewerOutOfScope, reapPass(ctx))
	w.record(gate, "kept", reviewerOutOfScope)
	return false
}

// ownsJob reports whether job belongs to the lane mergewake closes under (the
// configured review.panewire.owner). Only that lane's scope_forbidden refusal
// is mergewake's own failure; another lane's job is benign (hk 6010).
func (w *reviewerSweep) ownsJob(job integrations.JobRecord) bool {
	owner := w.s.Config.Review.Panewire.Owner
	return owner != "" && job.OwnerLane == owner
}

// reapScopeNoticeWithin bounds the delivery of a scope_forbidden notice, inside
// the reap context, so the serial repo sweep is not held past its deadline (hk 6014).
const reapScopeNoticeWithin = 10 * time.Second

// noteScopeForbidden counts one own-lane scope_forbidden refusal by its cause.
// The gate in notice_cause.go sends at most one notice a day. A counting
// failure is logged, not returned: it must not change the close.
func (w *reviewerSweep) noteScopeForbidden(ctx context.Context, l reviewerLabel, job integrations.JobRecord, closeErr error) {
	detail := fmt.Sprintf("close of %s refused: scope_forbidden (own lane %s)", l.name, job.OwnerLane)
	if d := fleetevent.Redact(integrations.Detail(closeErr)); d != "" {
		detail += ": " + d
	}
	ev := causeEvent{Cause: causeReapCloseScopeForbidden, PR: l.pr, Head: l.sha7, Pass: l.pass, Detail: detail, Within: reapScopeNoticeWithin}
	if err := w.s.noteCause(ctx, w.r, ev); err != nil && w.s.Log != nil {
		w.s.Log.Warn("reap close-scope-forbidden notice failed", "repo", w.r.Name, "label", l.name, "cause", ev.Cause, "error", err)
	}
}

// notClosable settles a close the hub refused for a reason no retry changes
// (job_unknown): a terminal event in the job's node dir means it has ended
// and the label is closed; otherwise one kept row with why, no error, and the
// close backs off like a failed one. A scope_forbidden refusal for another
// lane's job settles the same way through scopeForbidden.
func (w *reviewerSweep) notClosable(ctx context.Context, gate reapReceipt, l reviewerLabel, hs *reapHost, code, why string) bool {
	if kind := w.terminalEvent(ctx, hs, l.name); kind != "" {
		w.s.reapRetrySucceeded(w.r.Name, l.name)
		w.abandoned[l.name] = true
		w.record(gate, "closed", "already closed ("+code+", terminal event "+kind+")")
		return true
	}
	gate.RetryAfter = w.s.reapRetryFailed(w.r.Name, l.name, why, reapPass(ctx))
	w.record(gate, "kept", why)
	return false
}

// terminalEvent names the terminal event in a job's node dir, "" when none can
// be read. A host the daemon reaches directly is read in its job dir, which
// holds every terminal kind. A hub host's job dir belongs to its node, which the
// daemon cannot read: the node's job.result answers for it. Its terminal_event
// field, when the node sends one, is the authority, and "" there means none.
// A node that predates the field answers only completed, read as job.completed.
func (w *reviewerSweep) terminalEvent(ctx context.Context, hs *reapHost, label string) string {
	switch {
	case hs == nil:
		return ""
	case !hs.viaHub:
		kind, e := hs.exec.TerminalEvent(ctx, label)
		if e != nil {
			return ""
		}
		return kind
	}
	hub, e := w.s.reapHub()
	if e != nil || hub == nil {
		return ""
	}
	var v struct {
		Completed     bool    `json:"completed"`
		TerminalEvent *string `json:"terminal_event"`
	}
	if e := hub.Command(ctx, hs.host.MachineID(), "job.result", map[string]string{"job": label}, &v); e != nil {
		return ""
	}
	if v.TerminalEvent != nil {
		if reap.IsTerminalJobEvent(*v.TerminalEvent) {
			return *v.TerminalEvent
		}
		return ""
	}
	if v.Completed {
		return "job.completed"
	}
	return ""
}

// jobOtherRepo is the repository a job record's worktree origin names when it
// is not repo, "" when the record names no repository or names repo itself.
func jobOtherRepo(job integrations.JobRecord, repo string) string {
	if job.Worktree == nil {
		return ""
	}
	origin := reap.RepoFromRemote(job.Worktree.Origin)
	if origin == "" || strings.EqualFold(origin, repo) {
		return ""
	}
	return origin
}

// closeRetryBlocked also honors the last durable close failure after a daemon
// restart. The in-memory schedule provides increasing delays; the receipt
// keeps a restart from immediately repeating the same failing CLI call. A kept
// not-closable row (reviewerNotClosable or reviewerOutOfScope) restores the
// backoff the same way an abandon-job error row does (hk 6010).
func (w *reviewerSweep) closeRetryBlocked(label string, pass uint64) bool {
	if !w.s.reapRetryAllowed(w.r.Name, label, pass) {
		return true
	}
	for _, row := range w.streams[label] {
		if row.Result == "error" && strings.HasPrefix(row.Reason, "abandon job:") {
			key := row.RetryKey
			if key == "" {
				// A row an older version wrote has no id: its reason is the key.
				key = strings.TrimPrefix(row.Reason, "abandon job: ")
			}
			return w.s.reapRetryRestore(w.r.Name, label, key, pass)
		}
		if row.Result == "kept" && (row.Reason == reviewerNotClosable || row.Reason == reviewerOutOfScope) {
			if !row.RetryAfter.IsZero() {
				// The row's own deadline governs, so a standalone reap in a fresh
				// process retries once it has passed (hk 6014). Nothing is restored
				// in memory: the row is read again on every visit.
				return w.s.now().Before(row.RetryAfter)
			}
			return w.s.reapRetryRestore(w.r.Name, label, row.Reason, pass)
		}
	}
	return false
}

// reviewerStuckRole reports whether role is a reviewer seat or the worker seat
// a refused reviewer role is retried as. workerSeatArgs appends --role
// implementer, and a panewire that knows only the older spelling records that
// same seat as worker (panewireSpawnArgs, oldPanewireSpawnArgs). No other role
// writes a reviewer label, so no other role takes the stuck fallback.
func reviewerStuckRole(role string) bool {
	switch role {
	case "reviewer", "implementer", "worker":
		return true
	default:
		return false
	}
}

// stuckJob reports whether job is a reviewer job that never left claimed or
// spawned, carries no result, and is older than the stuck bound: panewire's
// node ends only jobs whose record finished, so such a job's pane and
// worktrees would otherwise wait forever.
func (w *reviewerSweep) stuckJob(job integrations.JobRecord, l reviewerLabel) bool {
	if job.State != "spawned" && job.State != "claimed" {
		return false
	}
	return len(job.Result) == 0 && reviewerStuckRole(job.Role) && !l.started.Add(w.stuckAfter).After(w.now)
}

// noteStuck marks a stuck reviewer once per label: its job is past the stuck
// bound with no result and its pane is still present, so the owner can see
// it. The pane is left for panewire's node and the worktrees keep waiting;
// the row is not terminal, so the label stays a candidate.
func (w *reviewerSweep) noteStuck(gate reapReceipt, l reviewerLabel, job integrations.JobRecord) {
	if w.stuck[l.name] {
		return
	}
	w.stuck[l.name] = true
	w.record(gate, "stuck-reviewer", fmt.Sprintf("job %s with no result for %s; pane left for panewire's node", job.State, w.now.Sub(l.started).Round(time.Minute)))
}

// paneListed reports whether id is in the host's agent or full pane list.
func paneListed(hs *reapHost, id string) bool {
	for _, list := range [][]reap.Pane{hs.panes, hs.all} {
		for _, p := range list {
			if p.ID == id {
				return true
			}
		}
	}
	return false
}

// labelListed reports whether any pane on the host carries the label. It is
// the absence proof for a job record that names no pane: the label, not an id,
// is then the only thing tying the sweep to a live reviewer.
func labelListed(hs *reapHost, name string) bool {
	for _, list := range [][]reap.Pane{hs.panes, hs.all} {
		for _, p := range list {
			if p.Name == name {
				return true
			}
		}
	}
	return false
}

// refreshPaneLists re-reads the host's full pane list and its agent list, so
// an absence is proven on a current scan rather than the discovery snapshot.
// Both are read because a label lives on the agent list; the full list answers
// a recorded pane id and may not name every pane.
func refreshPaneLists(ctx context.Context, hs *reapHost) error {
	if e := hs.refreshPanes(ctx); e != nil {
		return e
	}
	panes, e := hs.ops.Panes(ctx)
	if e != nil {
		return e
	}
	hs.panes = panes
	return nil
}

// freshPaneFinished re-reads the candidate's host and reports whether the same
// pane (id and cwd) is still idle or done on that read.
func freshPaneFinished(ctx context.Context, c reviewerCandidate) (bool, error) {
	if c.seen == nil || c.pane == nil {
		return false, nil
	}
	if e := refreshPaneLists(ctx, c.seen); e != nil {
		return false, e
	}
	for _, p := range c.seen.panes {
		if p.ID == c.pane.ID {
			return p.Cwd == c.pane.Cwd && paneFinished(p.Status), nil
		}
	}
	return false, nil
}

// stuckHosts names the hosts a stuck job could be on: the one it records, or
// every configured host plus this machine when it records none.
func (w *reviewerSweep) stuckHosts(ctx context.Context, job integrations.JobRecord) []*reapHost {
	if job.Host == "" && w.local == nil {
		// The job names no host: this machine's panes count too, so the local
		// fallback host must exist before an absence can be proven.
		w.localHost(ctx)
	}
	hosts := w.hosts
	if w.local != nil {
		found := false
		for _, hs := range hosts {
			if hs == w.local {
				found = true
				break
			}
		}
		if !found {
			hosts = append(append([]*reapHost{}, hosts...), w.local)
		}
	}
	var targets []*reapHost
	for _, hs := range hosts {
		if job.Host != "" && hs.host.Name != job.Host && hs.host.MachineID() != job.Host {
			continue
		}
		targets = append(targets, hs)
	}
	return targets
}

// stuckPaneGone reports whether the job's pane is absent from the pane lists
// of the host the job names — every scanned host when it names none. A job
// record that names no pane has its label scanned instead: a recorded id no
// longer names anything, so only a successful scan of every host the job could
// be on can prove that no pane carries the label. The discovery lists can be
// stale — a stuck job's pane may have spawned since the scan — so an absence is
// proven on a refreshed list, and a host that failed its scan, could not read
// its full list or could not be refreshed cannot prove it.
func (w *reviewerSweep) stuckPaneGone(ctx context.Context, l reviewerLabel, job integrations.JobRecord) bool {
	listed := func(hs *reapHost) bool {
		if job.PaneID != "" {
			return paneListed(hs, job.PaneID)
		}
		return labelListed(hs, l.name)
	}
	targets := w.stuckHosts(ctx, job)
	if len(targets) == 0 {
		return false
	}
	for _, hs := range targets {
		if hs.failed || hs.usageUnknown || listed(hs) {
			return false
		}
	}
	for _, hs := range targets {
		if e := refreshPaneLists(ctx, hs); e != nil || listed(hs) {
			return false
		}
	}
	return true
}

// abandonStuck closes a reviewer job that is still claimed or spawned, has no
// result and is past the stuck bound once its pane is proven gone: the job
// can never report, so the close is what lets panewire's sweep end the
// record and the archive finish. It reports whether the sweep may go on to
// the worktrees.
func (w *reviewerSweep) abandonStuck(ctx context.Context, gate reapReceipt, job integrations.JobRecord, l reviewerLabel, hs *reapHost) bool {
	reason := fmt.Sprintf("stuck reviewer job %s with no result for %s and no pane", job.State, w.now.Sub(l.started).Round(time.Minute))
	if !w.acting {
		w.record(gate, "would-abandon", reason)
		return true
	}
	if w.closeRetryBlocked(l.name, reapPass(ctx)) {
		return false
	}
	if job.OwnerLane == "" {
		w.record(gate, "error", "abandon job: owner lane unavailable")
		return false
	}
	cmd := w.s.Config.Review.Panewire.Command
	argv := integrations.HubArgv(w.s.Config.Reap.Hub, "job", "close", l.name, "--lane", job.OwnerLane, "--outcome", "abandoned", "--reason", reason)
	if _, e := reapRunner(ctx, w.s.Runner).Run(ctx, cmd, argv, "", ""); e != nil {
		if alreadyClosedRefusal(e) {
			return w.alreadyClosed(gate, l)
		}
		if unknownJobRefusal(e) {
			return w.unknownJob(ctx, gate, l, hs)
		}
		if scopeForbiddenRefusal(e) {
			return w.settleScopeForbidden(ctx, gate, l, hs, job, e)
		}
		// The retry key is the failure id, not the reason: the reason keeps
		// stderr, which can change per call. The row carries the id so a
		// restart restores the same backoff (closeRetryBlocked).
		key := abandonRetryKey(e)
		w.s.reapRetryFailed(w.r.Name, l.name, key, reapPass(ctx))
		gate.RetryKey = key
		w.record(gate, "error", "abandon job: "+fleetevent.Redact(integrations.Detail(e)))
		return false
	}
	w.s.reapRetrySucceeded(w.r.Name, l.name)
	w.abandoned[l.name] = true
	w.record(gate, "abandoned", reason)
	return true
}

// reviewerWorktreePath is the worktree panewire's node prepares for a
// spec-placed reviewer: <work root>/<label>-checkout. The node names it
// checkoutWorktreeName (panewire internal/fleet/checkout_spec.go): the job id
// plus "-checkout" under the node's work root, unless the id is longer than
// 110 characters and gets hashed. A reviewer label is far shorter than that,
// so its directory name is exactly <label>-checkout. "" means this host's
// config names no work root, so no path can be built there.
func reviewerWorktreePath(hs *reapHost, l reviewerLabel) string {
	root := hs.host.WorkRoot
	if root == "" || !filepath.IsAbs(root) {
		return ""
	}
	name := l.name + reviewerCheckoutSuffix
	if len(l.name) > 110 {
		// Panewire hashes a long job id; no built label reaches this, and a
		// guess would be a wrong path.
		return ""
	}
	return filepath.Join(root, name)
}

// scannedHost is the scanned host a recorded name or machine id names, nil
// when no configured host answers to it.
func (w *reviewerSweep) scannedHost(name string) *reapHost {
	if name == "" {
		return nil
	}
	for _, hs := range w.hosts {
		if hs.host.Name == name || hs.host.MachineID() == name {
			return hs
		}
	}
	return nil
}

// remembered is the remote worktree path a receipt recorded for a label on a
// host, "" when none did.
func (w *reviewerSweep) remembered(l reviewerLabel, hs *reapHost) string {
	if p := w.remote[l.name][hs.host.Name]; p != "" {
		return p
	}
	return w.remote[l.name][hs.host.MachineID()]
}

// removeUnrecorded releases the worktrees of a label whose job record is gone
// and which no pane uses. The host comes from the receipts, the audit row or
// the pane; when none names one, no remote path can be built and the label
// ends with a terminal row.
func (w *reviewerSweep) removeUnrecorded(ctx context.Context, l reviewerLabel, c reviewerCandidate) {
	if hs := w.scannedHost(c.host); hs != nil {
		if hs.failed {
			// The host's scan already reported its error; nothing about it
			// can be inspected or proven, so the label waits for a pass that
			// reads the host.
			return
		}
		if !hs.host.Local() && w.removeRemote(ctx, hs, l, w.remembered(l, hs)) == remoteStop {
			return
		}
	} else if c.host != "" {
		// A record names a host this pass could not scan, because it is not
		// configured right now: keep the label pending so a restored host
		// still gets its worktree.
		rec := w.row(l)
		rec.Host, rec.Worktree = c.host, l.name
		w.record(rec, "kept", "job-host-unconfigured")
	} else {
		// No record, receipt or audit row names a host, so no remote path can
		// be built: the label holds no worktree this sweep can find.
		rec := w.row(l)
		rec.Host, rec.Worktree = c.host, l.name
		if w.acting {
			w.record(rec, "kept", reviewerHostUnknown)
		} else {
			w.record(rec, "pending-cleanup", "remote host unavailable")
		}
	}
	w.removeLocal(ctx, l)
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
// only through the hub's guarded removal (I6). A path no record names falls
// back to the node's own name for a spec-placed reviewer,
// <host work root>/<label>-checkout.
func (w *reviewerSweep) removeRemote(ctx context.Context, hs *reapHost, l reviewerLabel, cwd string) remoteStep {
	if cwd == "" || !reviewerCheckoutDir(filepath.Base(filepath.Clean(cwd)), l.name) {
		cwd = reviewerWorktreePath(hs, l)
	}
	rec := w.row(l)
	rec.Host, rec.Cwd, rec.Worktree = hs.host.Name, cwd, cwd
	if cwd == "" {
		// No record and no work root name the worktree. This is a missing
		// configuration, not a dead end: the row stays pending with the key
		// that is missing, retried every pass, so adding the work root
		// releases the label and it never counts against the drain cap.
		rec.Worktree = l.name
		w.record(rec, "pending-cleanup", fmt.Sprintf(reviewerPathUnknown, hs.host.Name))
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
	case reviewerBudgetCut(ctx, e):
		w.record(rec, "pending-cleanup", reviewerBudgetHold)
		return remoteStop
	case e != nil:
		w.record(rec, "error", "worktree inspect: "+e.Error())
		return remoteStop
	case tree == nil:
		w.record(rec, "skipped", reviewerAlreadyGone)
		return remoteDone
	case !w.reviewedTree(tree, cwd, l):
		w.record(rec, "kept", "worktree identity changed")
		return remoteStop
	case !w.acting:
		w.record(rec, "would-remove", "")
		return remoteDone
	}
	// The drain cap bounds how many guarded removals one host takes in a pass;
	// the rest waits with a row of their own, so the label stays a candidate.
	if !w.takeDrain(hs.host.Name) {
		w.record(rec, "pending-cleanup", reviewerDrainHold)
		return remoteStop
	}
	removed, e := remover.WorktreeRemove(ctx, tree.Path, tree.Head, w.r.Branch, nil, true)
	if reviewerBudgetCut(ctx, e) {
		w.record(rec, "pending-cleanup", reviewerBudgetHold)
		return remoteStop
	}
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
		w.record(rec, "skipped", reviewerAlreadyGone)
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

// reviewerBudgetCut reports whether a reviewer step's error is the repo budget
// (or a stage share of it) ending the step, not the step failing. Such a step
// holds its checkout for a later sweep and writes no error row, so the pass's
// only stop is its budget stop.
func reviewerBudgetCut(ctx context.Context, err error) bool {
	return err != nil && (errors.Is(err, ErrReapRepoBudget) || context.Cause(ctx) == ErrReapRepoBudget)
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
	w.local = &reapHost{host: ex.Host, exec: ex, ops: ex, trees: map[string]*reap.Worktree{}}
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
	case reviewerBudgetCut(ctx, e):
		w.record(rec, "pending-cleanup", reviewerBudgetHold)
		return
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
		if !reap.Regenerable(w.s.Config.ReapRegenerable(w.r), f) {
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
		if reviewerBudgetCut(ctx, e) {
			w.record(rec, "pending-cleanup", reviewerBudgetHold)
			return
		}
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

// reviewerAlreadyGone is the reason a guarded removal records when the
// checkout was already gone before mergewake's removal ran.
const reviewerAlreadyGone = "worktree already gone"

// reviewerCheckoutRemoved reports whether a reviewer receipt proves its
// checkout left the disk: a reviewer-reap removal, or a removal that found the
// worktree already gone (hk 5665: another actor removed it first). A pane
// already gone is not a checkout removal.
func reviewerCheckoutRemoved(v reapReceipt) bool {
	if v.Action == "reviewer-reap" && v.Result == "removed" {
		return true
	}
	return v.Result == "skipped" && v.Reason == reviewerAlreadyGone
}
