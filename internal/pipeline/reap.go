package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/archive"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/reap"
)

// reapReceipt is one durable row of <state>/<repo>/reap.jsonl. Rows are
// written in acting and shadow modes; a "would-*" row still names the worktree
// so a later pass can find it. Dry-run prints rows without writing the file.
type reapReceipt struct {
	Time     time.Time `json:"time"`
	Action   string    `json:"action"` // close-pane, remove-worktree, delete-branch, archive-job, reviewer-cleanup, reviewer-reap, legacy-sweep
	Repo     string    `json:"repo"`
	PR       int       `json:"pr"`
	Host     string    `json:"host"`
	Pane     string    `json:"pane,omitempty"`
	Cwd      string    `json:"cwd,omitempty"`
	Worktree string    `json:"worktree,omitempty"`
	Branch   string    `json:"branch,omitempty"`
	Saved    string    `json:"saved,omitempty"` // report/log path captured before a pane close
	Result   string    `json:"result"`          // closed, removed, deleted, abandoned, would-*, skipped, error
	Reason   string    `json:"reason,omitempty"`
	Files    []string  `json:"files,omitempty"` // ignored paths named by a refusing removal skip
	Job      string    `json:"job,omitempty"`   // job dir name of a closed pane or archived job
	Task     string    `json:"task,omitempty"`  // Mergewake-Task of a job-bound row
	Open     []string  `json:"open,omitempty"`  // open PRs (owner/name#N) keeping a job's pane
	Label    string    `json:"label,omitempty"` // reviewer label of a reviewer-reap row
	Head     string    `json:"head,omitempty"`  // checked-out commit of a legacy-sweep row

	Archive *archive.Result `json:"archive,omitempty"` // Drive dir, hk ids and excluded files of an archive
}

func worktreeRemoveVerdict(v reap.WorktreeRemoveResult) (result, reason string) {
	switch v.Outcome {
	case "removed":
		return "removed", ""
	case "worktree-gone", "worktree-changed", "outside-work-root", "unregistered", "locked", "detached", "status-check-failed", "dirty", "unmerged-or-unpushed", "pane-in-use", "pane-scan-failed", "git_too_old", "busy", "git-failed":
		return "skipped", v.Outcome
	case "ignored-files":
		return "skipped", "ignored-files:" + filesSig(v.Files)
	default:
		return "skipped", "cleanup-unknown:" + v.Outcome
	}
}

// reapHeadCommits memoises a head commit for the process: a commit's message
// never changes, so the trailer lookup (prTrailer) reads it once per head.
// It is keyed by state dir so two services never share an answer; a read error
// is not cached.
var (
	reapHeadMu      sync.Mutex
	reapHeadCommits = map[string]github.Commit{}
)

func (s *Service) headCommit(ctx context.Context, r config.Repo, p github.Pull) (github.Commit, error) {
	k := fmt.Sprintf("%s\x00%d\x00%s", s.state(r.Name), p.Number, p.Head.SHA)
	reapHeadMu.Lock()
	c, ok := reapHeadCommits[k]
	reapHeadMu.Unlock()
	if ok {
		return c, nil
	}
	c, e := s.GH.Commit(ctx, r.Name, p.Head.SHA)
	if e != nil {
		return c, e
	}
	reapHeadMu.Lock()
	reapHeadCommits[k] = c
	reapHeadMu.Unlock()
	return c, nil
}

func (s *Service) reapLog(repo string) string { return filepath.Join(s.state(repo), "reap.jsonl") }

func (s *Service) reapReceipts(repo string) ([]reapReceipt, error) {
	b, err := os.ReadFile(s.reapLog(repo))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []reapReceipt
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var v reapReceipt
		if e := json.Unmarshal([]byte(line), &v); e != nil {
			return nil, fmt.Errorf("reap receipts corrupt: %w", e)
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Service) reapNote(v reapReceipt, dry bool) error {
	v.Time = s.now().UTC()
	if e := s.Print(v); e != nil {
		return e
	}
	if dry {
		return nil
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return appendLine(s.reapLog(v.Repo), b)
}

// reapHost caches one host's agent list, full pane list and per-cwd worktree
// inspection for a single pass; closed records panes this pass closed or would
// close, and usageUnknown blocks removals when the full pane scan failed.
// ops reaches the host's panes; exec runs worktree cleanup and is unused when
// viaHub, since the hub carries no cleanup command yet.
type reapHost struct {
	host         reap.Host
	ops          reap.Ops
	exec         reap.Exec
	viaHub       bool
	panes        []reap.Pane
	all          []reap.Pane
	trees        map[string]*reap.Worktree
	closed       map[string]bool
	failed       bool
	usageUnknown bool
}

// refreshPanes re-reads the full pane list so an in-use check runs on current
// state, not the discovery snapshot, and inspects any newly seen cwds.
func (hs *reapHost) refreshPanes(ctx context.Context) error {
	all, err := hs.ops.AllPanes(ctx)
	if err != nil {
		return err
	}
	hs.all = all
	for _, p := range all {
		if p.Cwd != "" {
			if _, ok := hs.trees[p.Cwd]; !ok {
				w, _ := hs.ops.Inspect(ctx, p.Cwd)
				hs.trees[p.Cwd] = w
			}
		}
	}
	return nil
}

// reclose closes a pane only when a fresh re-read still shows it unchanged:
// the discovery list is a stale snapshot, and a pane that went back to work
// since must survive. Over the hub the node itself re-reads and compares.
func (s *Service) reclose(ctx context.Context, hs *reapHost, pane reap.Pane) (string, string) {
	outcome, err := hs.ops.CloseExact(ctx, pane)
	switch {
	case err != nil:
		return "error", err.Error()
	case outcome != reap.CloseClosed:
		return "skipped", outcome
	}
	return "closed", ""
}

// reapHub builds the pass's panewire hub client from reap.hub, or nil when
// the transport is ssh.
func (s *Service) reapHub() (*reap.Hub, error) {
	if s.Config.Reap.Transport != config.ReapTransportPanewire {
		return nil, nil
	}
	cfg := s.Config.Reap.Hub
	origin, err := reap.HubOrigin(cfg.URL)
	if err != nil {
		return nil, err
	}
	client, token, err := reap.HubCredential(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("reap.hub.token_file: %w", err)
	}
	hub := &reap.Hub{URL: origin, Client: client, Token: token}
	if cfg.CFFile != "" {
		cf, err := reap.ReadEnvFile(cfg.CFFile)
		if err != nil {
			return nil, fmt.Errorf("reap.hub.cf_file: %w", err)
		}
		hub.CFID, hub.CFSecret = cf["CF_ACCESS_CLIENT_ID"], cf["CF_ACCESS_CLIENT_SECRET"]
		if hub.CFID == "" || hub.CFSecret == "" {
			return nil, errors.New("reap.hub.cf_file needs CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET")
		}
	}
	return hub, nil
}

// filesSig is a compact count-plus-hash identity for a set of paths, so a
// receipt reason stays small and the row dedupe still fires only when the
// refusing set actually changed.
func filesSig(files []string) string {
	sorted := append([]string(nil), files...)
	sort.Strings(sorted)
	h := sha256.Sum256([]byte(strings.Join(sorted, "\x00")))
	return fmt.Sprintf("%d-%x", len(sorted), h[:4])
}

func (hs *reapHost) paneInUse(path string) bool {
	clean := filepath.Clean(path)
	for _, pane := range hs.all {
		if pane.Cwd == "" || hs.closed[pane.ID] {
			continue
		}
		cwd := filepath.Clean(pane.Cwd)
		if cwd == clean || strings.HasPrefix(cwd, clean+string(filepath.Separator)) {
			return true
		}
		if w := hs.trees[pane.Cwd]; w != nil && filepath.Clean(w.Path) == clean {
			return true
		}
	}
	return false
}

// reapHistory replays reap.jsonl for one pass. lastRow remembers the newest
// logged result per receipt stream so a repeat pass only writes rows whose
// outcome changed: a shadow run logging the same would-* every poll, or a
// removed worktree logging worktree-gone forever, would otherwise grow
// reap.jsonl without bound.
type reapHistory struct {
	lastRow map[string]string
	// jobClosed is the newest job-bound close per job, so its done can still
	// follow a close whose done failed; prFinished a PR whose job reap
	// already marked done.
	jobClosed map[string]reapReceipt
	// jobEverClosed names every job a pass closed the pane of, done or not: a
	// job whose pane is gone for that reason is not a missing pane.
	jobEverClosed map[string]bool
	prFinished    map[int]bool
	// prPaths holds every host and worktree a PR's receipts ever named.
	prPaths map[int]map[string]bool
	// lastPRCheck is each PR's newest row that cost a GitHub call.
	lastPRCheck map[int]time.Time
}

// reapRowKey identifies one receipt stream: pane closes key by pane, worktree
// work keys by path.
func reapRowKey(v reapReceipt) string {
	if v.Action == "reviewer-reap" {
		return fmt.Sprintf("%s|%s|%s", v.Action, v.Label, reviewerStream(v))
	}
	path := v.Pane
	if v.Action == "archive-job" || v.Action == "job-done" {
		path = v.Job
	} else if v.Action != "close-pane" {
		// Rows predating the Worktree field still name their path in Cwd.
		switch {
		case v.Worktree != "":
			path = v.Worktree
		case v.Cwd != "":
			path = v.Cwd
		}
	}
	return fmt.Sprintf("%s|%d|%s|%s", v.Action, v.PR, v.Host, path)
}

func reapRowSig(v reapReceipt) string { return v.Result + "|" + v.Reason }

func newReapHistory(receipts []reapReceipt) *reapHistory {
	h := &reapHistory{
		lastRow:       map[string]string{},
		jobClosed:     map[string]reapReceipt{},
		jobEverClosed: map[string]bool{},
		prFinished:    map[int]bool{},
		prPaths:       map[int]map[string]bool{},
		lastPRCheck:   map[int]time.Time{},
	}
	jobDone := map[string]bool{}
	for _, rec := range receipts {
		h.lastRow[reapRowKey(rec)] = reapRowSig(rec)
		if rec.Action == "job-done" && rec.Result == "done" {
			jobDone[rec.Job] = true
		}
		if rec.Action == "close-pane" && rec.Job != "" && rec.Result == "closed" {
			h.jobEverClosed[rec.Job] = true
		}
		switch {
		case rec.Action == "close-pane" && rec.Task != "" && rec.Result == "closed":
			h.jobClosed[rec.Job] = rec
		case rec.Action == "job-done" && (rec.Result == "done" || strings.HasPrefix(rec.Reason, "already-")):
			h.prFinished[rec.PR] = true
		}
	}
	for _, rec := range receipts {
		// reviewer-cleanup and reviewer-reap rows name reviewer checkouts and
		// never get a remove-worktree row, so they must not hold a PR open.
		if rec.PR == 0 || reviewerRow(rec) {
			continue
		}
		path := rec.Worktree
		if path == "" {
			path = rec.Cwd
		}
		if path == "" {
			continue
		}
		if h.prPaths[rec.PR] == nil {
			h.prPaths[rec.PR] = map[string]bool{}
		}
		h.prPaths[rec.PR][rec.Host+"\x00"+path] = true
	}
	// A job reap already marked done needs no retry, whatever hk shows.
	for job := range jobDone {
		delete(h.jobClosed, job)
	}
	for _, rec := range receipts {
		// Archive rows need no GitHub call and must not delay a refetch; neither
		// do reviewer rows, which a review pass or the reviewer sweep writes.
		if rec.Action != "archive-job" && !reviewerRow(rec) && rec.Time.After(h.lastPRCheck[rec.PR]) {
			h.lastPRCheck[rec.PR] = rec.Time
		}
	}
	return h
}

// prDone reports whether every worktree a PR's receipts ever named already
// reached a terminal remove outcome. A closed pane alone is not done — its
// worktree cleanup may still be pending — so only an all-terminal set spares
// the GitHub refetch on every poll.
func (h *reapHistory) prDone(pr int) bool {
	paths := h.prPaths[pr]
	if len(paths) == 0 {
		return h.prFinished[pr]
	}
	for hp := range paths {
		host, path, _ := strings.Cut(hp, "\x00")
		sig := h.lastRow[reapRowKey(reapReceipt{Action: "remove-worktree", PR: pr, Host: host, Worktree: path})]
		if sig != "removed|" && sig != "worktree-gone|" {
			return false
		}
	}
	return true
}

// reapScanHost reads one host's agent list, full pane list and the worktree of
// every pane cwd. A host whose panes cannot be read comes back failed; one whose
// full pane list cannot be read has usageUnknown set. Under the panewire
// transport every remote host goes through the hub; this machine's own panes
// still run locally.
func (s *Service) reapScanHost(ctx context.Context, hub *reap.Hub, h reap.Host) (*reapHost, []error) {
	var errs []error
	hs := &reapHost{host: h, exec: reap.Exec{Runner: s.Runner, Host: h}, trees: map[string]*reap.Worktree{}, closed: map[string]bool{}}
	hs.ops = hs.exec
	if hub != nil && !h.Local() {
		hs.ops, hs.viaHub = reap.HubHost{Hub: hub, Machine: h.MachineID()}, true
	}
	panes, e := hs.ops.Panes(ctx)
	if e != nil {
		hs.failed = true
		return hs, append(errs, fmt.Errorf("%s: %w", h.Name, e))
	}
	hs.panes = panes
	// The full pane list also covers non-agent shells and jobs: without it
	// the usage check is unknown and removals must skip.
	all, e := hs.ops.AllPanes(ctx)
	if e != nil {
		hs.usageUnknown = true
		errs = append(errs, fmt.Errorf("%s: %w", h.Name, e))
	}
	hs.all = all
	for _, list := range [][]reap.Pane{panes, all} {
		for _, p := range list {
			if p.Cwd != "" {
				if _, ok := hs.trees[p.Cwd]; !ok {
					w, e := hs.ops.Inspect(ctx, p.Cwd)
					// A hub that refuses the credential answers nothing
					// about the worktree: fail the host instead of reading
					// every pane as outside one.
					if errors.Is(e, reap.ErrHubAuth) {
						hs.failed = true
						return hs, append(errs, fmt.Errorf("%s: %w", h.Name, e))
					}
					hs.trees[p.Cwd] = w
				}
			}
		}
	}
	return hs, errs
}

type reviewersOnlyKey struct{}

// reviewersOnly marks a Reap call that only wants an open PR's reviewer sweep:
// the call then ends without the "not a merged PR" refusal.
func reviewersOnly(ctx context.Context) bool { return ctx.Value(reviewersOnlyKey{}) != nil }

// Reap closes agent panes sitting in worktrees of merged-PR head branches, then
// removes the worktree and local branch once the merge is older than
// reap.worktree_after and no watch recovery PR landed. Anything dirty, unknown
// or ambiguous is skipped and reported, never forced.
func (s *Service) Reap(ctx context.Context, r config.Repo, prNum int, o Options) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.audit(r.Name, "reap-error", map[string]any{"error": retErr.Error()}, o.DryRun))
		}
	}()
	if !s.Config.Reap.Enabled {
		return errors.New("reap requires reap.enabled in host config")
	}
	if prNum < 0 {
		return errors.New("--pr must be positive")
	}
	after, e := config.Duration(s.Config.Reap.WorktreeAfter)
	if e != nil {
		return e
	}
	scan, e := config.Duration(s.Config.Reap.ScanAfter)
	if e != nil {
		return e
	}
	if len(s.Config.Hosts) == 0 {
		return errors.New("reap requires configured hosts")
	}
	now := s.now()
	// reap.act is the role's own acting gate, separate from daemon.act:
	// without it every mode stays observational no matter what --apply says.
	acting := o.Acting() && s.Config.Reap.Act
	var errs []error

	receipts, e := s.reapReceipts(r.Name)
	if e != nil {
		return e
	}
	// Legacy sweep receipts must not enter the regular replay.
	receipts = slices.DeleteFunc(receipts, legacyRow)
	h := newReapHistory(receipts)
	// A receipt-named PR beyond the listing horizon is refetched at most once
	// per day, measured from its newest row: each attempt appends a check-pr
	// row, so a persistent skip like reverted, unpushed or refused ignored
	// files stops costing a GitHub call every poll.
	const refetchAfter = 24 * time.Hour

	pulls := []github.Pull{}
	if prNum > 0 {
		p, e := s.GH.Pull(ctx, r.Name, prNum)
		if e != nil {
			return e
		}
		pulls = append(pulls, p)
	} else {
		batch, e := s.GH.MergedPulls(ctx, r.Name, r.Branch, now.Add(-scan))
		if e != nil {
			return e
		}
		known := map[int]bool{}
		for _, p := range batch {
			known[p.Number] = true
			pulls = append(pulls, p)
		}
		// Receipt-named PRs can be older than the listing horizon. A fetch
		// failure only loses that PR this pass; it never aborts the rest.
		for _, rec := range receipts {
			// The reviewer sweep covers its own rows whatever the PR's state.
			if rec.PR == 0 || rec.Action == "reviewer-reap" || known[rec.PR] || h.prDone(rec.PR) || now.Sub(h.lastPRCheck[rec.PR]) < refetchAfter {
				continue
			}
			known[rec.PR] = true
			// Stamp the attempt before the call: identical outcomes are
			// deduped and never write a row, so without this a persistent
			// skip would refetch every pass once the newest row aged out.
			// The stamp persists even under --dry-run — it is bookkeeping for
			// the GitHub call that just happened, so repeated dry-run polls
			// stay bounded too.
			if e := s.reapNote(reapReceipt{Action: "check-pr", Repo: r.Name, PR: rec.PR, Result: "checked"}, false); e != nil {
				errs = append(errs, e)
			}
			p, e := s.GH.Pull(ctx, r.Name, rec.PR)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d: %w", rec.PR, e))
				continue
			}
			pulls = append(pulls, p)
		}
	}
	sort.Slice(pulls, func(i, j int) bool { return pulls[i].Number < pulls[j].Number })

	hub, e := s.reapHub()
	if e != nil {
		return errors.Join(append(errs, e)...)
	}
	hosts := []*reapHost{}
	byName := map[string]*reapHost{}
	for _, h := range s.Config.Hosts {
		hs, hostErrs := s.reapScanHost(ctx, hub, h)
		errs = append(errs, hostErrs...)
		hosts = append(hosts, hs)
		byName[h.Name] = hs
	}
	// Errors are counted once, from errs: note itself appends every "error"
	// row, so no row-level failure can be missing from the summary.
	scanned, closed, planned, removed, deleted, skipped, archived, jobsDone := 0, 0, 0, 0, 0, 0, 0, 0
	note := func(v reapReceipt) {
		switch v.Result {
		case "closed":
			closed++
		case "done":
			jobsDone++
		case "would-close", "would-remove", "would-delete", "would-archive", "would-done", "would-abandon":
			planned++
		case "archived":
			archived++
		case "removed":
			removed++
		case "deleted":
			deleted++
		case "skipped", "kept", "pending-cleanup":
			skipped++
		case "error":
			subject := v.Worktree
			switch v.Action {
			case "close-pane":
				subject = "pane " + v.Pane
			case "archive-job", "job-done":
				subject = "job " + v.Job
			case "reviewer-reap":
				subject = "reviewer " + v.Label
			}
			errs = append(errs, fmt.Errorf("%s %s %s: %s", v.Host, v.Action, subject, v.Reason))
		}
		// Every row prints; only changed outcomes append to the receipt file.
		// A failed archive always appends: its time spaces the next retry.
		// So does a completed one, which carries its own Drive and hk record.
		repeat := h.lastRow[reapRowKey(v)] == reapRowSig(v) && !(v.Action == "archive-job" && (v.Result == "error" || v.Result == "archived"))
		h.lastRow[reapRowKey(v)] = reapRowSig(v)
		if e := s.reapNote(v, o.DryRun || repeat); e != nil {
			errs = append(errs, e)
		}
	}
	// reviewerNote is note plus one reviewer-reap audit record per new row: a
	// deduped repeat writes neither.
	reviewerNote := func(v reapReceipt) {
		repeat := h.lastRow[reapRowKey(v)] == reapRowSig(v)
		note(v)
		if repeat {
			return
		}
		if e := s.audit(r.Name, "reviewer-reap", map[string]any{
			"pr": v.PR, "label": v.Label, "host": v.Host, "pane": v.Pane, "worktree": v.Worktree, "result": v.Result, "reason": v.Reason,
		}, o.DryRun); e != nil {
			errs = append(errs, e)
		}
	}
	jobSeen := map[string]bool{}
	// matches reports whether a worktree is merged PR p's own: it is on the PR's
	// branch in this repo and its head is the PR head, or an ancestor of it, so
	// every commit in the worktree is in the merged PR. A head that is an
	// ancestor is asked of GitHub once per PR head per pass; a head GitHub does
	// not know (404) does not match and is not an error. Any other failed read
	// matches nothing for the rest of the pass and is reported once. A worktree
	// whose head is the PR head costs no GitHub call.
	type ancestryKey struct {
		pr   int
		head string
	}
	ancestry := map[ancestryKey]bool{}
	ancestryFailed := map[int]bool{}
	matches := func(w *reap.Worktree, p github.Pull) bool {
		if w == nil || w.Branch == "" || w.Branch != p.Head.Ref || p.Head.SHA == "" || w.Head == "" || !strings.EqualFold(w.Repo, r.Name) {
			return false
		}
		if w.Head == p.Head.SHA {
			return true
		}
		k := ancestryKey{p.Number, w.Head}
		if ok, seen := ancestry[k]; seen {
			return ok
		}
		if ancestryFailed[p.Number] {
			return false
		}
		ok, e := s.GH.Ancestor(ctx, r.Name, w.Head, p.Head.SHA)
		switch {
		case github.NotFound(e):
			ok, e = false, nil
		case e != nil:
			ancestryFailed[p.Number] = true
			errs = append(errs, fmt.Errorf("PR %d head ancestry: %w", p.Number, e))
			return false
		}
		ancestry[k] = ok
		return ok
	}

	// A call that wants only the reviewer sweep (the review path) never runs
	// the merged-PR cleanup, even if the PR merged while the review ran: a held
	// reviewer spawn must not close the author's job pane, remove its worktree,
	// delete its branch or archive its job.
	if reviewersOnly(ctx) && prNum > 0 {
		return errors.Join(append(errs, s.reapReviewers(ctx, r, prNum, receipts, hosts, acting, reviewerNote)...)...)
	}

	for _, p := range pulls {
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		if !(p.Merged || p.MergedAt != nil) || p.Base.Ref != r.Branch {
			if prNum > 0 {
				// A reviewer's job ends whatever its PR's state: sweep the
				// PR's reviewers before refusing the author's path.
				errs = append(errs, s.reapReviewers(ctx, r, prNum, receipts, hosts, acting, reviewerNote)...)
				return errors.Join(append(errs, fmt.Errorf("PR #%d is not a merged PR against %s", p.Number, r.Branch))...)
			}
			continue
		}
		scanned++
		var mergedAt *time.Time
		if p.MergedAt != nil {
			mergedAt = parsedTime(*p.MergedAt)
		}
		// A PR bound to a job reaps that job's pane, once per job per pass;
		// worktree matching is the fallback for a PR with no bound job. A
		// binding read error closes nothing this pass.
		bound, bindErr := s.reapBind(ctx, r, p)
		switch {
		case bindErr != nil:
			errs = append(errs, fmt.Errorf("PR %d job binding: %s", p.Number, integrations.Detail(bindErr)))
		case bound.Job != "":
			if !jobSeen[bound.Job] {
				jobSeen[bound.Job] = true
				match := func(w *reap.Worktree) bool { return matches(w, p) }
				s.reapJobPane(ctx, r, p, bound, hosts, byName, match, mergedAt, acting, h.jobClosed, h.jobEverClosed, note)
			}
		}
		for _, hs := range hosts {
			if hs.failed || bindErr != nil || bound.Job != "" {
				continue
			}
			for _, pane := range hs.panes {
				w := hs.trees[pane.Cwd]
				if !matches(w, p) {
					continue
				}
				rec := reapReceipt{Action: "close-pane", Repo: r.Name, PR: p.Number, Host: hs.host.Name, Pane: pane.ID, Cwd: pane.Cwd, Worktree: w.Path, Branch: w.Branch}
				switch {
				case pane.ID == "":
					rec.Result, rec.Reason = "skipped", "pane-id-missing"
				case pane.Status != "idle" && pane.Status != "done":
					// An allowlist, not a denylist: blocked, waiting, unknown
					// or empty states are live enough to keep.
					st := pane.Status
					if st == "" {
						st = "unknown"
					}
					rec.Result, rec.Reason = "skipped", "status:"+st
				case pane.Name == "":
					rec.Result, rec.Reason = "skipped", "unnamed"
				case reap.Protected(pane.Name, s.Config.Reap.Protect):
					rec.Result, rec.Reason = "skipped", "protected:"+pane.Name
				case hs.host.Jobs == "" && !hs.viaHub:
					// Through the hub the node owns its jobs directory.
					rec.Result, rec.Reason = "skipped", "jobs-dir-unset"
				case !reap.ValidJobName(pane.Name):
					rec.Result, rec.Reason = "skipped", "job-name-invalid"
				case pane.Focused:
					rec.Result, rec.Reason = "skipped", "operator-owned"
				default:
					// A PR whose task has no bound job still waits for the
					// task's other open PRs; then the composer, capture and
					// close rules of the job path apply (reapPaneClose).
					s.reapPaneClose(ctx, &rec, hs, pane, pane.Name, bound.Task, mergedAt, acting)
				}
				note(rec)
			}
		}

		if mergedAt == nil || now.Sub(*mergedAt) < after {
			continue
		}

		type cand struct {
			hs   *reapHost
			path string
		}
		var cands []cand
		seen := map[string]bool{}
		add := func(hs *reapHost, path string) {
			if hs == nil || hs.failed || path == "" {
				return
			}
			k := hs.host.Name + "\x00" + path
			if !seen[k] {
				seen[k] = true
				cands = append(cands, cand{hs, path})
			}
		}
		for _, hs := range hosts {
			for _, pane := range hs.panes {
				if w := hs.trees[pane.Cwd]; matches(w, p) {
					add(hs, w.Path)
				}
			}
		}
		for _, rec := range receipts {
			if rec.PR != p.Number || reviewerRow(rec) {
				continue
			}
			path := rec.Worktree
			if path == "" {
				path = rec.Cwd
			}
			// A worktree already removed or proven gone is terminal: re-adding
			// it as a candidate would only log the same skip every pass. Live
			// pane matches are never suppressed.
			sig := h.lastRow[reapRowKey(reapReceipt{Action: "remove-worktree", PR: p.Number, Host: rec.Host, Worktree: path})]
			if sig == "removed|" || sig == "worktree-gone|" {
				continue
			}
			add(byName[rec.Host], path)
		}
		// A PR with no discovered worktree on any host leaves no rows and
		// costs no GitHub calls beyond the listing: the merge-commit and
		// revert checks only run when there is real work to gate.
		if len(cands) == 0 {
			continue
		}
		if p.MergeCommitSHA == "" {
			// The pulls list endpoint omits merge_commit_sha; resolve it for
			// the few PRs that actually name work.
			fp, e := s.GH.Pull(ctx, r.Name, p.Number)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d merge commit: %w", p.Number, e))
				continue
			}
			p.MergeCommitSHA = fp.MergeCommitSHA
		}
		if p.MergeCommitSHA == "" {
			note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: "merge-commit-unknown"})
			continue
		}
		watcher, ok := s.GH.(interface {
			WatchRevert(context.Context, string, int, string) (*github.Pull, error)
		})
		if !ok {
			note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: "revert-check-unavailable"})
			continue
		}
		recovery, e := watcher.WatchRevert(ctx, r.Name, p.Number, p.MergeCommitSHA)
		if e != nil {
			errs = append(errs, fmt.Errorf("PR %d revert check: %w", p.Number, e))
			continue
		}
		if recovery != nil {
			reason := ""
			if recovery.MergedAt != nil {
				// Same convention as the watch role and outcomes ledger: a
				// recovery PR merged within seven days reverted this PR.
				if at := parsedTime(*recovery.MergedAt); at != nil && !at.Before(*mergedAt) && !at.After(mergedAt.Add(7*24*time.Hour)) {
					reason = "reverted"
				}
			} else if recovery.State == "open" {
				reason = "recovery-open"
			}
			if reason != "" {
				note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: reason})
				continue
			}
		}

		for _, c := range cands {
			rec := reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Cwd: c.path, Worktree: c.path, Branch: p.Head.Ref}
			w, e := c.hs.ops.Inspect(ctx, c.path)
			switch {
			case e != nil:
				rec.Result, rec.Reason = "skipped", "worktree-inspect-error"
				errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, c.path, e))
			case w == nil:
				rec.Result, rec.Reason = "skipped", "worktree-gone"
			case !matches(w, p):
				rec.Result, rec.Reason = "skipped", "worktree-changed"
			case c.hs.viaHub:
				if !acting {
					rec.Result = "would-remove"
					note(rec)
					brec := reapReceipt{Action: "delete-branch", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Worktree: w.Path, Branch: w.Branch, Result: "would-delete"}
					note(brec)
					continue
				}
				remover, ok := c.hs.ops.(interface {
					WorktreeRemove(context.Context, string, string, string, []string, bool) (reap.WorktreeRemoveResult, error)
				})
				if !ok {
					rec.Result, rec.Reason = "skipped", "cleanup-unknown:unsupported"
				} else {
					result, err := remover.WorktreeRemove(ctx, w.Path, w.Head, r.Branch, s.Config.Reap.Regenerable, false)
					if err != nil {
						rec.Result, rec.Reason = "error", err.Error()
					} else {
						rec.Files = result.Files
						rec.Result, rec.Reason = worktreeRemoveVerdict(result)
					}
				}
				note(rec)
				if rec.Result == "removed" {
					note(reapReceipt{Action: "delete-branch", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Worktree: w.Path, Branch: w.Branch, Result: "deleted"})
				}
				continue
			}
			if rec.Result != "" {
				note(rec)
				continue
			}
			rec.Worktree = w.Path
			main := filepath.Dir(w.Common)
			entries, e := c.hs.exec.WorktreeList(ctx, main)
			if e != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, w.Path, e))
				continue
			}
			var entry *reap.Entry
			for i := range entries {
				if entries[i].Path == w.Path {
					entry = &entries[i]
				}
			}
			remove := false
			switch {
			case c.hs.usageUnknown:
				rec.Result, rec.Reason = "skipped", "pane-scan-failed"
			case entry == nil:
				rec.Result, rec.Reason = "skipped", "unregistered"
			case entry.Locked:
				rec.Result, rec.Reason = "skipped", "locked"
			case entry.Branch == "":
				rec.Result, rec.Reason = "skipped", "detached"
			default:
				clean, e := c.hs.exec.Clean(ctx, w.Path)
				var ignored []string
				if e == nil {
					ignored, e = c.hs.exec.Ignored(ctx, w.Path)
				}
				var unsafe []string
				for _, f := range ignored {
					if !reap.Regenerable(s.Config.Reap.Regenerable, f) {
						unsafe = append(unsafe, f)
					}
				}
				switch {
				case e != nil:
					rec.Result, rec.Reason = "skipped", "status-check-failed"
					errs = append(errs, fmt.Errorf("%s %s: %w", c.hs.host.Name, w.Path, e))
				case !clean:
					rec.Result, rec.Reason = "skipped", "dirty"
				case len(unsafe) > 0:
					// `status --porcelain` hides gitignored files but
					// `worktree remove` deletes them: regenerable output is
					// expendable, anything else refuses and is named in files.
					rec.Files = unsafe
					rec.Result, rec.Reason = "skipped", "ignored-files:"+filesSig(unsafe)
				case c.hs.paneInUse(w.Path):
					rec.Result, rec.Reason = "skipped", "pane-in-use"
				case !c.hs.exec.Pushed(ctx, main, entry.Branch, r.Branch):
					rec.Result, rec.Reason = "skipped", "unmerged-or-unpushed"
				case !acting:
					rec.Result = "would-remove"
					remove = true
				default:
					// The pane list is a stale snapshot by now; a worker
					// spawned into the worktree after discovery must stop
					// the removal. Re-scan first, then re-check.
					switch e := c.hs.refreshPanes(ctx); {
					case e != nil:
						rec.Result, rec.Reason = "skipped", "pane-scan-failed"
						errs = append(errs, fmt.Errorf("%s pane list: %w", c.hs.host.Name, e))
					case c.hs.paneInUse(w.Path):
						rec.Result, rec.Reason = "skipped", "pane-in-use"
					default:
						if e := c.hs.exec.RemoveWorktree(ctx, main, w.Path); e != nil {
							rec.Result, rec.Reason = "error", e.Error()
						} else {
							rec.Result = "removed"
							remove = true
						}
					}
				}
			}
			note(rec)
			if !remove {
				continue
			}
			// -d only: git itself refuses a branch that never merged anywhere.
			brec := reapReceipt{Action: "delete-branch", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Worktree: w.Path, Branch: entry.Branch}
			if acting {
				if e := c.hs.exec.DeleteBranch(ctx, main, entry.Branch); e != nil {
					brec.Result, brec.Reason = "skipped", "refused: "+e.Error()
				} else {
					brec.Result = "deleted"
				}
			} else {
				brec.Result = "would-delete"
			}
			note(brec)
		}
	}
	errs = append(errs, s.reapReviewers(ctx, r, prNum, receipts, hosts, acting, reviewerNote)...)
	if s.Config.Reap.Archive.Enabled {
		errs = append(errs, s.reapArchive(ctx, r, prNum, receipts, byName, acting, note)...)
	}
	result := struct {
		Action   string `json:"action"`
		Repo     string `json:"repo"`
		Shadow   bool   `json:"shadow"`
		Scanned  int    `json:"prs_scanned"`
		Closed   int    `json:"closed"`
		Planned  int    `json:"planned"`
		Removed  int    `json:"removed"`
		Deleted  int    `json:"branches_deleted"`
		Archived int    `json:"jobs_archived"`
		JobsDone int    `json:"jobs_done"`
		Skipped  int    `json:"skipped"`
		ErrCount int    `json:"errors"`
	}{"reap", r.Name, !acting, scanned, closed, planned, removed, deleted, archived, jobsDone, skipped, len(errs)}
	return errors.Join(append(errs, s.audit(r.Name, "reap", result, o.DryRun))...)
}
