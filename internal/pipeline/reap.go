package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/archive"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
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
	RetryKey string    `json:"retry_key,omitempty"` // stable failure id a failed close backs off on (abandonRetryKey); a row without one keys on its reason
	Files    []string  `json:"files,omitempty"`     // ignored paths named by a refusing removal skip
	Job      string    `json:"job,omitempty"`       // job dir name of a closed pane or archived job
	Task     string    `json:"task,omitempty"`      // Mergewake-Task of a job-bound row
	Open     []string  `json:"open,omitempty"`      // open PRs (owner/name#N) keeping a job's pane
	Label    string    `json:"label,omitempty"`     // reviewer label of a reviewer-reap or reviewer-cleanup row
	Head     string    `json:"head,omitempty"`      // checked-out commit of a legacy-sweep row
	Source   string    `json:"source,omitempty"`    // what named a merged-PR worktree: pane, receipt or job-record

	// hostSkip is the typed host skip an error row reports instead of a
	// repository error, when the hub cannot serve the row's host. Never persisted.
	hostSkip error `json:"-"`

	Archive *archive.Result `json:"archive,omitempty"` // Drive dir, hk ids and excluded files of an archive

	// RetryAfter is the deadline of a kept not-closable row's backoff. A later
	// invocation, a fresh process, retries once it has passed (hk 6014).
	RetryAfter time.Time `json:"retry_after,omitzero"`
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
	var out []reapReceipt
	err := fsutil.ScanJSONL(s.reapLog(repo), func(v reapReceipt) error {
		out = append(out, v)
		return nil
	})
	return out, err
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
// inspection for a single pass; usageUnknown blocks removals when the full
// pane scan failed. ops reaches the host's panes; exec runs worktree cleanup
// and is unused when viaHub, since the hub carries no cleanup command yet.
type reapHost struct {
	host         reap.Host
	ops          reap.Ops
	exec         reap.Exec
	viaHub       bool
	panes        []reap.Pane
	all          []reap.Pane
	trees        map[string]*reap.Worktree
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
				// A failed inspect is not cached: a nil here would read as a
				// proven-gone worktree to the job-record source.
				if w, e := hs.ops.Inspect(ctx, p.Cwd); e == nil {
					hs.trees[p.Cwd] = w
				}
			}
		}
	}
	return nil
}

// reapHub builds the pass's panewire hub client from reap.hub, or nil when
// the transport is ssh.
func (s *Service) reapHub() (*reap.Hub, error) {
	if s.Config.Reap.Transport != config.ReapTransportPanewire {
		return nil, nil
	}
	return s.hubClient(s.Config.Reap.Hub)
}

// hubClient builds a panewire hub client from cfg, or nil when cfg names no
// hub. It is independent of reap.transport: a caller whose calls go through
// the panewire CLI (with the same reap.hub credential) reads the hub over
// HTTP on a host whose reap transport is ssh.
func (s *Service) hubClient(cfg config.ReapHub) (*reap.Hub, error) {
	if cfg.URL == "" {
		return nil, nil
	}
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
		if pane.Cwd == "" {
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

// reapRowSig names a row's result and reason. A kept not-closable row also
// names its backoff deadline: a refusal that repeats moves the deadline out, and
// that new deadline is state a later invocation must read, not a repeat (hk 6014).
func reapRowSig(v reapReceipt) string {
	if v.RetryAfter.IsZero() {
		return v.Result + "|" + v.Reason
	}
	return v.Result + "|" + v.Reason + "|" + v.RetryAfter.UTC().Format(time.RFC3339Nano)
}

// terminalWorktreeSig reports whether a worktree's latest row is a terminal
// outcome: removed, or proven gone (every writer records a gone worktree as
// skipped with reason worktree-gone). Such a worktree is never offered or
// inspected again.
func terminalWorktreeSig(sig string) bool {
	return sig == "removed|" || sig == "skipped|worktree-gone"
}

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
		if !terminalWorktreeSig(sig) {
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
	hs := &reapHost{host: h, exec: reap.Exec{Runner: reapRunner(ctx, s.Runner), Host: h}, trees: map[string]*reap.Worktree{}}
	hs.ops = hs.exec
	if hub != nil && !h.Local() {
		hs.ops, hs.viaHub = timedHubOps{Ops: reap.HubHost{Hub: hub, Machine: h.MachineID()}, telemetry: telemetryFrom(ctx)}, true
	}
	panes, e := hs.ops.Panes(ctx)
	if e != nil {
		hs.failed = true
		return hs, append(errs, hostScanFailure(ctx, h.Name, "host panes", e))
	}
	hs.panes = panes
	// The full pane list also covers non-agent shells and jobs: without it
	// the usage check is unknown and removals must skip.
	all, e := hs.ops.AllPanes(ctx)
	if e != nil {
		hs.usageUnknown = true
		errs = append(errs, hostUnreachableFailure(h.Name, fmt.Errorf("%s: %w", h.Name, e)))
		// A node the hub cannot serve now answers nothing about the host's
		// worktrees either: fail the host here instead of inspecting its panes.
		if isHostUnreachable(e) {
			hs.failed = true
			return hs, errs
		}
	}
	hs.all = all
	for _, list := range [][]reap.Pane{panes, all} {
		for _, p := range list {
			if e := reapBudgetCheck(ctx, "host pane/worktree scan"); e != nil {
				return hs, append(errs, e)
			}
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
					// A node that cannot be reached, or a hub call that timed
					// out against it, answers nothing about the host's other
					// panes: stop here and fail the host, rather than wait
					// once per pane. A budget cut is not an answer about the
					// node; the budget check above ends the scan.
					if e != nil && !errors.Is(e, ErrReapRepoBudget) && (isHostUnreachable(e) || isTimeout(e)) {
						hs.failed = true
						return hs, append(errs, hostScanFailure(ctx, h.Name, "worktree inspect", e))
					}
					// Only a proven answer is cached; a failed inspect is
					// retried by the next reader instead of read as gone.
					if e == nil {
						hs.trees[p.Cwd] = w
					}
				}
			}
		}
	}
	return hs, errs
}

type reviewersOnlyKey struct{}

var ErrReapRepoBudget = errors.New("reap repository budget exhausted")

// ReapNonBudgetErrors removes budget-stop leaves while preserving all other
// failures in a joined Reap result.
func ReapNonBudgetErrors(err error) error {
	var leaves []error
	var collect func(error)
	collect = func(current error) {
		if current == nil {
			return
		}
		if !errors.Is(current, ErrReapRepoBudget) {
			leaves = append(leaves, current)
			return
		}
		if many, ok := current.(interface{ Unwrap() []error }); ok {
			for _, child := range many.Unwrap() {
				collect(child)
			}
			return
		}
		if one, ok := current.(interface{ Unwrap() error }); ok {
			collect(one.Unwrap())
			return
		}
	}
	collect(err)
	return errors.Join(leaves...)
}

// ReapBudgetOnly is true only when a pass stopped for budget and has no other
// error leaf to audit or report.
func ReapBudgetOnly(err error) bool {
	return errors.Is(err, ErrReapRepoBudget) && ReapNonBudgetErrors(err) == nil
}

type reapRetry struct {
	failure         string
	attempts        int
	retryAt         time.Time
	nextAllowedPass uint64
}

type reapRetryState struct {
	mu      sync.Mutex
	retries map[string]reapRetry
	passes  map[string]uint64
}

var reapRetryStateInitMu sync.Mutex

func (s *Service) reapState() *reapRetryState {
	// Service is copied by a few restart tests. Keep the lock behind a pointer
	// so those copies remain vet-safe, and serialize zero-value initialization.
	reapRetryStateInitMu.Lock()
	defer reapRetryStateInitMu.Unlock()
	if s.reapRetryState == nil {
		s.reapRetryState = &reapRetryState{}
	}
	return s.reapRetryState
}

type reapPassKey struct{}

func reapPass(ctx context.Context) uint64 {
	pass, _ := ctx.Value(reapPassKey{}).(uint64)
	return pass
}

func (s *Service) nextReapPass(repo string, wholeRepo bool) uint64 {
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.passes == nil {
		state.passes = map[string]uint64{}
	}
	if wholeRepo {
		state.passes[repo]++
	}
	return state.passes[repo]
}

func (s *Service) reapRetryAllowed(repo, job string, pass uint64) bool {
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.retries == nil {
		return true
	}
	v, ok := state.retries[repo+"\x00"+job]
	return !ok || (pass >= v.nextAllowedPass && !s.now().Before(v.retryAt))
}

// reapRetryFailed records a failed close and returns the instant its backoff
// ends, the deadline a kept row persists (hk 6014).
func (s *Service) reapRetryFailed(repo, job, failure string, pass uint64) time.Time {
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.retries == nil {
		state.retries = map[string]reapRetry{}
	}
	k := repo + "\x00" + job
	v := state.retries[k]
	if v.failure != failure {
		v = reapRetry{failure: failure}
	}
	v.attempts++
	d := time.Minute * time.Duration(1<<min(v.attempts-1, 6))
	if d > time.Hour {
		d = time.Hour
	}
	v.retryAt = s.now().Add(d)
	v.nextAllowedPass = pass + 2 // skip the immediately following reap pass
	state.retries[k] = v
	return v.retryAt
}

func (s *Service) reapRetryRestore(repo, job, failure string, pass uint64) bool {
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.retries == nil {
		state.retries = map[string]reapRetry{}
	}
	k := repo + "\x00" + job
	if _, ok := state.retries[k]; ok {
		return false
	}
	// A durable failure receipt surviving a restart suppresses this pass even
	// when the prior pass took longer than the time backoff.
	state.retries[k] = reapRetry{failure: failure, attempts: 1, retryAt: s.now(), nextAllowedPass: pass + 1}
	return true
}

func (s *Service) reapRetrySucceeded(repo, job string) {
	state := s.reapState()
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.retries, repo+"\x00"+job)
}

type reapBudgetKey struct{}

// reapBudget is the deadline a context's budget ends at. A stage share sets
// passDeadline to the pass's own deadline, so a stage that ends at its share
// can tell that from the pass running out (stageShareEnded).
type reapBudget struct {
	deadline     time.Time
	passDeadline time.Time
}

type reapPassStateKey struct{}

type reapPassState struct {
	mu       sync.Mutex
	left     string
	inflight string
	// share is the first stage whose share ended while the pass still had
	// budget. It is not a pass stop: the pass goes on to its later stages.
	share string
}

func (s *reapPassState) recordStage(stage string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.left == "" {
		s.left = stage
	}
}

func (s *reapPassState) recordCut(operation string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight == "" {
		s.inflight = operation
	}
}

func (s *reapPassState) recordShare(stage string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.share == "" {
		s.share = stage
	}
}

func (s *reapPassState) snapshot() (left, inflight string) {
	if s == nil {
		return "", ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.left, s.inflight
}

// shareCut is the first stage whose share the pass cut short, "" when none did.
func (s *reapPassState) shareCut() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.share
}

// ReapBudgetStop marks a repository pass that stopped at a safe point or
// returned from an operation cut short by its budget.
type ReapBudgetStop struct {
	Left     string
	Failures error
}

func (e *ReapBudgetStop) Error() string {
	if e.Failures == nil {
		return fmt.Sprintf("%v; left: %s", ErrReapRepoBudget, e.Left)
	}
	return fmt.Sprintf("%v; left: %s; errors: %v", ErrReapRepoBudget, e.Left, e.Failures)
}

func (e *ReapBudgetStop) Unwrap() []error {
	if e.Failures == nil {
		return []error{ErrReapRepoBudget}
	}
	return []error{ErrReapRepoBudget, e.Failures}
}

func reapBudgetResult(state *reapPassState, failures error) error {
	left, inflight := state.snapshot()
	if left == "" && inflight == "" {
		return failures
	}
	if left == "" {
		left = "in-flight " + inflight
	}
	return &ReapBudgetStop{Left: left, Failures: failures}
}

// stageShareEnded reports whether ctx is a stage share that has run out while
// the pass still has budget. Such an end is local to the stage: the pass goes
// on to its later stages, so it is not recorded as the pass's stop.
func stageShareEnded(ctx context.Context) bool {
	b, _ := ctx.Value(reapBudgetKey{}).(reapBudget)
	return !b.passDeadline.IsZero() && time.Now().Before(b.passDeadline)
}

func budgetCut(ctx context.Context, err error, operation string) error {
	if err == nil || context.Cause(ctx) != ErrReapRepoBudget {
		return err
	}
	if state, _ := ctx.Value(reapPassStateKey{}).(*reapPassState); state != nil {
		if stageShareEnded(ctx) {
			state.recordShare(operation)
		} else {
			state.recordCut(operation)
		}
	}
	return fmt.Errorf("%w: %s: %v", ErrReapRepoBudget, operation, err)
}

// reapStageShare runs one stage on num/den of the repo budget the pass has
// left when the stage starts. The stage's deadline is a child of the pass's,
// so the stages after it keep the rest and the pass still ends at its own
// deadline; a stage the share ends reports the budget cause like any budget
// stop. A pass with no budget gets ctx back unchanged.
func reapStageShare(ctx context.Context, num, den int64) (context.Context, context.CancelFunc) {
	b, _ := ctx.Value(reapBudgetKey{}).(reapBudget)
	if b.deadline.IsZero() {
		return ctx, func() {}
	}
	left := max(time.Until(b.deadline), 0)
	deadline := time.Now().Add(left * time.Duration(num) / time.Duration(den))
	passDeadline := b.passDeadline
	if passDeadline.IsZero() {
		passDeadline = b.deadline
	}
	stage, cancel := context.WithDeadlineCause(ctx, deadline, ErrReapRepoBudget)
	return context.WithValue(stage, reapBudgetKey{}, reapBudget{deadline: deadline, passDeadline: passDeadline}), cancel
}

func reapBudgetCheck(ctx context.Context, remaining string) error {
	b, _ := ctx.Value(reapBudgetKey{}).(reapBudget)
	if !b.deadline.IsZero() && !time.Now().Before(b.deadline) {
		if state, _ := ctx.Value(reapPassStateKey{}).(*reapPassState); state != nil {
			if stageShareEnded(ctx) {
				state.recordShare(remaining)
			} else {
				state.recordStage(remaining)
			}
		}
		return fmt.Errorf("%w; left: %s", ErrReapRepoBudget, remaining)
	}
	return nil
}

// reviewersOnly marks a Reap call that only wants an open PR's reviewer sweep:
// the call then ends without the "not a merged PR" refusal.
func reviewersOnly(ctx context.Context) bool { return ctx.Value(reviewersOnlyKey{}) != nil }

// Reap removes the worktrees and local branches of merged-PR head branches
// once the merge is older than reap.worktree_after and no watch recovery PR
// landed. It never closes a pane: panewire's node closes an ended job's pane
// on pane-id + harness-session proof, so a worktree still in use simply waits
// (pane-in-use) until that close lands. Anything dirty, unknown
// or ambiguous is skipped and reported, never forced.
func (s *Service) Reap(ctx context.Context, r config.Repo, prNum int, o Options) (retErr error) {
	pass := s.nextReapPass(r.Name, prNum == 0)
	ctx = context.WithValue(ctx, reapPassKey{}, pass)
	passState := &reapPassState{}
	ctx = context.WithValue(ctx, reapPassStateKey{}, passState)
	budgetText := s.Config.Reap.RepoBudget
	if budgetText == "" {
		budgetText = config.RepoBudgetDefault
	}
	budget, budgetErr := config.Duration(budgetText)
	if budgetErr != nil {
		return fmt.Errorf("reap.repo_budget: %w", budgetErr)
	}
	var budgetDeadline time.Time
	if budget > 0 {
		mutationParent := ctx
		budgetDeadline = time.Now().Add(budget)
		budgetCtx, cancel := context.WithDeadlineCause(ctx, budgetDeadline, ErrReapRepoBudget)
		defer cancel()
		ctx = context.WithValue(budgetCtx, reapBudgetKey{}, reapBudget{deadline: budgetDeadline})
		ctx = reap.WithMutationParent(ctx, mutationParent)
	}
	defer func() {
		failures := ReapNonBudgetErrors(retErr)
		if failures != nil {
			failures = errors.Join(failures, s.audit(r.Name, "reap-error", map[string]any{"error": failures.Error()}, o.DryRun))
		}
		left, inflight := passState.snapshot()
		// A budget error with no recorded stop is an external operation cut by
		// the pass's own deadline. One cut by a stage share (the pass still has
		// budget) is not the pass's stop, so it is left unrecorded.
		if errors.Is(retErr, ErrReapRepoBudget) && left == "" && inflight == "" && context.Cause(ctx) == ErrReapRepoBudget {
			// GitHub transport boundaries carry the budget cause directly; they
			// cannot update this package's pass state at the call site.
			passState.recordCut("external operation")
		}
		retErr = reapBudgetResult(passState, failures)
	}()
	if !s.Config.Reap.Enabled {
		return errors.New("reap requires reap.enabled in host config")
	}
	if prNum < 0 {
		return errors.New("--pr must be positive")
	}
	after, e := config.OptionalDuration(s.Config.Reap.WorktreeAfter)
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
		if e := reapBudgetCheck(ctx, "host scans and cleanup"); e != nil {
			return errors.Join(append(errs, e)...)
		}
		hs, hostErrs := s.reapHostScan(ctx, hub, h)
		errs = append(errs, hostErrs...)
		for _, hostErr := range hostErrs {
			if errors.Is(hostErr, ErrReapRepoBudget) {
				return errors.Join(errs...)
			}
		}
		hosts = append(hosts, hs)
		byName[h.Name] = hs
	}
	// One hk job record read per job per pass, and the job-record source's
	// counters: the pane path and the worktree candidate source below share
	// both.
	var records *passJobRecords
	if jobs, ok := s.Sink.(reapJobs); ok {
		records = newPassJobRecords(reapTimedJobs{base: jobs, telemetry: telemetryFrom(ctx)})
	}
	reach := newReapJobReach()
	// Errors are counted once, from errs: note itself appends every "error"
	// row, so no row-level failure can be missing from the summary.
	scanned, planned, removed, deleted, skipped, archived := 0, 0, 0, 0, 0, 0
	noteErr := func(v reapReceipt) error {
		switch v.Result {
		case "would-remove", "would-delete", "would-archive", "would-abandon":
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
			case "archive-job":
				subject = "job " + v.Job
			case "reviewer-reap":
				subject = "reviewer " + v.Label
			}
			// A row-level error is the pass's report, not a failed write. A
			// row whose host the hub cannot serve reports that host skip instead.
			if v.hostSkip != nil {
				errs = append(errs, v.hostSkip)
				break
			}
			errs = append(errs, fmt.Errorf("%s %s %s: %s", v.Host, v.Action, subject, v.Reason))
		}
		// Every row prints; only changed outcomes append to the receipt file.
		// A failed or retained archive always appends: its time spaces the
		// next retry. So does a completed one, which carries its own Drive
		// and hk record. The day's process-scan-unavailable row always
		// appends too: its reason can stay identical for days while the
		// sweep stays blocked, and the day gate already limits it to one a
		// day (hk 4792).
		repeat := h.lastRow[reapRowKey(v)] == reapRowSig(v) && !dailyScanRow(v) && !(v.Action == "archive-job" && (v.Result == "error" || v.Result == "archived" || v.Result == "archived-retained"))
		h.lastRow[reapRowKey(v)] = reapRowSig(v)
		return s.reapNote(v, o.DryRun || repeat)
	}
	note := func(v reapReceipt) {
		if e := noteErr(v); e != nil {
			errs = append(errs, e)
		}
	}
	// reviewerNoteErr is note plus one reviewer-reap audit record per new row: a
	// repeat of the stream's last result and reason writes neither, and is not
	// printed either except by a dry run, whose output is the report. A repeated
	// error still counts, so the pass reports it. The day's
	// process-scan-unavailable row is never a repeat: see note. It returns the
	// write error so the reviewer sweep can hold its journal checkpoint until
	// the row is durable.
	reviewerNoteErr := func(v reapReceipt) error {
		repeat := h.lastRow[reapRowKey(v)] == reapRowSig(v) && !dailyScanRow(v)
		if repeat && !o.DryRun && v.Result != "error" {
			return nil
		}
		var out []error
		if e := noteErr(v); e != nil {
			out = append(out, e)
		}
		if !repeat {
			if e := s.audit(r.Name, "reviewer-reap", map[string]any{
				"pr": v.PR, "label": v.Label, "host": v.Host, "pane": v.Pane, "worktree": v.Worktree, "result": v.Result, "reason": v.Reason,
			}, o.DryRun); e != nil {
				out = append(out, e)
			}
		}
		return errors.Join(out...)
	}
	reviewerNote := func(v reapReceipt) {
		if e := reviewerNoteErr(v); e != nil {
			errs = append(errs, e)
		}
	}
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
	// reviewer spawn must not remove the author's worktree, delete its branch
	// or archive its job.
	if reviewersOnly(ctx) && prNum > 0 {
		// The sweep's rows report through noteErr, which appends to errs:
		// evaluate it before the outer append reads errs.
		revErrs := s.reapReviewers(ctx, r, prNum, receipts, now, hosts, acting, !o.DryRun, reviewerNoteErr)
		return errors.Join(append(errs, revErrs...)...)
	}

	for _, p := range pulls {
		if e := reapBudgetCheck(ctx, "remaining merged PRs, cleanup, reviewer sweep and archive"); e != nil {
			return errors.Join(append(errs, e)...)
		}
		if e = ctx.Err(); e != nil {
			return errors.Join(append(errs, e)...)
		}
		if !(p.Merged || p.MergedAt != nil) || p.Base.Ref != r.Branch {
			if prNum > 0 {
				// A reviewer's job ends whatever its PR's state: sweep the
				// PR's reviewers before refusing the author's path.
				revErrs := s.reapReviewers(ctx, r, prNum, receipts, now, hosts, acting, !o.DryRun, reviewerNoteErr)
				errs = append(errs, revErrs...)
				return errors.Join(append(errs, fmt.Errorf("PR #%d is not a merged PR against %s", p.Number, r.Branch))...)
			}
			continue
		}
		scanned++
		var mergedAt *time.Time
		if p.MergedAt != nil {
			mergedAt = parsedTime(*p.MergedAt)
		}
		// A PR bound to a job still resolves the binding here: the worktree
		// candidate source below reads the bound job's host and cwd, which
		// outlives any pane. A binding read error only loses that source
		// this pass.
		bound, bindErr := s.reapBind(ctx, r, p)
		if bindErr != nil {
			errs = append(errs, fmt.Errorf("PR %d job binding: %s", p.Number, integrations.Detail(bindErr)))
		}

		if mergedAt == nil {
			continue
		}
		// reap.worktree_after is an optional extra delay a host may set.
		// Zero, the default, leaves the watch verdict below as the only gate.
		if after > 0 && now.Sub(*mergedAt) < after {
			continue
		}

		type cand struct {
			hs     *reapHost
			path   string
			source string
		}
		var cands []cand
		seen := map[string]bool{}
		add := func(hs *reapHost, path, source string) {
			if hs == nil || hs.failed || path == "" {
				return
			}
			k := hs.host.Name + "\x00" + path
			if !seen[k] {
				seen[k] = true
				cands = append(cands, cand{hs: hs, path: path, source: source})
			}
		}
		for _, hs := range hosts {
			for _, pane := range hs.panes {
				if w := hs.trees[pane.Cwd]; matches(w, p) {
					add(hs, w.Path, "pane")
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
			if terminalWorktreeSig(sig) {
				continue
			}
			add(byName[rec.Host], path, "receipt")
		}
		// The job record is the third source: the task's bound job names the
		// host and cwd panewire wrote at spawn, which outlives a pane that
		// closed before reap saw it and a close row that recorded no cwd.
		if records != nil {
			settled := func(host, path string) bool {
				return terminalWorktreeSig(h.lastRow[reapRowKey(reapReceipt{Action: "remove-worktree", PR: p.Number, Host: host, Worktree: path})])
			}
			found, e := s.reapJobCandidates(ctx, p, bound, bindErr, records, byName, &reach, settled)
			if e != nil {
				errs = append(errs, e)
			}
			for _, jc := range found {
				// A removed or proven-gone worktree is terminal here too.
				sig := h.lastRow[reapRowKey(reapReceipt{Action: "remove-worktree", PR: p.Number, Host: jc.host, Worktree: jc.path})]
				if terminalWorktreeSig(sig) {
					continue
				}
				add(byName[jc.host], jc.path, "job-record")
			}
		}
		// A PR with no discovered worktree on any host leaves no rows and
		// costs no GitHub calls beyond the listing: the merge-commit and
		// revert checks only run when there is real work to gate.
		if len(cands) == 0 {
			continue
		}
		if p.MergeCommitSHA == "" {
			// The list body carries no merge_commit_sha under this client's API
			// version; Pull resolves it from GraphQL for the few PRs that name
			// work. It reads past the caches because a body cached while the PR
			// was open would not say it merged.
			fp, e := s.GH.Pull(github.Uncached(ctx), r.Name, p.Number)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d merge commit: %w", p.Number, e))
				continue
			}
			p.MergeCommitSHA = fp.MergeCommitSHA
		}
		if p.MergeCommitSHA == "" {
			seenHost := map[string]bool{}
			for _, c := range cands {
				if seenHost[c.hs.host.Name] {
					continue
				}
				seenHost[c.hs.host.Name] = true
				note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Result: "skipped", Reason: "merge-commit-unknown"})
			}
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
		reverted := false
		if recovery != nil {
			reason := ""
			if recovery.MergedAt != nil {
				// Same convention as the watch role and outcomes ledger: a
				// recovery PR merged within seven days reverted this PR.
				if at := parsedTime(*recovery.MergedAt); at != nil && !at.Before(*mergedAt) && !at.After(mergedAt.Add(7*24*time.Hour)) {
					reason, reverted = "reverted", true
				}
			} else if recovery.State == "open" {
				reason = "recovery-open"
			}
			if reverted {
				// The follow-up ends when the revert itself is covered by a
				// healthy watch verdict; until then the worktree stays. The
				// head branch is never deleted, so reverted work stays
				// fetchable from GitHub.
				covers, e := s.watchCovers(ctx, r, recovery.MergeCommitSHA)
				if e != nil {
					errs = append(errs, fmt.Errorf("PR %d revert verdict: %w", p.Number, e))
				}
				if covers {
					reason = ""
				}
			}
			if reason != "" {
				note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: reason})
				continue
			}
		}
		// The watch verdict for the merge commit on main is the gate: a PR
		// whose post-merge main has not passed CI keeps its worktree. A
		// reverted PR's own merge never turns green, so its follow-up above
		// decided instead.
		if !reverted {
			covers, e := s.watchCovers(ctx, r, p.MergeCommitSHA)
			if e != nil {
				errs = append(errs, fmt.Errorf("PR %d watch verdict: %w", p.Number, e))
			}
			if !covers {
				note(reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Result: "skipped", Reason: "watch-pending"})
				continue
			}
		}

		for _, c := range cands {
			if e := reapBudgetCheck(ctx, "remaining worktree candidates, reviewer sweep and archive"); e != nil {
				return errors.Join(append(errs, e)...)
			}
			rec := reapReceipt{Action: "remove-worktree", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Cwd: c.path, Worktree: c.path, Branch: p.Head.Ref, Source: c.source}
			w, e := c.hs.ops.Inspect(ctx, c.path)
			switch {
			case w != nil && strings.HasPrefix(w.Branch, workerBranchPrefix):
				// A worker checkout belongs to the worker-worktree pass, which
				// has its own discovery, rules and hub-only guard. This older
				// path neither removes nor records it.
				continue
			case e != nil:
				rec.Result, rec.Reason = "skipped", "worktree-inspect-error"
				errs = append(errs, hostUnreachableFailure(c.hs.host.Name, fmt.Errorf("%s %s: %w", c.hs.host.Name, c.path, e)))
			case w == nil:
				rec.Result, rec.Reason = "skipped", "worktree-gone"
			case !matches(w, p):
				rec.Result, rec.Reason = "skipped", "worktree-changed"
			case c.hs.viaHub:
				// Like the ssh path, a worktree a pane still uses waits:
				// the pane list is re-read immediately before an acting
				// removal, and the node re-checks once more at removal.
				if c.hs.paneInUse(w.Path) {
					rec.Result, rec.Reason = "skipped", "pane-in-use"
					note(rec)
					continue
				}
				if !acting {
					rec.Result = "would-remove"
					note(rec)
					brec := reapReceipt{Action: "delete-branch", Repo: r.Name, PR: p.Number, Host: c.hs.host.Name, Worktree: w.Path, Branch: w.Branch, Result: "would-delete"}
					note(brec)
					continue
				}
				switch e := c.hs.refreshPanes(ctx); {
				case e != nil:
					rec.Result, rec.Reason = "skipped", "pane-scan-failed"
					errs = append(errs, fmt.Errorf("%s pane list: %w", c.hs.host.Name, e))
				case c.hs.paneInUse(w.Path):
					rec.Result, rec.Reason = "skipped", "pane-in-use"
				default:
					remover, ok := c.hs.ops.(interface {
						WorktreeRemove(context.Context, string, string, string, []string, bool) (reap.WorktreeRemoveResult, error)
					})
					if !ok {
						rec.Result, rec.Reason = "skipped", "cleanup-unknown:unsupported"
					} else if result, err := remover.WorktreeRemove(ctx, w.Path, w.Head, r.Branch, s.Config.ReapRegenerable(r), false); err != nil {
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
			entries, e := c.hs.exec.WorktreeEntries(ctx, main)
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
					if !reap.Regenerable(s.Config.ReapRegenerable(r), f) {
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
						// A background process with no pane can hold the
						// checkout; fail closed when this machine cannot say.
						if reason := s.processInUse(ctx, w.Path); reason != "" {
							rec.Result, rec.Reason = "skipped", reason
							break
						}
						if e := budgetCut(ctx, c.hs.exec.RemoveWorktree(ctx, main, w.Path), "guarded worktree removal"); e != nil {
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
				if e := budgetCut(ctx, c.hs.exec.DeleteBranch(ctx, main, entry.Branch), "branch deletion"); e != nil {
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
	// The worker-worktree pass runs on whole-repo passes only: it discovers
	// wk/ checkouts from the hosts' own worktree lists, so a worktree no pane,
	// receipt or job record names is still removed once its work is on GitHub.
	if prNum == 0 {
		if e := reapBudgetCheck(ctx, "worker worktrees, reviewer sweep and archive"); e != nil {
			return errors.Join(append(errs, e)...)
		}
		// The worker stage gets two thirds of the budget the pass has left, so
		// the reviewer sweep and what follows keep the last third: a worker
		// stage that always fills the budget starves them on every pass.
		workerCtx, cancelWorker := reapStageShare(ctx, 2, 3)
		errs = append(errs, s.reapWorkerWorktrees(workerCtx, r, hosts, records, acting, !o.DryRun, note)...)
		cancelWorker()
	}
	if e := reapBudgetCheck(ctx, "reviewer sweep, balance, orphan scan and archive"); e != nil {
		return errors.Join(append(errs, e)...)
	}
	revErrs := s.reapReviewers(ctx, r, prNum, receipts, now, hosts, acting, !o.DryRun, reviewerNoteErr)
	errs = append(errs, revErrs...)
	// The fleet-wide reviewer balance runs on whole-repo passes only, once a
	// day: it counts checkouts created and removed per host across all
	// watched repos and notices when a host's removals stall (hk 5090).
	if acting && prNum == 0 {
		if err := s.reviewerBalance(ctx, o); err != nil {
			errs = append(errs, err)
		}
		if err := s.liveness(ctx, o); err != nil {
			errs = append(errs, err)
		}
	}
	if prNum == 0 {
		if e := reapBudgetCheck(ctx, "orphan scan and archive"); e != nil {
			return errors.Join(append(errs, e)...)
		}
		errs = append(errs, s.reapOrphanCheckouts(ctx, r, receipts, hosts, acting, reviewerNote)...)
	}
	if s.Config.Reap.Archive.Enabled {
		if e := reapBudgetCheck(ctx, "archive jobs"); e != nil {
			return errors.Join(append(errs, e)...)
		}
		errs = append(errs, s.reapArchive(ctx, r, prNum, receipts, byName, records, acting, note, func(v reapReceipt) { delete(h.lastRow, reapRowKey(v)) })...)
	}
	if e := reapBudgetCheck(ctx, "final receipt and pass audit"); e != nil {
		return errors.Join(append(errs, e)...)
	}
	result := struct {
		Action   string `json:"action"`
		Repo     string `json:"repo"`
		Shadow   bool   `json:"shadow"`
		Scanned  int    `json:"prs_scanned"`
		Planned  int    `json:"planned"`
		Removed  int    `json:"removed"`
		Deleted  int    `json:"branches_deleted"`
		Archived int    `json:"jobs_archived"`
		Skipped  int    `json:"skipped"`
		ErrCount int    `json:"errors"`
		// JobRecords is what the third discovery source reached: the job
		// records of the tasks of the merged PRs past worktree_after.
		JobRecords reapJobReach `json:"job_records"`
		// ShareCut names the stage whose budget share ended before its work
		// did while the pass still had budget; the pass itself completed.
		ShareCut string `json:"share_cut,omitempty"`
	}{"reap", r.Name, !acting, scanned, planned, removed, deleted, archived, skipped, len(errs), reach, passState.shareCut()}
	return errors.Join(append(errs, s.audit(r.Name, "reap", result, o.DryRun))...)
}
