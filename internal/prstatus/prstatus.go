// Package prstatus stores and serves each PR's last merge-gate verdict. The
// gate writes one snapshot per decision into mergewake state; the daemon's
// HTTP listener and the pr-status command read it back. Reading never calls
// GitHub, except for the single opt-in fresh query.
package prstatus

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

// SourceLastPass marks gate fields as read from the daemon's last pass.
const SourceLastPass = "last_pass"

// MaxResponse bounds one API response, in bytes.
const MaxResponse = 64 << 10

// Gate states. A closed or merged PR keeps its last gate fields under its
// final state.
const (
	Ready   = "ready"
	Blocked = "blocked"
	Pending = "pending"
	Merged  = "merged"
	Closed  = "closed"
)

// Caps keep one snapshot far under MaxResponse whatever GitHub returns.
const (
	maxReasons  = 50
	maxChecks   = 100
	maxPasses   = 20
	maxFindings = 50
)

// ErrNotFound means no snapshot exists for the PR.
var ErrNotFound = errors.New("no status snapshot for this PR")

// Check is one required check and its state: green, red, pending, cancelled
// or skipped.
type Check struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// Pass is one review pass recorded for the head.
type Pass struct {
	Pass   int    `json:"pass"`
	Lens   string `json:"lens,omitempty"`
	Family string `json:"family,omitempty"`
}

// Review is what the gate saw of the head's review: the passes bound to it,
// whether the done marker exists, and the head whose passes it inherited when
// its patch equals a reviewed one.
type Review struct {
	Passes    []Pass `json:"passes"`
	Done      bool   `json:"done"`
	Inherited string `json:"inherited_from,omitempty"`
}

// Threads splits the open review threads: Blocking holds the gate, Other
// are triage or advisory threads that wait for an answer or never block.
// Unresolved counts every unresolved thread, nil while the gate stopped
// before it read them.
type Threads struct {
	Blocking   int  `json:"blocking"`
	Other      int  `json:"other"`
	Unresolved *int `json:"unresolved,omitempty"`
}

// Finding is one unresolved review thread, from the evidence the gate pass
// already read. Location is path:line, Summary the first line of the finding
// (at most MaxSummary characters) and AuthorReplied whether the PR's author
// answered in the thread.
type Finding struct {
	ID            string `json:"id"`
	Location      string `json:"location"`
	Severity      string `json:"severity,omitempty"`
	Blocking      bool   `json:"blocking"`
	Summary       string `json:"summary"`
	URL           string `json:"url,omitempty"`
	AuthorReplied bool   `json:"author_replied"`
}

// MaxSummary bounds a finding's summary, in characters.
const MaxSummary = 200

// Snapshot is the gate's last decision for one PR head. BaseFresh is nil
// while the gate stopped before it compared the base.
//
// State, Reasons, Checks, Review and Findings are the gate's, as of ObservedAt
// and GateHead. A fresh read changes only Head, Fresh and FreshAt: Head is the
// newest head known, Fresh holds that read's check rollup, thread count and
// mergeable state, and FreshAt is null until a fresh read happened. The next
// gate decision replaces the whole snapshot.
type Snapshot struct {
	Repo       string          `json:"repo"`
	PR         int             `json:"pr"`
	Head       string          `json:"head"`
	GateHead   string          `json:"gate_head"`
	State      string          `json:"state"`
	Reasons    []string        `json:"reasons"`
	Checks     []Check         `json:"checks"`
	Review     Review          `json:"review"`
	Threads    Threads         `json:"threads"`
	Findings   []Finding       `json:"findings"`
	BaseFresh  *bool           `json:"base_fresh,omitempty"`
	BaseBehind bool            `json:"base_behind,omitempty"`
	ObservedAt time.Time       `json:"observed_at"`
	Fresh      *github.PRFresh `json:"fresh"`
	FreshAt    *time.Time      `json:"fresh_at"`
	Source     string          `json:"source"`
}

// Open reports whether the PR was still open when observed.
func (s Snapshot) Open() bool { return s.State != Merged && s.State != Closed }

func (s Snapshot) bounded() Snapshot {
	s.Reasons = capped(s.Reasons, maxReasons)
	s.Checks = capped(s.Checks, maxChecks)
	s.Review.Passes = capped(s.Review.Passes, maxPasses)
	s.Findings = capped(s.Findings, maxFindings)
	return s
}

func capped[T any](v []T, n int) []T {
	if v == nil {
		return []T{}
	}
	return v[:min(len(v), n)]
}

func dir(stateDir, repo string) string { return filepath.Join(stateDir, repo, "pr-status") }

func file(stateDir, repo string, n int) string {
	return filepath.Join(dir(stateDir, repo), strconv.Itoa(n)+".json")
}

// Write stores s atomically, replacing the PR's previous snapshot. A gate
// decision starts the snapshot over: any fresh read is older than it.
func Write(stateDir string, s Snapshot) error {
	if !config.ValidRepo(s.Repo) || s.PR <= 0 {
		return fmt.Errorf("invalid status snapshot key %s#%d", s.Repo, s.PR)
	}
	s.Source, s.GateHead, s.Fresh, s.FreshAt = SourceLastPass, cmp.Or(s.GateHead, s.Head), nil, nil
	return store(stateDir, s)
}

func store(stateDir string, s Snapshot) error {
	unlock, e := fsutil.Lock(file(stateDir, s.Repo, s.PR)+".lock", true)
	if e != nil {
		return e
	}
	defer unlock()
	return storeLocked(stateDir, s)
}

func storeLocked(stateDir string, s Snapshot) error {
	b, e := json.Marshal(s.bounded())
	if e != nil {
		return e
	}
	return fsutil.Atomic(file(stateDir, s.Repo, s.PR), append(b, '\n'), 0600)
}

// Read returns the PR's snapshot, or ErrNotFound.
func Read(stateDir, repo string, n int) (Snapshot, error) {
	b, e := os.ReadFile(file(stateDir, repo, n))
	if errors.Is(e, fs.ErrNotExist) {
		return Snapshot{}, ErrNotFound
	}
	if e != nil {
		return Snapshot{}, e
	}
	var s Snapshot
	if e = json.Unmarshal(b, &s); e != nil {
		return Snapshot{}, fmt.Errorf("status snapshot %s#%d: %w", repo, n, e)
	}
	return s, nil
}

// ListOpen returns the snapshots of repo's open PRs, newest PR first.
func ListOpen(stateDir, repo string) ([]Snapshot, error) {
	entries, e := os.ReadDir(dir(stateDir, repo))
	if errors.Is(e, fs.ErrNotExist) {
		return []Snapshot{}, nil
	}
	if e != nil {
		return nil, e
	}
	out := []Snapshot{}
	for _, entry := range entries {
		n, err := strconv.Atoi(strings.TrimSuffix(entry.Name(), ".json"))
		if err != nil || n <= 0 || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		s, err := Read(stateDir, repo, n)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if s.Open() {
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b Snapshot) int { return cmp.Compare(b.PR, a.PR) })
	return out, nil
}

// Response is one PR's answer: the snapshot, with a FreshError when a
// requested fresh read failed. Snapshot.Source stays last_pass: the gate
// fields always are.
type Response struct {
	Snapshot
	FreshError string `json:"fresh_error,omitempty"`
}

// Fresher makes the one GitHub query behind fresh=1.
type Fresher func(ctx context.Context, repo string, n int) (github.PRFresh, error)

// Respond reads the PR's snapshot. With a non-nil fresh it then makes the one
// fresh read and writes it through: the snapshot's head, fresh block and
// fresh_at take the new values, and the gate fields stay. When the head, the
// checks or the unresolved threads differ from what the snapshot held, the PR
// is marked for re-evaluation and changed is true; the caller wakes the pr
// role and drops stale evidence. The snapshot is read first, so a PR with none
// costs no GitHub call. A failed fresh read is reported in the response, not
// as an error, so the snapshot still gets through.
func Respond(ctx context.Context, stateDir, repo string, n int, fresh Fresher, now time.Time) (r Response, changed bool, err error) {
	s, e := Read(stateDir, repo, n)
	if e != nil {
		return Response{}, false, e
	}
	if fresh == nil {
		return Response{Snapshot: s}, false, nil
	}
	f, e := fresh(ctx, repo, n)
	if e != nil {
		return Response{Snapshot: s, FreshError: "GitHub read failed"}, false, nil
	}
	if f.ObservedAt.IsZero() {
		f.ObservedAt = now
	}
	changed = s.Open() && differs(s, f)
	next := s
	next.GateHead = cmp.Or(s.GateHead, s.Head)
	next.Head, next.Fresh, next.FreshAt = cmp.Or(f.Head, s.Head), &f, &f.ObservedAt
	current, wrote, e := update(stateDir, s, next)
	if e != nil {
		return Response{}, false, e
	}
	if !wrote {
		return Response{Snapshot: current}, false, nil
	}
	if changed {
		if e = MarkDirty(stateDir, repo, n, now); e != nil {
			return Response{}, false, e
		}
	}
	return Response{Snapshot: next}, changed, nil
}

// update stores next over the snapshot it was built from, read as old, unless
// a gate decision replaced that snapshot since: the newer decision's gate
// fields win and the read, taken against the old one, is dropped. It returns
// the snapshot now on disk when it did not write.
func update(stateDir string, old, next Snapshot) (current Snapshot, wrote bool, err error) {
	unlock, e := fsutil.Lock(file(stateDir, old.Repo, old.PR)+".lock", true)
	if e != nil {
		return Snapshot{}, false, e
	}
	defer unlock()
	current, e = Read(stateDir, old.Repo, old.PR)
	if e != nil {
		return Snapshot{}, false, e
	}
	if !current.ObservedAt.Equal(old.ObservedAt) || current.GateHead != cmp.Or(old.GateHead, old.Head) {
		return current, false, nil
	}
	return next, true, storeLocked(stateDir, next)
}

// differs reports whether a fresh read disagrees with the snapshot on the
// head, the checks (as success, failure or pending) or the unresolved thread
// count. Once a fresh read is stored it is the baseline, so the same news is
// not reported twice; before that the gate's own view is.
func differs(s Snapshot, f github.PRFresh) bool {
	if f.Head != "" && f.Head != s.Head {
		return true
	}
	was, now := checksClass(s.Checks), rollupClass(f.ChecksRollup)
	prev := s.Threads.Unresolved
	if s.Fresh != nil {
		was = rollupClass(s.Fresh.ChecksRollup)
		prev = nil
		if !s.Fresh.ThreadsCapped {
			prev = &s.Fresh.UnresolvedThreads
		}
	}
	if was != now {
		return true
	}
	return prev != nil && !f.ThreadsCapped && *prev != f.UnresolvedThreads
}

func checksClass(checks []Check) string {
	if len(checks) == 0 {
		return "none"
	}
	class := "success"
	for _, c := range checks {
		switch c.State {
		case "red":
			return "failure"
		case "pending", "cancelled", "":
			class = "pending"
		}
	}
	return class
}

func rollupClass(state string) string {
	switch state {
	case "":
		return "none"
	case "SUCCESS":
		return "success"
	case "FAILURE", "ERROR":
		return "failure"
	}
	return "pending"
}

// MarkDirty records that the PR should be re-evaluated first in the next pr
// pass. The marker holds when it was set, so a pass that started earlier does
// not clear it.
func MarkDirty(stateDir, repo string, n int, at time.Time) error {
	unlock, e := fsutil.Lock(dirtyFile(stateDir, repo, n)+".lock", true)
	if e != nil {
		return e
	}
	defer unlock()
	return fsutil.Atomic(dirtyFile(stateDir, repo, n), []byte(at.UTC().Format(time.RFC3339Nano)+"\n"), 0600)
}

func dirtyFile(stateDir, repo string, n int) string {
	return filepath.Join(dir(stateDir, repo), strconv.Itoa(n)+".dirty")
}

// DirtyPRs lists the repo's PRs marked for re-evaluation. An unreadable
// marker counts as set: re-evaluating once costs less than missing it.
func DirtyPRs(stateDir, repo string) map[int]bool {
	entries, _ := os.ReadDir(dir(stateDir, repo))
	out := map[int]bool{}
	for _, entry := range entries {
		if n, ok := strings.CutSuffix(entry.Name(), ".dirty"); ok {
			if i, err := strconv.Atoi(n); err == nil && i > 0 {
				out[i] = true
			}
		}
	}
	return out
}

// ClearDirty removes the PR's marker when it was set before since, the start
// of the pass that has just decided the PR; a later marker stays for the next
// pass. A lock failure leaves the marker: one more re-evaluation is cheap.
func ClearDirty(stateDir, repo string, n int, since time.Time) {
	path := dirtyFile(stateDir, repo, n)
	// Read and remove under MarkDirty's lock: a marker written in between
	// would otherwise be deleted unseen.
	unlock, e := fsutil.Lock(path+".lock", true)
	if e != nil {
		return
	}
	defer unlock()
	b, e := os.ReadFile(path)
	if e != nil {
		return
	}
	if at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b))); err == nil && !at.Before(since) {
		return
	}
	_ = os.Remove(path)
}
