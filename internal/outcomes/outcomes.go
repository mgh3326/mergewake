// Package outcomes maintains an append-only observation ledger and rep outbox.
package outcomes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type Outcome struct {
	Supersedes   string                    `json:"supersedes,omitempty"`
	ID           string                    `json:"outcome_id"`
	Revision     int                       `json:"revision"`
	Repo         string                    `json:"repo"`
	PR           int                       `json:"pr"`
	Task         string                    `json:"task_ref"`
	Head         string                    `json:"head"`
	Profile      string                    `json:"profile"`
	Model        string                    `json:"model"`
	Effort       string                    `json:"effort"`
	Family       string                    `json:"family"`
	SameFamily   bool                      `json:"same_family"`
	Experiment   string                    `json:"experiment_id"`
	Arm          string                    `json:"arm"`
	Grade        string                    `json:"grade"`
	Tier         string                    `json:"tier"`
	OpenedAt     time.Time                 `json:"opened_at"`
	MergedAt     *time.Time                `json:"merged_at"`
	ClosedAt     *time.Time                `json:"closed_at"`
	FirstCIGreen *bool                     `json:"first_ci_green"`
	Findings     map[string]int            `json:"findings_by_severity"`
	Reviews      map[string]map[string]int `json:"reviews_by_head"`
	// ReviewedHeads lists the heads that received a review, oldest first, in
	// the order they were reviewed; Reviews holds no order of its own.
	ReviewedHeads []string `json:"reviewed_heads,omitempty"`
	// ReviewersByHead names, by head and pass number, the reviewer each posted
	// pass ran as, as its receipt records it.
	ReviewersByHead map[string]map[string]string `json:"reviewers_by_head,omitempty"`
	Decision        string                       `json:"decision"`
	Action          string                       `json:"action"`
	Shadow          bool                         `json:"shadow"`
	Final           bool                         `json:"finalized"`
	TimeToMerge     *float64                     `json:"time_to_merge_s"`
	Reverted        *bool                        `json:"reverted_within_7d"`
	RevertPR        int                          `json:"revert_pr,omitempty"`
	RevertURL       string                       `json:"revert_url,omitempty"`
	// ReconcileFinished means this closed PR has no remaining outcome work.
	ReconcileFinished bool       `json:"reconcile_finished,omitempty"`
	ReconcileDueAt    *time.Time `json:"reconcile_due_at,omitempty"`
	UpdatedAt         time.Time  `json:"updated_at"`
	// ReviewBlocked holds, by head, why that head's review cannot start, such
	// as "blocked: missing-task-trailer". A blocked head has no review.
	ReviewBlocked map[string]string `json:"review_blocked_by_head,omitempty"`
}

// revisionContent omits reconciliation-only state, which is persisted in the
// ledger but must not change an outcome's identity or published representation.
func (v Outcome) revisionContent() Outcome {
	v.ReconcileFinished = false
	v.ReconcileDueAt = nil
	return v
}

// SetReviewer records the reviewer of one pass on a head.
func (v *Outcome) SetReviewer(head, pass, reviewer string) {
	if v.ReviewersByHead == nil {
		v.ReviewersByHead = map[string]map[string]string{}
	}
	if v.ReviewersByHead[head] == nil {
		v.ReviewersByHead[head] = map[string]string{}
	}
	v.ReviewersByHead[head][pass] = reviewer
}

// AddReviewedHead records that head has just been reviewed. A head is added
// once, so it keeps the place it was first reviewed in.
func (v *Outcome) AddReviewedHead(head string) {
	if !slices.Contains(v.ReviewedHeads, head) {
		v.ReviewedHeads = append(v.ReviewedHeads, head)
	}
}

// SetReviewedHeadOrder takes heads, oldest first, as the review order. A head
// it does not list keeps its recorded place after them.
func (v *Outcome) SetReviewedHeadOrder(heads []string) {
	var out []string
	for _, h := range slices.Concat(heads, v.ReviewedHeads) {
		if !slices.Contains(out, h) {
			out = append(out, h)
		}
	}
	v.ReviewedHeads = out
}

// reviewedHeads lists the heads with a review, oldest first. A reviewed head
// with no recorded place (an outcome kept before the order was) follows in
// sorted order, and ordered is then false: the order is not known for it.
func (v Outcome) reviewedHeads() (heads []string, ordered bool) {
	for _, h := range v.ReviewedHeads {
		if _, ok := v.Reviews[h]; ok && !slices.Contains(heads, h) {
			heads = append(heads, h)
		}
	}
	ordered = len(heads) == len(v.Reviews)
	for _, h := range slices.Sorted(maps.Keys(v.Reviews)) {
		if !slices.Contains(heads, h) {
			heads = append(heads, h)
		}
	}
	return heads, ordered
}

func (v Outcome) Attributed() bool {
	return v.Profile != "" && v.Model != "" && v.Grade != "" && v.Tier != ""
}

func (v Outcome) ReviewedHead() bool { _, ok := v.Reviews[v.Head]; return ok }

// Evidence deliberately keeps unknown observations out of the clean-pass count.
func (v Outcome) Evidence() string {
	if v.Blockers() > 0 || (v.Reverted != nil && *v.Reverted) {
		return "failure"
	}
	if !v.Final || !v.Attributed() || v.Decision != "landed" || v.FirstCIGreen == nil || v.Reverted == nil || !v.ReviewedHead() {
		return "pending"
	}
	if !*v.FirstCIGreen || v.Findings["unspecified"] > 0 {
		return "with_fixes"
	}
	return "clean_pass"
}
func (v Outcome) Blockers() int {
	return v.Findings["blocking"] + v.Findings["critical"] + v.Findings["high"] + v.Findings["major"]
}

type Event struct {
	Version int       `json:"version"`
	ID      string    `json:"event_id"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Outcome *Outcome  `json:"outcome,omitempty"`
	Rep     string    `json:"rep,omitempty"`
}
type Ledger struct {
	Dir string
	// Now stamps events and outcome revisions; nil means time.Now.
	Now func() time.Time
}

func (l Ledger) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

type State struct {
	Latest    map[string]Outcome
	Published map[string]string
}

func emptyState() State {
	return State{Latest: map[string]Outcome{}, Published: map[string]string{}}
}

// clone returns a State that shares no map, slice or pointer with s.
func (s State) clone() State {
	out := State{Latest: make(map[string]Outcome, len(s.Latest)), Published: maps.Clone(s.Published)}
	for task, v := range s.Latest {
		out.Latest[task] = v.clone()
	}
	return out
}

// clone returns a copy of v that shares no map, slice or pointer with v.
func (v Outcome) clone() Outcome {
	v.MergedAt = clonePtr(v.MergedAt)
	v.ClosedAt = clonePtr(v.ClosedAt)
	v.FirstCIGreen = clonePtr(v.FirstCIGreen)
	v.TimeToMerge = clonePtr(v.TimeToMerge)
	v.Reverted = clonePtr(v.Reverted)
	v.ReconcileDueAt = clonePtr(v.ReconcileDueAt)
	v.Findings = maps.Clone(v.Findings)
	v.Reviews = cloneNested(v.Reviews)
	v.ReviewedHeads = slices.Clone(v.ReviewedHeads)
	v.ReviewersByHead = cloneNested(v.ReviewersByHead)
	v.ReviewBlocked = maps.Clone(v.ReviewBlocked)
	return v
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	c := *p
	return &c
}

func cloneNested[V any](m map[string]map[string]V) map[string]map[string]V {
	if m == nil {
		return nil
	}
	out := make(map[string]map[string]V, len(m))
	for k, inner := range m {
		out[k] = maps.Clone(inner)
	}
	return out
}

// ledgerFile is what this process knows of one ledger file: the state of the
// file's bytes before offset. A call decodes only the bytes after offset, so
// a row another process appended is seen by the next call.
type ledgerFile struct {
	dev, ino uint64
	offset   int64
	// lines is how many complete lines the cache has decoded, so an audit row
	// can name a line by its number in the file.
	lines int
	// tail holds the bytes just before offset. A file replaced in place keeps
	// its inode, so the tail is what tells it apart from the one decoded.
	tail  []byte
	state State
}

// ledgerTailBytes is how many bytes before offset a ledgerFile keeps as tail.
const ledgerTailBytes = 4096

// ledgerFiles holds one ledgerFile per ledger path. flock orders the calls of
// all processes; ledgerMu orders the calls of this process, whose goroutines
// share the cache.
var (
	ledgerMu    sync.Mutex
	ledgerFiles = map[string]*ledgerFile{}
)

func Key(repo string, pr int) string { return fmt.Sprintf("https://github.com/%s/pull/%d", repo, pr) }

// locked calls fn with the ledger state under the flock. The State is the
// cache's own: fn must not keep it or change it, and must clone what it edits.
func (l Ledger) locked(fn func(*os.File, State) error) error {
	if err := os.MkdirAll(l.Dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(l.Dir, "outcomes.jsonl")
	ledgerMu.Lock()
	defer ledgerMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	state, err := load(path, f)
	if err != nil {
		return err
	}
	return fn(f, state)
}

// load returns the state of the locked ledger file f at path. It decodes only
// the bytes appended since the cache last decoded when f is still the file the
// cache holds. On first use, for a different or replaced file, or for a file
// that is now shorter than the cache covers, it decodes from the start. A
// decode error drops the cache entry, so the next call starts from the start.
func load(path string, f *os.File) (State, error) {
	fi, err := f.Stat()
	if err != nil {
		return State{}, err
	}
	dev, ino := fileID(fi)
	lf := ledgerFiles[path]
	if lf == nil || lf.dev != dev || lf.ino != ino || fi.Size() < lf.offset || !lf.tailMatches(f) {
		lf = &ledgerFile{dev: dev, ino: ino, state: emptyState()}
		ledgerFiles[path] = lf
	}
	consumed, lines, err := applyEvents(path, io.NewSectionReader(f, lf.offset, fi.Size()-lf.offset), &lf.state, lf.lines)
	if err == nil {
		lf.offset += consumed
		lf.lines = lines
		err = lf.setTail(f)
	}
	if err != nil {
		delete(ledgerFiles, path)
		return State{}, err
	}
	return lf.state, nil
}

// fileID names the file behind fi by its device and inode.
func fileID(fi os.FileInfo) (dev, ino uint64) {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev), uint64(st.Ino)
	}
	return 0, 0
}

// tailMatches reports whether the bytes before offset are still the tail the
// cache holds.
func (lf *ledgerFile) tailMatches(f *os.File) bool {
	got := make([]byte, len(lf.tail))
	if _, err := f.ReadAt(got, lf.offset-int64(len(lf.tail))); err != nil {
		return false
	}
	return slices.Equal(got, lf.tail)
}

// setTail keeps the last ledgerTailBytes before offset.
func (lf *ledgerFile) setTail(f *os.File) error {
	n := min(lf.offset, ledgerTailBytes)
	lf.tail = make([]byte, n)
	_, err := f.ReadAt(lf.tail, lf.offset-n)
	return err
}

// applyEvents applies the complete lines of r to s, in order. before is how
// many lines precede r in the file at path. It returns how many bytes and how
// many lines it consumed; without an error that is all of r's complete lines.
// A line that does not decode is skipped and audited, so one damaged row never
// fails the ledger's readers. A final line with no newline is left for later.
func applyEvents(path string, r io.Reader, s *State, before int) (int64, int, error) {
	j := fsutil.NewJSONL(path, before, r)
	for line, ok := j.Next(); ok; line, ok = j.Next() {
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			j.Skip(line)
			continue
		}
		if e.Version != 1 {
			return j.Consumed(), j.Line(), errors.New("unsupported outcomes ledger version")
		}
		switch e.Kind {
		case "outcome":
			if e.Outcome == nil {
				return j.Consumed(), j.Line(), errors.New("missing outcome")
			}
			s.Latest[e.Outcome.Task] = *e.Outcome
		case "published":
			s.Published[e.ID] = e.Rep
		default:
			return j.Consumed(), j.Line(), errors.New("unknown outcomes event")
		}
	}
	return j.Consumed(), j.Line(), j.Err()
}

func appendEvent(f *os.File, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return fsutil.AppendTo(f, b)
}
func (l Ledger) Read() (State, error) {
	var s State
	err := l.locked(func(_ *os.File, state State) error { s = state.clone(); return nil })
	return s, err
}
func (l Ledger) Update(repo string, pr int, fn func(*Outcome)) error {
	return l.locked(func(f *os.File, s State) error {
		key := Key(repo, pr)
		// fn edits maps in place, so it gets a copy of the row, never the cache's.
		v := s.Latest[key].clone()
		if v.Task == "" {
			v = Outcome{Repo: repo, PR: pr, Task: key, Decision: "unknown", Reviews: map[string]map[string]int{}}
		}
		beforeRevision, _ := json.Marshal(v.revisionContent())
		beforeData, _ := json.Marshal(v)
		fn(&v)
		afterData, _ := json.Marshal(v)
		if string(beforeData) == string(afterData) {
			return nil
		}
		afterRevision, _ := json.Marshal(v.revisionContent())
		if string(beforeRevision) == string(afterRevision) {
			return appendEvent(f, Event{Version: 1, ID: v.ID, At: v.UpdatedAt, Kind: "outcome", Outcome: &v})
		}
		v.Revision++
		v.UpdatedAt = l.now().UTC()
		v.Supersedes = v.ID
		v.ID = ""
		data, _ := json.Marshal(v.revisionContent())
		sum := sha256.Sum256(data)
		v.ID = hex.EncodeToString(sum[:])
		return appendEvent(f, Event{Version: 1, ID: v.ID, At: v.UpdatedAt, Kind: "outcome", Outcome: &v})
	})
}

// Reps is replaceable by panewire or another shared bench transport.
type Reps interface {
	Publish(context.Context, Outcome) (string, error)
	Read(context.Context) ([]Outcome, error)
}
type CLI struct {
	Runner  integrations.Runner
	Command string
	// Environ is the environment an outcomes child starts from; nil means
	// os.Environ.
	Environ func() []string
}

// childEnvDropped names the daemon's own hk service variables. An outcomes
// child never inherits them. panewire reads HANDOFFKEEP_URL with
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE or its own hk-svc.env, not hk's config.env.
// See docs/config.md, "Outcomes".
var childEnvDropped = []string{
	"HANDOFFKEEP_URL",
	"HANDOFFKEEP_TOKEN",
	"HANDOFFKEEP_TOKEN_SVC_MERGEWAKE",
	"HANDOFFKEEP_CF_ACCESS_CLIENT_ID",
	"HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET",
}

// childEnv is environ without every entry whose name, before the first "=",
// is in childEnvDropped. It is never nil: a nil environment would inherit the
// daemon's whole environment.
func childEnv(environ []string) []string {
	out := make([]string, 0, len(environ))
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(childEnvDropped, name) {
			out = append(out, kv)
		}
	}
	return out
}

// envRunner is a Runner that can replace a child's environment, as
// integrations.Exec does.
type envRunner interface {
	RunEnv(context.Context, string, []string, string, string, []string) ([]byte, error)
}

// run invokes the outcomes command with childEnv. A Runner without RunEnv (a
// test fake) gets a plain Run.
func (c CLI) run(ctx context.Context, args []string) ([]byte, error) {
	r, ok := c.Runner.(envRunner)
	if !ok {
		return c.Runner.Run(ctx, c.Command, args, "", "")
	}
	environ := c.Environ
	if environ == nil {
		environ = os.Environ
	}
	return r.RunEnv(ctx, c.Command, args, "", "", childEnv(environ()))
}

// repRow parses one line of `reps list` text output: the rep ref after "id="
// (srv:N, origin:N or local:N) and the mergewake outcome its notes carry.
func repRow(line string) (ref string, v Outcome, ok bool) {
	rest, found := strings.CutPrefix(line, "id=")
	if !found {
		return "", Outcome{}, false
	}
	ref, _, _ = strings.Cut(rest, " ")
	i := strings.Index(line, " notes=")
	if i < 0 {
		return "", Outcome{}, false
	}
	var envelope struct {
		Source   string         `json:"source"`
		Observed map[string]int `json:"observed_findings_by_severity"`
		Outcome  Outcome        `json:"outcome"`
	}
	d := json.NewDecoder(strings.NewReader(line[i+7:]))
	if d.Decode(&envelope) != nil || envelope.Source != "mergewake" || envelope.Outcome.ID == "" {
		return "", Outcome{}, false
	}
	if envelope.Outcome.Findings == nil && envelope.Observed != nil {
		envelope.Outcome.Findings = envelope.Observed
	}
	return ref, envelope.Outcome, true
}

func decodeNotes(b []byte) []Outcome {
	out := []Outcome{}
	for _, line := range strings.Split(string(b), "\n") {
		// Only confirmed server rows count; local pending/lost records are not a receipt.
		if ref, v, ok := repRow(line); ok && strings.HasPrefix(ref, "srv:") {
			out = append(out, v)
		}
	}
	return out
}

// earlierRep finds a row an earlier add left for outcome id. shared is true
// for a srv: row, the shared store's receipt. Otherwise ref is the first
// origin: or local: row: a rep that stayed on this machine. That is no
// receipt, but another add would only make another local copy.
func earlierRep(b []byte, id string) (ref string, shared bool) {
	for _, line := range strings.Split(string(b), "\n") {
		r, v, ok := repRow(line)
		if !ok || v.ID != id {
			continue
		}
		switch {
		case strings.HasPrefix(r, "srv:"):
			return r, true
		case ref == "" && (strings.HasPrefix(r, "origin:") || strings.HasPrefix(r, "local:")):
			ref = r
		}
	}
	return ref, false
}
func (c CLI) Read(ctx context.Context) ([]Outcome, error) {
	b, err := c.run(ctx, []string{"reps", "list", "--limit", "100000"})
	if err != nil {
		return nil, err
	}
	return decodeNotes(b), nil
}
func (c CLI) Publish(ctx context.Context, v Outcome) (string, error) {
	// Checking the stable event ID closes the success-before-local-ack crash
	// window, and stops another add after a reply that stayed local.
	b, err := c.run(ctx, []string{"reps", "list", "--profile", v.Profile, "--limit", "100000"})
	if err != nil {
		return "", err
	}
	if ref, shared := earlierRep(b, v.ID); shared {
		return "confirmed:" + v.ID, nil
	} else if ref != "" {
		if strings.HasPrefix(ref, "origin:") {
			b, err := c.run(ctx, []string{"reps", "pending", "retry", ref})
			if err != nil {
				return "", fmt.Errorf("rep delivered locally only (%s); retry failed: %s; retained in outbox", ref, integrations.Detail(err))
			}
			var receipt struct {
				ID int `json:"id"`
			}
			if err := json.Unmarshal(b, &receipt); err != nil {
				return "", fmt.Errorf("rep delivered locally only (%s); retry failed: invalid JSON response: %s; retained in outbox", ref, err)
			}
			if receipt.ID <= 0 {
				return "", fmt.Errorf("rep delivered locally only (%s); retry failed: response has no positive server id; retained in outbox", ref)
			}
			return fmt.Sprintf("srv:%d", receipt.ID), nil
		}
		return "", fmt.Errorf("rep delivered locally only (%s), not shared; retained in outbox", ref)
	}
	if v.Profile == "" || v.Model == "" || v.Grade == "" || v.Tier == "" {
		return "", errors.New("outcome attribution incomplete; retained in outbox")
	}
	wire := v.revisionContent()
	if v.Effort == "" {
		// An effort-less rep gets an inferred default rung downstream, so it must not count in an experiment arm.
		wire.Experiment, wire.Arm = "", ""
	}
	if !v.ReviewedHead() && v.Blockers() == 0 {
		wire.Findings = nil
	}
	// rounds counts the reviewed heads; high_by_head is their critical+high
	// counts, oldest first, and is left out when that order is not known.
	heads, ordered := v.reviewedHeads()
	fields := map[string]any{
		"source": "mergewake", "schema": 1, "sample_key": v.Task, "outcome_id": v.ID, "revision": v.Revision, "supersedes": v.Supersedes,
		"first_ci_green": v.FirstCIGreen, "findings_by_severity": wire.Findings, "reverted_within_7d": v.Reverted, "time_to_merge_s": v.TimeToMerge, "same_family": v.SameFamily,
		"experiment_id": wire.Experiment, "arm": wire.Arm, "evidence": v.Evidence(), "outcome_rule": "first-ci-green+no-critical-high+no-revert-7d/v1", "outcome": wire, "observed_findings_by_severity": v.Findings, "followup": v.Revision > 1,
	}
	if ordered {
		highByHead := make([]int, 0, len(heads))
		for _, h := range heads {
			highByHead = append(highByHead, v.Reviews[h]["critical"]+v.Reviews[h]["high"])
		}
		fields["high_by_head"] = highByHead
	}
	notes, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	completed := "0"
	if v.Decision == "landed" {
		completed = "1"
	}
	args := []string{"reps", "add", "--profile", v.Profile, "--model", v.Model}
	if v.Effort != "" {
		args = append(args, "--effort", v.Effort)
	}
	args = append(args, "--task", v.Task, "--tier", v.Tier, "--role", "impl", "--grade", v.Grade, "--rounds", strconv.Itoa(len(heads)), "--blockers-found", strconv.Itoa(v.Blockers()), "--completed", completed, "--notes", string(notes))
	b, err = c.run(ctx, args)
	if err != nil {
		return "", err
	}
	reply := strings.TrimSpace(string(b))
	if !strings.HasPrefix(reply, "recorded rep id=srv:") {
		return "", errors.New("rep not confirmed by shared store; retained in outbox")
	}
	return strings.TrimPrefix(reply, "recorded rep id="), nil
}

// publishTimeout bounds one outcome's publication.
const publishTimeout = 45 * time.Second

// lockPublish takes the publish lock: an exclusive flock on a file of its own,
// held by one flush from reading the outbox to its last receipt. Flushes in
// this process, in another daemon and in `mergewake outcomes sync` queue on it,
// so an outcome is never published by two flushes at once. It is not the ledger
// lock: Update and Read never wait for it. A wait ends with ctx.
func (l Ledger) lockPublish(ctx context.Context) (func(), error) {
	if err := os.MkdirAll(l.Dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(l.Dir, "outcomes.publish.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// outbox lists, in key order, the outcomes a flush of repo has to publish: the
// latest revision of each final, attributed row with no receipt. The ledger
// lock is held only for this read.
func (l Ledger) outbox(repo string) ([]Outcome, error) {
	var pending []Outcome
	err := l.locked(func(_ *os.File, s State) error {
		for _, key := range slices.Sorted(maps.Keys(s.Latest)) {
			v := s.Latest[key]
			if repo != "" && v.Repo != repo {
				continue
			}
			if !v.Final || !v.Attributed() || s.Published[v.ID] != "" {
				continue
			}
			pending = append(pending, v.clone())
		}
		return nil
	})
	return pending, err
}

// receipt appends the published event of outcome id unless one is there. The
// ledger lock is held only for this append, never during a publish.
func (l Ledger) receipt(id, rep string) error {
	return l.locked(func(f *os.File, s State) error {
		if s.Published[id] != "" {
			return nil
		}
		return appendEvent(f, Event{Version: 1, ID: id, At: l.now().UTC(), Kind: "published", Rep: rep})
	})
}

// Flush never deletes observations. A failed publish is retried with the exact
// same event payload. A newer terminal snapshot supersedes older local revisions.
// repo limits a flush to one repository's outcomes; an empty repo flushes all,
// so a per-repo loop never publishes (or reports) another repo's records.
// Failed publishes return a *PublishError once every outcome was tried.
//
// The ledger lock is held to read the outbox and to append each receipt, never
// during a publish. One flush at a time holds the publish lock, and it reads the
// outbox after taking it; with CLI.Publish checking the outcome's event ID in
// the shared store first, an outcome is published once, whether two flushes
// start together or a process dies between the publish and its receipt.
// When ctx ends the flush stops and returns its error.
func (l Ledger) Flush(ctx context.Context, reps Reps, repo string) error {
	unlock, err := l.lockPublish(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	pending, err := l.outbox(repo)
	if err != nil {
		return err
	}
	var errs []error
	for _, v := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
		rep, err := reps.Publish(publishCtx, v)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, fmt.Errorf("%s profile=%s effort=%s: %s", v.Task, v.Profile, v.Effort, integrations.Detail(err)))
			continue
		}
		if err = l.receipt(v.ID, rep); err != nil {
			return err
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return &PublishError{Count: len(errs), Err: errors.Join(errs...)}
}

// PublishError is a Flush whose ledger reads and writes succeeded but whose
// publication of Count outcomes failed; they stay in the outbox for the next
// flush. Any other Flush error is a ledger or IO failure.
type PublishError struct {
	Count int
	Err   error
}

func (e *PublishError) Error() string { return e.Err.Error() }
func (e *PublishError) Unwrap() error { return e.Err }

type Aggregate struct {
	AttributionMissing int            `json:"attribution_missing"`
	GradeUnknown       int            `json:"grade_unknown"`
	WithFixes          int            `json:"with_fixes"`
	CleanPass          int            `json:"clean_pass"`
	Failure            int            `json:"failure"`
	Pending            int            `json:"pending"`
	ProfileEffort      string         `json:"profile_effort"`
	Experiment         string         `json:"experiment_id"`
	Arm                string         `json:"arm"`
	Seen               int            `json:"seen"`
	Finalized          int            `json:"finalized"`
	Landed             int            `json:"landed"`
	Blocked            int            `json:"blocked"`
	FirstCIKnown       int            `json:"first_ci_known"`
	FirstCIGreen       int            `json:"first_ci_green"`
	Findings           map[string]int `json:"findings_by_severity"`
	RevertKnown        int            `json:"revert_known"`
	Reverted           int            `json:"reverted_within_7d"`
	MeanTimeToMerge    *float64       `json:"mean_time_to_merge_s"`
	mergeTime          float64
	mergeCount         int
}

func Report(values []Outcome, since time.Time) []Aggregate {
	latest := map[string]Outcome{}
	for _, v := range values {
		old, ok := latest[v.Task]
		if !ok || v.UpdatedAt.After(old.UpdatedAt) || (v.UpdatedAt.Equal(old.UpdatedAt) && v.Revision > old.Revision) {
			latest[v.Task] = v
		}
	}
	groups := map[string]*Aggregate{}
	for _, task := range slices.Sorted(maps.Keys(latest)) {
		v := latest[task]
		if !since.IsZero() && v.OpenedAt.Before(since) {
			continue
		}
		profile, experiment, arm := v.Profile+"@"+v.Effort, v.Experiment, v.Arm
		switch {
		case v.Profile == "":
			profile = "unknown"
		case v.Effort == "":
			profile, experiment, arm = v.Profile+"@inferred", "", ""
		}
		key := profile + "\x00" + experiment + "\x00" + arm
		g := groups[key]
		if g == nil {
			g = &Aggregate{ProfileEffort: profile, Experiment: experiment, Arm: arm, Findings: map[string]int{}}
			groups[key] = g
		}
		switch v.Evidence() {
		case "clean_pass":
			g.CleanPass++
		case "failure":
			g.Failure++
		case "with_fixes":
			g.WithFixes++
		default:
			g.Pending++
		}
		if !v.Attributed() {
			g.AttributionMissing++
		}
		if v.Grade == "" {
			g.GradeUnknown++
		}
		g.Seen++
		if v.Final {
			g.Finalized++
		}
		if v.Decision == "landed" {
			g.Landed++
		}
		if v.Decision == "blocked" {
			g.Blocked++
		}
		if v.FirstCIGreen != nil {
			g.FirstCIKnown++
			if *v.FirstCIGreen {
				g.FirstCIGreen++
			}
		}
		for severity, count := range v.Findings {
			g.Findings[severity] += count
		}
		if v.Reverted != nil {
			g.RevertKnown++
			if *v.Reverted {
				g.Reverted++
			}
		}
		if v.TimeToMerge != nil {
			g.mergeTime += *v.TimeToMerge
			g.mergeCount++
		}
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []Aggregate{}
	for _, k := range keys {
		g := groups[k]
		if g.mergeCount > 0 {
			avg := g.mergeTime / float64(g.mergeCount)
			g.MeanTimeToMerge = &avg
		}
		out = append(out, *g)
	}
	return out
}
