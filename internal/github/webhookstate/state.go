// Package webhookstate keeps what verified GitHub deliveries carry: per
// repository, the open PRs, the latest state of every check on a head commit and
// the reviews of a PR. The github client reads it before the API and compares
// the two (see docs/webhook-state.md).
//
// Every value has an ordering key taken from the event itself. A delivery that
// is older than, or equal to, what is stored never overwrites it, and a delivery
// id that was already applied is a no-op. The state is bounded, written beside
// the other daemon state and pruned when a PR closes.
package webhookstate

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// MaxAge is how long a stored value stays usable as an answer after it was last
// confirmed by an API read (or, for a PR, first seen). A lost delivery can hide
// a change for at most this long, which is what the evidence cache allows today.
const MaxAge = 10 * time.Minute

const (
	maxPRs         = 2000
	maxHeads       = 1024
	maxRunsPerHead = 2000
	maxReviews     = 500
	maxDeliveries  = 1024
	maxGone        = 2048
	// BodyLimit bounds a PR body; GitHub itself caps one at 65,536 characters.
	BodyLimit = 256 << 10
	// plainReviewBody bounds a review body that carries no mergewake marker; the
	// gate reads only marked bodies, whole.
	plainReviewBody = 1024
	// reviewBodyLimit bounds a marked review body: GitHub caps one at 65,536 characters.
	reviewBodyLimit = 4 * 65536
)

// PR is the part of a pull request the gate reads. Mergeability is deliberately
// absent: it is never taken from a delivery.
type PR struct {
	Number         int       `json:"number"`
	State          string    `json:"state"`
	Draft          bool      `json:"draft"`
	Merged         bool      `json:"merged"`
	Title          string    `json:"title,omitempty"`
	Body           string    `json:"body,omitempty"`
	HTMLURL        string    `json:"html_url,omitempty"`
	UserLogin      string    `json:"user_login,omitempty"`
	UserType       string    `json:"user_type,omitempty"`
	HeadSHA        string    `json:"head_sha"`
	HeadRef        string    `json:"head_ref"`
	HeadRepo       string    `json:"head_repo,omitempty"`
	BaseSHA        string    `json:"base_sha"`
	BaseRef        string    `json:"base_ref"`
	BaseRepo       string    `json:"base_repo,omitempty"`
	Labels         []string  `json:"labels,omitempty"`
	MergeCommitSHA string    `json:"merge_commit_sha,omitempty"`
	ChangedFiles   int       `json:"changed_files"`
	Additions      int       `json:"additions"`
	Deletions      int       `json:"deletions"`
	CreatedAt      string    `json:"created_at,omitempty"`
	ClosedAt       *string   `json:"closed_at,omitempty"`
	MergedAt       *string   `json:"merged_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
	// FirstSeen and SyncedAt are when a delivery first created the entry and when
	// an API read last confirmed it.
	FirstSeen time.Time `json:"first_seen"`
	SyncedAt  time.Time `json:"synced_at,omitempty"`
}

// Run is one check run, the latest of its (suite, name).
type Run struct {
	ID          int64     `json:"id"`
	SuiteID     int64     `json:"suite_id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion,omitempty"`
	DetailsURL  string    `json:"details_url,omitempty"`
	App         string    `json:"app,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

// Status is the latest commit status of one context.
type Status struct {
	ID        int64     `json:"id"`
	Context   string    `json:"context"`
	State     string    `json:"state"`
	TargetURL string    `json:"target_url,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Suite is a check suite's own state. The gate reads runs and statuses; the
// suite is kept for the change record that follows.
type Suite struct {
	ID         int64     `json:"id"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion,omitempty"`
	App        string    `json:"app,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
}

// Head is every check fact stored for one commit. Runs are keyed by suite and
// name; Synced is when an API read last supplied the whole list, so a head that
// deliveries alone created is never taken for complete.
type Head struct {
	Runs     map[string]Run    `json:"runs,omitempty"`
	Statuses map[string]Status `json:"statuses,omitempty"`
	Suites   map[int64]Suite   `json:"suites,omitempty"`
	Synced   time.Time         `json:"synced,omitempty"`
	Touched  time.Time         `json:"touched"`
}

// Review is one submitted review. State is upper case, as the API spells it.
type Review struct {
	ID          int64     `json:"id"`
	Login       string    `json:"login"`
	State       string    `json:"state"`
	CommitID    string    `json:"commit_id"`
	Body        string    `json:"body"`
	SubmittedAt time.Time `json:"submitted_at,omitempty"`
}

// reviewSet is a PR's reviews; Synced is when an API read last supplied the list.
type reviewSet struct {
	Reviews map[int64]Review `json:"reviews"`
	Synced  time.Time        `json:"synced,omitempty"`
}

type repoState struct {
	PRs     map[int]*PR        `json:"prs"`
	Heads   map[string]*Head   `json:"heads"`
	Reviews map[int]*reviewSet `json:"reviews"`
	// Gone holds the updated_at of each closed PR: a delivery no newer than that
	// is a straggler and must not bring the PR back.
	Gone       map[int]time.Time `json:"gone"`
	Deliveries []string          `json:"deliveries"`
	// Distrusted is when this process last wrote to the repository: nothing
	// confirmed before it can answer a read.
	Distrusted time.Time `json:"-"`
	dirty      bool
}

// Counters are the shadow-comparison tallies.
type Counters struct {
	Hits       int64 `json:"webhook_state_hits"`
	Misses     int64 `json:"webhook_state_misses"`
	Mismatches int64 `json:"webhook_state_mismatches"`
}

// Store is the per-repository state under one directory.
type Store struct {
	Dir string
	// Log receives the mismatch lines; nil discards them.
	Log *slog.Logger
	// Now stamps entries; nil means time.Now.
	Now func() time.Time

	mu                       sync.Mutex
	repos                    map[string]*repoState
	hits, misses, mismatches atomic.Int64
}

// Open returns a store rooted at dir; files are read when a repository is first used.
func Open(dir string) *Store { return &Store{Dir: dir, repos: map[string]*repoState{}} }

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Store) path(repo string) (string, error) {
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || owner == "" || name == "" || strings.ContainsAny(repo[len(owner)+1:], "/\\") || strings.Contains(repo, "..") {
		return "", fmt.Errorf("webhook state: bad repository %q", repo)
	}
	return filepath.Join(s.Dir, "webhook-state", owner, name+".json"), nil
}

// repo returns repo's state, loading it on first use. A missing or unreadable
// file starts empty: the state is a cache of what the API can say again.
// s.mu is held.
func (s *Store) repo(repo string) (*repoState, error) {
	if r, ok := s.repos[repo]; ok {
		return r, nil
	}
	p, e := s.path(repo)
	if e != nil {
		return nil, e
	}
	r := &repoState{}
	if _, e := fsutil.ReadJSON(p, r); e != nil {
		r = &repoState{}
		s.warn("webhook_state load failed; starting empty", "repo", repo, "error", e.Error())
	}
	if r.PRs == nil {
		r.PRs = map[int]*PR{}
	}
	if r.Heads == nil {
		r.Heads = map[string]*Head{}
	}
	if r.Reviews == nil {
		r.Reviews = map[int]*reviewSet{}
	}
	if r.Gone == nil {
		r.Gone = map[int]time.Time{}
	}
	s.repos[repo] = r
	return r, nil
}

func (s *Store) warn(msg string, args ...any) {
	if s.Log != nil {
		s.Log.Warn(msg, args...)
	}
}

// Flush writes every changed repository. A caller that wants a crash to lose
// little calls it on a short timer and on shutdown.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for name, r := range s.repos {
		if !r.dirty {
			continue
		}
		p, e := s.path(name)
		if e == nil {
			e = fsutil.WriteJSON(p, r)
		}
		if e != nil {
			errs = append(errs, e)
			continue
		}
		r.dirty = false
	}
	return errors.Join(errs...)
}

// Counters returns the shadow tallies.
func (s *Store) Counters() Counters {
	return Counters{s.hits.Load(), s.misses.Load(), s.mismatches.Load()}
}

// Hit, Miss and Mismatch count one shadow read that found stored state, found
// none, and differed from the API in one field.
func (s *Store) Hit()      { s.hits.Add(1) }
func (s *Store) Miss()     { s.misses.Add(1) }
func (s *Store) Mismatch() { s.mismatches.Add(1) }

// Distrust makes every value confirmed so far unusable as an answer: this
// process just wrote to repo, and the delivery that reports the write may not
// have arrived.
func (s *Store) Distrust(repo string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, e := s.repo(repo); e == nil {
		r.Distrusted = s.now()
	}
}

func (r *repoState) usable(basis, now time.Time) bool {
	return !basis.IsZero() && basis.After(r.Distrusted) && now.Sub(basis) < MaxAge
}

// PR returns the stored PR.
func (s *Store) PR(repo string, n int) (PR, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil || r.PRs[n] == nil {
		return PR{}, false
	}
	return clonePR(*r.PRs[n]), true
}

// UsablePR is PR for a caller that will answer a read with it: the entry must
// be recent enough and untouched by a write of this process.
func (s *Store) UsablePR(repo string, n int) (PR, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil || r.PRs[n] == nil {
		return PR{}, false
	}
	p := r.PRs[n]
	if !r.usable(cmp.Or(p.SyncedAt, p.FirstSeen), s.now()) {
		return PR{}, false
	}
	return clonePR(*p), true
}

// PRForHead names the stored PR whose head is sha, or 0.
func (s *Store) PRForHead(repo, sha string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil {
		return 0
	}
	for n, p := range r.PRs {
		if p.HeadSHA == sha {
			return n
		}
	}
	return 0
}

// PRsForHead returns every open PR currently associated with sha.
func (s *Store) PRsForHead(repo, sha string) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil {
		return nil
	}
	var out []int
	for n, p := range r.PRs {
		if p.HeadSHA == sha {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

// Head returns a copy of the checks stored for sha; usable is whether they are
// complete and recent enough to answer a read.
func (s *Store) Head(repo, sha string) (h Head, ok, usable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil || r.Heads[sha] == nil {
		return Head{}, false, false
	}
	return cloneHead(*r.Heads[sha]), true, r.usable(r.Heads[sha].Synced, s.now())
}

// Reviews returns the stored reviews of PR n, oldest first.
func (s *Store) Reviews(repo string, n int) (out []Review, ok, usable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil || r.Reviews[n] == nil {
		return nil, false, false
	}
	set := r.Reviews[n]
	for _, v := range set.Reviews {
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b Review) int {
		return cmp.Or(a.SubmittedAt.Compare(b.SubmittedAt), cmp.Compare(a.ID, b.ID))
	})
	return out, true, r.usable(set.Synced, s.now())
}

func clonePR(p PR) PR {
	p.Labels = slices.Clone(p.Labels)
	return p
}

func cloneHead(h Head) Head {
	h.Runs = cloneMap(h.Runs)
	h.Statuses = cloneMap(h.Statuses)
	h.Suites = cloneMap(h.Suites)
	return h
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// BoundBody is the body as stored: whole when it is a PR body within the
// limit or a review body carrying a mergewake marker (the pass marker must
// survive), cut otherwise. Both sides of a comparison use it.
func BoundBody(body string, review bool) string {
	limit := BodyLimit
	if review {
		limit = plainReviewBody
		if strings.Contains(body, "<!-- mergewake-") {
			limit = reviewBodyLimit
		}
	}
	if len(body) <= limit {
		return body
	}
	cut := limit
	for cut > 0 && body[cut]&0xC0 == 0x80 {
		cut--
	}
	return body[:cut]
}

// Diff is one field on which the stored and the API value differ.
type Diff struct{ Field, Stored, API string }

// DiffPR lists the fields of the stored PR that differ from the API's.
func DiffPR(stored, api PR) []Diff {
	var out []Diff
	add := func(field string, s, a any) {
		if fmt.Sprint(s) != fmt.Sprint(a) {
			out = append(out, Diff{field, fmt.Sprint(s), fmt.Sprint(a)})
		}
	}
	add("state", stored.State, api.State)
	add("draft", stored.Draft, api.Draft)
	add("merged", stored.Merged, api.Merged)
	add("head_sha", stored.HeadSHA, api.HeadSHA)
	add("head_ref", stored.HeadRef, api.HeadRef)
	add("base_sha", stored.BaseSHA, api.BaseSHA)
	add("base_ref", stored.BaseRef, api.BaseRef)
	sl, al := slices.Sorted(slices.Values(stored.Labels)), slices.Sorted(slices.Values(api.Labels))
	add("labels", strings.Join(sl, ","), strings.Join(al, ","))
	add("body", BoundBody(stored.Body, false), BoundBody(api.Body, false))
	add("updated_at", stored.UpdatedAt.UTC().Format(time.RFC3339), api.UpdatedAt.UTC().Format(time.RFC3339))
	return out
}

// DiffReviews lists, per review id, what differs between the stored list and
// the API's.
func DiffReviews(stored, api []Review) []Diff {
	byID := map[int64]Review{}
	for _, r := range stored {
		byID[r.ID] = r
	}
	var out []Diff
	seen := map[int64]bool{}
	for _, a := range api {
		seen[a.ID] = true
		s, ok := byID[a.ID]
		id := fmt.Sprint("review ", a.ID)
		if !ok {
			out = append(out, Diff{id + " missing", "absent", "present"})
			continue
		}
		if s.State != a.State {
			out = append(out, Diff{id + " state", s.State, a.State})
		}
		if s.CommitID != a.CommitID {
			out = append(out, Diff{id + " commit_id", s.CommitID, a.CommitID})
		}
		if s.Login != a.Login {
			out = append(out, Diff{id + " login", s.Login, a.Login})
		}
		if BoundBody(s.Body, true) != BoundBody(a.Body, true) {
			out = append(out, Diff{id + " body", fmt.Sprintf("%d bytes", len(s.Body)), fmt.Sprintf("%d bytes", len(a.Body))})
		}
	}
	for _, s := range stored {
		if !seen[s.ID] {
			out = append(out, Diff{fmt.Sprint("review ", s.ID, " missing"), "present", "absent"})
		}
	}
	return out
}

// LogDiff writes one webhook_state mismatch line per field and counts it.
func (s *Store) LogDiff(repo string, pr int, head string, diffs []Diff) {
	for _, d := range diffs {
		s.Mismatch()
		if s.Log != nil {
			s.Log.Warn("webhook_state", "result", "mismatch", "repo", repo, "pr", pr, "head", head, "field", d.Field, "stored", clip(d.Stored), "api", clip(d.API))
		}
	}
}

func clip(v string) string {
	if len(v) > 120 {
		return v[:120] + "…"
	}
	return v
}
