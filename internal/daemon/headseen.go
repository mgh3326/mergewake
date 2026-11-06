package daemon

import (
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
)

// HeadRecord is what mergewake knows about when it met one PR head.
type HeadRecord struct {
	PR int `json:"pr"`
	// Seen is the earlier of the first delivery that named the head and the
	// start of its first evaluation; SeenBy says which: "webhook" or "eval".
	Seen   time.Time `json:"seen"`
	SeenBy string    `json:"seen_by"`
	// FirstEval is the start of the first evaluation that read the head.
	FirstEval time.Time `json:"first_eval"`
	// Evals counts the evaluations that read the head since the daemon started.
	Evals int `json:"evals,omitempty"`
}

const (
	headSeenKeep = 48 * time.Hour
	headSeenMax  = 256
)

// HeadSeen remembers, per repo and head commit, when mergewake first saw the
// head, so the time from a push to its reviewer is measured from the push and a
// restart does not reset it. It keeps one file per repo, `head-seen.json` in
// the state directory, written only when a head is new or first evaluated.
// A nil *HeadSeen records nothing.
type HeadSeen struct {
	// Dir is the state directory; the file of a repo is Dir/<repo>/head-seen.json.
	Dir string
	Log *slog.Logger

	mu    sync.Mutex
	repos map[string]map[string]*HeadRecord
}

// OpenHeadSeen returns a store over the state directory dir.
func OpenHeadSeen(dir string) *HeadSeen { return &HeadSeen{Dir: dir} }

func (h *HeadSeen) file(repo string) string { return filepath.Join(h.Dir, repo, "head-seen.json") }

// heads returns repo's records, loading them on first use. Called with mu held.
func (h *HeadSeen) heads(repo string) map[string]*HeadRecord {
	if m, ok := h.repos[repo]; ok {
		return m
	}
	if h.repos == nil {
		h.repos = map[string]map[string]*HeadRecord{}
	}
	m := map[string]*HeadRecord{}
	if _, err := fsutil.ReadJSON(h.file(repo), &m); err != nil {
		// An unreadable file only costs the history: the heads are met anew.
		h.warn("head-seen state unreadable", repo, err)
		m = map[string]*HeadRecord{}
	}
	h.repos[repo] = m
	return m
}

func (h *HeadSeen) warn(msg, repo string, err error) {
	if h.Log != nil {
		h.Log.Warn(msg, "repo", repo, "error", err.Error())
	}
}

// save writes repo's records, after dropping the old ones. Called with mu held.
func (h *HeadSeen) save(repo string, at time.Time) {
	m := h.heads(repo)
	for sha, rec := range m {
		if at.Sub(rec.Seen) > headSeenKeep {
			delete(m, sha)
		}
	}
	if len(m) > headSeenMax {
		shas := make([]string, 0, len(m))
		for sha := range m {
			shas = append(shas, sha)
		}
		slices.SortFunc(shas, func(a, b string) int { return m[b].Seen.Compare(m[a].Seen) })
		for _, sha := range shas[headSeenMax:] {
			delete(m, sha)
		}
	}
	if err := fsutil.WriteJSON(h.file(repo), m); err != nil {
		h.warn("head-seen state not saved", repo, err)
	}
}

// Delivered notes that a delivery at time at named head sha of PR pr.
func (h *HeadSeen) Delivered(repo string, pr int, sha string, at time.Time) {
	if h == nil || sha == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.heads(repo)
	if rec, ok := m[sha]; ok {
		// A delivery that arrives after the first evaluation changes nothing; one
		// that was queued before it (the clock of a late handler) wins.
		if !at.Before(rec.Seen) {
			return
		}
		rec.Seen, rec.SeenBy = at, "webhook"
	} else {
		m[sha] = &HeadRecord{PR: pr, Seen: at, SeenBy: "webhook"}
	}
	h.save(repo, at)
}

// Evaluating notes that an evaluation of PR pr started at time at and read head
// sha. It returns the record as it stands, this evaluation counted.
func (h *HeadSeen) Evaluating(repo string, pr int, sha string, at time.Time) HeadRecord {
	if h == nil || sha == "" {
		return HeadRecord{PR: pr, Seen: at, SeenBy: "eval", FirstEval: at, Evals: 1}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.heads(repo)
	rec, ok := m[sha]
	if !ok {
		rec = &HeadRecord{PR: pr, Seen: at, SeenBy: "eval"}
		m[sha] = rec
	}
	rec.PR = pr
	rec.Evals++
	if rec.FirstEval.IsZero() {
		rec.FirstEval = at
		h.save(repo, at)
	} else if !ok {
		h.save(repo, at)
	}
	return *rec
}
