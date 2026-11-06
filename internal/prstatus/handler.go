package prstatus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

// freshTimeout bounds the one GitHub query behind fresh=1.
const freshTimeout = 20 * time.Second

// Handler serves GET /v1/pr-status from the snapshots under StateDir. It
// never calls GitHub unless the request carries fresh=1, and then only through
// Fresh. Only a fresh read writes, and only to that PR's snapshot and marker.
//
// A fresh read that finds the head, checks or unresolved threads changed marks
// the PR for re-evaluation and calls Wake (the pr role's coalescing trigger)
// and Invalidate (the repo's cached evidence); either may be nil.
type Handler struct {
	StateDir string
	Fresh    Fresher
	Wake     func()
	// Enqueue, when set, evaluates the PR whose status read found a change.
	Enqueue    func(repo string, n int)
	Invalidate func(repo string)
	// Now stamps markers and reads that carry no time; nil means time.Now.
	Now func() time.Time
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		reply(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	q := r.URL.Query()
	repo := q.Get("repo")
	if !config.ValidRepo(repo) {
		reply(w, http.StatusBadRequest, map[string]string{"error": "repo must be owner/name"})
		return
	}
	fresh := q.Get("fresh") == "1"
	if v := q.Get("fresh"); v != "" && v != "0" && v != "1" {
		reply(w, http.StatusBadRequest, map[string]string{"error": "fresh must be 0 or 1"})
		return
	}
	if !q.Has("pr") {
		if fresh {
			reply(w, http.StatusBadRequest, map[string]string{"error": "fresh needs pr"})
			return
		}
		h.list(w, repo)
		return
	}
	n, e := strconv.Atoi(q.Get("pr"))
	if e != nil || n <= 0 {
		reply(w, http.StatusBadRequest, map[string]string{"error": "pr must be a positive integer"})
		return
	}
	var f Fresher
	if fresh {
		if h.Fresh == nil {
			reply(w, http.StatusNotImplemented, map[string]string{"error": "fresh reads are not available"})
			return
		}
		f = h.Fresh
	}
	ctx, cancel := context.WithTimeout(r.Context(), freshTimeout)
	defer cancel()
	now := time.Now
	if h.Now != nil {
		now = h.Now
	}
	resp, changed, e := Respond(ctx, h.StateDir, repo, n, f, now().UTC())
	if changed {
		if h.Invalidate != nil {
			h.Invalidate(repo)
		}
		if h.Enqueue != nil {
			h.Enqueue(repo, n)
		}
		if h.Wake != nil {
			h.Wake()
		}
	}
	switch {
	case errors.Is(e, ErrNotFound):
		reply(w, http.StatusNotFound, map[string]string{"error": ErrNotFound.Error()})
	case e != nil:
		reply(w, http.StatusInternalServerError, map[string]string{"error": "status snapshot unreadable"})
	default:
		reply(w, http.StatusOK, resp)
	}
}

// listResponse is the repo's open PRs, trimmed from the oldest PR until the
// body fits MaxResponse.
type listResponse struct {
	Repo      string     `json:"repo"`
	PRs       []Snapshot `json:"prs"`
	Truncated bool       `json:"truncated,omitempty"`
}

func (h Handler) list(w http.ResponseWriter, repo string) {
	snaps, e := ListOpen(h.StateDir, repo)
	if e != nil {
		reply(w, http.StatusInternalServerError, map[string]string{"error": "status snapshots unreadable"})
		return
	}
	out := listResponse{Repo: repo, PRs: snaps}
	for {
		b, _ := json.Marshal(out)
		if len(b) <= MaxResponse || len(out.PRs) == 0 {
			break
		}
		out.PRs, out.Truncated = out.PRs[:len(out.PRs)-1], true
	}
	reply(w, http.StatusOK, out)
}

func reply(w http.ResponseWriter, code int, v any) {
	b, e := json.Marshal(v)
	if e != nil {
		code, b = http.StatusInternalServerError, []byte(`{"error":"encode failed"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write(append(b, '\n'))
}
