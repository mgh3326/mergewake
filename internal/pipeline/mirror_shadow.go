package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// shadowState is the diff shadow counters. It lives behind a pointer so a
// Service value stays copyable (the daemon and tests copy it).
type shadowState struct {
	mu       sync.Mutex
	equal    int
	mismatch int
	reasons  map[string]int
	seen     map[string]bool
}

// shadowInit guards the one-time creation of s.shadow.
var shadowInit sync.Mutex

func (s *Service) shadowState() *shadowState {
	shadowInit.Lock()
	defer shadowInit.Unlock()
	if s.shadow == nil {
		s.shadow = &shadowState{}
	}
	return s.shadow
}

// ShadowStats is what the diff shadow compare has recorded since the process
// started: the mirror patches that equalled the API's, and the reasons the
// others did not.
type ShadowStats struct {
	Equal    int            `json:"equal"`
	Mismatch int            `json:"mismatch"`
	Reasons  map[string]int `json:"reasons"`
}

// ShadowStats returns the diff shadow counters.
func (s *Service) ShadowStats() ShadowStats {
	st := s.shadowState()
	st.mu.Lock()
	defer st.mu.Unlock()
	out := ShadowStats{Equal: st.equal, Mismatch: st.mismatch, Reasons: map[string]int{}}
	for reason, n := range st.reasons {
		out.Reasons[reason] = n
	}
	return out
}

// shadowBudget bounds one shadow compare: the PR read and the mirror diff
// together. It never shortens anything the caller waits for.
const shadowBudget = 10 * time.Second

// ShadowDiff records whether the mirror builds the same patch GitHub returned.
// It is the Diff observer: it runs on its own goroutine after the caller has
// the API diff, and it changes nothing for the review. A mismatch is data for
// the operator, never an error.
func (s *Service) ShadowDiff(ctx context.Context, repo string, n int, apiDiff string) {
	if s.Mirror == nil || strings.TrimSpace(apiDiff) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, shadowBudget)
	defer cancel()
	start := time.Now()
	var key string
	defer func() {
		if r := recover(); r != nil {
			if key != "" {
				s.shadowRelease(key)
			}
			s.recordShadow(repo, n, "", "mismatch", "panic", start)
		}
	}()
	pr, e := s.GH.Pull(ctx, repo, n)
	if e != nil || pr.Base.SHA == "" || pr.Head.SHA == "" {
		s.recordShadow(repo, n, "", "mismatch", "pull", start)
		return
	}
	key = fmt.Sprintf("%s#%d@%s", repo, n, pr.Head.SHA)
	if !s.shadowClaim(key) {
		return
	}
	local, reason, e := s.Mirror.Diff(ctx, repo, pr.Base.SHA, pr.Head.SHA)
	if e != nil || local == "" {
		// No patch was built, so a later read may try again: a fetch this call
		// asked for can make the next attempt succeed.
		s.shadowRelease(key)
		if e != nil {
			reason = "error"
		}
		s.recordShadow(repo, n, pr.Head.SHA, "mismatch", reason, start)
		return
	}
	result, reason := compareDiff(apiDiff, local)
	s.recordShadow(repo, n, pr.Head.SHA, result, reason, start)
}

// compareDiff compares the API patch with the local one by the patch hashes
// mergewake uses for patch ids: the stable patch id, then the exact changed
// text. A stable id that differs is classified; a stable id that matches while
// the exact text differs is a "text" mismatch (whitespace or a header line).
func compareDiff(api, local string) (result, reason string) {
	apiLoose, apiStrict := patchHashes(api)
	localLoose, localStrict := patchHashes(local)
	switch {
	case apiLoose == "" || localLoose == "":
		return "mismatch", "empty"
	case apiLoose != localLoose:
		return "mismatch", mismatchReason(api, local)
	case apiStrict != localStrict:
		return "mismatch", "text"
	default:
		return "equal", ""
	}
}

// mismatchReason names the likely difference between two patches whose stable
// ids disagree.
func mismatchReason(api, local string) string {
	switch {
	case hasDiffLine(api, "rename from ") || hasDiffLine(local, "rename from "),
		hasDiffLine(api, "copy from ") || hasDiffLine(local, "copy from "):
		return "rename"
	case hasDiffLine(api, "GIT binary patch") || hasDiffLine(local, "GIT binary patch"),
		hasDiffLine(api, "Binary files ") || hasDiffLine(local, "Binary files "):
		return "binary"
	case hunkShape(api) != hunkShape(local):
		return "hunk shape"
	default:
		return "content"
	}
}

func hasDiffLine(diff, prefix string) bool {
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

// hunkShape is the file and hunk headers of a patch, without content: the part
// a difference in hunk structure shows up in.
func hunkShape(diff string) string {
	var b strings.Builder
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "diff --git ") || strings.HasPrefix(line, "@@ ") {
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	return b.String()
}

// shadowClaim marks a head as being compared and reports whether the caller
// owns that comparison. A head is compared once; the map is dropped when it
// grows past its bound, which can at worst repeat a comparison.
func (s *Service) shadowClaim(key string) bool {
	st := s.shadowState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.seen == nil || len(st.seen) > 1024 {
		st.seen = map[string]bool{}
	}
	if st.seen[key] {
		return false
	}
	st.seen[key] = true
	return true
}

// shadowRelease lets a later read compare a head this call could not build.
func (s *Service) shadowRelease(key string) {
	st := s.shadowState()
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.seen, key)
}

// recordShadow counts one result and logs it. The log line is its own, never
// the github requests line.
func (s *Service) recordShadow(repo string, n int, head, result, reason string, start time.Time) {
	st := s.shadowState()
	st.mu.Lock()
	if result == "equal" {
		st.equal++
	} else {
		st.mismatch++
		if st.reasons == nil {
			st.reasons = map[string]int{}
		}
		st.reasons[reason]++
	}
	equal, mismatch := st.equal, st.mismatch
	st.mu.Unlock()
	if s.Log == nil {
		return
	}
	fields := []any{"repo", repo, "pr", n, "head", head, "result", result, "compare_ms", time.Since(start).Milliseconds(), "shadow_equal", equal, "shadow_mismatch", mismatch}
	if reason != "" {
		fields = append(fields, "reason", reason)
	}
	if s.Mirror != nil {
		stats := s.Mirror.Stats()
		fields = append(fields, "mirror_fetches", stats.Fetches, "mirror_failures", stats.Failures, "mirror_fetch_ms", stats.FetchMS)
	}
	s.Log.Info("mirror shadow", fields...)
}
