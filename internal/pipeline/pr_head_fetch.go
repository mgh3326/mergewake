package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/mgh3326/mergewake/internal/fleetevent"
	"github.com/mgh3326/mergewake/internal/github"
)

// prHeadErrorBytes bounds each git error a fallback carries into its audit row
// and skip reason.
const prHeadErrorBytes = 512

// fallbackBranchBytes is the longest branch name panewire's remote node
// accepts in a checkout spec.
const fallbackBranchBytes = 200

// fetchPRHead brings pr's head into the checkout at root. It fetches
// pull/<n>/head into the remote-tracking ref panewire builds a remote
// reviewer's detached spec from. GitHub can lose that ref for an open PR while
// its branch is intact; then, for a same-repo PR only, it fetches the branch
// into refs/remotes/origin/<branch>, which panewire also accepts, and goes on
// only when that branch's tip is the exact head: the remote node fetches the
// branch and requires FETCH_HEAD to equal the head, so a branch that moved
// cannot carry the review, and the next pass reviews the new head. A taken
// fallback deletes a local pull/<n>/head left by an earlier fetch, which
// panewire would prefer and the remote node could not fetch. Any failure of
// the first fetch takes the fallback: its stderr is not parsed. Each attempt
// writes one pr-head-fallback row. The returned error is the skip reason; the
// first fetch's stderr is kept as operator detail.
func (s *Service) fetchPRHead(ctx context.Context, root, repo string, pr github.Pull) error {
	_, first := s.gitIn(ctx, root, "fetch", "--no-tags", "origin", prHeadRefspec(pr.Number), pr.Base.Ref)
	if first == nil {
		return nil
	}
	ref := pr.Head.Ref
	firstDetail := gitStderrDetail(first)
	if !strings.EqualFold(pr.Head.Repo.FullName, repo) || !fallbackBranch(ref) {
		return withErrorDetail(fmt.Errorf("fetch PR head: %w", first), firstDetail)
	}
	firstText := boundedGitError(first)
	result, e := s.fetchBranchAtHead(ctx, root, ref, pr, firstText)
	// The pass goes on without the row: a lost audit line is no reason to
	// leave the PR unreviewed.
	row := map[string]any{"repo": repo, "pr": pr.Number, "head": pr.Head.SHA, "ref": ref, "error": firstText, "result": result}
	if firstDetail != "" {
		row["detail"] = firstDetail
	}
	_ = s.audit(repo, "pr-head-fallback", row, false)
	return e
}

// fetchBranchAtHead is the fallback itself. result is "ok", "fetch-failed",
// "head-moved" or "pull-ref-kept" (the stale pull/<n>/head could not be
// deleted).
func (s *Service) fetchBranchAtHead(ctx context.Context, root, ref string, pr github.Pull, firstText string) (result string, err error) {
	tracking := "refs/remotes/origin/" + ref
	if _, e := s.gitIn(ctx, root, "fetch", "--no-tags", "origin", "+refs/heads/"+ref+":"+tracking, pr.Base.Ref); e != nil {
		return "fetch-failed", withErrorDetail(fmt.Errorf("fetch PR head: %s; fetch branch %s: %s", firstText, ref, boundedGitError(e)), gitStderrDetail(e))
	}
	out, e := s.gitIn(ctx, root, "rev-parse", "--verify", tracking)
	if e != nil {
		return "head-moved", withErrorDetail(fmt.Errorf("fetch PR head: %s; read branch %s: %s", firstText, ref, boundedGitError(e)), gitStderrDetail(e))
	}
	if tip := strings.TrimSpace(string(out)); !strings.EqualFold(tip, pr.Head.SHA) {
		return "head-moved", fmt.Errorf("fetch PR head: %s; branch %s is at %s, not head %s", firstText, ref, short7(tip), short7(pr.Head.SHA))
	}
	pull := fmt.Sprintf("refs/remotes/origin/pull/%d/head", pr.Number)
	if _, e := s.gitIn(ctx, root, "update-ref", "-d", pull); e != nil {
		return "pull-ref-kept", withErrorDetail(fmt.Errorf("fetch PR head: %s; delete stale %s: %s", firstText, pull, boundedGitError(e)), gitStderrDetail(e))
	}
	return "ok", nil
}

// boundedGitError is a git failure as an audit row or skip reason carries it:
// redacted, then cut to prHeadErrorBytes.
func boundedGitError(e error) string {
	return clip(fleetevent.Redact(e.Error()), prHeadErrorBytes)
}

// fallbackBranch reports whether ref is a plain branch name that is safe in a
// fetch refspec and that panewire's transport carries: no option-like leading
// "-", no refspec or revision syntax (":", "*", "^", "~", "?", "[", "\\",
// "..", "@{"), no whitespace or control character, no component git refuses
// (leading ".", ".lock" suffix, ""), not under "pull/" (panewire skips
// refs/remotes/origin/pull/* when it looks for a branch) and at most
// fallbackBranchBytes long (the remote node refuses a longer name).
func fallbackBranch(ref string) bool {
	if ref == "" || ref == "@" || len(ref) > fallbackBranchBytes || strings.HasPrefix(ref, "pull/") ||
		strings.HasPrefix(ref, "-") || strings.Contains(ref, "..") || strings.Contains(ref, "@{") || strings.HasSuffix(ref, ".") {
		return false
	}
	for _, c := range ref {
		if c <= ' ' || c == 0x7f || strings.ContainsRune(`:*^~?[\`, c) {
			return false
		}
	}
	for part := range strings.SplitSeq(ref, "/") {
		if part == "" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}
