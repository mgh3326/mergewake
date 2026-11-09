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

// fetchPRHead brings pr's head into the checkout at root for the reviewer
// checkout. It fetches pull/<n>/head into the remote-tracking ref panewire
// builds a remote reviewer's detached spec from. GitHub can lose that ref for
// an open PR while its branch is intact; then, for a same-repo PR only, it
// fetches the branch into refs/remotes/origin/<branch>, which panewire also
// accepts, and continues only when that branch contains the exact head. Any
// failure of the first fetch takes the fallback: its stderr is not parsed.
// The returned error is the pass's skip reason.
func (s *Service) fetchPRHead(ctx context.Context, root, repo string, pr github.Pull) error {
	_, first := s.gitIn(ctx, root, "fetch", "--no-tags", "origin", prHeadRefspec(pr.Number), pr.Base.Ref)
	if first == nil {
		return nil
	}
	ref := pr.Head.Ref
	if !strings.EqualFold(pr.Head.Repo.FullName, repo) || !fallbackBranch(ref) {
		return fmt.Errorf("fetch PR head: %w", first)
	}
	firstText := clip(fleetevent.Redact(first.Error()), prHeadErrorBytes)
	tracking := "refs/remotes/origin/" + ref
	if _, e := s.gitIn(ctx, root, "fetch", "--no-tags", "origin", "+refs/heads/"+ref+":"+tracking, pr.Base.Ref); e != nil {
		return fmt.Errorf("fetch PR head: %s; fetch branch %s: %s", firstText, ref, clip(e.Error(), prHeadErrorBytes))
	}
	if _, e := s.gitIn(ctx, root, "merge-base", "--is-ancestor", pr.Head.SHA, tracking); e != nil {
		return fmt.Errorf("fetch PR head: %s; branch %s does not contain head %s: %s", firstText, ref, short7(pr.Head.SHA), clip(e.Error(), prHeadErrorBytes))
	}
	// The pass goes on without the row: a lost audit line is no reason to
	// leave the PR unreviewed.
	_ = s.audit(repo, "pr-head-fallback", map[string]any{"repo": repo, "pr": pr.Number, "head": pr.Head.SHA, "ref": ref, "error": firstText}, false)
	return nil
}

// fallbackBranch reports whether ref is a plain branch name that is safe in a
// fetch refspec: no option-like leading "-", no refspec or revision syntax
// (":", "*", "^", "~", "?", "[", "\\", "..", "@{"), no whitespace or control
// character, and no component git refuses (leading ".", ".lock" suffix, "").
func fallbackBranch(ref string) bool {
	if ref == "" || ref == "@" || strings.HasPrefix(ref, "-") || strings.Contains(ref, "..") || strings.Contains(ref, "@{") || strings.HasSuffix(ref, ".") {
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
