package pipeline

import (
	"context"
	"strings"
	"sync"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// A base merge is a head that merges the base into an already reviewed head: a
// merge commit whose first parent is the reviewed head and whose second parent
// is on the PR's base branch. Such a head adds no review round, because the
// base brings in text the author did not write. When the merge is clean, the
// first parent's review covers the head as well (see cleanBaseMergePasses).
//
// Every check reads the first parent's review from the PR's history and the
// head's commit, so a head that is not a merge commit costs one commit lookup,
// which the GitHub client caches by SHA, and nothing else.

// baseMergeMu guards Service.ancestorOnBase and Service.cleanMerged. Only true
// answers are cached: both facts stay true for the life of the SHAs, while a
// false answer may be transient (an unreadable object, a moved base).
var baseMergeMu sync.Mutex

// baseMerge names the two parents of a base-merge head, and Reviewed, the reviewed
// head whose review the merge inherits: its first parent, or the reviewed head
// behind an inherited first parent (see reviewedAncestor).
type baseMerge struct{ First, Second, Reviewed string }

// maxBaseMergeChain bounds how many inherited base merges sit between a head and
// the reviewed head it inherits from.
const maxBaseMergeChain = 8

// baseMergeShape names the parents of head when head is a merge commit whose
// second parent is an ancestor of the PR's base. It does not look at the first
// parent's review. A head whose commit cannot be read is not a base merge.
func (s *Service) baseMergeShape(ctx context.Context, repo string, pr github.Pull, head string) (baseMerge, bool) {
	c, e := s.GH.Commit(ctx, repo, head)
	if e != nil || len(c.Parents) != 2 {
		return baseMerge{}, false
	}
	m := baseMerge{First: c.Parents[0].SHA, Second: c.Parents[1].SHA}
	if m.First == "" || m.Second == "" || m.First == m.Second {
		return baseMerge{}, false
	}
	onBase, e := s.onBase(ctx, repo, m.Second, pr.Base.SHA)
	if e != nil || !onBase {
		return baseMerge{}, false
	}
	return m, true
}

// reviewedAncestor finds the reviewed head that head's review comes from: head
// itself when history has passes for it. Otherwise head must be a base merge
// whose first parent is an inherited head, so the walk continues from that
// parent, at most depth merges deep. Each inherited merge on the way must be a
// clean merge, so its tree is its first parent's tree plus base text only and no
// unreviewed text hides behind the chain.
func (s *Service) reviewedAncestor(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, head string, needed, depth int) (string, bool) {
	if _, complete := distinctPasses(passesFor(history, head), needed); complete {
		return head, true
	}
	if depth <= 0 {
		return "", false
	}
	m, ok := s.baseMergeShape(ctx, r.Name, pr, head)
	if !ok || !s.cleanMerge(ctx, r, head, m.First, m.Second) {
		return "", false
	}
	return s.reviewedAncestor(ctx, r, pr, history, m.First, needed, depth-1)
}

// baseMergeOf reports whether head is a base merge of a reviewed head: a merge
// commit whose second parent is an ancestor of the PR's base and whose first
// parent has a review in history, directly or through inherited base merges (see
// reviewedAncestor). A head whose commit cannot be read is not a base merge.
func (s *Service) baseMergeOf(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, head string, needed int) (baseMerge, bool) {
	m, ok := s.baseMergeShape(ctx, r.Name, pr, head)
	if !ok {
		return baseMerge{}, false
	}
	reviewed, ok := s.reviewedAncestor(ctx, r, pr, history, m.First, needed, maxBaseMergeChain)
	if !ok {
		return baseMerge{}, false
	}
	m.Reviewed = reviewed
	return m, true
}

// baseMerges maps each reviewed head in history that is a base merge to its
// parents. A base merge needs its first parent reviewed too, so a history with
// fewer than two reviewed heads has none and reads no commit at all.
func (s *Service) baseMerges(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass) map[string]baseMerge {
	out := map[string]baseMerge{}
	heads := reviewedHeads(history)
	if len(heads) < 2 {
		return out
	}
	needed, settled, err := s.reviewPassesNeeded(ctx, r, pr)
	if err != nil || !settled {
		return out
	}
	for _, head := range heads {
		if m, ok := s.baseMergeOf(ctx, r, pr, history, head, needed); ok {
			out[head] = m
		}
	}
	return out
}

// reviewedHeads lists the distinct heads in history, in history order.
func reviewedHeads(history []github.ReviewedPass) []string {
	var heads []string
	seen := map[string]bool{}
	for _, p := range history {
		if !seen[p.Head] {
			seen[p.Head] = true
			heads = append(heads, p.Head)
		}
	}
	return heads
}

// countedHistory is history without the passes of the base-merge heads whose
// first parent has a complete review, so reviewRounds counts no round for them.
// Their passes still exist and still gate: only the round count skips them. The
// review is complete for the policy's needed passes, which is read only when a
// base merge exists, and an unsettled or unreadable policy counts every head as
// before.
func (s *Service) countedHistory(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass) []github.ReviewedPass {
	merges := s.baseMerges(ctx, r, pr, history)
	if len(merges) == 0 {
		return history
	}
	needed, settled, e := s.reviewPassesNeeded(ctx, r, pr)
	if e != nil || !settled {
		return history
	}
	skip := map[string]bool{}
	for head, m := range merges {
		if _, complete := distinctPasses(passesFor(history, m.Reviewed), needed); complete {
			skip[head] = true
		}
	}
	if len(skip) == 0 {
		return history
	}
	out := make([]github.ReviewedPass, 0, len(history))
	for _, p := range history {
		if !skip[p.Head] {
			out = append(out, p)
		}
	}
	return out
}

// cleanBaseMergePasses inherits the review of the head's first parent when the
// head is a base merge with a complete review and a clean merge: its tree is the
// one git makes of its two parents with no conflict. It returns the inherited
// head and its receipts, which the caller records as an inherited review. A head
// whose cleanness cannot be read is not inherited and is reviewed as before.
func (s *Service) cleanBaseMergePasses(ctx context.Context, r config.Repo, pr github.Pull, history []github.ReviewedPass, needed int) (string, []github.PassReceipt, bool) {
	if len(history) == 0 {
		// Nothing is reviewed yet, so no first parent can carry a review.
		return "", nil, false
	}
	m, ok := s.baseMergeOf(ctx, r, pr, history, pr.Head.SHA, needed)
	if !ok {
		return "", nil, false
	}
	receipts := passesFor(history, m.Reviewed)
	if _, complete := distinctPasses(receipts, needed); !complete {
		return "", nil, false
	}
	if !s.cleanMerge(ctx, r, pr.Head.SHA, m.First, m.Second) {
		return "", nil, false
	}
	return m.Reviewed, receipts, true
}

// onBase reports whether sha is an ancestor of base, the PR's base commit. A
// true answer is cached per pair.
func (s *Service) onBase(ctx context.Context, repo, sha, base string) (bool, error) {
	if sha == "" || base == "" {
		return false, nil
	}
	key := repo + "@" + sha + ".." + base
	baseMergeMu.Lock()
	cached := s.ancestorOnBase[key]
	baseMergeMu.Unlock()
	if cached {
		return true, nil
	}
	ok, e := s.GH.Ancestor(ctx, repo, sha, base)
	if e != nil || !ok {
		return false, e
	}
	baseMergeMu.Lock()
	defer baseMergeMu.Unlock()
	if s.ancestorOnBase == nil || len(s.ancestorOnBase) > 512 {
		s.ancestorOnBase = map[string]bool{}
	}
	s.ancestorOnBase[key] = true
	return true, nil
}

// cleanMerge reports whether head's tree equals the tree git merges first and
// second into without a conflict. It reads the repo root's objects, fetching a
// commit that is missing, and it is false whenever any of that cannot be read.
// Only a clean answer is cached: git merge-tree writes the merged tree and exits
// non-zero on a conflict.
func (s *Service) cleanMerge(ctx context.Context, r config.Repo, head, first, second string) bool {
	root := config.Expand(r.Root)
	if root == "" || s.Runner == nil {
		return false
	}
	key := root + " " + head + " " + first + " " + second
	baseMergeMu.Lock()
	cached := s.cleanMerged[key]
	baseMergeMu.Unlock()
	if cached {
		return true
	}
	git := func(args ...string) (string, error) {
		b, e := s.gitIn(ctx, root, args...)
		return strings.TrimSpace(string(b)), e
	}
	for _, sha := range []string{head, first, second} {
		if _, e := git("cat-file", "-e", sha+"^{commit}"); e != nil {
			if _, e := git("fetch", "--no-tags", "origin", sha); e != nil {
				return false
			}
			if _, e := git("cat-file", "-e", sha+"^{commit}"); e != nil {
				return false
			}
		}
	}
	want, e := git("rev-parse", head+"^{tree}")
	if e != nil {
		return false
	}
	got, e := git("merge-tree", "--write-tree", first, second)
	if e != nil || strings.SplitN(got, "\n", 2)[0] != want {
		return false
	}
	baseMergeMu.Lock()
	defer baseMergeMu.Unlock()
	if s.cleanMerged == nil || len(s.cleanMerged) > 512 {
		s.cleanMerged = map[string]bool{}
	}
	s.cleanMerged[key] = true
	return true
}
