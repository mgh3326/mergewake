package pipeline

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// reviewDiffBudget bounds the diff text one review pass hands a reviewer. The
// GitHub API path refuses a larger answer, and the local fallback elides file
// content rather than passing an unbounded prompt.
const reviewDiffBudget = 2 << 20

// diffResult is the diff a pass reads and where it came from. source is empty
// for the GitHub API and "local" for the checkout fallback; summarized lists
// every file whose content is not in text (a pure rename, or a block elided to
// fit reviewDiffBudget), always by path so nothing is silently dropped.
type diffResult struct {
	text       string
	source     string
	summarized []string
}

// reviewDiff reads the PR patch. GitHub answers HTTP 406 to the diff media type
// for very large PRs; only then does it build the same merge-base patch from the
// local checkout. Every other error is returned unchanged. The GitHub patch is
// bounded by boundDiff, the same as the local one.
func (s *Service) reviewDiff(ctx context.Context, r config.Repo, pr github.Pull) (diffResult, error) {
	text, e := s.GH.Diff(ctx, r.Name, pr.Number)
	if e == nil {
		text, summarized := boundDiff(text)
		return diffResult{text: text, summarized: summarized}, nil
	}
	if !isHTTPStatus(e, http.StatusNotAcceptable) {
		return diffResult{}, e
	}
	d, localErr := s.localDiff(ctx, r, pr)
	if localErr != nil {
		return diffResult{}, fmt.Errorf("%w; local diff fallback: %v", e, localErr)
	}
	return d, nil
}

func isHTTPStatus(e error, code int) bool {
	var a *github.APIError
	return errors.As(e, &a) && a.Code == code
}

// localDiff builds the PR-only patch from the configured checkout: the merge
// base of the PR base and head to the head. It fetches the same refs the review
// checkout uses, then applies the same reviewer size budget as the API path.
func (s *Service) localDiff(ctx context.Context, r config.Repo, pr github.Pull) (diffResult, error) {
	root := config.Expand(r.Root)
	if root == "" {
		return diffResult{}, errors.New("repo root is not configured")
	}
	if pr.Head.SHA == "" || pr.Base.SHA == "" {
		return diffResult{}, errors.New("PR head or base SHA is missing")
	}
	if s.Runner == nil {
		return diffResult{}, errors.New("no command runner for the local checkout")
	}
	git := func(args ...string) (string, error) {
		b, e := s.gitIn(ctx, root, args...)
		return string(b), e
	}
	field := func(args ...string) (string, error) {
		out, e := git(args...)
		return strings.TrimSpace(out), e
	}
	if _, e := field("rev-parse", "--git-dir"); e != nil {
		return diffResult{}, errors.New("repo root is not a git checkout")
	}
	// The same PR-head fetch the review checkout uses, branch fallback
	// included; an empty base ref asks for the base commit instead.
	fetch := pr
	if fetch.Base.Ref == "" {
		fetch.Base.Ref = pr.Base.SHA
	}
	if _, e := s.fetchPRHead(ctx, root, r.Name, fetch); e != nil {
		return diffResult{}, fmt.Errorf("fetch PR refs: %w", e)
	}
	have := func(sha string) bool {
		_, e := field("cat-file", "-e", sha+"^{commit}")
		return e == nil
	}
	if !have(pr.Base.SHA) {
		// A rewritten base branch can leave the PR's recorded base commit out of
		// the branch fetch; ask for that object directly as a last resort.
		if _, e := field("fetch", "--no-tags", "origin", pr.Base.SHA); e != nil || !have(pr.Base.SHA) {
			return diffResult{}, fmt.Errorf("base commit %s is not in the local checkout", pr.Base.SHA)
		}
	}
	if !have(pr.Head.SHA) {
		return diffResult{}, fmt.Errorf("head commit %s is not in the local checkout", pr.Head.SHA)
	}
	mergeBase, e := field("merge-base", pr.Base.SHA, pr.Head.SHA)
	if e != nil || mergeBase == "" {
		return diffResult{}, errors.New("base and head have no merge base in the local checkout")
	}
	text, e := git("diff", "--no-color", "--no-ext-diff", "--no-textconv", "--find-renames", mergeBase+"..."+pr.Head.SHA)
	if e != nil {
		return diffResult{}, fmt.Errorf("build local diff: %w", e)
	}
	text, summarized := summarizeLocalDiff(text, reviewDiffBudget)
	if strings.TrimSpace(text) == "" {
		return diffResult{}, errors.New("local diff is empty")
	}
	return diffResult{text: text, source: "local", summarized: summarized}, nil
}

// baseChangesDropped checks only commits brought in by first-parent merges on
// the PR branch. A commit counts when it added a line that survived to the
// current merge base but not to the PR head. The work is bounded so an unusual
// history fails closed instead of making the gate unbounded.
func (s *Service) baseChangesDropped(ctx context.Context, r config.Repo, pr github.Pull) ([]string, error) {
	root := config.Expand(r.Root)
	if root == "" || s.Runner == nil || pr.Head.SHA == "" || pr.Base.SHA == "" {
		return nil, errors.New("base-drop check lacks checkout, runner, or PR commits")
	}
	git := func(args ...string) (string, error) {
		b, err := s.gitIn(ctx, root, args...)
		return string(b), err
	}
	field := func(args ...string) (string, error) {
		out, err := git(args...)
		return strings.TrimSpace(out), err
	}
	if _, err := field("rev-parse", "--git-dir"); err != nil {
		return nil, errors.New("repo root is not a git checkout")
	}
	fetch := pr
	if fetch.Base.Ref == "" {
		fetch.Base.Ref = pr.Base.SHA
	}
	if _, err := s.fetchPRHead(ctx, root, r.Name, fetch); err != nil {
		return nil, fmt.Errorf("fetch PR refs: %w", err)
	}
	have := func(sha string) bool {
		_, err := field("cat-file", "-e", sha+"^{commit}")
		return err == nil
	}
	if !have(pr.Base.SHA) {
		if _, err := field("fetch", "--no-tags", "origin", pr.Base.SHA); err != nil || !have(pr.Base.SHA) {
			return nil, fmt.Errorf("base commit %s is not in the local checkout", pr.Base.SHA)
		}
	}
	if !have(pr.Head.SHA) {
		return nil, fmt.Errorf("head commit %s is not in the local checkout", pr.Head.SHA)
	}
	mergeBase, err := field("merge-base", pr.Base.SHA, pr.Head.SHA)
	if err != nil || mergeBase == "" {
		return nil, errors.New("base and head have no merge base")
	}
	mergesText, err := field("rev-list", "--first-parent", "--merges", mergeBase+".."+pr.Head.SHA)
	if err != nil {
		return nil, fmt.Errorf("list PR merges: %w", err)
	}
	merges := nonemptyLines(mergesText)
	if len(merges) > baseDropMergeLimit {
		return nil, fmt.Errorf("PR has more than %d first-parent merges", baseDropMergeLimit)
	}
	affected := map[string]bool{}
	commitsSeen := map[string]bool{}
	lineCount := 0
	for _, merge := range merges {
		parentsText, err := field("rev-list", "--parents", "-n", "1", merge)
		if err != nil {
			return nil, fmt.Errorf("read merge %s: %w", merge, err)
		}
		parents := strings.Fields(parentsText)
		if len(parents) != 3 {
			return nil, fmt.Errorf("merge %s does not have two parents", merge)
		}
		baseAncestor, err := field("merge-base", pr.Base.SHA, parents[2])
		if err != nil {
			return nil, fmt.Errorf("check merge %s second parent against base: %w", merge, err)
		}
		if baseAncestor != parents[2] {
			continue
		}
		forkPoint, err := field("merge-base", parents[1], parents[2])
		if err != nil || forkPoint == "" {
			return nil, fmt.Errorf("find fork point for merge %s", merge)
		}
		introducedText, err := field("rev-list", parents[2], "^"+forkPoint)
		if err != nil {
			return nil, fmt.Errorf("list commits introduced by merge %s: %w", merge, err)
		}
		introduced := nonemptyLines(introducedText)
		if len(introduced) > baseDropCommitLimit-len(commitsSeen) {
			return nil, fmt.Errorf("base history exceeds %d commits", baseDropCommitLimit)
		}
		for _, commit := range introduced {
			if commitsSeen[commit] {
				continue
			}
			commitsSeen[commit] = true
			parentText, err := field("rev-list", "--parents", "-n", "1", commit)
			if err != nil {
				return nil, fmt.Errorf("read base commit %s: %w", commit, err)
			}
			commitParents := strings.Fields(parentText)
			args := []string{"diff-tree", "--no-commit-id", "--unified=0", "--no-ext-diff", "-r"}
			if len(commitParents) == 1 {
				args = append(args, "--root", commit)
			} else {
				args = append(args, commitParents[1], commit)
			}
			diff, err := git(args...)
			if err != nil {
				return nil, fmt.Errorf("read added lines for %s: %w", commit, err)
			}
			for _, line := range addedFileLines(diff) {
				lineCount++
				if lineCount > baseDropLineLimit {
					return nil, fmt.Errorf("base history exceeds %d added lines", baseDropLineLimit)
				}
				atBase, err := fileHasLine(git, mergeBase, line.path, line.text)
				if err != nil {
					return nil, fmt.Errorf("read merge-base file %s: %w", line.path, err)
				}
				if !atBase {
					continue
				}
				atHead, err := fileHasLine(git, pr.Head.SHA, line.path, line.text)
				if err != nil {
					return nil, fmt.Errorf("read PR file %s: %w", line.path, err)
				}
				if !atHead {
					affected[commit] = true
				}
			}
		}
	}
	var dropped []string
	for commit := range affected {
		dropped = append(dropped, commit)
	}
	slices.Sort(dropped)
	return dropped, nil
}

const (
	baseDropMergeLimit  = 64
	baseDropCommitLimit = 1000
	baseDropLineLimit   = 100000
)

type addedBaseLine struct{ path, text string }

func nonemptyLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func addedFileLines(diff string) []addedBaseLine {
	var result []addedBaseLine
	path := ""
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "+++ b/") {
			path = strings.TrimPrefix(line, "+++ b/")
			continue
		}
		if path != "" && strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
			result = append(result, addedBaseLine{path: path, text: strings.TrimPrefix(line, "+")})
		}
	}
	return result
}

func fileHasLine(git func(...string) (string, error), revision, path, want string) (bool, error) {
	text, err := git("show", revision+":"+path)
	if err != nil {
		// A missing path means the line is absent. Check the tree separately so
		// a missing blob or other object error fails closed.
		tree, treeErr := git("ls-tree", "-r", "--name-only", revision)
		if treeErr != nil {
			return false, treeErr
		}
		if !slices.Contains(strings.Split(strings.TrimSpace(tree), "\n"), path) {
			return false, nil
		}
		return false, err
	}
	for _, line := range strings.Split(text, "\n") {
		if line == want {
			return true, nil
		}
	}
	return false, nil
}

// boundDiff applies reviewDiffBudget to a GitHub API patch. A patch within the
// budget is returned byte for byte with no summarized paths; a larger one is
// summarized exactly as the local fallback summarizes its patch, so both paths
// hand the reviewer the same bounded text.
func boundDiff(text string) (string, []string) {
	if len(text) <= reviewDiffBudget {
		return text, nil
	}
	return summarizeLocalDiff(text, reviewDiffBudget)
}

// summarizeLocalDiff applies the size bound the API path applies. Pure renames
// carry no content and are listed by path (git diff -M). When the patch is over
// budget, the largest elidable file blocks are elided first (ties: later file
// first) until the rest fits; each elided block keeps its header and a digest of
// the omitted text. The output keeps the original file order, and every
// summarized path is returned, so the pass body can name it. If the headers
// alone exceed the budget, the result is still over it and the caller refuses it.
func summarizeLocalDiff(diff string, budget int) (string, []string) {
	files := splitDiff(diff)
	if len(files) == 0 {
		return diff, nil
	}
	blocks := make([]string, len(files))
	headers := make([]string, len(files))
	stubLen := make([]int, len(files))
	total := 0
	for i, f := range files {
		blocks[i] = diffBlock(f)
		total += len(blocks[i])
		if elidable(f) {
			headers[i] = elidedHeader(f)
			stubLen[i] = len(headers[i]) + len(elidedNote) + hex.EncodedLen(sha256.Size) + 1
		}
	}
	// Elide the largest blocks first. A block whose stub would not be smaller
	// is never elided, so eliding can only shrink the output.
	var order []int
	for i, f := range files {
		if elidable(f) && stubLen[i] < len(blocks[i]) {
			order = append(order, i)
		}
	}
	slices.SortFunc(order, func(a, b int) int {
		if c := cmp.Compare(len(blocks[b]), len(blocks[a])); c != 0 {
			return c
		}
		return cmp.Compare(b, a)
	})
	elided := make([]bool, len(files))
	for _, i := range order {
		if total <= budget {
			break
		}
		elided[i] = true
		total += stubLen[i] - len(blocks[i])
	}
	var b strings.Builder
	var summarized []string
	for i, f := range files {
		if pureRename(f) {
			summarized = append(summarized, diffFilePath(f))
		}
		if !elided[i] {
			b.WriteString(blocks[i])
			continue
		}
		b.WriteString(headers[i])
		b.WriteString(elidedNote)
		fmt.Fprintf(&b, "%x\n", sha256.Sum256([]byte(blocks[i])))
		if p := diffFilePath(f); p != "" {
			summarized = append(summarized, p)
		}
	}
	return b.String(), summarized
}

const elidedNote = "# mergewake: content summarized to fit the local diff budget; sha256 "

func pureRename(f diffFile) bool {
	if len(f.hunks) != 0 {
		return false
	}
	for _, line := range f.head {
		if strings.HasPrefix(line, "rename to ") || strings.HasPrefix(line, "copy to ") {
			return true
		}
	}
	return false
}

// elidable reports whether a file block carries content that may be replaced
// by a header: hunks, or the base85 body git emits for a binary file.
func elidable(f diffFile) bool {
	if len(f.hunks) != 0 {
		return true
	}
	for _, line := range f.head {
		if strings.HasPrefix(line, "GIT binary patch") {
			return true
		}
	}
	return false
}

// diffBlock reproduces one file block exactly, header lines then hunks.
func diffBlock(f diffFile) string {
	var b strings.Builder
	for _, line := range f.head {
		b.WriteString(line)
	}
	for _, h := range f.hunks {
		for _, line := range h.lines {
			b.WriteString(line)
		}
	}
	return b.String()
}

// elidedHeader keeps the metadata that names a file (paths, rename, mode,
// binary marker) and drops its content.
func elidedHeader(f diffFile) string {
	var b strings.Builder
	for _, line := range f.head {
		t := strings.TrimRight(line, "\r\n")
		keep := false
		for _, prefix := range []string{
			"diff --git ", "index ", "similarity index ", "rename from ", "rename to ",
			"copy from ", "copy to ", "new file mode ", "deleted file mode ",
			"old mode ", "new mode ", "Binary files ",
		} {
			keep = keep || strings.HasPrefix(t, prefix)
		}
		if !keep {
			continue
		}
		b.WriteString(line)
		if !strings.HasSuffix(line, "\n") {
			b.WriteString("\n")
		}
	}
	return b.String()
}

// diffFilePath is the new-side path, or the old one for a deletion, or the
// target of a rename.
func diffFilePath(f diffFile) string {
	var plus, minus string
	for _, line := range f.head {
		t := strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(t, "rename to "):
			return cleanDiffPath(strings.TrimPrefix(t, "rename to "))
		case strings.HasPrefix(t, "copy to "):
			return cleanDiffPath(strings.TrimPrefix(t, "copy to "))
		case strings.HasPrefix(t, "+++ "):
			plus = cleanDiffPath(strings.TrimPrefix(t, "+++ "))
		case strings.HasPrefix(t, "--- "):
			minus = cleanDiffPath(strings.TrimPrefix(t, "--- "))
		}
	}
	if plus != "" && plus != "/dev/null" {
		return plus
	}
	if minus != "" && minus != "/dev/null" {
		return minus
	}

	if len(f.head) > 0 {
		if old, new, ok := parseDiffGitPaths(strings.TrimRight(f.head[0], "\r\n")); ok {
			if p := cleanDiffPath(new); p != "" {
				return p
			}
			return cleanDiffPath(old)
		}
	}
	return ""
}

// parseDiffGitPaths splits a "diff --git <old> <new>" header into its two
// raw paths, honoring git's C-style quoting (core.quotePath): a path with
// control characters, quotes, backslashes or high bytes arrives
// double-quoted with C escapes, while plain spaces stay bare. ok is false
// when the header cannot be split without guessing: a bare line whose
// paths hold spaces is ambiguous, since the separator cannot be told
// apart from a space inside a path.
func parseDiffGitPaths(line string) (old, new string, ok bool) {
	rest, found := strings.CutPrefix(line, "diff --git ")
	if !found || rest == "" {
		return "", "", false
	}
	// A bare path never holds a double quote (git would have quoted it),
	// so the first quote, if any, opens a quoted token.
	if !strings.HasPrefix(rest, `"`) {
		if i := strings.IndexByte(rest, '"'); i >= 0 {
			// Bare old path, quoted new path: the first quote ends the
			// old side, the quoted token runs to the end of the line.
			first, second := rest[:i], rest[i:]
			tok, leftover, ok := cutQuoted(second)
			if !ok || strings.TrimRight(leftover, "\r\n") != "" {
				return "", "", false
			}
			if !strings.HasSuffix(first, " ") {
				return "", "", false
			}
			return strings.TrimSuffix(first, " "), tok, true
		}
		// Both sides are bare: sound only when they agree on one path,
		// i.e. rest rebuilds as a/<p> b/<p>. A path holding " b/" has
		// more than one separator and fails the rebuild instead of
		// guessing.
		sep := strings.Index(rest, " b/")
		if sep < 0 {
			return "", "", false
		}
		first, second := rest[:sep], rest[sep+1:]
		if !strings.HasPrefix(first, "a/") || !strings.HasPrefix(second, "b/") {
			return "", "", false
		}
		p := strings.TrimPrefix(second, "b/")
		if "a/"+p+" b/"+p != rest {
			return "", "", false
		}
		return "a/" + p, "b/" + p, true
	}
	tok, leftover, ok := cutQuoted(rest)
	if !ok || leftover == "" || leftover[0] != ' ' {
		return "", "", false
	}
	second := leftover[1:]
	if strings.HasPrefix(second, `"`) {
		tok2, leftover2, ok := cutQuoted(second)
		if !ok || strings.TrimRight(leftover2, "\r\n") != "" {
			return "", "", false
		}
		return tok, tok2, true
	}
	// Quoted old path, bare new path running to the end of the line.
	return tok, strings.TrimRight(second, "\r\n"), true
}

// cutQuoted splits a leading double-quoted token (git's C-style quoting,
// with \" and \\ escapes) from the rest of the line. ok is false when the
// quotes never close.
func cutQuoted(s string) (tok, rest string, ok bool) {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			return s[:i+1], s[i+1:], true
		}
	}
	return "", "", false
}

func cleanDiffPath(p string) string {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, `"`) {
		if v, e := strconv.Unquote(p); e == nil {
			p = v
		}
	}
	for _, prefix := range []string{"a/", "b/"} {
		if strings.HasPrefix(p, prefix) {
			return strings.TrimPrefix(p, prefix)
		}
	}
	return p
}
