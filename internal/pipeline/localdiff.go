package pipeline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
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
// local checkout. Every other error is returned unchanged.
func (s *Service) reviewDiff(ctx context.Context, r config.Repo, pr github.Pull) (diffResult, error) {
	text, e := s.GH.Diff(ctx, r.Name, pr.Number)
	if e == nil {
		return diffResult{text: text}, nil
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
		b, e := s.Runner.Run(ctx, "git", append([]string{"-C", root}, args...), "", "")
		return string(b), e
	}
	field := func(args ...string) (string, error) {
		out, e := git(args...)
		return strings.TrimSpace(out), e
	}
	if _, e := field("rev-parse", "--git-dir"); e != nil {
		return diffResult{}, errors.New("repo root is not a git checkout")
	}
	baseRef := pr.Base.Ref
	if baseRef == "" {
		baseRef = pr.Base.SHA
	}
	if _, e := field("fetch", "--no-tags", "origin", prHeadRefspec(pr.Number), baseRef); e != nil {
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

// summarizeLocalDiff applies the size bound the API path applies. Pure renames
// carry no content and are listed by path (git diff -M); when the patch is still
// over budget it keeps complete file blocks in their stable order while they
// fit, and replaces each later block with its header and a digest of the
// omitted text. Every summarized path is returned, so the pass body can name it.
func summarizeLocalDiff(diff string, budget int) (string, []string) {
	files := splitDiff(diff)
	if len(files) == 0 {
		return diff, nil
	}
	header := make([]string, len(files))
	// suffix[i] reserves the headers of files i and later that could still be
	// elided; a pure rename keeps its (tiny) block and is never headed alone.
	suffix := make([]int, len(files)+1)
	for i := len(files) - 1; i >= 0; i-- {
		header[i] = elidedHeader(files[i])
		if elidable(files[i]) {
			suffix[i] = suffix[i+1] + len(header[i]) + len(elidedNote) + 65
		} else {
			suffix[i] = suffix[i+1]
		}
	}
	var b strings.Builder
	var summarized []string
	used := 0
	for i, f := range files {
		block := diffBlock(f)
		if pureRename(f) {
			summarized = append(summarized, diffFilePath(f))
		}
		if !elidable(f) || used+len(block)+suffix[i+1] <= budget {
			b.WriteString(block)
			used += len(block)
			continue
		}
		b.WriteString(header[i])
		b.WriteString(elidedNote)
		fmt.Fprintf(&b, "%x\n", sha256.Sum256([]byte(block)))
		used += len(header[i]) + len(elidedNote) + 65
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
		if parts := strings.Fields(strings.TrimRight(f.head[0], "\r\n")); len(parts) >= 4 {
			return cleanDiffPath(parts[3])
		}
	}
	return ""
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
