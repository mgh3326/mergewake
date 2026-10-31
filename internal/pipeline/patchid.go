package pipeline

import (
	"context"
	"crypto/sha1"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"

	"github.com/mgh3326/mergewake/internal/github"
)

// summedHash is a SHA-1 per file, summed byte-wise with carry the way
// `git patch-id --stable` does, so the order of the files does not matter.
type summedHash struct {
	ctx   hash.Hash
	total [sha1.Size]byte
	open  bool
}

func newSummedHash() *summedHash { return &summedHash{ctx: sha1.New()} }

func (h *summedHash) write(b []byte) { h.ctx.Write(b); h.open = true }

// flush folds the file in progress into the total and starts the next one.
func (h *summedHash) flush() {
	if !h.open {
		return
	}
	sum := h.ctx.Sum(nil)
	carry := 0
	for i := range h.total {
		carry += int(h.total[i]) + int(sum[i])
		h.total[i] = byte(carry)
		carry >>= 8
	}
	h.ctx.Reset()
	h.open = false
}

// patchHashes reads a unified diff once and returns two ids. loose is
// `git patch-id --stable`: each file's diff with all whitespace removed and
// hunk line numbers left out. strict is the same sum over the exact text of
// every changed line and file header (context lines left out, so changes near
// the patch that a merged base brings in do not count), which a whitespace-only
// edit changes.
//
// It follows git's state machine, with two deliberate differences: a file
// always ends at the next "diff" line (git folds a hunkless rename or mode
// change into the following file, which makes the id depend on file order), and
// an unexpected line after a hunk's counts are exhausted is hashed instead of
// ending the read, so a diff is never silently truncated into a shorter id.
func patchHashes(diff string) (loose, strict string) {
	l, x := newSummedHash(), newSummedHash()
	before, after := -1, -1
	patchlen := 0
	blobs := ""
	for _, raw := range strings.Split(diff, "\n") {
		line := strings.TrimSuffix(raw, "\r")
		if patchlen == 0 && !strings.HasPrefix(line, "diff ") {
			continue
		}
		if strings.HasPrefix(line, "diff ") {
			l.flush()
			x.flush()
			before, after = -1, -1
			blobs = ""
		}
		if before == -1 {
			switch {
			case strings.HasPrefix(line, "GIT binary patch"), strings.HasPrefix(line, "Binary files"):
				before, after = 0, 0
				// A binary diff carries no content, so its blob ids stand for it.
				l.write([]byte(blobs))
			case strings.HasPrefix(line, "index "):
				blobs = blobIDs(line)
				continue
			case strings.HasPrefix(line, "--- "):
				before, after = 1, 1
			}
		}
		if before == 0 && after == 0 && strings.HasPrefix(line, "@@ -") {
			before, after = hunkCounts(line)
			continue
		}
		if line != "" {
			if c := line[0]; c == '-' || c == ' ' {
				before--
			}
			if c := line[0]; c == '+' || c == ' ' {
				after--
			}
		}
		clean := strings.Map(func(r rune) rune {
			if r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == '\v' || r == '\f' {
				return -1
			}
			return r
		}, line)
		patchlen += len(clean)
		l.write([]byte(clean))
		if !strings.HasPrefix(line, " ") {
			x.write([]byte(raw + "\n"))
		}
	}
	l.flush()
	x.flush()
	if patchlen == 0 {
		return "", ""
	}
	return fmt.Sprintf("%x", l.total), fmt.Sprintf("%x", x.total)
}

// patchID is `git patch-id --stable` of the diff, or "" for no diff.
func patchID(diff string) string {
	loose, _ := patchHashes(diff)
	return loose
}

// reviewPatchID names the patch a review covers: the stable patch id, then a
// digest of the exact changed text. Merging a base into the head leaves both
// alone, but a whitespace-only edit, which `git patch-id` ignores and which can
// change behaviour (indentation, a Makefile recipe), is a different patch and
// gets its own review.
func reviewPatchID(diff string) string {
	loose, strict := patchHashes(diff)
	if loose == "" {
		return ""
	}
	return loose + ":" + strict[:12]
}

// blobIDs is the pre- and post-image blob ids of an "index a..b mode" line,
// cut to seven characters because the abbreviation length grows with the
// repository.
func blobIDs(line string) string {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return ""
	}
	a, b, ok := strings.Cut(fields[1], "..")
	if !ok {
		return ""
	}
	return a[:min(len(a), 7)] + ".." + b[:min(len(b), 7)]
}

// hunkCounts reads the old and new line counts of an "@@ -a,b +c,d @@" header.
func hunkCounts(line string) (before, after int) {
	before, after = 1, 1
	parts := strings.Fields(line)
	if len(parts) < 3 {
		return 0, 0
	}
	count := func(span string) int {
		if _, n, ok := strings.Cut(span[1:], ","); ok {
			v, e := strconv.Atoi(n)
			if e != nil {
				return 0
			}
			return v
		}
		return 1
	}
	return count(parts[1]), count(parts[2])
}

// patchMu guards Service.patches. The id of a head never changes, so it is
// read from GitHub once per head.
var patchMu sync.Mutex

func patchKey(repo string, n int, head string) string {
	return fmt.Sprintf("%s#%d@%s", repo, n, head)
}

// patchIDOf returns the patch id of the PR at head, or "" when the diff cannot
// be read: an unknown patch is never equal to another, so the head is then
// reviewed on its own.
func (s *Service) patchIDOf(ctx context.Context, repo string, n int, head string) string {
	key := patchKey(repo, n, head)
	patchMu.Lock()
	id, ok := s.patches[key]
	patchMu.Unlock()
	if ok {
		return id
	}
	diff, e := s.GH.Diff(ctx, repo, n)
	if e != nil {
		return ""
	}
	return s.rememberPatch(key, reviewPatchID(diff))
}

func (s *Service) rememberPatch(key, id string) string {
	patchMu.Lock()
	defer patchMu.Unlock()
	if s.patches == nil || len(s.patches) > 512 {
		s.patches = map[string]string{}
	}
	s.patches[key] = id
	return id
}

// passesFor lists the receipts the trusted review identity posted on one head.
func passesFor(history []github.ReviewedPass, head string) []github.PassReceipt {
	var out []github.PassReceipt
	for _, p := range history {
		if p.Head == head {
			out = append(out, p.PassReceipt)
		}
	}
	return out
}

// inheritedPasses finds an earlier head with the same patch id whose recorded
// passes satisfy needed. Only the passes recorded for this patch count: a
// receipt with another patch id, or none, is not verified for it. The passes
// are the trusted identity's receipts, so a pushed ref or a forged head cannot
// stand in for a review.
func inheritedPasses(history []github.ReviewedPass, patch, head string, needed int) (string, []github.PassReceipt, bool) {
	if patch == "" {
		return "", nil, false
	}
	seen := map[string]bool{}
	for _, p := range history {
		if p.Patch != patch || p.Head == head || seen[p.Head] {
			continue
		}
		seen[p.Head] = true
		var receipts []github.PassReceipt
		for _, q := range history {
			if q.Head == p.Head && q.Patch == patch {
				receipts = append(receipts, q.PassReceipt)
			}
		}
		if _, ok := distinctPasses(receipts, needed); ok {
			return p.Head, receipts, true
		}
	}
	return "", nil, false
}
