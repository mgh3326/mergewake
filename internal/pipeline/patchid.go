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

// patchID is `git patch-id --stable` over a unified diff: a SHA-1 of each
// file's diff with whitespace removed and hunk line numbers left out, the
// per-file digests summed so file order does not matter. A base merged into
// the head leaves the PR diff (merge-base..head) text unchanged, so the id
// names the patch rather than the commit.
//
// It follows git's state machine, except that an unexpected line after a
// hunk's counts are exhausted (git stops reading there) is hashed instead, so
// a diff is never silently truncated into a shorter, equal id.
func patchID(diff string) string {
	var (
		total  [sha1.Size]byte
		ctx    hash.Hash = sha1.New()
		hashed bool
	)
	flush := func() {
		if !hashed {
			return
		}
		sum := ctx.Sum(nil)
		carry := 0
		for i := range total {
			carry += int(total[i]) + int(sum[i])
			total[i] = byte(carry)
			carry >>= 8
		}
		ctx.Reset()
		hashed = false
	}
	before, after := -1, -1
	patchlen := 0
	blobs := ""
	found := false
	for _, line := range strings.Split(diff, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if patchlen == 0 && !strings.HasPrefix(line, "diff ") {
			continue
		}
		if before == -1 {
			switch {
			case strings.HasPrefix(line, "GIT binary patch"), strings.HasPrefix(line, "Binary files"):
				before, after = 0, 0
				// A binary diff carries no content, so its blob ids stand for it.
				ctx.Write([]byte(blobs))
			case strings.HasPrefix(line, "index "):
				blobs = blobIDs(line)
				continue
			case strings.HasPrefix(line, "--- "):
				before, after = 1, 1
			}
		}
		if before == 0 && after == 0 {
			if strings.HasPrefix(line, "@@ -") {
				before, after = hunkCounts(line)
				continue
			}
			if strings.HasPrefix(line, "diff ") {
				flush()
				before, after = -1, -1
				blobs = ""
			}
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
		ctx.Write([]byte(clean))
		hashed, found = true, true
	}
	flush()
	if !found {
		return ""
	}
	return fmt.Sprintf("%x", total)
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
	return s.rememberPatch(key, patchID(diff))
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
// passes satisfy needed. The passes that count are the trusted identity's
// receipts, so a pushed ref or a forged head cannot stand in for a review.
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
		receipts := passesFor(history, p.Head)
		if _, ok := distinctPasses(receipts, needed); ok {
			return p.Head, receipts, true
		}
	}
	return "", nil, false
}
