package github

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// HKReviewsPrimary switches where review history is read from. false (phase 1,
// shadow): GitHub stays the source and hk's record of the same passes is only
// compared with it. true (phase 2): a PR head hk holds passes for is served
// from hk without listing pulls/{n}/reviews; a head hk lacks still reads
// GitHub.
const HKReviewsPrimary = false

// hkReviewTimeout bounds one hk read: hk is a second opinion in phase 1 and a
// fast path in phase 2, so a slow hk costs that read, never the pass.
const hkReviewTimeout = 3 * time.Second

// HKReview is one review pass hk recorded for a PR (an item of a change's
// reviews[]), reduced to what the gate needs.
type HKReview struct {
	Head       string
	Patch      string
	Round      int
	Pass       int
	Lens       string
	Family     string
	Executor   string
	FinishedAt *time.Time
}

// HKChange is the review record hk holds for one PR: the head hk last saw and
// the passes recorded, oldest first. No Reviews means "no hk data" (hk has no
// change for the PR, no passes on it, or no such endpoint), never an error.
type HKChange struct {
	Head    string
	Reviews []HKReview
}

// HKReviewSource reads a PR's review record from hk.
type HKReviewSource interface {
	ReviewRecords(ctx context.Context, repo string, n int) (HKChange, error)
}

// HKReviewStats counts hk review reads: Hits found passes, Misses found none
// (no change, no passes, or a failed read), Mismatches are the differences
// from GitHub's record that were logged.
type HKReviewStats struct{ Hits, Misses, Mismatches int64 }

type hkReviewCounters struct{ hits, misses, mismatches atomic.Int64 }

// UseHK reads review history from src alongside GitHub. primary selects phase 2
// (see HKReviewsPrimary); callers pass the constant.
func (c *Client) UseHK(src HKReviewSource, primary bool) {
	c.hk, c.hkPrimary = src, primary
}

// HKReviewStats is the running count of hk review reads.
func (c *Client) HKReviewStats() HKReviewStats {
	return HKReviewStats{c.hkCounts.hits.Load(), c.hkCounts.misses.Load(), c.hkCounts.mismatches.Load()}
}

func (c *Client) hkLog(fields map[string]any) {
	fields["msg"] = "hk_review_history"
	var out io.Writer = c.Stderr
	if out == nil {
		out = os.Stderr
	}
	_ = json.NewEncoder(out).Encode(fields)
}

// hkChange reads the PR's review record from hk once per pass. It never fails:
// a failed read is one log line and counts as no data.
func (c *Client) hkChange(ctx context.Context, repo string, n int) HKChange {
	if c.hk == nil {
		return HKChange{}
	}
	ch, _ := memoized(ctx, c, memoKey{client: c, repo: repo, kind: "hk-reviews", id: strconv.Itoa(n)}, func() (HKChange, error) {
		timeout := cmp.Or(c.HKTimeout, hkReviewTimeout)
		rctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ch, e := c.hk.ReviewRecords(rctx, repo, n)
		if e != nil {
			c.hkLog(map[string]any{"repo": repo, "pr": n, "error": e.Error()})
			ch = HKChange{}
		}
		if len(ch.Reviews) > 0 {
			c.hkCounts.hits.Add(1)
		} else {
			c.hkCounts.misses.Add(1)
		}
		return ch, nil
	})
	return ch
}

// hkPrimaryChange is hk's record of the PR in phase 2, and nothing in phase 1,
// where hk is only compared against GitHub after GitHub has answered.
func (c *Client) hkPrimaryChange(ctx context.Context, repo string, n int) HKChange {
	if !c.hkPrimary {
		return HKChange{}
	}
	return c.hkChange(ctx, repo, n)
}

// hkServesHistory reports whether phase 2 reads the whole PR history from hk:
// hk holds passes for the head it last saw.
func hkServesHistory(hk HKChange) bool {
	return hk.Head != "" && hasHead(hk.Reviews, hk.Head)
}

func hasHead(rs []HKReview, head string) bool {
	return slices.ContainsFunc(rs, func(r HKReview) bool { return r.Head == head })
}

// hkPasses maps hk's records to the passes the gate reads. hk does not carry
// the reviewer, same-family flag, delta, quiet flag or outside-diff count that
// GitHub's posted marker does, and keeps only the hex part of the patch id.
func hkPasses(rs []HKReview) []ReviewedPass {
	out := make([]ReviewedPass, 0, len(rs))
	for _, r := range rs {
		out = append(out, ReviewedPass{Head: r.Head, SubmittedAt: r.FinishedAt, PassReceipt: PassReceipt{
			Pass: r.Pass, Lens: r.Lens, Family: r.Family, Executor: r.Executor, Patch: r.Patch, Round: r.Round,
		}})
	}
	return out
}

// hkHeads is the distinct heads hk holds passes for, oldest first, with the
// round of each head's first pass.
func hkHeads(rs []HKReview) []ReviewedHead {
	var out []ReviewedHead
	seen := map[string]bool{}
	for _, r := range rs {
		if !seen[r.Head] {
			seen[r.Head] = true
			out = append(out, ReviewedHead{SHA: r.Head, Round: r.Round})
		}
	}
	return out
}

var (
	hkTokenRun = regexp.MustCompile(`[^a-z0-9_.-]+`)
	hkPatchHex = regexp.MustCompile(`^[0-9a-f]{7,64}$`)
)

// hkToken is how mergewake fits a free-text name to hk's token rule.
func hkToken(s string) string {
	t := strings.Trim(hkTokenRun.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), "-"), "-")
	if len(t) > 64 {
		t = strings.Trim(t[:64], "-")
	}
	return t
}

// hkPatch is the part of a receipt's patch that hk stores as patch_id.
func hkPatch(p string) string {
	hex, _, _ := strings.Cut(p, ":")
	hex = strings.ToLower(strings.TrimSpace(hex))
	if hkPatchHex.MatchString(hex) {
		return hex
	}
	return ""
}

// ghAsHK is a GitHub-posted pass in the form hk stores it, so the two compare.
func ghAsHK(p ReviewedPass) HKReview {
	return HKReview{
		Head: p.Head, Pass: p.Pass, Lens: cmp.Or(hkToken(p.Lens), "default"), Round: max(p.Round, 1),
		Family: hkToken(p.Family), Patch: hkPatch(p.Patch),
	}
}

type hkPassKey struct {
	head string
	pass int
	lens string
}

func keyOf(r HKReview) hkPassKey { return hkPassKey{r.Head, r.Pass, r.Lens} }

// compareHK logs one hk_review_history line per difference between hk's passes
// and the ones read from GitHub, per (head, pass, lens), and counts them. GitHub
// stays the answer; this only measures whether hk could be.
func (c *Client) compareHK(repo string, n int, hk []HKReview, gh []HKReview) {
	if len(hk) == 0 {
		return
	}
	diff := func(r HKReview, field string, hkv, ghv any) {
		c.hkCounts.mismatches.Add(1)
		c.hkLog(map[string]any{"repo": repo, "pr": n, "head": r.Head, "pass": r.Pass, "lens": r.Lens, "field": field, "hk": fmt.Sprint(hkv), "github": fmt.Sprint(ghv)})
	}
	hkBy := map[hkPassKey]HKReview{}
	for _, r := range hk {
		if _, dup := hkBy[keyOf(r)]; !dup {
			hkBy[keyOf(r)] = r
		}
	}
	seen := map[hkPassKey]bool{}
	for _, g := range gh {
		seen[keyOf(g)] = true
		h, ok := hkBy[keyOf(g)]
		switch {
		case !ok:
			diff(g, "presence", "absent", "present")
		default:
			if h.Round != g.Round {
				diff(g, "round", h.Round, g.Round)
			}
			if h.Family != g.Family {
				diff(g, "family", h.Family, g.Family)
			}
			if h.Patch != g.Patch {
				diff(g, "patch", h.Patch, g.Patch)
			}
		}
	}
	for _, h := range hk {
		if !seen[keyOf(h)] {
			seen[keyOf(h)] = true
			diff(h, "presence", "present", "absent")
		}
	}
}

// compareHKHeads is compareHK for ReviewedHeads, which knows heads and rounds
// only.
func (c *Client) compareHKHeads(repo string, n int, hk []HKReview, gh []ReviewedHead) {
	if len(hk) == 0 {
		return
	}
	diff := func(head, field string, hkv, ghv any) {
		c.hkCounts.mismatches.Add(1)
		c.hkLog(map[string]any{"repo": repo, "pr": n, "head": head, "field": field, "hk": fmt.Sprint(hkv), "github": fmt.Sprint(ghv)})
	}
	hkBy := map[string]ReviewedHead{}
	for _, h := range hkHeads(hk) {
		hkBy[h.SHA] = h
	}
	seen := map[string]bool{}
	for _, g := range gh {
		seen[g.SHA] = true
		h, ok := hkBy[g.SHA]
		switch {
		case !ok:
			diff(g.SHA, "presence", "absent", "present")
		case h.Round != max(g.Round, 1):
			diff(g.SHA, "round", h.Round, max(g.Round, 1))
		}
	}
	for _, h := range hkHeads(hk) {
		if !seen[h.SHA] {
			diff(h.SHA, "presence", "present", "absent")
		}
	}
}
