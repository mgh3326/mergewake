package pipeline

import (
	"slices"
	"unicode/utf8"
)

// Size bounds on what mergewake hands panewire. panewire refuses a payload
// over either limit after the work is queued, and mergewake then records
// "intact" and never retries, so every send is kept inside the limit here.
// The values are read from panewire at f55738f0.
const (
	// escalateAuthorFindingsMax is panewire's escalateAuthorFindingsMax
	// (32 << 10), internal/fleet/escalate_author.go: readEscalateFindings
	// refuses a findings file larger than this with exit 2.
	escalateAuthorFindingsMax = 32 << 10

	// hubMaxBriefBytes is panewire's hubMaxBriefBytes (64 << 10),
	// internal/fleet/hub_transport.go: consultBrief refuses the brief, the
	// preamble plus the question document, when it is larger than this.
	hubMaxBriefBytes = 64 << 10

	// consultBriefMargin is what the question document body leaves for the
	// rest of the brief. At f55738f0 that is consultPreamble (483 bytes,
	// internal/fleet/consult.go) + "Question document: hk:doc/" (26 bytes) +
	// the doc key (at most 200 bytes, internal/spawnargs/args.go ValidDocKey)
	// + "\n\n" (2 bytes): 711 bytes at most. 4 KiB leaves room for a longer
	// preamble without a code change.
	consultBriefMargin = 4 << 10

	// consultQuestionMax bounds the question document body mergewake writes.
	consultQuestionMax = hubMaxBriefBytes - consultBriefMargin
)

// findingTier ranks a finding for the size bounds: critical first, then the
// other must-fix severities (mustFix), then everything else. A tier keeps its
// findings in their current order.
func findingTier(sev string) int {
	switch {
	case sev == "critical":
		return 0
	case mustFix(sev):
		return 1
	}
	return 2
}

// keepFindings chooses which findings to send under limit bytes. sevs holds
// one severity per finding in current order. size(keep, cut) is the encoded
// size of the findings at keep (ascending indices) with each text field cut
// to cut bytes; it must not shrink as cut grows or as keep grows.
//
// Text is cut before any finding is dropped: when every finding fits, the
// largest cut that still fits is used and nothing is dropped. Only when even
// empty text does not fit are findings dropped, lowest tier first, with the
// cut at 0. The returned keep is in current order.
func keepFindings(sevs []string, limit int, size func(keep []int, cut int) int) (keep []int, cut int) {
	order := make([]int, len(sevs))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return findingTier(sevs[a]) - findingTier(sevs[b])
	})
	// take returns the first n findings in priority order, in current order.
	take := func(n int) []int {
		kept := slices.Clone(order[:n])
		slices.Sort(kept)
		return kept
	}

	all := len(order)
	if size(take(all), 0) <= limit {
		lo, hi := 0, limit
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if size(take(all), mid) <= limit {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		return take(all), lo
	}

	lo, hi := 0, all
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if size(take(mid), 0) <= limit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return take(lo), 0
}

// cutText keeps at most n bytes of s, ending on a rune boundary so the result
// stays valid UTF-8 (and so JSON encodes it without replacement characters).
func cutText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
