package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// A policy that still sets a removed key is refused when it is read, and the
// error names the key: policy files decode strictly, so a key no field reads
// is an error, not a silent no-op.
func TestRemovedPolicyKeysAreRefused(t *testing.T) {
	for _, tt := range []struct{ key, extra string }{
		{"require_up_to_date", "require_up_to_date: true\n"},
		{"auto_update_branch", "auto_update_branch: false\n"},
		{"main_checks", "main_checks: [ci-ok]\n"},
		{"bot_updates", "bot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [uv.lock]\n  land: patch\n"},
		{"any_ref_timeout", "any_ref_workflow: ci-any-ref.yml\nany_ref_timeout: 20m\n"},
	} {
		t.Run(tt.key, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.policyExtra = tt.extra
			_, e := s.Policy(context.Background(), r, shaB)
			if e == nil || !strings.Contains(e.Error(), tt.key) {
				t.Fatalf("policy with %s: error %v, want one naming the key", tt.key, e)
			}
		})
	}
}

// A PR whose base moved on is merged on its own head under the default
// policy: no branch update, no base_stale hold, and no update row.
func TestMovedBaseDefaultPolicyMergesOnOwnHead(t *testing.T) {
	s, g, n, out, r := baseBehindSetup(t, true, "behind", "")
	for i := 0; i < 2; i++ {
		if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if len(g.shas) != 1 || g.shas[0] != shaA || g.mergeCount != 1 {
		t.Fatalf("merge shas=%v count=%d", g.shas, g.mergeCount)
	}
	if n.count != 0 || strings.Contains(out.String(), "base_stale") || strings.Contains(out.String(), "base_updated") {
		t.Fatalf("events=%v output=%s", n.events, out.String())
	}
}

// watch_checks is the one name for the checks that judge a main-branch commit.
func TestWatchChecksSelectMainChecks(t *testing.T) {
	s, g, _, _, r := setup(t)
	g.policyExtra = "watch_checks: [ci-ok]\n"
	p, e := s.Policy(context.Background(), r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	if got := p.WatchSet(); !slices.Equal(got, []string{"ci-ok"}) {
		t.Fatalf("watch set %v", got)
	}
}
