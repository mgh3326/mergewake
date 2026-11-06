package pipeline

import (
	"context"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// Land tells the context's merge hook once per PR it merges, and nothing when
// the gate holds the merge back or the pass only decides.
func TestLandCallsMergeHookOncePerMerge(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ci       github.CI
		opts     Options
		withHook bool
		calls    int
	}{
		{"merged", github.Green, Options{}, true, 1},
		{"merged without a hook", github.Green, Options{}, false, 0},
		{"red CI", github.Red, Options{}, true, 0},
		{"shadow", github.Green, Options{Shadow: true}, true, 0},
		{"dry run", github.Green, Options{DryRun: true}, true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			markReviewed(g)
			g.ci = tt.ci
			var got [][2]any
			ctx := context.Background()
			if tt.withHook {
				ctx = WithMergeHook(ctx, func(repo string, pr int) { got = append(got, [2]any{repo, pr}) })
			}
			if e := s.Land(ctx, r, 7, tt.opts); e != nil {
				t.Fatal(e)
			}
			if len(got) != tt.calls || tt.calls == 1 && got[0] != [2]any{r.Name, 7} {
				t.Fatalf("hook calls %v, want %d", got, tt.calls)
			}
			if tt.calls == 1 && g.mergeCount != 1 {
				t.Fatalf("merged %d times", g.mergeCount)
			}
		})
	}
}
