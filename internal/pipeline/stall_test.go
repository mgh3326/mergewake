package pipeline

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
)

func TestStallSkipsClosedAndMergedPRs(t *testing.T) {
	for _, state := range []string{"closed", "merged", "closed-after-snapshot", "merged-after-snapshot", "head-changed-after-snapshot"} {
		t.Run(state, func(t *testing.T) {
			s, g, n, c, r := stallSetup(t)
			s.GH = g
			prs := []github.Pull{g.pull}
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil {
				t.Fatal(e)
			}
			c.t = c.t.Add(2 * time.Hour)
			switch state {
			case "closed":
				prs[0].State = "closed"
			case "merged":
				prs[0].Merged = true
			case "closed-after-snapshot":
				g.pull.State = "closed"
			case "merged-after-snapshot":
				g.pull.Merged = true
			case "head-changed-after-snapshot":
				g.pull.Head.SHA = shaC
			}
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil || n.count != 0 {
				t.Fatalf("error=%v events=%v", e, n.events)
			}
			var waiting map[string]time.Time
			if _, e := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-wait.json"), &waiting); e != nil || len(waiting) != 0 {
				t.Fatalf("stale clock retained: %v error=%v", waiting, e)
			}
		})
	}
}

func TestStallSkipsOnlyCompleteInheritedVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		blocking bool
		passes   int
		patch    string
		want     int
	}{
		{"routine inherited", false, 1, patchOne, 0},
		{"blocking inherited", true, 2, patchOne, 0},
		{"blocking incomplete", true, 1, patchOne, 1},
		{"different patch", false, 1, patchTwo, 1},
		{"unknown patch", false, 1, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, n, c, r := stallSetup(t)
			s.GH = g
			g.full = patchOne
			g.blocking = tc.blocking
			prs := []github.Pull{g.pull}
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil {
				t.Fatal(e)
			}
			// The review arrives after a waiting clock was already persisted.
			g.passes = map[string][]github.PassReceipt{shaB: {{Pass: 1, Family: "anthropic", Lens: "correctness", Patch: reviewPatchID(tc.patch)}}}
			if tc.passes == 2 {
				g.passes[shaB] = append(g.passes[shaB], github.PassReceipt{Pass: 2, Family: "openai", Lens: "state-risk", Patch: reviewPatchID(tc.patch)})
			}
			c.t = c.t.Add(2 * time.Hour)
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil || n.count != tc.want {
				t.Fatalf("error=%v events=%v want=%d", e, n.events, tc.want)
			}
			var waiting map[string]time.Time
			if _, e := fsutil.ReadJSON(filepath.Join(s.state(r.Name), "review-wait.json"), &waiting); e != nil || (tc.want == 0 && len(waiting) != 0) {
				t.Fatalf("waiting=%v error=%v", waiting, e)
			}
		})
	}
}

type stallErrorGH struct {
	*roundGH
	historyErr, pullErr error
}

func (g *stallErrorGH) ReviewHistory(ctx context.Context, repo string, n int, login string) ([]github.ReviewedPass, error) {
	if g.historyErr != nil {
		return nil, g.historyErr
	}
	return g.roundGH.ReviewHistory(ctx, repo, n, login)
}

func (g *stallErrorGH) Pull(ctx context.Context, repo string, n int) (github.Pull, error) {
	if g.pullErr != nil {
		return github.Pull{}, g.pullErr
	}
	return g.roundGH.Pull(ctx, repo, n)
}

func TestStallReadFailurePreservesClockWithoutAlarm(t *testing.T) {
	for _, fail := range []string{"history", "pull"} {
		t.Run(fail, func(t *testing.T) {
			s, rg, n, c, r := stallSetup(t)
			g := &stallErrorGH{roundGH: rg}
			s.GH = g
			prs := []github.Pull{g.pull}
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil {
				t.Fatal(e)
			}
			c.t = c.t.Add(time.Hour)
			if fail == "history" {
				g.historyErr = errors.New("history unavailable")
			} else {
				g.pullErr = errors.New("PR unavailable")
			}
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e == nil || n.count != 0 {
				t.Fatalf("error=%v events=%v", e, n.events)
			}
			g.historyErr, g.pullErr = nil, nil
			if e := s.CheckStalls(context.Background(), r, prs, Options{}); e != nil || n.count != 1 {
				t.Fatalf("clock lost after failure: error=%v events=%v", e, n.events)
			}
		})
	}
}
