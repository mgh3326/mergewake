package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

type checkAPI struct {
	*fakeGH
	client *github.Client
}

func (g checkAPI) Check(ctx context.Context, repo, sha string, checks []string) (github.CI, error) {
	return g.client.Check(ctx, repo, sha, checks)
}

// Replay a real check-run payload through Client.Check, rather than supplying
// Pending directly: green -> cancelled -> merge commit -> red main.
func TestWatchReplayCancelledRunPayload(t *testing.T) {
	s, g, sink, n, r := setup(t)
	g.enabled = true
	g.head = shaD
	side := strings.Repeat("e", 40)
	g.parents2 = map[string][]string{shaD: {shaC}, shaC: {shaB, side}, shaB: {shaA}}
	pr := func(num int, sha string) github.Pull {
		return github.Pull{Number: num, Merged: true, State: "closed", MergeCommitSHA: sha, ChangedFiles: 1, Base: github.Ref{Ref: "main"}}
	}
	g.commitPRs = map[string][]github.Pull{shaD: {}, shaC: {pr(7, shaC)}, shaB: {pr(6, shaB)}, shaA: {pr(5, shaA)}}
	cancelled := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/statuses") {
			fmt.Fprint(w, `[]`)
			return
		}
		if !strings.HasSuffix(req.URL.Path, "/check-runs") {
			http.NotFound(w, req)
			return
		}
		conclusion := "failure"
		if strings.Contains(req.URL.Path, shaA) {
			conclusion = "success"
		}
		if strings.Contains(req.URL.Path, shaB) {
			conclusion = "cancelled"
			cancelled++
		}
		fmt.Fprintf(w, `{"check_runs":[{"id":1,"name":"CI","status":"completed","conclusion":%q}]}`, conclusion)
	}))
	defer server.Close()
	client := github.New("")
	client.BaseURL = server.URL
	client.HTTP = server.Client()
	s.GH = checkAPI{fakeGH: g, client: client}
	var out strings.Builder
	s.Out = &out
	if err := s.Watch(context.Background(), r, Options{}); err != nil {
		t.Fatal(err)
	}
	if cancelled == 0 || !strings.Contains(out.String(), `"action":"hold"`) || !strings.Contains(out.String(), `"candidates":[6,7]`) {
		t.Fatal(out.String())
	}
	if g.revertCount != 0 || sink.records != 1 || n.count != 1 {
		t.Fatalf("reverts=%d reports=%d notifications=%d", g.revertCount, sink.records, n.count)
	}
}

func TestMergeCommitCulpritHoldsInAllModes(t *testing.T) {
	for _, mode := range []string{"active", "shadow", "dry"} {
		t.Run(mode, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.head = shaC
			g.parents2 = map[string][]string{shaC: {shaA, shaB}}
			g.checks = map[string]github.CI{shaC: github.Red, shaA: github.Green}
			g.pull.Merged = true
			g.pull.State = "closed"
			g.pull.MergeCommitSHA = shaC
			var out strings.Builder
			s.Out = &out
			if err := s.Watch(context.Background(), r, Options{Shadow: mode == "shadow", DryRun: mode == "dry"}); err != nil {
				t.Fatal(err)
			}
			if g.revertCount != 0 || g.latch || strings.Contains(out.String(), "open-revert-pr") || !strings.Contains(out.String(), `"action":"hold"`) || !strings.Contains(out.String(), `"candidates":[7]`) {
				t.Fatal(out.String())
			}
		})
	}
}

func TestFakeRevertRefusesMergeCommit(t *testing.T) {
	_, g, _, _, r := setup(t)
	g.parents2 = map[string][]string{shaC: {shaA, shaB}}
	if _, err := g.Revert(context.Background(), r.Name, r.Branch, shaD, shaC, 7); err == nil {
		t.Fatal("fake accepted merge commit")
	}
	if g.revertCount != 0 || g.latch {
		t.Fatal("refused revert mutated state")
	}
}

func TestRecoveryPauseRequiresBaseRepository(t *testing.T) {
	for _, tt := range []struct {
		name, repo string
		ci         github.CI
		shadow     bool
		action     string
		merges     int
	}{
		{"same repo recovery", "example/project", github.Red, false, "merge", 1},
		{"fork recovery branch", "fork/project", github.Red, false, "pause-red-main", 0},
		{"unknown repository", "", github.Red, false, "pause-red-main", 0},
		{"fork unknown main", "fork/project", github.Pending, false, "pause-main-unknown", 0},
		{"shadow unknown main", "fork/project", github.Pending, true, "would-pause-main-unknown", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.enabled = true
			g.pull.Head.Ref = github.RecoveryBranch
			g.pull.Head.Repo.FullName = tt.repo
			g.checks = map[string]github.CI{shaB: tt.ci, shaA: github.Green}
			if err := s.Land(context.Background(), r, 7, Options{Shadow: tt.shadow}); err != nil {
				t.Fatal(err)
			}
			state, err := s.ledger().Read()
			if err != nil {
				t.Fatal(err)
			}
			if g.mergeCount != tt.merges || state.Latest["https://github.com/"+r.Name+"/pull/7"].Action != tt.action {
				t.Fatalf("merges=%d state=%+v", g.mergeCount, state)
			}
		})
	}
}

func TestWatchReportedBounded(t *testing.T) {
	s, _, sink, n, r := setup(t)
	file := filepath.Join(s.state(r.Name), "watch-reported")
	if err := os.MkdirAll(s.state(r.Name), 0700); err != nil {
		t.Fatal(err)
	}
	var prior strings.Builder
	for i := 0; i < 130; i++ {
		fmt.Fprintf(&prior, "head-%d:hold\n", i)
	}
	if err := os.WriteFile(file, []byte(prior.String()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.reportWatch(context.Background(), r, watchResult{Head: "head-130", Action: "hold"}, "head-130:hold", Options{}); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	keys := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(keys) != 128 || keys[0] != "head-3:hold" || keys[127] != "head-130:hold" {
		t.Fatalf("%v", keys)
	}
	if err := s.reportWatch(context.Background(), r, watchResult{Head: "head-130", Action: "hold"}, "head-130:hold", Options{}); err != nil {
		t.Fatal(err)
	}
	if sink.records != 1 || n.count != 1 {
		t.Fatal("repeat report not deduplicated")
	}
	after, err := os.ReadFile(file)
	if err != nil || !reflect.DeepEqual(b, after) {
		t.Fatal("dedupe changed keys", err)
	}
}

func TestPullDecodesRecoveryRepository(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"head": map[string]any{"ref": github.RecoveryBranch, "repo": map[string]any{"full_name": "fork/project"}}})
	}))
	defer server.Close()
	client := github.New("")
	client.BaseURL = server.URL
	client.HTTP = server.Client()
	pr, err := client.Pull(context.Background(), "example/project", 7)
	if err != nil || pr.Head.Repo.FullName != "fork/project" {
		t.Fatalf("%+v %v", pr, err)
	}
}
