package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/pipeline"
)

// (g) skip_bots keeps dropping every bot the policy does not list; a listed bot
// and every human stay.
func TestSkipBotPullsKeepsOnlyListedBots(t *testing.T) {
	var prs []github.Pull
	raw := `[{"number":1,"user":{"login":"renovate[bot]","type":"Bot"}},{"number":2,"user":{"login":"dependabot[bot]","type":"Bot"}},{"number":3,"user":{"login":"someone","type":"User"}},{"number":4,"user":{"login":"renovate","type":"User"}}]`
	if e := json.Unmarshal([]byte(raw), &prs); e != nil {
		t.Fatal(e)
	}
	numbers := func(prs []github.Pull) (out []int) {
		for _, pr := range prs {
			out = append(out, pr.Number)
		}
		return out
	}
	for _, tt := range []struct {
		name   string
		skip   bool
		listed []string
		want   []int
	}{
		{"skip_bots, nobody listed", true, nil, []int{3, 4}},
		{"skip_bots, renovate listed", true, []string{"renovate[bot]"}, []int{1, 3, 4}},
		{"skip_bots off", false, []string{"renovate[bot]"}, []int{1, 2, 3, 4}},
	} {
		if got := numbers(skipBotPulls(prs, tt.skip, tt.listed...)); fmt.Sprint(got) != fmt.Sprint(tt.want) {
			t.Errorf("%s: kept %v, want %v", tt.name, got, tt.want)
		}
	}
}

// botHub serves two open PRs by bots against a repo whose policy lists only
// renovate[bot]: PR 1 is a green Renovate patch update of lock files, PR 2 the
// same change by an unlisted bot. It records every merge and every request
// that would start or claim a review.
type botHub struct {
	mu      sync.Mutex
	merges  []string
	reviews []string
	touched []string
	policy  string
	// conflicting makes the PRs report mergeable false and state dirty;
	// trailer is appended to their bodies.
	conflicting bool
	trailer     string
}

const (
	botHead1 = "1111111111111111111111111111111111111111"
	botHead2 = "2222222222222222222222222222222222222222"
	botBase  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func (h *botHub) pull(n int, head, login string) string {
	body := "| Package | Update | Change |\\n|---|---|---|\\n| vite | patch | [`8.3.4` -> `8.3.6`](https://example.test) |\\n"
	mergeable, state := true, "clean"
	if h.conflicting {
		mergeable, state = false, "dirty"
	}
	return fmt.Sprintf(`{"number":%d,"state":"open","title":"update vite","body":%q,"changed_files":1,"head":{"sha":%q,"ref":"deps%d"},"base":{"ref":"main","sha":%q},"user":{"login":%q,"type":"Bot"},"mergeable":%t,"mergeable_state":%q}`, n, strings.ReplaceAll(body, "\\n", "\n")+h.trailer, head, n, botBase, login, mergeable, state)
}

func (h *botHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	path := r.URL.Path
	switch {
	case path == "/graphql":
		body, _ := io.ReadAll(r.Body)
		head := botHead1
		if strings.Contains(string(body), `"number":2`) || strings.Contains(string(body), "number:2") {
			head = botHead2
		}
		fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"author":{"login":"renovate","__typename":"Bot"},"headRefOid":%q,"reviewDecision":"","reviews":{"nodes":[]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`, head)
	case strings.HasSuffix(path, "/pulls"):
		if r.URL.Query().Get("state") == "closed" {
			fmt.Fprint(w, `[]`)
			return
		}
		fmt.Fprintf(w, "[%s,%s]", h.pull(1, botHead1, "renovate[bot]"), h.pull(2, botHead2, "dependabot[bot]"))
	case strings.HasSuffix(path, "/pulls/1"):
		fmt.Fprint(w, h.pull(1, botHead1, "renovate[bot]"))
	case strings.HasSuffix(path, "/pulls/2"):
		h.touched = append(h.touched, path)
		fmt.Fprint(w, h.pull(2, botHead2, "dependabot[bot]"))
	case strings.HasSuffix(path, "/pulls/1/merge"):
		var in struct{ SHA string }
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &in)
		h.merges = append(h.merges, in.SHA)
		fmt.Fprint(w, `{"merged":true}`)
	case strings.Contains(path, "/git/ref/heads/mergewake/") || strings.Contains(path, "/git/refs"):
		h.reviews = append(h.reviews, r.Method+" "+path)
		http.Error(w, `{"message":"Not Found"}`, 404)
	case strings.HasSuffix(path, "/git/ref/heads/main"):
		fmt.Fprintf(w, `{"object":{"sha":%q}}`, botBase)
	case strings.HasSuffix(path, "/git/trees/tree"):
		fmt.Fprint(w, `{"tree":[{"path":"policy.yaml","mode":"100644","type":"blob","sha":"blob"}],"truncated":false}`)
	case strings.HasSuffix(path, "/git/blobs/blob"):
		fmt.Fprintf(w, `{"content":%q,"encoding":"base64","size":%d}`, base64.StdEncoding.EncodeToString([]byte(h.policy)), len(h.policy))
	case strings.HasSuffix(path, "/actions/runs"):
		fmt.Fprint(w, `{"workflow_runs":[]}`)
	case strings.HasSuffix(path, "/check-runs"):
		fmt.Fprint(w, `{"check_runs":[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]}`)
	case strings.HasSuffix(path, "/files"):
		fmt.Fprint(w, `[{"filename":"package-lock.json","status":"modified","additions":1,"deletions":1,"changes":2}]`)
	case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/reviews"), strings.HasSuffix(path, "/comments"):
		fmt.Fprint(w, `[]`)
	case strings.Contains(path, "/compare/"):
		fmt.Fprintf(w, `{"status":"ahead","merge_base_commit":{"sha":%q}}`, botBase)
	case strings.Contains(path, "/commits/"):
		fmt.Fprint(w, `{"commit":{"tree":{"sha":"tree"},"message":"update"}}`)
	default:
		http.Error(w, `{"message":"Not Found"}`, 404)
	}
}

func pollBotHub(t *testing.T, policy string, o pipeline.Options) *botHub {
	t.Helper()
	h := &botHub{policy: policy}
	srv := httptest.NewServer(h)
	defer srv.Close()
	repo := config.Repo{Name: "o/one", Branch: "main", Policy: "policy.yaml"}
	skip := true
	cfg := config.Config{StateDir: t.TempDir(), Repos: []config.Repo{repo}, WatchLimit: 10, Review: config.Review{Login: "reviewer"}}
	cfg.Daemon.PR.SkipBots = skip
	gh := github.New("")
	gh.BaseURL = srv.URL
	gh.Stderr = io.Discard
	s := &pipeline.Service{Config: cfg, GH: gh, Runner: integrations.Exec{}, Sink: integrations.Noop{}, Notifier: integrations.Noop{}, Out: io.Discard}
	_ = pollPRs(context.Background(), s, gh, repo, o)
	return h
}

// A pass lands the listed bot's patch PR, bound to its head, without claiming
// or starting any review, and does not touch the unlisted bot's PR.
func TestPollPRsLandsListedBotPatchWithoutReview(t *testing.T) {
	h := pollBotHub(t, "version: 1\nenabled: true\nrequired_checks: [CI]\nbot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [\"**/package-lock.json\"]\n  land: patch\n", pipeline.Options{})
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.merges) != 1 || h.merges[0] != botHead1 {
		t.Fatalf("merges=%v, want one bound to %s", h.merges, botHead1)
	}
	if len(h.reviews) != 0 {
		t.Fatalf("a review was claimed or started: %v", h.reviews)
	}
	if len(h.touched) != 0 {
		t.Fatalf("the unlisted bot's PR was handled: %v", h.touched)
	}
}

// (h) Without bot_updates the same pass drops both bots, as before.
func TestPollPRsWithoutBotUpdatesDropsEveryBot(t *testing.T) {
	h := pollBotHub(t, "version: 1\nenabled: true\nrequired_checks: [CI]\n", pipeline.Options{})
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.merges) != 0 || len(h.reviews) != 0 || len(h.touched) != 0 {
		t.Fatalf("merges=%v reviews=%v touched=%v", h.merges, h.reviews, h.touched)
	}
}

// (i) A shadow pass decides but never merges.
func TestPollPRsShadowNeverMergesBotPatch(t *testing.T) {
	h := pollBotHub(t, "version: 1\nenabled: true\nrequired_checks: [CI]\nbot_updates:\n  authors: [\"renovate[bot]\"]\n  paths: [\"**/package-lock.json\"]\n  land: patch\n", pipeline.Options{Shadow: true})
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.merges) != 0 || len(h.reviews) != 0 {
		t.Fatalf("merges=%v reviews=%v", h.merges, h.reviews)
	}
}
