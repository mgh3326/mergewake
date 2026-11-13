package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

var updateFeedGolden = flag.Bool("update-feed-golden", false, "rewrite testdata/feed")

var feedNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

const feedToken = "s3cr3t-webhook-token"

// feedGH adds what the feed reads beyond fakeGH: PRs by number and the closed
// list.
type feedGH struct {
	*fakeGH
	pulls                  map[int]github.Pull
	closed                 []github.Pull
	closedReads, pullReads int
	pullErr                error
}

func (g *feedGH) Pull(_ context.Context, _ string, n int) (github.Pull, error) {
	g.pullReads++
	if g.pullErr != nil {
		return github.Pull{}, g.pullErr
	}
	return g.pulls[n], nil
}

func (g *feedGH) ClosedPulls(context.Context, string, string) ([]github.Pull, error) {
	g.closedReads++
	return g.closed, nil
}

// feedHook is a fake Discord: one path per channel, recording what arrived and
// answering through respond (204 when nil).
type feedHook struct {
	*httptest.Server
	mu      sync.Mutex
	got     []feedPost
	respond func(w http.ResponseWriter, r *http.Request, n int) bool
}

type feedPost struct {
	Channel string
	Body    []byte
	At      time.Time
}

func newFeedHook(t *testing.T) *feedHook {
	t.Helper()
	h := &feedHook{}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body bytes.Buffer
		_, _ = body.ReadFrom(r.Body)
		h.mu.Lock()
		h.got = append(h.got, feedPost{strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/"+feedToken), "/"), body.Bytes(), time.Now()})
		n := len(h.got)
		respond := h.respond
		h.mu.Unlock()
		if respond == nil || !respond(w, r, n) {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *feedHook) posts() []feedPost {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]feedPost(nil), h.got...)
}

func (h *feedHook) wait(t *testing.T, n int) []feedPost {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := h.posts(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("got %d posts, want %d", len(h.posts()), n)
	return nil
}

// feedLog is the sender's log, read by a test while the sender may write it.
type feedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *feedLog) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(b)
}

func (l *feedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func feedConfig(t *testing.T, h *feedHook, dir string) config.Discord {
	t.Helper()
	file := filepath.Join(dir, "discord.env")
	var b strings.Builder
	for _, kind := range []string{"PR", "CI", "REVERT"} {
		fmt.Fprintf(&b, "HOOK_%s=%s/%s/%s\n", kind, h.URL, strings.ToLower(kind), feedToken)
	}
	if e := os.WriteFile(file, []byte(b.String()), 0600); e != nil {
		t.Fatal(e)
	}
	return config.Discord{
		Enabled:      true,
		WebhooksFile: file,
		Kinds:        map[string]string{"pr": "HOOK_PR", "ci": "HOOK_CI", "revert": "HOOK_REVERT"},
	}
}

func feedSetup(t *testing.T) (*Service, *feedGH, *feedHook, config.Repo, *feedLog) {
	t.Helper()
	s, base, _, _, r := setup(t)
	g := &feedGH{fakeGH: base, pulls: map[int]github.Pull{}}
	s.GH = g
	s.Now = func() time.Time { return feedNow }
	h := newFeedHook(t)
	s.Config.Discord = feedConfig(t, h, t.TempDir())
	logs := &feedLog{}
	s.Feed = NewFeed(s.Config, slog.New(slog.NewJSONHandler(logs, nil)))
	s.Feed.FirstRetry = 5 * time.Millisecond
	return s, g, h, r, logs
}

// runFeed runs the sender until the test ends.
func runFeed(t *testing.T, f *Feed) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// holdEvent is a revert-channel event for the delivery tests. They exercise the
// outbox, retries and dedupe, not the event's content.
func holdEvent(r config.Repo, head string, at time.Time) feedEvent {
	return feedEvent{Repo: r.Name, Head: head, Kind: "revert.hold", Message: watchMessage(r, watchResult{Head: head, Action: "hold"}, at)}
}

func feedPull(n int, head string) github.Pull {
	return github.Pull{
		Number: n, State: "open", Title: "Add feature", HTMLURL: fmt.Sprintf("https://github.com/example/project/pull/%d", n),
		CreatedAt: feedNow.Add(time.Minute).Format(time.RFC3339),
		User:      github.PullUser{Login: "pr-author", Type: "User"},
		Head:      github.Ref{SHA: head, Ref: "feature/thing"}, Base: github.Ref{Ref: "main"},
		ChangedFiles: 3, Additions: 40, Deletions: 2,
	}
}

func outboxLen(t *testing.T, f *Feed) int {
	t.Helper()
	files, e := os.ReadDir(f.outboxDir())
	if errors.Is(e, os.ErrNotExist) {
		return 0
	}
	if e != nil {
		t.Fatal(e)
	}
	return len(files)
}

func checkGolden(t *testing.T, name string, body []byte) {
	t.Helper()
	var pretty bytes.Buffer
	if e := json.Indent(&pretty, body, "", "  "); e != nil {
		t.Fatal(e)
	}
	pretty.WriteByte('\n')
	file := filepath.Join("testdata", "feed", name+".json")
	if *updateFeedGolden {
		if e := os.WriteFile(file, pretty.Bytes(), 0644); e != nil {
			t.Fatal(e)
		}
	}
	want, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(pretty.Bytes(), want) {
		t.Fatalf("%s differs:\n%s", file, pretty.String())
	}
}

// Every embed kind, produced by its real hook and read back from the fake
// webhook, so the golden covers both the bytes and the channel they went to.
func TestFeedGolden(t *testing.T) {
	merged := feedNow.Add(2 * time.Minute).Format(time.RFC3339)
	for _, tt := range []struct {
		name, channel string
		run           func(t *testing.T, s *Service, g *feedGH, r config.Repo)
	}{
		{"pr-opened", "pr", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			g.pulls[7] = feedPull(7, shaA)
			s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, shaA)}, Options{})
		}},
		{"pr-merged", "pr", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			pr := feedPull(7, shaA)
			pr.State, pr.MergedAt, pr.ClosedAt = "closed", &merged, &merged
			g.pulls[7], g.closed = pr, []github.Pull{pr}
			s.FeedPRs(context.Background(), r, nil, Options{})
		}},
		{"pr-closed", "pr", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			pr := feedPull(7, shaA)
			pr.State, pr.ClosedAt = "closed", &merged
			g.pulls[7], g.closed = pr, []github.Pull{pr}
			s.FeedPRs(context.Background(), r, nil, Options{})
		}},
		{"ci-pr", "ci", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			g.required = []string{"test", "lint", "build"}
			g.checkNames = map[string]map[string]github.CI{shaA: {"test": github.Red, "lint": github.Green, "build": github.Red}}
			g.ci = github.Green
			g.pulls[7] = feedPull(7, shaA)
			s.Config.Discord.Kinds = map[string]string{"ci": "HOOK_CI"}
			s.Feed.Config = s.Config.Discord
			s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, shaA)}, Options{})
		}},
		{"ci-branch", "ci", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			g.head, g.ci = shaB, github.Red
			g.required = []string{"test"}
			g.authorName = "A Committer"
			g.messages = map[string]string{shaB: "fix: the subject line\n\nthe body stays out"}
			_ = s.Watch(context.Background(), r, Options{Shadow: true})
		}},
		{"revert-hold", "revert", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			w := watchResult{Head: shaB, Action: "hold", Reason: "culprit is not a single identifiable merged PR", Candidates: []int{7, 8}}
			if e := s.reportWatch(context.Background(), r, w, shaB+":x", Options{}); e != nil {
				t.Fatal(e)
			}
		}},
		{"revert-culprit", "revert", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			w := watchResult{Head: shaB, Action: "report-culprit", Culprit: shaA, PR: 7}
			if e := s.reportWatch(context.Background(), r, w, shaB+":"+shaA, Options{}); e != nil {
				t.Fatal(e)
			}
		}},
		{"revert-opened", "revert", func(t *testing.T, s *Service, g *feedGH, r config.Repo) {
			g.enabled, g.head = true, shaC
			g.parents = map[string]string{shaC: shaB, shaB: shaA}
			g.checks = map[string]github.CI{shaC: github.Red, shaB: github.Red, shaA: github.Green}
			g.pull.MergedAt, g.pull.MergeCommitSHA, g.pull.Body = &merged, shaB, "Mergewake-Task: 42"
			g.pulls[7] = g.pull
			s.Config.Sink.Type = "handoffkeep"
			s.Config.Discord.Kinds = map[string]string{"revert": "HOOK_REVERT"}
			s.Feed.Config = s.Config.Discord
			if e := s.saveRecovery(r.Name, recoveryIncident{
				RedSince: feedNow, Culprit: shaB, CulpritPR: 7, Head: shaC, Baseline: shaA, Task: "42",
				Runs: map[string][]bool{shaA: {true, true}, shaB: {false, false}}, Confirmed: true,
			}); e != nil {
				t.Fatal(e)
			}
			if e := s.Watch(context.Background(), r, Options{}); e != nil {
				t.Fatal(e)
			}
			if g.revertCount != 1 {
				t.Fatalf("%d reverts", g.revertCount)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, h, r, logs := feedSetup(t)
			tt.run(t, s, g, r)
			runFeed(t, s.Feed)
			var got feedPost
			for _, p := range h.wait(t, 1) {
				if p.Channel == tt.channel {
					got = p
				}
			}
			if got.Channel == "" {
				t.Fatalf("nothing on %s: %+v (%s)", tt.channel, h.posts(), logs)
			}
			checkGolden(t, tt.name, got.Body)
		})
	}
}

func TestFeedDedupeAcrossRestart(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	g.pulls[7] = feedPull(7, shaA)
	open := []github.Pull{feedPull(7, shaA)}
	s.FeedPRs(context.Background(), r, open, Options{})
	s.FeedPRs(context.Background(), r, open, Options{}) // queued, not yet sent
	s.Feed.drain(context.Background())
	if got := h.posts(); len(got) != 1 {
		t.Fatalf("%d posts", len(got))
	}
	reads := g.pullReads

	restarted := NewFeed(s.Config, nil)
	s.Feed = restarted
	s.FeedPRs(context.Background(), r, open, Options{})
	// A force push is the same PR, not a newly opened one.
	s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, shaB)}, Options{})
	restarted.drain(context.Background())
	if got := h.posts(); len(got) != 1 {
		t.Fatalf("%d posts after restart", len(got))
	}
	if g.pullReads != reads {
		t.Fatalf("a claimed key must not read the PR again: %d -> %d", reads, g.pullReads)
	}
}

// CI dedupes by head: the same head stays quiet, a new red head posts again.
func TestFeedCIOncePerHead(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	g.ci = github.Red
	g.pulls[7] = feedPull(7, shaA)
	for _, head := range []string{shaA, shaA, shaB} {
		s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, head)}, Options{})
	}
	s.Feed.drain(context.Background())
	ci := 0
	for _, p := range h.posts() {
		if p.Channel == "ci" {
			ci++
		}
	}
	if ci != 2 {
		t.Fatalf("%d ci posts, want 2", ci)
	}
}

// What was queued survives a restart and posts once. A crash after recording
// delivery but before clearing the outbox does not post again.
func TestFeedOutboxSurvivesRestart(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	g.pulls[7] = feedPull(7, shaA)
	s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, shaA)}, Options{})
	if len(h.posts()) != 0 || outboxLen(t, s.Feed) != 1 {
		t.Fatalf("posts=%d outbox=%d", len(h.posts()), outboxLen(t, s.Feed))
	}
	restarted := NewFeed(s.Config, nil)
	restarted.drain(context.Background())
	if len(h.posts()) != 1 || outboxLen(t, restarted) != 0 {
		t.Fatalf("posts=%d outbox=%d", len(h.posts()), outboxLen(t, restarted))
	}

	ev := holdEvent(r, shaA, feedNow)
	if e := restarted.Emit(ev); e != nil {
		t.Fatal(e)
	}
	sent, e := loadSentKeys(restarted.sentFile(r.Name), feedSentKeep)
	if e != nil {
		t.Fatal(e)
	}
	if e = sent.add(ev.key()); e != nil {
		t.Fatal(e)
	}
	restarted.drain(context.Background())
	if len(h.posts()) != 1 || outboxLen(t, restarted) != 0 {
		t.Fatalf("posts=%d outbox=%d", len(h.posts()), outboxLen(t, restarted))
	}
}

// A 429 is honoured for as long as it says, whatever else is queued, and does
// not count as a failure: more 429s than max_failures still deliver.
func TestFeedHonours429RetryAfter(t *testing.T) {
	s, _, h, r, logs := feedSetup(t)
	s.Feed.Config.MaxFailures = 1
	h.respond = func(w http.ResponseWriter, _ *http.Request, n int) bool {
		if n > 2 {
			return false
		}
		w.Header().Set("Retry-After", "0.25")
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	}
	if e := s.Feed.Emit(holdEvent(r, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	runFeed(t, s.Feed)
	got := h.wait(t, 3)
	for i := 1; i < 3; i++ {
		if gap := got[i].At.Sub(got[i-1].At); gap < 250*time.Millisecond {
			t.Fatalf("retry %d came after %s, before Retry-After", i, gap)
		}
	}
	if strings.Contains(logs.String(), "dropped") {
		t.Fatalf("a rate limit must not drop: %s", logs)
	}
}

// The hook returns while the webhook hangs: the poll loop only ever writes the
// outbox.
func TestFeedHungWebhookDoesNotStallPoll(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 16)
	h.respond = func(http.ResponseWriter, *http.Request, int) bool {
		entered <- struct{}{}
		<-release
		return false
	}
	t.Cleanup(func() { close(release) })
	var open []github.Pull
	for n := 1; n <= 5; n++ {
		g.pulls[n] = feedPull(n, shaA)
		open = append(open, feedPull(n, shaA))
	}
	runFeed(t, s.Feed)
	s.FeedPRs(context.Background(), r, open[:1], Options{})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the sender never reached the webhook")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.FeedPRs(context.Background(), r, open, Options{})
		s.feedWatch(r, watchResult{Head: shaA, Action: "hold"}, Options{})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a hung webhook stalled the poll loop")
	}
}

// A failing webhook gets max_failures attempts, then one log line with no URL
// in it, and the event does not come back. Another channel keeps delivering.
func TestFeedDownWebhookDropsToLogLine(t *testing.T) {
	s, _, h, r, logs := feedSetup(t)
	s.Feed.Config.MaxFailures = 3
	h.respond = func(w http.ResponseWriter, req *http.Request, _ int) bool {
		if strings.HasPrefix(req.URL.Path, "/revert/") {
			w.WriteHeader(http.StatusBadGateway)
			return true
		}
		return false
	}
	if e := s.Feed.Emit(holdEvent(r, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	if e := s.Feed.Emit(feedEvent{Repo: r.Name, PR: 7, Head: shaA, Kind: "pr.opened", Message: prMessage(r, feedPull(7, shaA), "pr.opened", feedNow)}); e != nil {
		t.Fatal(e)
	}
	runFeed(t, s.Feed)
	h.wait(t, 4)
	deadline := time.Now().Add(5 * time.Second)
	for outboxLen(t, s.Feed) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	reverts, prs := 0, 0
	for _, p := range h.posts() {
		switch p.Channel {
		case "revert":
			reverts++
		case "pr":
			prs++
		}
	}
	if reverts != 3 || prs != 1 {
		t.Fatalf("revert=%d pr=%d", reverts, prs)
	}
	line := logs.String()
	if strings.Count(line, "discord message dropped") != 1 || !strings.Contains(line, "3 failures, last: HTTP 502") {
		t.Fatalf("log: %s", line)
	}
	if strings.Contains(line, feedToken) || strings.Contains(line, h.URL) {
		t.Fatalf("log leaks the webhook: %s", line)
	}
	if e := s.Feed.Emit(holdEvent(r, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	if outboxLen(t, s.Feed) != 0 {
		t.Fatal("a dropped event must stay claimed")
	}
}

// A connection error also counts, and its text carries no URL.
func TestFeedUnreachableWebhookLeaksNothing(t *testing.T) {
	s, _, h, r, logs := feedSetup(t)
	s.Feed.Config.MaxFailures = 1
	h.Close()
	if e := s.Feed.Emit(holdEvent(r, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	s.Feed.drain(context.Background())
	if line := logs.String(); !strings.Contains(line, "discord message dropped") || strings.Contains(line, feedToken) {
		t.Fatalf("log: %s", line)
	}
}

// A refused request cannot succeed on retry: one attempt, one log line.
func TestFeedRefusedDropsAtOnce(t *testing.T) {
	s, _, h, r, logs := feedSetup(t)
	h.respond = func(w http.ResponseWriter, _ *http.Request, _ int) bool {
		w.WriteHeader(http.StatusNotFound)
		return true
	}
	if e := s.Feed.Emit(holdEvent(r, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	s.Feed.drain(context.Background())
	if len(h.posts()) != 1 || outboxLen(t, s.Feed) != 0 || !strings.Contains(logs.String(), "refused: HTTP 404") {
		t.Fatalf("posts=%d outbox=%d log=%s", len(h.posts()), outboxLen(t, s.Feed), logs)
	}
}

// An opted-out repo posts nothing through any path, and spends no API reads.
func TestFeedOptOutPostsNothing(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	s.Config.Discord.OptOut = []string{r.Name}
	s.Feed = NewFeed(s.Config, nil)
	g.ci = github.Red
	g.head = shaB
	g.pulls[7] = feedPull(7, shaA)
	merged := feedNow.Add(time.Minute).Format(time.RFC3339)
	closed := feedPull(8, shaC)
	closed.MergedAt, closed.ClosedAt = &merged, &merged
	g.pulls[8], g.closed = closed, []github.Pull{closed}

	ctx := context.Background()
	s.FeedPRs(ctx, r, []github.Pull{feedPull(7, shaA)}, Options{})
	s.feedBranchCI(ctx, r, shaB, []string{"CI"}, Options{})
	s.feedWatch(r, watchResult{Head: shaB, Action: "hold"}, Options{})
	s.Feed.drain(ctx)
	if len(h.posts()) != 0 || outboxLen(t, s.Feed) != 0 {
		t.Fatalf("posts=%d outbox=%d", len(h.posts()), outboxLen(t, s.Feed))
	}
	if g.closedReads != 0 || g.pullReads != 0 {
		t.Fatalf("closed=%d pull=%d", g.closedReads, g.pullReads)
	}

	// Opting out also stops what was queued before the config changed.
	s2, _, h2, r2, _ := feedSetup(t)
	if e := s2.Feed.Emit(holdEvent(r2, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	s2.Config.Discord.OptOut = []string{r2.Name}
	later := NewFeed(s2.Config, nil)
	later.drain(ctx)
	if len(h2.posts()) != 0 || outboxLen(t, later) != 0 {
		t.Fatalf("posts=%d outbox=%d", len(h2.posts()), outboxLen(t, later))
	}
}

// A kind with no channel posts nothing and reads nothing, while another kind
// on the same repo still works.
func TestFeedUnmappedKindIsOff(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	s.Config.Discord.Kinds = map[string]string{"revert": "HOOK_REVERT"}
	s.Feed = NewFeed(s.Config, nil)
	g.ci = github.Red
	g.pulls[7] = feedPull(7, shaA)
	s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, shaA)}, Options{})
	s.feedWatch(r, watchResult{Head: shaA, Action: "hold"}, Options{})
	s.Feed.drain(context.Background())
	got := h.posts()
	if len(got) != 1 || got[0].Channel != "revert" || g.closedReads != 0 {
		t.Fatalf("%+v closed=%d", got, g.closedReads)
	}
}

// Turning the feed on does not replay history: PRs from before its first pass
// stay quiet, later ones post, and the baseline outlives a restart.
func TestFeedBaselineSkipsHistory(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	before := feedNow.Add(-time.Hour).Format(time.RFC3339)
	after := feedNow.Add(time.Hour).Format(time.RFC3339)
	old := feedPull(1, shaA)
	old.CreatedAt = before
	oldMerged := feedPull(2, shaA)
	oldMerged.MergedAt, oldMerged.ClosedAt = &before, &before
	g.pulls[1], g.pulls[2], g.closed = old, oldMerged, []github.Pull{oldMerged}
	s.FeedPRs(context.Background(), r, []github.Pull{old}, Options{})
	s.Feed.drain(context.Background())
	if got := h.posts(); len(got) != 0 {
		t.Fatalf("history posted: %s", got[0].Body)
	}

	s.Now = func() time.Time { return feedNow.Add(2 * time.Hour) }
	s.Feed = NewFeed(s.Config, nil)
	merged := feedPull(3, shaB)
	merged.MergedAt, merged.ClosedAt = &after, &after
	g.pulls[3], g.closed = merged, []github.Pull{oldMerged, merged}
	s.FeedPRs(context.Background(), r, []github.Pull{old}, Options{})
	s.Feed.drain(context.Background())
	got := h.posts()
	if len(got) != 1 || !strings.Contains(string(got[0].Body), "example/project#3") {
		t.Fatalf("%+v", got)
	}
}

// A comment on an old PR puts it back in the closed list; past the window it
// stays quiet even after its key left the bounded sent file.
func TestFeedClosedWindow(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	s.FeedPRs(context.Background(), r, nil, Options{}) // sets the baseline
	merged := feedNow.Add(time.Minute).Format(time.RFC3339)
	pr := feedPull(7, shaA)
	pr.MergedAt, pr.ClosedAt = &merged, &merged
	g.pulls[7], g.closed = pr, []github.Pull{pr}
	s.Now = func() time.Time { return feedNow.Add(feedClosedWindow + 2*time.Minute) }
	s.FeedPRs(context.Background(), r, nil, Options{})
	s.Feed.drain(context.Background())
	if got := h.posts(); len(got) != 0 {
		t.Fatalf("%s", got[0].Body)
	}
}

// A draft posts only once it is ready; a bot PR stays out under skip_bots.
func TestFeedDraftsAndBots(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	s.Config.Daemon.PR.SkipBots = true
	draft := feedPull(7, shaA)
	draft.Draft = true
	g.ci = github.Red
	merged := feedNow.Add(time.Minute).Format(time.RFC3339)
	bot := feedPull(9, shaC)
	bot.User.Type = "Bot"
	bot.MergedAt, bot.ClosedAt = &merged, &merged
	g.pulls[7], g.pulls[9], g.closed = feedPull(7, shaA), bot, []github.Pull{bot}
	s.Config.Discord.Kinds = map[string]string{"pr": "HOOK_PR", "ci": "HOOK_CI"}
	s.Feed = NewFeed(s.Config, nil)
	s.FeedPRs(context.Background(), r, []github.Pull{draft}, Options{})
	s.Feed.drain(context.Background())
	if got := h.posts(); len(got) != 0 {
		t.Fatalf("%s", got[0].Body)
	}
	s.FeedPRs(context.Background(), r, []github.Pull{feedPull(7, shaA)}, Options{})
	s.Feed.drain(context.Background())
	got := h.posts()
	if len(got) != 2 || got[0].Channel != "pr" || got[1].Channel != "ci" {
		t.Fatalf("%+v", got)
	}
}

// A dry run posts nothing and writes nothing; shadow still reports PRs but not
// a revert that did not happen.
func TestFeedDryRunAndShadow(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	g.pulls[7] = feedPull(7, shaA)
	open := []github.Pull{feedPull(7, shaA)}
	s.FeedPRs(context.Background(), r, open, Options{DryRun: true})
	s.feedWatch(r, watchResult{Head: shaA, Action: "hold"}, Options{DryRun: true})
	if outboxLen(t, s.Feed) != 0 {
		t.Fatal("dry run queued")
	}
	if _, e := os.Stat(filepath.Join(s.state(r.Name), "discord-baseline")); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("dry run wrote state: %v", e)
	}
	w := watchResult{Head: shaB, Action: "hold", Shadow: true}
	if e := s.reportWatch(context.Background(), r, w, shaB+":x", Options{Shadow: true}); e != nil {
		t.Fatal(e)
	}
	if outboxLen(t, s.Feed) != 0 {
		t.Fatal("shadow hold queued")
	}
	s.FeedPRs(context.Background(), r, open, Options{Shadow: true})
	s.Feed.drain(context.Background())
	if got := h.posts(); len(got) != 1 || got[0].Channel != "pr" {
		t.Fatalf("%+v", got)
	}
}

// A failed read claims nothing, so the next pass posts the event; the problem
// is recorded, and the role never sees an error to back off on.
func TestFeedReadFailureRetriesNextPass(t *testing.T) {
	s, g, h, r, _ := feedSetup(t)
	g.pulls[7] = feedPull(7, shaA)
	open := []github.Pull{feedPull(7, shaA)}
	g.pullErr = errors.New("boom")
	s.FeedPRs(context.Background(), r, open, Options{})
	if outboxLen(t, s.Feed) != 0 {
		t.Fatal("queued on a failed read")
	}
	b, e := os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl"))
	if e != nil || !strings.Contains(string(b), "discord-feed-error") {
		t.Fatalf("%s %v", b, e)
	}
	g.pullErr = github.ErrPollSkipped
	before := len(b)
	s.FeedPRs(context.Background(), r, open, Options{})
	if b, _ = os.ReadFile(filepath.Join(s.state(r.Name), "decisions.jsonl")); len(b) != before {
		t.Fatalf("a skipped poll is not an error: %s", b[before:])
	}
	g.pullErr = nil
	s.FeedPRs(context.Background(), r, open, Options{})
	s.Feed.drain(context.Background())
	if len(h.posts()) != 1 {
		t.Fatalf("%d posts", len(h.posts()))
	}
}

// Messages wait for a webhooks file that is not there yet instead of dropping,
// and a rotated URL takes effect without a restart.
func TestFeedWaitsForWebhooksFile(t *testing.T) {
	s, _, h, r, logs := feedSetup(t)
	saved, e := os.ReadFile(s.Config.Discord.WebhooksFile)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(s.Config.Discord.WebhooksFile); e != nil {
		t.Fatal(e)
	}
	if e = s.Feed.Emit(holdEvent(r, shaA, feedNow)); e != nil {
		t.Fatal(e)
	}
	if wait := s.Feed.drain(context.Background()); wait != feedFileRetry {
		t.Fatalf("wait=%s", wait)
	}
	if outboxLen(t, s.Feed) != 1 || !strings.Contains(logs.String(), "webhooks file unreadable") {
		t.Fatalf("outbox=%d log=%s", outboxLen(t, s.Feed), logs)
	}
	if e = os.WriteFile(s.Config.Discord.WebhooksFile, saved, 0600); e != nil {
		t.Fatal(e)
	}
	s.Feed.drain(context.Background())
	if len(h.posts()) != 1 {
		t.Fatalf("%d posts", len(h.posts()))
	}
}

// A full outbox drops the new message to a log line and keeps its key claimed.
func TestFeedOutboxFullDrops(t *testing.T) {
	s, _, _, r, logs := feedSetup(t)
	if e := os.MkdirAll(s.Feed.outboxDir(), 0700); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < feedOutboxMax; i++ {
		if e := os.WriteFile(filepath.Join(s.Feed.outboxDir(), fmt.Sprintf("%d.json", i)), []byte("{}"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	ev := holdEvent(r, shaA, feedNow)
	if e := s.Feed.Emit(ev); e != nil {
		t.Fatal(e)
	}
	if outboxLen(t, s.Feed) != feedOutboxMax || !strings.Contains(logs.String(), "outbox full") {
		t.Fatalf("outbox=%d log=%s", outboxLen(t, s.Feed), logs)
	}
	seen, e := s.Feed.seen(r.Name)
	if e != nil || !seen(ev.key()) {
		t.Fatalf("key not claimed: %v", e)
	}
}

// Roles and the sender race on one state: a key is queued exactly once.
func TestFeedConcurrentEmitClaimsOnce(t *testing.T) {
	s, _, h, r, _ := feedSetup(t)
	ev := holdEvent(r, shaA, feedNow)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := s.Feed.Emit(ev); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	s.Feed.drain(context.Background())
	if len(h.posts()) != 1 {
		t.Fatalf("%d posts", len(h.posts()))
	}
}
