package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/outcomes"
)

// fakeHK is an hk server for POST /v1/task-events: it records every event it
// receives and answers with status (200 when zero), after delay if set.
type fakeHK struct {
	mu     sync.Mutex
	events []integrations.TaskEvent
	auth   []string
	status int
	code   string
	delay  time.Duration
	// refuseExtended answers 400 invalid_event to an event that carries any
	// head/base/merge/reviews field, as an hk that predates them does.
	refuseExtended bool
	// rules applies a copy of hk's review validation (hkRulesCheck) to every
	// event: a violation answers 400 invalid_context, as hk does.
	rules bool
	// refuseReviews answers 400 to any event that carries reviews[].
	refuseReviews bool
	// message is added to every 400 as hk's "message" field.
	message string
	// why holds the violation behind each rules refusal.
	why []string
}

func (h *fakeHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/task-events" {
		http.NotFound(w, r)
		return
	}
	raw, _ := io.ReadAll(r.Body)
	var ev integrations.TaskEvent
	_ = json.Unmarshal(raw, &ev)
	h.mu.Lock()
	h.events = append(h.events, ev)
	h.auth = append(h.auth, r.Header.Get("Authorization"))
	status, code, delay := h.status, h.code, h.delay
	if h.refuseExtended && (ev.HeadSHA != "" || ev.BaseSHA != "" || ev.MergeSHA != "" || len(ev.Reviews) > 0) {
		status, code = http.StatusBadRequest, "invalid_event"
	}
	if h.refuseReviews && len(ev.Reviews) > 0 {
		status, code = http.StatusBadRequest, "invalid_event"
	}
	if h.rules {
		if why := hkRulesCheck(raw); why != "" {
			h.why = append(h.why, why)
			status, code = http.StatusBadRequest, "invalid_context"
		}
	}
	message := h.message
	h.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if status != 0 && status != http.StatusOK {
		w.WriteHeader(status)
		body := map[string]string{"error": code}
		if message != "" {
			body["message"] = message
		}
		_ = json.NewEncoder(w).Encode(body)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"applied": true, "redelivered": false, "state": "reviewing"})
}

func (h *fakeHK) received() []integrations.TaskEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]integrations.TaskEvent(nil), h.events...)
}

// hkSink is the test sink with the real hk task-event client.
type hkSink struct {
	fakeSink
	hk integrations.Handoffkeep
}

func (s *hkSink) TaskJob(context.Context, string) (string, string, error) {
	return "", "fixture (grade B, T2)", nil
}

func (s *hkSink) PostTaskEvent(ctx context.Context, ev integrations.TaskEvent) (integrations.TaskEventResult, error) {
	return s.hk.PostTaskEvent(ctx, ev)
}

// mergeGH turns the fake PR merged once Merge succeeds, as GitHub does.
type mergeGH struct{ *fakeGH }

func (g mergeGH) Merge(ctx context.Context, repo string, n int, sha string) error {
	if e := g.fakeGH.Merge(ctx, repo, n, sha); e != nil {
		return e
	}
	at := time.Now().UTC().Format(time.RFC3339)
	g.pull.State, g.pull.Merged, g.pull.MergedAt = "closed", true, &at
	return nil
}

const testPRURL = "https://github.com/example/project/pull/7"

// hkSetup is setup with a fake hk reached through the sink's real credential
// convention (HANDOFFKEEP_URL/HANDOFFKEEP_TOKEN) and a PR bound to task 2909.
func hkSetup(t *testing.T) (*Service, *fakeGH, *fakeHK, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	hk := &fakeHK{}
	srv := httptest.NewServer(hk)
	t.Cleanup(srv.Close)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	s.Sink = &hkSink{hk: integrations.Handoffkeep{Runner: s.Runner, Command: "handoffkeep", EventAttempts: 3, EventBackoff: time.Millisecond, EventTimeout: 200 * time.Millisecond}}
	g.pull.Body = "Change.\n\nMergewake-Task: 2909\n"
	g.pull.CreatedAt = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	return s, g, hk, r
}

// taskEventAudit reads the hk-task-event rows of the repo's audit log.
func taskEventAudit(t *testing.T, s *Service, repo string) []taskEventRecord {
	t.Helper()
	f, e := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	var out []taskEventRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var row struct {
			Command string          `json:"command"`
			Value   taskEventRecord `json:"value"`
		}
		if e := json.Unmarshal(sc.Bytes(), &row); e != nil {
			t.Fatal(e)
		}
		if row.Command == "hk-task-event" {
			out = append(out, row.Value)
		}
	}
	return out
}

func wantEvent(t *testing.T, got integrations.TaskEvent, source, ref, pr, reverted, revert string) {
	t.Helper()
	if got.Source != source || got.SourceRef != ref || got.TaskID != 2909 || got.PR != pr || got.RevertedPR != reverted || got.RevertPR != revert || got.At == nil {
		t.Fatalf("event %+v, want source=%s ref=%s pr=%s reverted=%s revert=%s task=2909", got, source, ref, pr, reverted, revert)
	}
}

// Land posts mergewake.merge once, after GitHub accepted the merge.
func TestTaskEventMerge(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	g.enabled = true
	markReviewed(g)
	s.GH = mergeGH{g}
	if e := s.Land(context.Background(), r, 7, Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if g.mergeCount != 1 || len(got) != 1 {
		t.Fatalf("merges=%d events=%+v", g.mergeCount, got)
	}
	wantEvent(t, got[0], integrations.SourceMerge, testPRURL, testPRURL, "", "")
	if hk.auth[0] != "Bearer test-token" {
		t.Fatal("credential not sent")
	}
	rows := taskEventAudit(t, s, r.Name)
	if len(rows) != 1 || rows[0].Source != integrations.SourceMerge || rows[0].Task != "2909" || rows[0].Result == nil || !rows[0].Result.Applied {
		t.Fatalf("audit %+v", rows)
	}
}

// A PR that never merges, here one the gate blocks, posts no merge event.
func TestTaskEventNoMergeNoEvent(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	g.enabled = true // not reviewed: blocked
	if e := s.Land(context.Background(), r, 7, Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 0 || len(hk.received()) != 0 {
		t.Fatalf("merges=%d events=%+v", g.mergeCount, hk.received())
	}
}

// The first acting observation of an open, ready PR posts mergewake.pr; later
// polls do not post it again. Drafts, dry runs and shadow observation post
// nothing.
func TestTaskEventPROpened(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	ctx := context.Background()
	draft := g.pull
	draft.Draft = true
	for _, tc := range []struct {
		pr github.Pull
		o  Options
	}{{draft, Options{}}, {g.pull, Options{Shadow: true}}, {g.pull, Options{DryRun: true}}} {
		if e := s.ObservePR(ctx, r, tc.pr, tc.o); e != nil {
			t.Fatal(e)
		}
	}
	if got := hk.received(); len(got) != 0 {
		t.Fatalf("posted before an acting observation of a ready PR: %+v", got)
	}
	for range 2 {
		if e := s.ObservePR(ctx, r, g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	got := hk.received()
	if len(got) != 1 {
		t.Fatalf("events %+v", got)
	}
	wantEvent(t, got[0], integrations.SourcePR, testPRURL, testPRURL, "", "")
}

// A failed post is logged and not repeated by the next observation: the
// client's bounded retries are the only retries.
func TestTaskEventPRFailedNotRepeated(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.status = http.StatusBadGateway
	for range 2 {
		if e := s.ObservePR(context.Background(), r, g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if got := hk.received(); len(got) != 3 {
		t.Fatalf("tries %d, want the 3 of one call", len(got))
	}
	rows := taskEventAudit(t, s, r.Name)
	if len(rows) != 1 || rows[0].Source != integrations.SourcePR || !strings.Contains(rows[0].Error, "HTTP 502") {
		t.Fatalf("audit %+v", rows)
	}
}

// Review posts mergewake.review once, when it holds the pass claim and runs
// a reviewer; a head already reviewed posts nothing.
func TestTaskEventReview(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if g.postCount != 1 || len(got) != 1 {
		t.Fatalf("reviews=%d events=%+v", g.postCount, got)
	}
	wantEvent(t, got[0], integrations.SourceReview, testPRURL, testPRURL, "", "")
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	if len(hk.received()) != 1 {
		t.Fatal("an already reviewed head posted a review event")
	}
}

// mergewake.revert waits for the revert PR to merge, then posts once with the
// revert PR as source_ref.
func TestTaskEventRevert(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	now := time.Now().UTC()
	merged := now.Add(-2 * time.Hour).Format(time.RFC3339)
	g.pull.State, g.pull.Merged, g.pull.MergedAt = "closed", true, &merged
	source := &outcomeGH{fakeGH: g, first: true, recovery: github.Pull{Number: 9, State: "open"}}
	s.GH = source
	ctx := context.Background()
	if e := s.ObservePR(ctx, r, g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	if e := s.ledger().Update(r.Name, 7, func(v *outcomes.Outcome) { v.RevertPR = 9 }); e != nil {
		t.Fatal(e)
	}
	if e := s.ObservePR(ctx, r, g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
	if got := hk.received(); len(got) != 0 {
		t.Fatalf("an open revert PR posted: %+v", got)
	}
	reverted := now.Add(-time.Hour).Format(time.RFC3339)
	source.recovery.State, source.recovery.MergedAt = "closed", &reverted
	for range 2 {
		if e := s.ObservePR(ctx, r, g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	got := hk.received()
	if len(got) != 1 {
		t.Fatalf("events %+v", got)
	}
	revertURL := "https://github.com/example/project/pull/9"
	wantEvent(t, got[0], integrations.SourceRevert, revertURL, testPRURL, testPRURL, revertURL)
	if got[0].At.Format(time.RFC3339) != reverted {
		t.Fatalf("at %v, want the revert merge time", got[0].At)
	}
}

// Without a Mergewake-Task trailer, in the body or the head commit, and with
// hk_task_events off, no event point posts anything.
func TestTaskEventSkipped(t *testing.T) {
	off := false
	for name, prep := range map[string]func(*fakeGH, *config.Repo){
		"no trailer": func(g *fakeGH, _ *config.Repo) { g.pull.Body = "Change.\n" },
		"config off": func(_ *fakeGH, r *config.Repo) { r.HKTaskEvents = &off },
	} {
		t.Run(name, func(t *testing.T) {
			s, g, hk, r := hkSetup(t)
			prep(g, &r)
			g.enabled = true
			ctx := context.Background()
			if e := s.Review(ctx, r, 7, "openai", Options{}); e != nil && !(name == "no trailer" && errors.Is(e, ErrReviewUnavailable)) {
				t.Fatal(e)
			}
			markReviewed(g)
			s.GH = mergeGH{g}
			if e := s.Land(ctx, r, 7, Options{}); e != nil {
				t.Fatal(e)
			}
			wantPosts := 1
			if name == "no trailer" {
				wantPosts = 0
			}
			if g.postCount != wantPosts || g.mergeCount != 1 {
				t.Fatalf("reviews=%d merges=%d", g.postCount, g.mergeCount)
			}
			if got := hk.received(); len(got) != 0 {
				t.Fatalf("posted %+v", got)
			}
		})
	}
}

// An hk that fails, hangs or does not know the task leaves the merge and
// Land's result unchanged; the failure is in the audit log. A 5xx is retried
// up to EventAttempts tries, a 404 or other 4xx is not retried. The merge event
// carries head_sha, so a 400 is sent once more without the new fields (two
// tries, the second also refused).
func TestTaskEventFailureLeavesLand(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		code         string
		delay        time.Duration
		tries        int
		errorMention string
	}{
		{name: "500", status: 500, tries: 3, errorMention: "HTTP 500"},
		{name: "timeout", delay: 2 * time.Second, tries: 3, errorMention: "request failed"},
		{name: "404", status: 404, code: "not_found", tries: 1, errorMention: "HTTP 404 not_found"},
		{name: "400", status: 400, code: "invalid_request", tries: 2, errorMention: "HTTP 400 invalid_request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g, hk, r := hkSetup(t)
			hk.status, hk.code, hk.delay = tc.status, tc.code, tc.delay
			g.enabled = true
			markReviewed(g)
			s.GH = mergeGH{g}
			if e := s.Land(context.Background(), r, 7, Options{Observed: true}); e != nil {
				t.Fatalf("land returned %v", e)
			}
			if g.mergeCount != 1 || landAction(t, s, r.Name) != "merge" {
				t.Fatalf("merges=%d", g.mergeCount)
			}
			if got := hk.received(); len(got) != tc.tries {
				t.Fatalf("tries=%d want %d", len(got), tc.tries)
			}
			rows := taskEventAudit(t, s, r.Name)
			if len(rows) != 1 || rows[0].Source != integrations.SourceMerge || rows[0].Task != "2909" || rows[0].Result != nil || !strings.Contains(rows[0].Error, tc.errorMention) {
				t.Fatalf("audit %+v", rows)
			}
		})
	}
}

// revertSetup is a merged PR whose outcome row is final and attributed, with
// watch revert PR #9 recorded, so reconciliation would skip the row once its
// seven-day verdict is set.
func revertSetup(t *testing.T, mergedAgo time.Duration) (*Service, *outcomeGH, *fakeHK, config.Repo) {
	t.Helper()
	s, g, hk, r := hkSetup(t)
	merged := time.Now().UTC().Add(-mergedAgo).Format(time.RFC3339)
	g.pull.State, g.pull.Merged, g.pull.MergedAt = "closed", true, &merged
	g.pull.Body = "Mergewake-Author-Profile: example\nMergewake-Author-Model: example-model\nMergewake-Grade: B\nMergewake-Tier: T1\nMergewake-Task: 2909\n"
	source := &outcomeGH{fakeGH: g, first: true, recovery: github.Pull{Number: 9, State: "open"}}
	s.GH = source
	if e := s.ledger().Update(r.Name, 7, func(v *outcomes.Outcome) { v.RevertPR = 9 }); e != nil {
		t.Fatal(e)
	}
	return s, source, hk, r
}

// reconcile runs ReconcileOutcomes, tolerating only the outcome publish
// error of the fake store, which these tests do not exercise.
func reconcile(t *testing.T, s *Service, r config.Repo, o Options) {
	t.Helper()
	if e := s.ReconcileOutcomes(context.Background(), r, map[int]bool{}, o); e != nil && !strings.Contains(e.Error(), "retained in outbox") {
		t.Fatal(e)
	}
}

func mergeRevert(g *outcomeGH, ago time.Duration) {
	at := time.Now().UTC().Add(-ago).Format(time.RFC3339)
	g.recovery.State, g.recovery.MergedAt = "closed", &at
}

// A shadow pass that sets the reverted verdict does not use up the revert
// event: the next acting reconciliation posts it, once.
func TestTaskEventRevertAfterShadowPass(t *testing.T) {
	s, g, hk, r := revertSetup(t, 2*time.Hour)
	mergeRevert(g, time.Hour)
	reconcile(t, s, r, Options{Shadow: true})
	state, _ := s.ledger().Read()
	if v := state.Latest[outcomes.Key(r.Name, 7)]; v.Reverted == nil || !*v.Reverted || !v.Final || !v.Attributed() || v.FirstCIGreen == nil {
		t.Fatalf("shadow pass did not finalize the row: %+v", v)
	}
	if len(hk.received()) != 0 {
		t.Fatal("shadow pass posted")
	}
	for range 2 {
		reconcile(t, s, r, Options{})
	}
	got := hk.received()
	if len(got) != 1 {
		t.Fatalf("events %+v", got)
	}
	revertURL := "https://github.com/example/project/pull/9"
	wantEvent(t, got[0], integrations.SourceRevert, revertURL, testPRURL, testPRURL, revertURL)
}

// A revert PR merged after the seven-day verdict closed as not reverted still
// posts its event.
func TestTaskEventRevertAfterWindow(t *testing.T) {
	s, g, hk, r := revertSetup(t, 8*24*time.Hour)
	reconcile(t, s, r, Options{})
	state, _ := s.ledger().Read()
	if v := state.Latest[outcomes.Key(r.Name, 7)]; v.Reverted == nil || *v.Reverted {
		t.Fatalf("verdict %+v", v.Reverted)
	}
	if len(hk.received()) != 0 {
		t.Fatal("an open revert PR posted")
	}
	mergeRevert(g, time.Minute)
	reconcile(t, s, r, Options{})
	if got := hk.received(); len(got) != 1 || got[0].Source != integrations.SourceRevert {
		t.Fatalf("events %+v", got)
	}
}

// A revert merged before revertEventWindow is settled without a post, so the
// first acting pass over an old ledger does not replay it.
func TestTaskEventOldRevertNotReplayed(t *testing.T) {
	s, g, hk, r := revertSetup(t, 20*24*time.Hour)
	mergeRevert(g, 19*24*time.Hour)
	for range 2 {
		reconcile(t, s, r, Options{})
	}
	if got := hk.received(); len(got) != 0 {
		t.Fatalf("replayed %+v", got)
	}
	sent, _ := s.taskEventKeys(r.Name)
	if !sent.has(revertEventKey(r.Name, 9)) {
		t.Fatal("old revert left pending")
	}
}

// A hung hk never spends the review deadline: the reviewer runs and posts its
// pass, and the review event goes out after the run.
func TestTaskEventReviewKeepsDeadline(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	s.Config.Review.Timeout = "1500ms"
	s.Sink.(*hkSink).hk.EventTimeout = 700 * time.Millisecond
	s.Runner = deadlineRunner{s.Runner.(*fakeRunner)}
	hk.delay = 2 * time.Second // each of 3 tries times out at 200ms
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	if g.postCount != 1 {
		t.Fatalf("review posts %d", g.postCount)
	}
	got := hk.received()
	if len(got) != 3 || got[0].Source != integrations.SourceReview {
		t.Fatalf("events %+v", got)
	}
}

// deadlineRunner is a reviewer that fails once its deadline has passed, as a
// real reviewer process is killed.
type deadlineRunner struct{ *fakeRunner }

func (r deadlineRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return r.fakeRunner.Run(ctx, command, args, input, dir)
}

// A task ID beyond int64 is audited and never posted, and the PR is not
// retried on every poll.
func TestTaskEventTaskIDOverflow(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	g.pull.Body = "Mergewake-Task: 9223372036854775808\n"
	for range 2 {
		if e := s.ObservePR(context.Background(), r, g.pull, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if got := hk.received(); len(got) != 0 {
		t.Fatalf("posted %+v", got)
	}
	rows := taskEventAudit(t, s, r.Name)
	if len(rows) != 1 || !strings.Contains(rows[0].Error, "out of range") {
		t.Fatalf("audit %+v", rows)
	}
}

// Unreadable sent keys keep a final row with a revert PR in reconciliation:
// the read failure is audited each poll instead of the row being skipped.
func TestTaskEventRevertKeysUnreadable(t *testing.T) {
	s, g, hk, r := revertSetup(t, 2*time.Hour)
	mergeRevert(g, time.Hour)
	reconcile(t, s, r, Options{Shadow: true}) // finalizes the row
	if e := os.MkdirAll(filepath.Join(s.state(r.Name), "hk-task-events"), 0700); e != nil {
		t.Fatal(e)
	}
	reconcile(t, s, r, Options{})
	rows := taskEventAudit(t, s, r.Name)
	if len(rows) != 1 || !strings.Contains(rows[0].Error, "sent keys unreadable") || len(hk.received()) != 0 {
		t.Fatalf("audit %+v events %d", rows, len(hk.received()))
	}
	if e := os.Remove(filepath.Join(s.state(r.Name), "hk-task-events")); e != nil {
		t.Fatal(e)
	}
	reconcile(t, s, r, Options{})
	if got := hk.received(); len(got) != 1 || got[0].Source != integrations.SourceRevert {
		t.Fatalf("events %+v", got)
	}
}
