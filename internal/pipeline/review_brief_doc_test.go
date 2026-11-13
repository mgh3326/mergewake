package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/hktest"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// hkBriefSink is the panewire test sink with the hk sink's brief PUT: the
// real integrations.Handoffkeep client over HTTP, against whatever hk the
// environment names (the hktest fake, a stalled server, or none).
type hkBriefSink struct{ *taskJobSink }

func (hkBriefSink) PutBrief(ctx context.Context, key, job string, task *int64, body []byte) error {
	p, ok := any(integrations.Handoffkeep{}).(interface {
		PutBrief(context.Context, string, string, *int64, []byte) error
	})
	if !ok {
		return errors.New("integrations.Handoffkeep has no PutBrief")
	}
	return p.PutBrief(ctx, key, job, task, body)
}

// briefDocSetup is panewireSetup with the hk brief sink. hk is nil when the
// caller points the environment at another hk itself.
func briefDocSetup(t *testing.T, runner *panewireRunner, hk *hktest.Server) (*Service, *fakeGH, config.Repo) {
	t.Helper()
	s, g, r := panewireSetup(t, runner)
	runner.hk = hk
	s.Sink = hkBriefSink{runner.sink}
	return s, g, r
}

// runBriefPass runs pass 1 of PR 7 directly and returns the outcome, the
// plan (with the placed label) and the executor that built the brief.
func runBriefPass(t *testing.T, s *Service, g *fakeGH, r config.Repo, timeout time.Duration) (passOutcome, *passPlan, *panewireExecutor) {
	t.Helper()
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: "diff --git a/tool b/tool\n+new\n"}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	plan := &passPlan{pass: 1, lens: "default"}
	out, e := x.runPass(ctx, plan, func() (string, bool, error) { return "lease", true, nil })
	if e != nil {
		t.Fatalf("pass: %v", e)
	}
	return out, plan, x
}

// wantBrief is the brief text the pass hands its reviewer.
func wantBrief(t *testing.T, s *Service, x *panewireExecutor, plan *passPlan) string {
	t.Helper()
	family := x.builderFamily(context.Background(), "author-job")
	return x.brief(plan, plan.label, filepath.Join(s.Config.Review.Panewire.Jobs, plan.label), family)
}

const briefFindings = `{"findings":[{"path":"tool","line":1,"body":"A concrete bug","severity":"high"}]}`

// An in-limit brief is stored as hk doc review/<label> (kind brief, the job,
// the task, origin job) and the reviewer spawns from it: no review-brief.md
// is written, the job dir itself stays.
func TestReviewBriefDocInLimitSpawnsFromHKDoc(t *testing.T) {
	hk := hktest.New(t)
	runner := &panewireRunner{findings: briefFindings, family: "openai"}
	s, g, r := briefDocSetup(t, runner, hk)
	out, plan, x := runBriefPass(t, s, g, r, time.Minute)
	if len(out.skips) != 0 || len(out.findings) != 1 || len(runner.spawns) != 1 {
		t.Fatalf("outcome %+v spawns=%d", out, len(runner.spawns))
	}
	label := plan.label
	key := "review/" + label
	if got := arg(runner.spawns[0], "-p"); got != "hk:doc/"+key {
		t.Fatalf("-p %q, want hk:doc/%s", got, key)
	}
	d, ok := hk.Documents()[key]
	if !ok {
		t.Fatalf("no hk doc %s in %v", key, hk.Documents())
	}
	if d.Kind != "brief" || d.Job != label || d.TaskID == nil || *d.TaskID != 42 || d.OriginClass != "job" || d.Session != "" {
		t.Fatalf("doc fields kind=%q job=%q task=%v origin=%q session=%q", d.Kind, d.Job, d.TaskID, d.OriginClass, d.Session)
	}
	if want := wantBrief(t, s, x, plan); d.Body != want {
		t.Fatalf("doc body is not the brief:\n%s\n--- want ---\n%s", d.Body, want)
	}
	jobDir := filepath.Join(s.Config.Review.Panewire.Jobs, label)
	if _, e := os.Stat(filepath.Join(jobDir, "review-brief.md")); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("review-brief.md written beside an hk brief: %v", e)
	}
	if st, e := os.Stat(jobDir); e != nil || !st.IsDir() {
		t.Fatalf("job dir gone: %v", e)
	}
	rows := auditRows(t, s, r, "review-brief-hk")
	if len(rows) != 1 || rows[0]["status"] != "hk" || rows[0]["key"] != key || rows[0]["label"] != label {
		t.Fatalf("audit %v", rows)
	}
}

// stalledHK points the svc-mergewake credential at a server that never
// answers until the request is abandoned or the test ends.
func stalledHK(t *testing.T) {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	// Cleanups run last-in first-out: the handler is released before Close
	// waits for it.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", hktest.Token)
	t.Setenv("HANDOFFKEEP_TOKEN", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
}

// Contract (a): a brief PUT that fails for any reason never stops the
// review. The pass writes review-brief.md and spawns from it as before,
// records one fallback row, and ends exactly like a pass whose PUT landed.
func TestReviewBriefDocPutFailureFallsBackToLocalBrief(t *testing.T) {
	normal := normalBriefPass(t)
	for name, arrange := range map[string]func(t *testing.T) *hktest.Server{
		"refused": func(t *testing.T) *hktest.Server {
			hk := hktest.New(t)
			hk.Fail("PUT /v1/documents/{key}", http.StatusInternalServerError, "internal")
			return hk
		},
		"unconfigured": func(t *testing.T) *hktest.Server {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("HANDOFFKEEP_URL", "")
			t.Setenv("HANDOFFKEEP_TOKEN_SVC_MERGEWAKE", "")
			t.Setenv("HANDOFFKEEP_TOKEN", "")
			return nil
		},
		"timeout": func(t *testing.T) *hktest.Server {
			stalledHK(t)
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			hk := arrange(t)
			runner := &panewireRunner{findings: briefFindings, family: "openai"}
			s, g, r := briefDocSetup(t, runner, hk)
			out, plan, x := runBriefPass(t, s, g, r, 8*time.Second)
			assertLocalBriefFallback(t, s, r, runner, out, normal, plan, x)
			if hk != nil {
				if _, ok := hk.Documents()["review/"+plan.label]; ok {
					t.Fatal("a refused PUT stored the doc")
				}
			}
		})
	}
}

// assertLocalBriefFallback checks a pass whose brief PUT did not count: it
// ended exactly like the normal pass, spawned from review-brief.md holding
// the brief, left one fallback row with a reason and no review-failed row.
func assertLocalBriefFallback(t *testing.T, s *Service, r config.Repo, runner *panewireRunner, out, normal passOutcome, plan *passPlan, x *panewireExecutor) {
	t.Helper()
	if !reflect.DeepEqual(out, normal) {
		t.Fatalf("fallback outcome %+v, normal pass %+v", out, normal)
	}
	jobDir := filepath.Join(s.Config.Review.Panewire.Jobs, plan.label)
	briefPath := filepath.Join(jobDir, "review-brief.md")
	if len(runner.spawns) != 1 || arg(runner.spawns[0], "-p") != briefPath {
		t.Fatalf("spawns %v, want -p %s", runner.spawns, briefPath)
	}
	body, e := os.ReadFile(briefPath)
	if e != nil || string(body) != wantBrief(t, s, x, plan) {
		t.Fatalf("local brief: %v\n%s", e, body)
	}
	rows := auditRows(t, s, r, "review-brief-hk")
	if len(rows) != 1 || rows[0]["status"] != "fallback" || rows[0]["key"] != "review/"+plan.label || rows[0]["reason"] == "" || rows[0]["reason"] == nil {
		t.Fatalf("audit %v", rows)
	}
	if failed := auditRows(t, s, r, "review-failed"); len(failed) != 0 {
		t.Fatalf("review-failed rows %v", failed)
	}
}

// normalBriefPass is the outcome of a pass whose brief PUT landed.
func normalBriefPass(t *testing.T) passOutcome {
	t.Helper()
	hk := hktest.New(t)
	runner := &panewireRunner{findings: briefFindings, family: "openai"}
	s, g, r := briefDocSetup(t, runner, hk)
	out, _, _ := runBriefPass(t, s, g, r, 8*time.Second)
	return out
}

// An hk that answers the brief PUT 200 without acknowledging the key and the
// sha256 of the brief sent has not proven it holds the brief: the pass
// spawns from the local review-brief.md, as for a refused PUT.
func TestReviewBriefDocUnverifiedAcknowledgementFallsBack(t *testing.T) {
	normal := normalBriefPass(t)
	for name, answer := range map[string]func(key, sha string) string{
		"empty": func(key, sha string) string { return `{}` },
		"null":  func(key, sha string) string { return `null` },
		"wrong key": func(key, sha string) string {
			return `{"document":{"key":"review/other","sha256":"` + sha + `"},"changed":true}`
		},
		"wrong sha": func(key, sha string) string {
			return `{"document":{"key":"` + key + `","sha256":"` + strings.Repeat("0", 64) + `"},"changed":true}`
		},
		"no sha":     func(key, sha string) string { return `{"document":{"key":"` + key + `"},"changed":true}` },
		"unreadable": func(key, sha string) string { return `{"document":` },
	} {
		t.Run(name, func(t *testing.T) {
			hk := hktest.New(t)
			hk.AnswerDocumentPut(answer)
			runner := &panewireRunner{findings: briefFindings, family: "openai"}
			s, g, r := briefDocSetup(t, runner, hk)
			out, plan, x := runBriefPass(t, s, g, r, 8*time.Second)
			if docPuts(hk) != 1 {
				t.Fatalf("doc PUTs %d, want 1", docPuts(hk))
			}
			assertLocalBriefFallback(t, s, r, runner, out, normal, plan, x)
		})
	}
}

// A task sink that cannot store hk documents leaves the brief local: no PUT
// is tried and the pass spawns from review-brief.md.
func TestReviewBriefDocSinkWithoutPutBriefFallsBack(t *testing.T) {
	normal := normalBriefPass(t)
	hk := hktest.New(t)
	runner := &panewireRunner{findings: briefFindings, family: "openai"}
	s, g, r := panewireSetup(t, runner)
	runner.hk = hk
	if _, ok := s.Sink.(interface {
		PutBrief(context.Context, string, string, *int64, []byte) error
	}); ok {
		t.Fatal("the plain panewire test sink has PutBrief")
	}
	out, plan, x := runBriefPass(t, s, g, r, 8*time.Second)
	assertLocalBriefFallback(t, s, r, runner, out, normal, plan, x)
	if docPuts(hk) != 0 {
		t.Fatalf("doc PUTs %d without a doc-capable sink", docPuts(hk))
	}
}

// A brief PUT never spends the time the pass needs to spawn. Through
// RetryReview, a review whose deadline is barely enough to spawn (a pass
// spawns while at least 4/3 s remain, see minReviewerWait) still spawns its
// reviewer when hk stalls, and nothing counts as a review failure: no
// review-failed row, no failure toward escalation. 1.7 s keeps the case
// stable: without a reserve the stalled PUT takes a quarter of it and leaves
// the pane wait under a second for any remaining time below 1.77 s, while the
// pass itself spends only in-memory fakes before the brief.
func TestReviewBriefDocStalledHKNearDeadlineStillSpawnsThroughRetry(t *testing.T) {
	stalledHK(t)
	runner := &panewireRunner{findings: briefFindings, family: "openai"}
	s, g, r := briefDocSetup(t, runner, nil)
	s.Config.Review.Timeout = "1700ms"
	s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 1}
	err := s.RetryReview(context.Background(), r, g.pull, Options{})
	if len(runner.spawns) != 1 {
		t.Fatalf("spawns=%d err=%v review-failed=%v", len(runner.spawns), err, auditRows(t, s, r, "review-failed"))
	}
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	label := arg(runner.spawns[0], "-l")
	if got, want := arg(runner.spawns[0], "-p"), filepath.Join(s.Config.Review.Panewire.Jobs, label, "review-brief.md"); got != want {
		t.Fatalf("-p %q, want %q", got, want)
	}
	if failed := auditRows(t, s, r, "review-failed"); len(failed) != 0 {
		t.Fatalf("review-failed rows %v", failed)
	}
	if st := retryStateOf(t, s, r); st.Failures != 0 || st.Escalated {
		t.Fatalf("retry state %+v", st)
	}
	if _, failed := resultLostNotices(s); failed != 0 {
		t.Fatalf("review-failed notices: %v", s.Notifier.(*fakeNotifier).events)
	}
	if g.postCount != 1 {
		t.Fatalf("posts=%d", g.postCount)
	}
	rows := auditRows(t, s, r, "review-brief-hk")
	if len(rows) != 1 || rows[0]["status"] != "fallback" || rows[0]["key"] != "review/"+label || rows[0]["reason"] != "no-time" {
		t.Fatalf("audit %v", rows)
	}
}

// A brief over the inline limit keeps today's path exactly: the attachment
// plus the local review-prompt.md pointer, and no hk doc.
func TestReviewBriefDocOversizeKeepsAttachment(t *testing.T) {
	hk := hktest.New(t)
	runner := &panewireRunner{findings: `{"findings":[]}`, family: "openai"}
	s, g, r := briefDocSetup(t, runner, hk)
	x := &panewireExecutor{s: s, repo: r, pr: g.pull, diff: "diff --git a/tool b/tool\n+new\n", context: strings.Repeat("context ", 10000)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plan := &passPlan{pass: 2, lens: "state-risk"}
	if _, e := x.runPass(ctx, plan, func() (string, bool, error) { return "lease", true, nil }); e != nil {
		t.Fatal(e)
	}
	jobDir := filepath.Join(s.Config.Review.Panewire.Jobs, plan.label)
	if len(runner.spawns) != 1 || arg(runner.spawns[0], "-p") != filepath.Join(jobDir, "review-prompt.md") {
		t.Fatalf("spawns %v", runner.spawns)
	}
	body, e := os.ReadFile(filepath.Join(jobDir, "review-brief.md"))
	if e != nil || len(body) <= inlineBriefLimit {
		t.Fatalf("local brief %d bytes: %v", len(body), e)
	}
	prompt, e := os.ReadFile(filepath.Join(jobDir, "review-prompt.md"))
	if e != nil {
		t.Fatal(e)
	}
	for id, b := range runner.sink.files {
		if string(b) != string(body) || !strings.Contains(string(prompt), "attach get "+id) {
			t.Fatalf("attachment %s does not carry the brief", id)
		}
	}
	if len(runner.sink.files) != 1 {
		t.Fatalf("attachments %d", len(runner.sink.files))
	}
	for _, q := range hk.Requests() {
		if strings.HasPrefix(q.Path, "/v1/documents/") {
			t.Fatalf("oversize brief went to an hk doc: %s", q.Route())
		}
	}
	if rows := auditRows(t, s, r, "review-brief-hk"); len(rows) != 0 {
		t.Fatalf("audit %v", rows)
	}
}

// docPuts counts the brief PUTs hk received.
func docPuts(hk *hktest.Server) int {
	n := 0
	for _, q := range hk.Requests() {
		if q.Method == http.MethodPut && strings.HasPrefix(q.Path, "/v1/documents/") {
			n++
		}
	}
	return n
}

// A resumed pass replays its recorded spawn: it never PUTs the brief again
// and keeps -p hk:doc/review/<label> byte for byte.
func TestReviewBriefDocResumedReplayKeepsRecordedArgs(t *testing.T) {
	hk := hktest.New(t)
	runner := &panewireRunner{family: "openai", exits: []int{panewireWaitTimeout}}
	s, g, r := briefDocSetup(t, runner, hk)
	s.Config.Notifier.Lane = "desk"
	c := &clock{t: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	s.Now = c.now
	runner.sink.missing = &integrations.JobRecord{State: "claimed"}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if len(runner.spawns) != 1 {
		t.Fatalf("first run spawns=%d", len(runner.spawns))
	}
	first := runner.spawns[0]
	label := arg(first, "-l")
	if arg(first, "-p") != "hk:doc/review/"+label || docPuts(hk) != 1 {
		t.Fatalf("first spawn %v, %d doc PUTs", first, docPuts(hk))
	}
	if rj := readReviewerJob(t, s, r, 1); !slices.Equal(panewireSpawnArgs(rj.Args), first) {
		t.Fatalf("record %v != spawn %v", rj.Args, first)
	}
	stored := hk.Documents()["review/"+label]
	g.leaseTimes[claimRef(7, shaA, 1)] = time.Now().Add(-time.Hour)
	c.t = c.t.Add(2 * time.Second)
	runner.afterRun = func() {
		runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai", Result: json.RawMessage(`{"findings":[]}`)}
	}
	if err := s.Review(context.Background(), r, 7, "", Options{}); err != nil {
		t.Fatal(err)
	}
	if len(runner.spawns) != 2 || g.postCount != 1 {
		t.Fatalf("spawns=%d posts=%d", len(runner.spawns), g.postCount)
	}
	replay := runner.spawns[1]
	if !slices.Equal(withoutWait(first), withoutWait(replay)) {
		t.Fatalf("replay changed the spawn:\n%v\n%v", first, replay)
	}
	if docPuts(hk) != 1 || hk.Documents()["review/"+label] != stored {
		t.Fatalf("resumed pass re-PUT the brief: %d PUTs", docPuts(hk))
	}
	if rows := auditRows(t, s, r, "review-brief-hk"); len(rows) != 1 {
		t.Fatalf("audit %v", rows)
	}
}

// Contract (e): with the brief in hk, the J= line still names the job dir a
// same-host reviewer writes findings.json in, but a file left there while the
// record carries no result is not read: with no host on the record the
// result is lost, and the file's findings are never posted.
func TestReviewBriefDocJobDirFindingsAreNotRead(t *testing.T) {
	hk := hktest.New(t)
	runner := &panewireRunner{findings: briefFindings, family: "openai", legacyOnly: true}
	s, g, r := briefDocSetup(t, runner, hk)
	s.Config.Notifier.Lane = "desk"
	runner.sink.missing = &integrations.JobRecord{State: "completed", Family: "openai"}
	e := s.Review(context.Background(), r, 7, "", Options{})
	var lost *reviewResultLost
	if !errors.As(e, &lost) || len(runner.spawns) != 1 || g.postCount != 0 || len(g.passes[shaA]) != 0 {
		t.Fatalf("err=%v spawns=%d posts=%d", e, len(runner.spawns), g.postCount)
	}
	sp := runner.spawns[0]
	label := arg(sp, "-l")
	jobDir := filepath.Join(s.Config.Review.Panewire.Jobs, label)
	if arg(sp, "-p") != "hk:doc/review/"+label {
		t.Fatalf("-p %q", arg(sp, "-p"))
	}
	if _, e := os.Stat(filepath.Join(jobDir, "findings.json")); e != nil {
		t.Fatalf("the same-host reviewer did not write in the job dir: %v", e)
	}
	if !strings.Contains(hk.Documents()["review/"+label].Body, "J='"+jobDir+"'") {
		t.Fatal("the brief's J= line no longer names the job dir")
	}
	if rows := auditRows(t, s, r, "review-result-lost"); len(rows) != 1 || rows[0]["label"] != label {
		t.Fatalf("lost rows=%v", rows)
	}
}
