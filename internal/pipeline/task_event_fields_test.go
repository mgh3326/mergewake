package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

var eventToken = regexp.MustCompile(`^[a-z0-9_.-]+$`)

// eventAuditRows reads the audit rows of one command from the repo's decision log.
func eventAuditRows(t *testing.T, s *Service, repo, command string) []map[string]any {
	t.Helper()
	f, e := os.Open(filepath.Join(s.state(repo), "decisions.jsonl"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var row struct {
			Command string         `json:"command"`
			Value   map[string]any `json:"value"`
		}
		if e := json.Unmarshal(sc.Bytes(), &row); e != nil {
			t.Fatal(e)
		}
		if row.Command == command {
			out = append(out, row.Value)
		}
	}
	return out
}

// Every task event carries the PR's head and base SHA. A two-pass review event
// carries one reviews[] record per posted pass, with the reviewed head, lens,
// pass, round and the finding counts.
func TestTaskEventReviewsTwoPasses(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if g.postCount != 2 || len(got) != 1 {
		t.Fatalf("reviews=%d events=%+v", g.postCount, got)
	}
	ev := got[0]
	if ev.HeadSHA != shaA || ev.BaseSHA != shaB || ev.MergeSHA != "" {
		t.Fatalf("shas %q %q %q", ev.HeadSHA, ev.BaseSHA, ev.MergeSHA)
	}
	if len(ev.Reviews) != 2 {
		t.Fatalf("reviews %+v", ev.Reviews)
	}
	for i, want := range []struct {
		pass int
		lens string
	}{{1, "correctness"}, {2, "state-risk"}} {
		rv := ev.Reviews[i]
		if rv.ReviewedHead != shaA || rv.ReviewedBase != shaB || rv.Pass != want.pass || rv.Lens != want.lens || rv.Round != 1 {
			t.Fatalf("review %d: %+v", i, rv)
		}
		// The fake reviewer posts one finding with no severity: counted nowhere.
		if rv.Critical+rv.High+rv.Medium+rv.Low != 0 || rv.StartedAt == nil || rv.FinishedAt == nil || rv.FinishedAt.Before(*rv.StartedAt) {
			t.Fatalf("review %d: %+v", i, rv)
		}
	}
}

// The merge event carries merge_sha, read from GitHub after the merge, and no
// reviews.
func TestTaskEventMergeSHA(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	g.enabled = true
	g.pull.MergeCommitSHA = shaC
	markReviewed(g)
	s.GH = mergeGH{g}
	if e := s.Land(context.Background(), r, 7, Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if len(got) != 1 {
		t.Fatalf("events %+v", got)
	}
	ev := got[0]
	if ev.Source != integrations.SourceMerge || ev.MergeSHA != shaC || ev.HeadSHA != shaA || ev.BaseSHA != shaB || len(ev.Reviews) != 0 {
		t.Fatalf("event %+v", ev)
	}
}

// A merge commit GitHub does not show yet, or one that is not a SHA, is left
// out; the event still goes.
func TestTaskEventMergeSHAUnknown(t *testing.T) {
	for name, sha := range map[string]string{"empty": "", "garbage": "not-a-sha"} {
		t.Run(name, func(t *testing.T) {
			s, g, hk, r := hkSetup(t)
			g.enabled = true
			g.pull.MergeCommitSHA = sha
			markReviewed(g)
			s.GH = mergeGH{g}
			if e := s.Land(context.Background(), r, 7, Options{Observed: true}); e != nil {
				t.Fatal(e)
			}
			got := hk.received()
			if len(got) != 1 || got[0].MergeSHA != "" || got[0].HeadSHA != shaA {
				t.Fatalf("events %+v", got)
			}
		})
	}
}

// An hk that answers 400 to every extended payload gets the event again with
// head and base but no reviews, then plain; that succeeds, and the refusal is
// audited with hk's error code, the resend form and nothing of the payload.
func TestTaskEventFieldsRefusedResend(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	hk.refuseExtended = true
	g.blocking = true
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e != nil {
		t.Fatal(e)
	}
	got := hk.received()
	if len(got) != 3 {
		t.Fatalf("want the extended post, one without reviews and one plain: %+v", got)
	}
	if len(got[0].Reviews) != 2 || got[0].HeadSHA == "" {
		t.Fatalf("first post %+v", got[0])
	}
	if len(got[1].Reviews) != 0 || got[1].HeadSHA == "" {
		t.Fatalf("second post must keep head and base, without reviews: %+v", got[1])
	}
	wantEvent(t, got[2], integrations.SourceReview, testPRURL, testPRURL, "", "")
	if got[2].HeadSHA != "" || got[2].BaseSHA != "" || got[2].MergeSHA != "" || len(got[2].Reviews) != 0 {
		t.Fatalf("resend kept new fields: %+v", got[2])
	}
	rows := eventAuditRows(t, s, r.Name, "hk-task-event")
	if len(rows) != 1 || rows[0]["error"] != nil || rows[0]["result"] == nil {
		t.Fatalf("audit %+v", rows)
	}
	refused := eventAuditRows(t, s, r.Name, "task-event-fields-refused")
	if len(refused) != 1 || refused[0]["code"] != "invalid_event" || refused[0]["source"] != integrations.SourceReview || refused[0]["source_ref"] != testPRURL {
		t.Fatalf("refused audit %+v", refused)
	}
	if len(refused[0]) != 5 || refused[0]["resend"] != integrations.ResendPlain {
		t.Fatalf("refused audit carries more than source, task, source_ref, code and resend: %+v", refused[0])
	}
}

// A lens (or family, executor, contract source) that is not a [a-z0-9_.-] token
// is normalised or left out, never sent raw; counts follow the severity.
func TestPassRecordTokens(t *testing.T) {
	pr := github.Pull{Head: github.Ref{SHA: shaA}, Base: github.Ref{SHA: "short"}}
	receipt := github.PassReceipt{Pass: 2, Round: 3, Lens: "State Risk/é", Family: "  ", Executor: "pane wire", Fallback: "x", Patch: "!!"}
	at := time.Now()
	fs := []github.Finding{{Severity: "critical"}, {Severity: "blocking"}, {Severity: "high"}, {Severity: "major"}, {Severity: "minor"}, {Severity: ""}}
	rec := passRecord(pr, receipt, fs, "linked task 3118", at, at)
	if rec.Lens != "state-risk" || rec.Family != "" || rec.Executor != "pane-wire" || rec.PatchID != "" || rec.ContractSource != "linked-task-3118" {
		t.Fatalf("%+v", rec)
	}
	if rec.ReviewedBase != "" || rec.ReviewedHead != shaA || !rec.Fallback || rec.Pass != 2 || rec.Round != 3 {
		t.Fatalf("%+v", rec)
	}
	if rec.Critical != 1 || rec.High != 2 || rec.Medium != 1 || rec.Low != 1 {
		t.Fatalf("counts %+v", rec)
	}
	b, _ := json.Marshal(rec)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"lens", "family", "executor", "patch_id", "contract_source"} {
		if v, ok := m[k].(string); ok && !eventToken.MatchString(v) {
			t.Fatalf("%s %q is not a token", k, v)
		}
	}
	if _, ok := m["family"]; ok {
		t.Fatal("empty family sent")
	}
}

// failingPostGH stores the review and then fails the call, as a lost
// response after GitHub accepted the write does.
type failingPostGH struct{ *fakeGH }

func (g failingPostGH) PostReview(ctx context.Context, repo string, n int, sha string, f []github.Finding, meta github.PassReceipt) error {
	_ = g.fakeGH.PostReview(ctx, repo, n, sha, f, meta)
	return errors.New("response lost")
}

// A pass GitHub stored although PostReview returned an error is still a
// posted pass: its record goes in the event, because no later run will post it.
func TestTaskEventReviewsStoredDespiteError(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	s.GH = failingPostGH{g}
	if e := s.Review(context.Background(), r, 7, "openai", Options{Observed: true}); e == nil {
		t.Fatal("review did not return the PostReview error")
	}
	got := hk.received()
	if len(got) != 1 || len(got[0].Reviews) != 1 || got[0].Reviews[0].Pass != 1 || got[0].Reviews[0].ReviewedHead != shaA {
		t.Fatalf("events %+v", got)
	}
}

// slowPullGH blocks the PR read until its context ends.
type slowPullGH struct{ *fakeGH }

func (g slowPullGH) Pull(ctx context.Context, repo string, n int) (github.Pull, error) {
	<-ctx.Done()
	return github.Pull{}, ctx.Err()
}

// A stalled merge commit read costs merge_sha, not the event: the read has its
// own budget, and the post still has its deadline.
func TestTaskEventMergeSHALookupBounded(t *testing.T) {
	s, g, hk, r := hkSetup(t)
	s.GH = slowPullGH{g}
	start := time.Now()
	if got := s.postTaskEvent(context.Background(), r, g.pull, integrations.SourceMerge, nil, time.Now()); got != postSent {
		t.Fatalf("outcome %v", got)
	}
	if time.Since(start) > mergeCommitBudget+5*time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
	ev := hk.received()
	if len(ev) != 1 || ev[0].MergeSHA != "" || ev[0].HeadSHA != shaA {
		t.Fatalf("events %+v", ev)
	}
}
