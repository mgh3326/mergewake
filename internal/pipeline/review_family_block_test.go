package pipeline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func familyBlockSetup(t *testing.T) blockFixture {
	t.Helper()
	f := blockSetup(t)
	f.g.pull.Body = "Mergewake-Task: 42\n"
	f.s.Config.Review.DefaultFamily = ""
	f.g.authorName, f.g.authorEmail = "person", "person@example.com"
	f.g.committerName, f.g.committerEmail = "GitHub", "noreply@github.com"
	return f
}

func TestUnknownFamilyBlocksAndRecovers(t *testing.T) {
	f := familyBlockSetup(t)
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("want block, got %v", e)
	}
	if got := f.blocked(t, 7); got != "blocked: author-family-unknown" {
		t.Fatalf("block: %q", got)
	}
	if f.gh.posts != 1 || f.n.count != 1 || !strings.Contains(f.gh.comments[0].Body, "Mergewake-Author-Family: <family>") || !strings.Contains(f.gh.comments[0].Body, "head commit") {
		t.Fatalf("comments=%+v events=%+v", f.gh.comments, f.n.events)
	}
	if !strings.Contains(f.gh.comments[0].Body, "If these are already present, no changes to them are needed.") {
		t.Fatalf("comment implies satisfied requirements need changes: %s", f.gh.comments[0].Body)
	}
	if f.g.postCount != 0 || f.g.claimCount != 0 || f.g.refs[doneRef(7, shaA)] {
		t.Fatal("blocked head wrote review state")
	}
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	if f.gh.posts != 1 || f.n.count != 1 {
		t.Fatalf("posts=%d events=%d", f.gh.posts, f.n.count)
	}
	f.g.pull.Body += fleetBody
	if e := f.review(t); e != nil {
		t.Fatal(e)
	}
	if f.blocked(t, 7) != "" || f.g.postCount != 1 || !f.g.refs[doneRef(7, shaA)] {
		t.Fatalf("block=%q posts=%d refs=%v", f.blocked(t, 7), f.g.postCount, f.g.refs)
	}
}

func TestUnknownFamilyNonFleetUnavailable(t *testing.T) {
	f := familyBlockSetup(t)
	f.g.pull.User.Login = "contributor"
	if e := f.review(t); !errors.Is(e, ErrReviewUnavailable) {
		t.Fatal(e)
	}
	if f.gh.posts != 0 || f.n.count != 0 || f.blocked(t, 7) != "" {
		t.Fatalf("posts=%d events=%d block=%q", f.gh.posts, f.n.count, f.blocked(t, 7))
	}
}

func TestUnknownFamilyBlockLandAndNotice(t *testing.T) {
	f := familyBlockSetup(t)
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	var out bytes.Buffer
	f.s.Out = &out
	if e := f.s.Land(context.Background(), f.r, 7, Options{DryRun: true}); e != nil {
		t.Fatal(e)
	}
	records := auditRecords(t, out.Bytes(), "land")
	if len(records) != 1 {
		t.Fatalf("land: %s", out.String())
	}
	v := records[0]
	if !strings.Contains(out.String(), `"review_block":"blocked: author-family-unknown"`) || !strings.Contains(out.String(), `"reasons":["review_missing"]`) {
		t.Fatalf("land: %+v", v)
	}
	on := true
	f.s.Config.Daemon.PR.Notify.Enabled = &on
	if e := f.s.NotifyPR(context.Background(), f.r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	notices := auditRecords(t, out.Bytes(), "pr-notify")
	if len(notices) != 1 || notices[0]["review_block"] != "blocked: author-family-unknown" || !strings.Contains(f.n.events[len(f.n.events)-1].Text, "review_missing (blocked: author-family-unknown)") {
		t.Fatalf("audit=%s events=%+v", out.String(), f.n.events)
	}
}

func TestFixedTaskBlockClearsBeforeFamilyError(t *testing.T) {
	f := blockSetup(t)
	if e := f.review(t); !errors.Is(e, ErrReviewBlocked) {
		t.Fatal(e)
	}
	f.g.pull.Body = "Mergewake-Task: 42\n"
	// The head commit is memoised per head; forget it so the next read fails.
	reapHeadMu.Lock()
	clear(reapHeadCommits)
	reapHeadMu.Unlock()
	f.g.commitErr = map[string]error{shaA: errors.New("commit unavailable")}
	if e := f.review(t); e == nil || errors.Is(e, ErrReviewBlocked) {
		t.Fatalf("want commit error: %v", e)
	}
	if got := f.blocked(t, 7); got != "" {
		t.Fatalf("stale block: %q", got)
	}
}

func TestGateKeepsVerdictWhenBlockLedgerUnreadable(t *testing.T) {
	f := familyBlockSetup(t)
	if e := os.MkdirAll(f.s.Config.StateDir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(f.s.Config.StateDir, "outcomes.jsonl"), []byte("invalid json\n"), 0600); e != nil {
		t.Fatal(e)
	}
	p, e := f.s.Policy(context.Background(), f.r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	v, e := f.s.mergeGate(context.Background(), f.r, f.g.pull, p, shaB)
	if e != nil || v.State != "blocked" || strings.Join(v.Reasons, ",") != "review_missing" || v.Review.Block != "" {
		t.Fatalf("verdict=%+v error=%v", v, e)
	}
}
