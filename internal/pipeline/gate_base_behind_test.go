package pipeline

import (
	"bytes"
	"context"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// mergeRecorder remembers the sha each merge call was bound to.
type mergeRecorder struct {
	*fakeGH
	shas []string
}

func (m *mergeRecorder) Merge(ctx context.Context, repo string, n int, sha string) error {
	m.shas = append(m.shas, sha)
	return m.fakeGH.Merge(ctx, repo, n, sha)
}

// baseBehindSetup is a reviewed, green PR whose base moved on after its head,
// with GitHub reporting state for it. Its task binds a worker job, so a READY
// verdict has the author lane it goes to.
func baseBehindSetup(t *testing.T, stale bool, state, extraPolicy string) (*Service, *mergeRecorder, *fakeNotifier, *bytes.Buffer, config.Repo) {
	t.Helper()
	s, g, n, r := notifySetup(t)
	g.enabled = true
	g.policyExtra = extraPolicy
	g.stale = stale
	g.pull.MergeableState = state
	g.pull.Body = "Mergewake-Task: 42\n"
	s.Sink = &jobSink{jobs: map[string]string{"42": "w-7"}, recs: map[string]integrations.JobRecord{"w-7": {Role: "worker", Label: "worker-lane"}}}
	if state == "dirty" {
		no := false
		g.pull.Mergeable = &no
	}
	rec := &mergeRecorder{fakeGH: g}
	s.GH = rec
	var out bytes.Buffer
	s.Out = &out
	return s, rec, n, &out, r
}

func baseBehindFlag(t *testing.T, out *bytes.Buffer, command string, land bool) any {
	t.Helper()
	records := auditRecords(t, out.Bytes(), command)
	if len(records) != 1 {
		t.Fatalf("%s records: %s", command, out.String())
	}
	if land {
		review, _ := records[0]["review"].(map[string]any)
		return review["base_behind"]
	}
	return records[0]["base_behind"]
}

// (a) GitHub merges a conflict-free PR that is behind its base, so a stale
// base alone neither blocks the gate nor moves the branch.
func TestBaseBehindCleanPRIsReadyAndLands(t *testing.T) {
	s, g, n, out, r := baseBehindSetup(t, true, "clean", "")
	if v := gateAt(t, s, g.fakeGH, shaB); v.State != "ready" || reasons(v) != "" || !v.Review.BaseBehind {
		t.Fatalf("gate: %+v", v)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatalf("events=%v", n.events)
	}
	if got := baseBehindFlag(t, out, "pr-notify", false); got != true {
		t.Fatalf("notify base_behind = %v", got)
	}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if got := baseBehindFlag(t, out, "land", true); got != true {
		t.Fatalf("land base_behind = %v", got)
	}
	if len(g.shas) != 1 || g.shas[0] != shaA || g.mergeCount != 1 {
		t.Fatalf("merge shas=%v count=%d", g.shas, g.mergeCount)
	}
}

// (a) With the default policy a head GitHub reports behind its base is ready:
// the stale base is recorded, the branch is not updated, and the PR lands.
func TestBaseBehindDefaultPolicyLandsBehindHead(t *testing.T) {
	s, g, n, out, r := baseBehindSetup(t, true, "behind", "")
	if v := gateAt(t, s, g.fakeGH, shaB); v.State != "ready" || reasons(v) != "" || !v.Review.BaseBehind {
		t.Fatalf("gate: %+v", v)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if n.count != 0 {
		t.Fatalf("events=%v", n.events)
	}
	if got := baseBehindFlag(t, out, "pr-notify", false); got != true {
		t.Fatalf("notify base_behind = %v", got)
	}
	if e := s.Land(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if got := baseBehindFlag(t, out, "land", true); got != true {
		t.Fatalf("land base_behind = %v", got)
	}
	if g.mergeCount != 1 {
		t.Fatalf("merges=%d", g.mergeCount)
	}
}

// (c) A behind head that also conflicts is blocked on conflict alone.
func TestBaseBehindConflictBlocks(t *testing.T) {
	s, g, _, _, r := baseBehindSetup(t, true, "dirty", "")
	v := gateAt(t, s, g.fakeGH, shaB)
	if v.State != "blocked" || reasons(v) != "conflict" {
		t.Fatalf("gate: %+v", v)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
}

// (d) A fresh base is ready and carries no base_behind.
func TestBaseBehindFreshBaseHasNoFlag(t *testing.T) {
	s, g, _, out, r := baseBehindSetup(t, false, "clean", "")
	if v := gateAt(t, s, g.fakeGH, shaB); v.State != "ready" || reasons(v) != "" || v.Review.BaseBehind {
		t.Fatalf("gate: %+v", v)
	}
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatal(e)
	}
	if got := baseBehindFlag(t, out, "pr-notify", false); got != nil {
		t.Fatalf("base_behind = %v, want absent", got)
	}
}
