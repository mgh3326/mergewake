package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/policy"
)

const botPolicy = `bot_updates:
  authors: ["renovate[bot]"]
  paths: ["**/package-lock.json", "**/pnpm-lock.yaml", "uv.lock", "**/package.json"]
  land: patch
`

// renovateBody is the update table Renovate writes into a PR body.
func renovateBody(update, from, to string) string {
	return "This PR contains the following updates:\n\n" +
		"| Package | Type | Update | Change |\n|---|---|---|---|\n" +
		"| [vite](https://example.test/vite) | devDependencies | " + update + " | [`" + from + "` -> `" + to + "`](https://example.test/diff) |\n\n" +
		"### Release Notes\n\n| not | an | update table |\n|---|---|---|\n| a | b | c |\n"
}

var lockFiles = []policy.File{
	{Path: "frontend/x/pnpm-lock.yaml", Kind: "modified"},
	{Path: "package-lock.json", Kind: "modified"},
}

// botSetup is a service whose repo opted into bot_updates and whose PR 7 is a
// green, mergeable Renovate patch update that changes only lock files. No
// review exists for it.
func botSetup(t *testing.T, extraPolicy string) (*Service, *fakeGH, *fakeNotifier, *bytes.Buffer, func(context.Context, Options) error) {
	t.Helper()
	s, g, _, n, r := setup(t)
	r.Lane = "desk"
	g.enabled = true
	g.policyExtra = botPolicy + extraPolicy
	g.files = lockFiles
	g.pull.ChangedFiles = len(lockFiles)
	g.pull.Title = "chore(deps): update vite to v8.3.6"
	g.pull.Body = renovateBody("patch", "8.3.4", "8.3.6")
	g.pull.User = github.PullUser{Login: "renovate[bot]", Type: "Bot"}
	out := &bytes.Buffer{}
	s.Out = out
	s.Config.Review.Executor = "panewire"
	return s, g, n, out, func(ctx context.Context, o Options) error { return s.Land(ctx, r, 7, o) }
}

func noReview(t *testing.T, s *Service, g *fakeGH) {
	t.Helper()
	if runner := s.Runner.(*fakeRunner); runner.calls != 0 || g.claimCount != 0 || g.postCount != 0 {
		t.Fatalf("a review ran: spawns=%d claims=%d posts=%d", runner.calls, g.claimCount, g.postCount)
	}
}

func notices(n *fakeNotifier) (out []string) {
	for _, e := range n.events {
		if strings.Contains(e.Text, "BOT_UPDATE") {
			out = append(out, e.Text)
		}
	}
	return out
}

// (a) A routine Renovate patch update that changes only lock files lands, bound
// to the head, with no review, and says so in the land record.
func TestBotUpdateLandsPatchBoundToHead(t *testing.T) {
	s, g, n, out, land := botSetup(t, "")
	if e := land(context.Background(), Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 {
		t.Fatalf("merges=%d, want 1 (the fake refuses a merge not bound to head %s)", g.mergeCount, g.pull.Head.SHA)
	}
	noReview(t, s, g)
	if !strings.Contains(out.String(), `"command":"land"`) || !strings.Contains(out.String(), `"reason":"bot-patch"`) {
		t.Fatalf("land record: %s", out)
	}
	if len(n.events) != 1 || !strings.Contains(n.events[0].Text, "MERGED") {
		t.Fatalf("want one MERGED event: %+v", n.events)
	}
}

// (b) A minor bump is not merged. The desk hears once for the head, naming the
// type, and not again on the next pass; a new head is a new notice.
func TestBotUpdateMinorNotLandedNoticeOnce(t *testing.T) {
	for _, tt := range []struct{ name, body string }{
		{"update column", renovateBody("minor", "8.3.4", "8.4.0")},
		{"versions only", "| Package | Change |\n|---|---|\n| vite | `8.3.4` → `8.4.0` |\n"},
		{"column says patch, versions say minor", renovateBody("patch", "8.3.4", "8.4.0")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, _, land := botSetup(t, "")
			g.pull.Body = tt.body
			for range 2 {
				if e := land(context.Background(), Options{}); e != nil {
					t.Fatal(e)
				}
			}
			if g.mergeCount != 0 {
				t.Fatal("a minor bump merged")
			}
			noReview(t, s, g)
			got := notices(n)
			if len(got) != 1 || !strings.Contains(got[0], "minor") || !strings.Contains(got[0], "8.4.0") {
				t.Fatalf("want one notice naming minor: %q", got)
			}
			if n.events[0].Lane != "desk" || n.events[0].Type != "decision" {
				t.Fatalf("event=%+v", n.events[0])
			}
			g.pull.Head.SHA = shaC
			if e := land(context.Background(), Options{}); e != nil {
				t.Fatal(e)
			}
			if got := notices(n); len(got) != 2 {
				t.Fatalf("a new head must be told once more: %q", got)
			}
		})
	}
}

// (c) (d) A file outside paths, or a blocking path, is told and not merged.
func TestBotUpdateOtherFilesAndBlockingClassNotLanded(t *testing.T) {
	for _, tt := range []struct {
		name, extra, want string
		files             []policy.File
	}{
		{"outside paths", "", "app/x.py", append([]policy.File{{Path: "app/x.py", Kind: "modified"}}, lockFiles...)},
		{"renamed from outside paths", "", "old/lock.json", []policy.File{{Path: "package-lock.json", Previous: "old/lock.json", Kind: "renamed"}}},
		{"blocking class", "blocking:\n  - paths: ['uv.lock']\n    reason: lockfile\n", "blocking class", []policy.File{{Path: "uv.lock", Kind: "modified"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, _, land := botSetup(t, tt.extra)
			g.files, g.pull.ChangedFiles = tt.files, len(tt.files)
			for range 2 {
				if e := land(context.Background(), Options{}); e != nil {
					t.Fatal(e)
				}
			}
			if g.mergeCount != 0 {
				t.Fatal("merged")
			}
			noReview(t, s, g)
			if got := notices(n); len(got) != 1 || !strings.Contains(got[0], tt.want) {
				t.Fatalf("want one notice with %q: %q", tt.want, got)
			}
		})
	}
}

// (e) A red required check is told; a pending one waits quietly; a cancelled
// one waits for a rerun, then blocks; a skipped one holds nothing.
func TestBotUpdateChecks(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ci       github.CI
		merged   int
		notified bool
	}{
		{"red", github.Red, 0, true},
		{"pending", github.Pending, 0, false},
		{"cancelled 5 min ago", github.Cancelled, 0, false},
		{"cancelled 40 min ago", github.Cancelled, 0, true},
		{"skipped", github.Skipped, 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, _, land := botSetup(t, "")
			s.Now = func() time.Time { return cancelNow }
			g.ci = tt.ci
			g.cancelledAt = map[string]time.Time{"CI": cancelNow.Add(-5 * time.Minute)}
			if strings.HasSuffix(tt.name, "40 min ago") {
				g.cancelledAt["CI"] = cancelNow.Add(-40 * time.Minute)
			}
			if e := land(context.Background(), Options{}); e != nil {
				t.Fatal(e)
			}
			noReview(t, s, g)
			if g.mergeCount != tt.merged || (len(notices(n)) == 1) != tt.notified {
				t.Fatalf("merges=%d notices=%q", g.mergeCount, notices(n))
			}
			if tt.name == "red" && !strings.Contains(notices(n)[0], "check_failed:CI") {
				t.Fatalf("notice=%q", notices(n))
			}
			if strings.HasSuffix(tt.name, "40 min ago") && !strings.Contains(notices(n)[0], "check_cancelled:CI") {
				t.Fatalf("notice=%q", notices(n))
			}
		})
	}
}

// (f) A body with no parseable update table is not patch.
func TestBotUpdateUnknownUpdateType(t *testing.T) {
	for name, body := range map[string]string{
		"no table":                  "Bumps vite from 8.3.4 to 8.3.6.",
		"empty":                     "",
		"no change cell":            "| Package | Change |\n|---|---|\n| vite | updated |\n",
		"prerelease":                "| Package | Change |\n|---|---|\n| vite | `8.3.4` -> `8.3.6-rc.1` |\n",
		"downgrade":                 "| Package | Change |\n|---|---|\n| vite | `8.3.6` -> `8.3.4` |\n",
		"column patch, no versions": "| Package | Update | Change |\n|---|---|---|\n| vite | patch | updated |\n",
		"column patch, unparseable": "| Package | Update | Change |\n|---|---|---|\n| vite | patch | `3.11-alpine` -> `3.11.1-alpine` |\n",
		"column patch, downgrade":   "| Package | Update | Change |\n|---|---|---|\n| vite | patch | `8.3.6` -> `8.3.4` |\n",
		"column patch, unchanged":   "| Package | Update | Change |\n|---|---|---|\n| vite | patch | `8.3.4` -> `8.3.4` |\n",
		"one row not patch": "| Package | Update | Change |\n|---|---|---|\n" +
			"| a | patch | `1.0.0` -> `1.0.1` |\n| b | digest | `abc` -> `def` |\n",
	} {
		t.Run(name, func(t *testing.T) {
			s, g, n, _, land := botSetup(t, "")
			g.pull.Body = body
			if e := land(context.Background(), Options{}); e != nil {
				t.Fatal(e)
			}
			noReview(t, s, g)
			got := notices(n)
			if g.mergeCount != 0 || len(got) != 1 {
				t.Fatalf("merges=%d notices=%q", g.mergeCount, got)
			}
			if name == "one row not patch" {
				if !strings.Contains(got[0], "update type digest") {
					t.Fatalf("notice=%q", got)
				}
			} else if !strings.Contains(got[0], "update type unknown") {
				t.Fatalf("notice=%q", got)
			}
		})
	}
}

// A grouped update lands only when every row is a patch bump.
func TestBotUpdateGroupedRowsAllPatch(t *testing.T) {
	s, g, _, _, land := botSetup(t, "")
	g.pull.Body = "| Package | Update | Change |\n|---|---|---|\n| a | patch | `1.0.0` → `1.0.1` |\n| b | patch | `v2.3.4` → `v2.3.9` |\n"
	if e := land(context.Background(), Options{}); e != nil {
		t.Fatal(e)
	}
	noReview(t, s, g)
	if g.mergeCount != 1 {
		t.Fatalf("merges=%d", g.mergeCount)
	}
}

// A conflict, an unresolved thread or a stale base is told, not merged; an
// unknown mergeable state waits quietly.
func TestBotUpdateMergeability(t *testing.T) {
	no := false
	for _, tt := range []struct {
		name     string
		edit     func(g *fakeGH)
		want     string
		notified bool
	}{
		{"conflict", func(g *fakeGH) { g.pull.Mergeable = &no; g.pull.MergeableState = "dirty" }, "conflict", true},
		{"threads", func(g *fakeGH) { g.findings = 2 }, "threads_open:2", true},
		{"behind", func(g *fakeGH) { g.stale = true; g.pull.MergeableState = "behind" }, "base_stale", true},
		{"unknown", func(g *fakeGH) { g.pull.Mergeable = nil; g.pull.MergeableState = "unknown" }, "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, n, _, land := botSetup(t, "require_up_to_date: true\n")
			yes := true
			g.pull.Mergeable, g.pull.MergeableState = &yes, "clean"
			tt.edit(g)
			if e := land(context.Background(), Options{}); e != nil {
				t.Fatal(e)
			}
			noReview(t, s, g)
			got := notices(n)
			if g.mergeCount != 0 || (len(got) == 1) != tt.notified || (tt.want != "" && !strings.Contains(strings.Join(got, ""), tt.want)) {
				t.Fatalf("merges=%d notices=%q", g.mergeCount, got)
			}
		})
	}
}

// (h) Without bot_updates a bot PR meets today's review gate; a human account
// that merely carries the bot's name is never treated as the bot.
func TestBotUpdateAbsentOrNotTheBotKeepsReviewGate(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy bool
		user   github.PullUser
	}{
		{"no bot_updates section", false, github.PullUser{Login: "renovate[bot]", Type: "Bot"}},
		{"unlisted bot", true, github.PullUser{Login: "dependabot[bot]", Type: "Bot"}},
		{"human with the bot's name", true, github.PullUser{Login: "renovate", Type: "User"}},
		{"human with the bot's login", true, github.PullUser{Login: "renovate[bot]", Type: "User"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, g, n, out, land := botSetup(t, "")
			if !tt.policy {
				g.policyExtra = ""
			}
			g.pull.User = tt.user
			if e := land(context.Background(), Options{}); e != nil {
				t.Fatal(e)
			}
			if g.mergeCount != 0 || len(notices(n)) != 0 {
				t.Fatalf("merges=%d notices=%q", g.mergeCount, notices(n))
			}
			if strings.Contains(out.String(), "bot-patch") || !strings.Contains(out.String(), "review_missing") {
				t.Fatalf("record: %s", out)
			}
		})
	}
}

// (i) Shadow and dry-run never merge; the record says what would happen. Shadow
// still tells the desk about a PR that will not land; dry-run stays silent.
func TestBotUpdateShadowAndDryRunNeverMerge(t *testing.T) {
	for _, tt := range []struct {
		name   string
		o      Options
		action string
	}{
		{"shadow", Options{Shadow: true}, `"action":"merge"`},
		{"dry-run", Options{DryRun: true}, `"action":"merge"`},
	} {
		t.Run(tt.name+" landable", func(t *testing.T) {
			_, g, n, out, land := botSetup(t, "")
			if e := land(context.Background(), tt.o); e != nil {
				t.Fatal(e)
			}
			if g.mergeCount != 0 || len(n.events) != 0 {
				t.Fatalf("merges=%d events=%+v", g.mergeCount, n.events)
			}
			if !strings.Contains(out.String(), tt.action) || !strings.Contains(out.String(), `"reason":"bot-patch"`) {
				t.Fatalf("record: %s", out)
			}
		})
		t.Run(tt.name+" not landable", func(t *testing.T) {
			_, g, n, _, land := botSetup(t, "")
			g.pull.Body = renovateBody("minor", "1.0.0", "1.1.0")
			if e := land(context.Background(), tt.o); e != nil {
				t.Fatal(e)
			}
			want := 0
			if tt.o.Shadow {
				want = 1
			}
			if g.mergeCount != 0 || len(notices(n)) != want {
				t.Fatalf("merges=%d notices=%q", g.mergeCount, notices(n))
			}
		})
	}
	t.Run("policy not enabled", func(t *testing.T) {
		_, g, _, out, land := botSetup(t, "")
		g.enabled = false
		if e := land(context.Background(), Options{}); e != nil {
			t.Fatal(e)
		}
		if g.mergeCount != 0 || !strings.Contains(out.String(), `"shadow":true`) {
			t.Fatalf("merges=%d record: %s", g.mergeCount, out)
		}
	})
}

// The head can move between the decision and the merge: the PR is then not
// merged on the old verdict.
func TestBotUpdateHeadMovedDuringDecisionNotMerged(t *testing.T) {
	_, g, _, _, land := botSetup(t, "")
	g.nextHead = shaC
	if e := land(context.Background(), Options{}); e == nil || !strings.Contains(e.Error(), "changed during decision") {
		t.Fatalf("err=%v", e)
	}
	if g.mergeCount != 0 {
		t.Fatal("merged a head the gate did not see")
	}
}

func TestVersionKind(t *testing.T) {
	for _, tt := range []struct{ from, to, want string }{
		{"8.3.4", "8.3.6", "patch"},
		{"v8.3.4", "v8.3.5", "patch"},
		{"8.3.4", "8.4.0", "minor"},
		{"8.3.9", "8.4.0", "minor"},
		{"8.3.4", "9.0.0", "major"},
		{"8.3.4", "8.3.4", ""},
		{"8.3.4", "8.3.3", ""},
		{"8.4.0", "8.3.9", ""},
		{"8.3.4", "8.3.5-rc.1", ""},
		{"8.3", "8.4", ""},
		{"99999999999999999999.0.0", "99999999999999999999.0.1", ""},
	} {
		if got := versionKind(tt.from, tt.to); got != tt.want {
			t.Errorf("versionKind(%q, %q) = %q, want %q", tt.from, tt.to, got, tt.want)
		}
	}
}

// Tables inside fenced code blocks are examples: they never count as updates,
// and a fence closes only on a matching one.
func TestBotUpdateIgnoresFencedTables(t *testing.T) {
	example := "| Package | Update | Change |\n|---|---|---|\n| vite | patch | `1.0.0` -> `1.0.1` |\n"
	for _, tt := range []struct {
		name, body string
		want       int
	}{
		{"only a fenced example", "```\n" + example + "```\n", 0},
		{"tilde fence", "~~~md\n" + example + "~~~\n", 0},
		{"longer outer fence ignores inner ones", "````\n```\n~~~\n" + example + "````\n", 0},
		{"mismatched fence char does not close", "```\n~~~\n" + example + "```\n", 0},
		{"shorter fence does not close", "````\n```\n" + example + "````\n", 0},
		{"real table after a fence", "```\nx\n```\n" + example, 1},
		{"unclosed fence swallows the rest", "```\n" + example, 0},
	} {
		if got := len(bodyUpdates(tt.body)); got != tt.want {
			t.Errorf("%s: %d rows, want %d", tt.name, got, tt.want)
		}
	}
	// A real minor update plus a fenced patch example is not patch.
	body := renovateBody("minor", "8.3.4", "8.4.0") + "```\n" + example + "```\n"
	if got := nonPatchReason(body); !strings.Contains(got, "minor") {
		t.Errorf("reason=%q", got)
	}
	// A patch example in a fence is no update table.
	if got := nonPatchReason("```\n" + example + "```\n"); got != "update type unknown" {
		t.Errorf("reason=%q", got)
	}
}

// A PR retargeted between the decision and the merge is not merged.
func TestBotUpdateRetargetedDuringDecisionNotMerged(t *testing.T) {
	_, g, _, _, land := botSetup(t, "")
	g.nextBase = "release"
	if e := land(context.Background(), Options{}); e == nil || !strings.Contains(e.Error(), "changed during decision") {
		t.Fatalf("err=%v", e)
	}
	if g.mergeCount != 0 {
		t.Fatal("merged into a base the gate did not see")
	}
}

// A bot patch that lands on a base that moved on records it, as the review gate
// does, and is not held by it.
func TestBotUpdateRecordsBaseBehind(t *testing.T) {
	_, g, _, out, land := botSetup(t, "")
	g.stale = true
	if e := land(context.Background(), Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 || !strings.Contains(out.String(), `"base_behind":true`) {
		t.Fatalf("merges=%d record: %s", g.mergeCount, out)
	}
}

// (d) A bot patch GitHub reports behind its base lands under the default
// policy, and is held as base_stale only when the repo requires up-to-date
// branches.
func TestBotUpdateBehindLandsUnlessRequired(t *testing.T) {
	_, g, n, out, land := botSetup(t, "")
	g.stale = true
	g.pull.MergeableState = "behind"
	if e := land(context.Background(), Options{}); e != nil {
		t.Fatal(e)
	}
	if g.mergeCount != 1 || len(notices(n)) != 0 || !strings.Contains(out.String(), `"base_behind":true`) {
		t.Fatalf("merges=%d notices=%q record: %s", g.mergeCount, notices(n), out)
	}
}
