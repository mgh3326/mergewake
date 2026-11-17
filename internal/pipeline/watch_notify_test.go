package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// notifySHAs is a main chain for the notify-check tests: ng is the green
// baseline, nm1 and nm2 land before the first red head, nm3 continues the
// streak, ng2 is green again and nr2 opens the second streak.
var (
	notifyNG  = strings.Repeat("6", 40)
	notifyNM1 = strings.Repeat("7", 40)
	notifyNM2 = strings.Repeat("8", 40)
	notifyNM3 = strings.Repeat("9", 40)
	notifyNG2 = strings.Repeat("0", 40)
	notifyNR2 = strings.Repeat("e", 40)
)

const notifyCheckName = "Deploy stage — VerifyAll"

// notifyStreakSetup builds the chain ng <- nm1 <- nm2 <- nm3 <- ng2 <- nr2 with the
// notify check red on nm2, nm3 and nr2 and green on ng and ng2. The watch set
// stays green on every head, so only the notify check moves.
func notifyStreakSetup(t *testing.T) (*Service, *fakeGH, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, _, n, r := setup(t)
	g.enabled = true
	g.head = notifyNG
	g.parents = map[string]string{
		notifyNM1: notifyNG,
		notifyNM2: notifyNM1,
		notifyNM3: notifyNM2,
		notifyNG2: notifyNM3,
		notifyNR2: notifyNG2,
	}
	state := func(v github.CI) map[string]github.CI { return map[string]github.CI{notifyCheckName: v} }
	g.checkNames = map[string]map[string]github.CI{
		notifyNG:  state(github.Green),
		notifyNM2: state(github.Red),
		notifyNM3: state(github.Red),
		notifyNG2: state(github.Green),
		notifyNR2: state(github.Red),
	}
	merged := func(n int, sha, title string) github.Pull {
		return github.Pull{Number: n, State: "closed", Merged: true, MergeCommitSHA: sha, Title: title, Base: github.Ref{Ref: r.Branch}}
	}
	g.commitPRs = map[string][]github.Pull{
		notifyNM1: {merged(21, notifyNM1, "first merge")},
		notifyNM2: {merged(22, notifyNM2, "second merge")},
		notifyNM3: {merged(23, notifyNM3, "third merge")},
		notifyNR2: {merged(24, notifyNR2, "fourth merge")},
	}
	g.policyExtra = "notify_checks: ['" + notifyCheckName + "']\n"
	r.Lane = "design-lane"
	return s, g, n, r
}

func notifyStreakPass(t *testing.T, s *Service, g *fakeGH, r config.Repo, head string) watchResult {
	t.Helper()
	g.head = head
	return lastWatch(t, s, r, Options{})
}

// red, red, green, red sends two notices: the first names the check and the
// first red SHA and lists the merges between the last green and it; the
// second streak sends a new notice. The check is never reverted.
func TestNotifyChecksRedStreakNotices(t *testing.T) {
	s, g, n, r := notifyStreakSetup(t)

	// Green baseline: the streak state records the last green head, no notice.
	got := notifyStreakPass(t, s, g, r, notifyNG)
	if got.Action != "healthy" || n.count != 0 {
		t.Fatalf("green pass: %+v events=%d", got, n.count)
	}

	// First red head: one notice with both merges since the green baseline,
	// including nm1 which was never an observed head.
	got = notifyStreakPass(t, s, g, r, notifyNM2)
	if got.Action != "healthy" || n.count != 1 {
		t.Fatalf("first red pass: %+v events=%d", got, n.count)
	}
	first := n.events[0]
	for _, want := range []string{
		`"` + notifyCheckName + `"`,
		"first red " + notifyNM2[:7],
		"last green " + notifyNG[:7],
		"#21 first merge", "#22 second merge",
	} {
		if !strings.Contains(first.Text, want) {
			t.Fatalf("notice lacks %q: %s", want, first.Text)
		}
	}
	for _, notWant := range []string{"#23", "#24"} {
		if strings.Contains(first.Text, notWant) {
			t.Fatalf("notice lists %s outside the interval: %s", notWant, first.Text)
		}
	}
	if first.Lane != "design-lane" || first.Type != "info" {
		t.Fatalf("notice lane=%q type=%q", first.Lane, first.Type)
	}

	// Still red: the same streak stays quiet.
	got = notifyStreakPass(t, s, g, r, notifyNM3)
	if got.Action != "healthy" || n.count != 1 {
		t.Fatalf("second red pass: %+v events=%d", got, n.count)
	}

	// Green ends the streak; still no notice.
	got = notifyStreakPass(t, s, g, r, notifyNG2)
	if got.Action != "healthy" || n.count != 1 {
		t.Fatalf("green pass: %+v events=%d", got, n.count)
	}

	// A new red streak sends one new notice with its own merge list.
	got = notifyStreakPass(t, s, g, r, notifyNR2)
	if got.Action != "healthy" || n.count != 2 {
		t.Fatalf("new streak pass: %+v events=%d", got, n.count)
	}
	second := n.events[1]
	for _, want := range []string{
		`"` + notifyCheckName + `"`,
		"first red " + notifyNR2[:7],
		"last green " + notifyNG2[:7],
		"#24 fourth merge",
	} {
		if !strings.Contains(second.Text, want) {
			t.Fatalf("second notice lacks %q: %s", want, second.Text)
		}
	}
	for _, notWant := range []string{"#21", "#22", "#23"} {
		if strings.Contains(second.Text, notWant) {
			t.Fatalf("second notice lists %s outside the interval: %s", notWant, second.Text)
		}
	}

	if g.revertCount != 0 || g.mergeCount != 0 {
		t.Fatalf("notify check reverted or merged: reverts=%d merges=%d", g.revertCount, g.mergeCount)
	}
	if _, e := os.Stat(filepath.Join(s.state(r.Name), "recovery.json")); !os.IsNotExist(e) {
		t.Fatalf("notify check wrote recovery state: %v", e)
	}
	streaks, e := os.ReadFile(filepath.Join(s.state(r.Name), "notify-checks.json"))
	if e != nil || !strings.Contains(string(streaks), notifyCheckName) {
		t.Fatalf("streak state missing: %q %v", streaks, e)
	}
}

// checkStatesSpy records the check names each CheckStates read asks for.
type checkStatesSpy struct {
	*fakeGH
	asked [][]string
}

func (g *checkStatesSpy) CheckStates(ctx context.Context, repo, sha string, required []string) (map[string]github.CI, error) {
	g.asked = append(g.asked, append([]string(nil), required...))
	return g.fakeGH.CheckStates(ctx, repo, sha, required)
}

// A repo without notify_checks behaves as today: the checks are never read,
// no streak state is written and nothing is notified.
func TestNotifyChecksAbsentNoNotice(t *testing.T) {
	s, g, _, n, r := setup(t)
	g.enabled = true
	g.head = notifyNM2
	g.parents = map[string]string{notifyNM2: notifyNM1, notifyNM1: notifyNG}
	spy := &checkStatesSpy{fakeGH: g}
	s.GH = spy

	got := lastWatch(t, s, r, Options{})
	if got.Action != "healthy" || n.count != 0 {
		t.Fatalf("pass: %+v events=%d", got, n.count)
	}
	for _, names := range spy.asked {
		for _, name := range names {
			if name == notifyCheckName {
				t.Fatalf("notify check read without the policy key: %v", spy.asked)
			}
		}
	}
	if _, e := os.Stat(filepath.Join(s.state(r.Name), "notify-checks.json")); !os.IsNotExist(e) {
		t.Fatalf("streak state written without the policy key: %v", e)
	}
}

// notify_checks names must be nonempty and unique, as for watch_checks.
func TestNotifyChecksValidation(t *testing.T) {
	for _, tt := range []struct{ name, extra string }{
		{"blank", "notify_checks: [' ']\n"},
		{"duplicate", "notify_checks: [deploy-ok, deploy-ok]\n"},
		{"in watch set", "notify_checks: [CI]\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, g, _, _, r := setup(t)
			g.policyExtra = tt.extra
			if _, e := s.Policy(context.Background(), r, shaB); e == nil || !strings.Contains(e.Error(), "notify_checks") {
				t.Fatalf("want an error naming notify_checks, got %v", e)
			}
		})
	}
	s, g, _, _, r := setup(t)
	g.policyExtra = "notify_checks: [deploy-ok, verify-all]\n"
	p, e := s.Policy(context.Background(), r, shaB)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.NotifyChecks) != 2 || p.NotifyChecks[0] != "deploy-ok" {
		t.Fatalf("notify checks %v", p.NotifyChecks)
	}
}
