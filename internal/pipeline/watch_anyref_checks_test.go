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

// perCheckGH answers Check from per-check states, the way the real client
// aggregates them: any red requested check is red, a requested check with no
// state is pending, otherwise green.
type perCheckGH struct {
	*anyRefGH
	states map[string]map[string]github.CI
	asked  []string
}

func (g *perCheckGH) Check(_ context.Context, _, sha string, checks []string) (github.CI, error) {
	g.asked = append(g.asked, sha)
	out := github.Green
	for _, n := range checks {
		switch g.states[sha][n] {
		case github.Red:
			return github.Red, nil
		case github.Green:
		default:
			out = github.Pending
		}
	}
	return out, nil
}

// anyRefChecksSetup builds G, U1, U2, U3, R on main with watch set
// [ci-ok, image]. G is green on both. The intermediate commits carry only
// ci-ok, as any-ref CI posts it. head is R's state per check.
func anyRefChecksSetup(t *testing.T, policyKeys string, ciOK [3]github.CI, head map[string]github.CI) (*Service, *perCheckGH, *fakeNotifier, config.Repo) {
	t.Helper()
	s, g, _, n, r := anyRefSetup(t, false)
	g.policyExtra = "watch_checks: [ci-ok, image]\n" + policyKeys
	p := &perCheckGH{anyRefGH: g, states: map[string]map[string]github.CI{
		anyG:  {"ci-ok": github.Green, "image": github.Green},
		anyU1: {"ci-ok": ciOK[0]},
		anyU2: {"ci-ok": ciOK[1]},
		anyU3: {"ci-ok": ciOK[2]},
		anyR:  head,
	}}
	s.GH = p
	return s, p, n, r
}

var (
	redOnCIOK  = map[string]github.CI{"ci-ok": github.Red, "image": github.Green}
	redOnImage = map[string]github.CI{"ci-ok": github.Green, "image": github.Red}
	u3Red      = [3]github.CI{github.Green, github.Green, github.Red}
)

// (a) With any_ref_checks: [ci-ok], bisect judges the intermediate commits by
// ci-ok alone and blames the first red one.
func TestWatchAnyRefChecksBlamesFirstRed(t *testing.T) {
	s, g, _, r := anyRefChecksSetup(t, "any_ref_checks: [ci-ok]\n", u3Red, redOnCIOK)
	got := lastWatch(t, s, r, Options{})
	if got.Action != "report-culprit" || got.Culprit != anyU3 || got.PR != 13 {
		t.Fatalf("%+v", got)
	}
	if len(g.dispatched) != 0 {
		t.Fatalf("dispatched %v", g.dispatched)
	}
}

// (b) Without the key the same evidence is unknown for image and holds.
func TestWatchAnyRefChecksUnsetHolds(t *testing.T) {
	s, _, _, r := anyRefChecksSetup(t, "", u3Red, redOnCIOK)
	got := lastWatch(t, s, r, Options{})
	if got.Action != "hold" || got.Reason != "incomplete CI evidence in recovery interval" || got.Culprit != "" {
		t.Fatalf("%+v", got)
	}
}

// (c) A head red only on a check any-ref CI does not report cannot be
// attributed: hold with the reason, query no ancestor, blame nothing.
func TestWatchAnyRefChecksHeadRedOutsideHolds(t *testing.T) {
	s, g, n, r := anyRefChecksSetup(t, "any_ref_checks: [ci-ok]\n", u3Red, redOnImage)
	got := lastWatch(t, s, r, Options{})
	if got.Action != "hold" || got.Reason != "red only on checks any-ref CI does not report: image" || got.Culprit != "" || got.PR != 0 {
		t.Fatalf("%+v", got)
	}
	for _, sha := range g.asked {
		if sha != anyR {
			t.Fatalf("queried ancestor %s before the hold", sha[:7])
		}
	}
	if n.count != 1 {
		t.Fatalf("notifications=%d", n.count)
	}
}

// (e) The newest head is judged by the whole watch set: green on ci-ok and red
// on image is not healthy and records no checkpoint.
func TestWatchAnyRefChecksHeadUsesFullWatchSet(t *testing.T) {
	both := func(v github.CI) map[string]github.CI { return map[string]github.CI{"ci-ok": v, "image": v} }
	for _, tt := range []struct {
		name   string
		head   map[string]github.CI
		action string
		mark   bool
	}{
		{"green on both", both(github.Green), "healthy", true},
		{"green on ci-ok, red on image", redOnImage, "hold", false},
		{"green on ci-ok, image pending", map[string]github.CI{"ci-ok": github.Green}, "waiting-for-ci", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _, _, r := anyRefChecksSetup(t, "any_ref_checks: [ci-ok]\n", u3Red, tt.head)
			got := lastWatch(t, s, r, Options{})
			if got.Action != tt.action {
				t.Fatalf("%+v", got)
			}
			_, e := os.Stat(filepath.Join(s.state(r.Name), "last-green"))
			if (e == nil) != tt.mark {
				t.Fatalf("checkpoint written=%t, want %t", e == nil, tt.mark)
			}
		})
	}
}

// A head red on any_ref_checks too still bisects, and the hold reason never
// appears.
func TestWatchAnyRefChecksHeadRedOnBothBisects(t *testing.T) {
	head := map[string]github.CI{"ci-ok": github.Red, "image": github.Red}
	s, _, _, r := anyRefChecksSetup(t, "any_ref_checks: [ci-ok]\n", u3Red, head)
	got := lastWatch(t, s, r, Options{})
	if got.Action != "report-culprit" || strings.Contains(got.Reason, "any-ref CI does not report") {
		t.Fatalf("%+v", got)
	}
}
