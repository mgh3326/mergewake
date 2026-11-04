package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// anyRefGH records workflow dispatches on top of the pipeline fake.
type anyRefGH struct {
	*fakeGH
	dispatched []string
	refs       []string
	err        error
}

func (g *anyRefGH) DispatchWorkflow(_ context.Context, _, workflow, ref string, inputs map[string]string) error {
	if workflow != "ci-any-ref.yml" {
		return &github.APIError{Code: 404}
	}
	g.refs = append(g.refs, ref)
	g.dispatched = append(g.dispatched, inputs["ref"])
	return g.err
}

var (
	anyG  = strings.Repeat("1", 40)
	anyU1 = strings.Repeat("2", 40)
	anyU2 = strings.Repeat("3", 40)
	anyU3 = strings.Repeat("4", 40)
	anyR  = strings.Repeat("5", 40)
)

// anyRefSetup builds G, U1, U2, U3, R on main: G green, R red, U1-U3 with no
// watch evidence. The culprit's PR touches a blocking path so the final pass
// reports it instead of opening a revert.
func anyRefSetup(t *testing.T, key bool) (*Service, *anyRefGH, *fakeSink, *fakeNotifier, config.Repo) {
	t.Helper()
	s, f, sink, n, r := setup(t)
	f.enabled = true
	f.blocking = true
	f.head = anyR
	f.parents = map[string]string{anyR: anyU3, anyU3: anyU2, anyU2: anyU1, anyU1: anyG}
	f.checks = map[string]github.CI{anyG: github.Green, anyR: github.Red}
	f.commitPRs = map[string][]github.Pull{}
	for i, sha := range []string{anyU1, anyU2, anyU3, anyR} {
		f.commitPRs[sha] = []github.Pull{{Number: 11 + i, State: "closed", Merged: true, MergeCommitSHA: sha, ChangedFiles: 1, Base: github.Ref{Ref: "main"}}}
	}
	if key {
		f.policyExtra = "any_ref_workflow: ci-any-ref.yml\n"
	}
	g := &anyRefGH{fakeGH: f}
	s.GH = g
	return s, g, sink, n, r
}

// lastWatch runs one watch pass and returns its final watch audit value.
func lastWatch(t *testing.T, s *Service, r config.Repo, o Options) watchResult {
	t.Helper()
	var out strings.Builder
	s.Out = &out
	if e := s.Watch(context.Background(), r, o); e != nil {
		t.Fatal(e)
	}
	var got watchResult
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec struct {
			Command string          `json:"command"`
			Value   json.RawMessage `json:"value"`
		}
		if e := json.Unmarshal([]byte(line), &rec); e != nil {
			t.Fatal(e)
		}
		if rec.Command == "watch" {
			got = watchResult{}
			if e := json.Unmarshal(rec.Value, &got); e != nil {
				t.Fatal(e)
			}
		}
	}
	return got
}

func reasonFor(sha string) string { return "waiting for any-ref CI on " + sha[:7] }

// (a) Each pass dispatches only the commit bisect needs next, the fake then
// posts its result, and the final pass names the culprit full evidence names.
func TestWatchAnyRefBisectDispatches(t *testing.T) {
	names := map[string]string{anyU1: "U1", anyU2: "U2", anyU3: "U3", anyR: "R"}
	for _, tt := range []struct {
		name  string
		truth [3]github.CI
	}{
		{"culprit U1", [3]github.CI{github.Red, github.Red, github.Red}},
		{"culprit U2", [3]github.CI{github.Green, github.Red, github.Red}},
		{"culprit U3", [3]github.CI{github.Green, github.Green, github.Red}},
		{"culprit R", [3]github.CI{github.Green, github.Green, github.Green}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			truth := map[string]github.CI{anyU1: tt.truth[0], anyU2: tt.truth[1], anyU3: tt.truth[2]}

			// Control: full evidence, key unset.
			full, fg, _, _, fr := anyRefSetup(t, false)
			for sha, v := range truth {
				fg.checks[sha] = v
			}
			want := lastWatch(t, full, fr, Options{})
			if want.Action != "report-culprit" || want.Culprit == "" {
				t.Fatalf("control: %+v", want)
			}

			s, g, _, n, r := anyRefSetup(t, true)
			var got watchResult
			for pass := 1; pass <= 6; pass++ {
				before := len(g.dispatched)
				got = lastWatch(t, s, r, Options{})
				if got.Action != "hold" {
					break
				}
				if len(g.dispatched) != before+1 {
					t.Fatalf("pass %d dispatched %v", pass, g.dispatched[before:])
				}
				sha := g.dispatched[len(g.dispatched)-1]
				if got.Reason != reasonFor(sha) || got.AnyRefDispatch != sha {
					t.Fatalf("pass %d: %+v", pass, got)
				}
				// The any-ref run finishes and posts its decisive status.
				g.checks[sha] = truth[sha]
			}
			var seq []string
			for _, sha := range g.dispatched {
				seq = append(seq, names[sha])
			}
			t.Logf("dispatch sequence: %v; culprit %s", seq, names[got.Culprit])
			if got.Action != "report-culprit" || got.Culprit != want.Culprit || got.PR != want.PR {
				t.Fatalf("got %+v want %+v (dispatches %v)", got, want, seq)
			}
			if limit := anyRefLimit(4); len(g.dispatched) > limit {
				t.Fatalf("%d dispatches over bound %d", len(g.dispatched), limit)
			}
			if names[g.dispatched[0]] != "U2" {
				t.Fatalf("first dispatch %v, want bisect's midpoint U2", seq)
			}
			for _, ref := range g.refs {
				if ref != "main" {
					t.Fatalf("dispatched on ref %q", ref)
				}
			}
			// Waiting is quiet; only the culprit report reaches the desk.
			if n.count != 1 {
				t.Fatalf("notifications=%d", n.count)
			}
		})
	}
	t.Run("dispatch bound", func(t *testing.T) {
		s, g, _, n, r := anyRefSetup(t, true)
		st := anyRefIncident{Head: anyR, Limit: anyRefLimit(4)}
		for i := 0; i < st.Limit; i++ {
			st.Dispatches = append(st.Dispatches, anyRefDispatch{SHA: strings.Repeat(string(rune('a'+i)), 40), At: time.Now(), Head: anyR})
		}
		if e := s.saveAnyRef(r.Name, st); e != nil {
			t.Fatal(e)
		}
		for pass := 0; pass < 2; pass++ {
			got := lastWatch(t, s, r, Options{})
			if got.Action != "hold" || !strings.Contains(got.Reason, "any-ref CI dispatch bound reached (4 of 4)") {
				t.Fatalf("%+v", got)
			}
		}
		if len(g.dispatched) != 0 || n.count != 1 {
			t.Fatalf("dispatches=%v notifications=%d", g.dispatched, n.count)
		}
	})
}

// (b) A fresh Service over the same state dir never dispatches a sha twice.
func TestWatchAnyRefRestartNoDuplicate(t *testing.T) {
	s, g, sink, n, r := anyRefSetup(t, true)
	fresh := func() *Service {
		return &Service{Config: s.Config, GH: g, Runner: s.Runner, Sink: sink, Notifier: n}
	}
	if got := lastWatch(t, fresh(), r, Options{}); got.Reason != reasonFor(anyU2) {
		t.Fatalf("%+v", got)
	}
	// Restart before the result arrives: still waiting, no second dispatch.
	for i := 0; i < 2; i++ {
		if got := lastWatch(t, fresh(), r, Options{}); got.Reason != reasonFor(anyU2) || got.AnyRefDispatch != "" {
			t.Fatalf("%+v", got)
		}
	}
	g.checks[anyU2] = github.Red
	if got := lastWatch(t, fresh(), r, Options{}); got.Reason != reasonFor(anyU1) {
		t.Fatalf("%+v", got)
	}
	if got := lastWatch(t, fresh(), r, Options{}); got.Reason != reasonFor(anyU1) {
		t.Fatalf("%+v", got)
	}
	seen := map[string]bool{}
	for _, sha := range g.dispatched {
		if seen[sha] {
			t.Fatalf("sha dispatched twice: %v", g.dispatched)
		}
		seen[sha] = true
	}
	if len(g.dispatched) != 2 {
		t.Fatalf("%v", g.dispatched)
	}
}

// (c) Without the key, watch holds as before and never dispatches.
func TestWatchAnyRefKeyUnsetHolds(t *testing.T) {
	s, g, _, _, r := anyRefSetup(t, false)
	got := lastWatch(t, s, r, Options{})
	if got.Action != "hold" || got.Reason != "incomplete CI evidence in recovery interval" || got.AnyRefDispatch != "" {
		t.Fatalf("%+v", got)
	}
	if len(g.dispatched) != 0 {
		t.Fatalf("%v", g.dispatched)
	}
}

// (d) Shadow and dry runs dispatch nothing and say what they would dispatch.
func TestWatchAnyRefShadowAndDryRun(t *testing.T) {
	for _, mode := range []string{"policy shadow", "shadow flag", "dry run"} {
		t.Run(mode, func(t *testing.T) {
			s, g, _, n, r := anyRefSetup(t, true)
			g.enabled = mode != "policy shadow"
			o := Options{Shadow: mode == "shadow flag", DryRun: mode == "dry run"}
			for i := 0; i < 2; i++ {
				got := lastWatch(t, s, r, o)
				if got.Action != "hold" || got.AnyRefDispatch != anyU2 || got.Reason != "would dispatch any-ref CI on "+anyU2[:7] {
					t.Fatalf("%+v", got)
				}
			}
			if len(g.dispatched) != 0 || n.count != 0 {
				t.Fatalf("dispatches=%v notifications=%d", g.dispatched, n.count)
			}
			if _, e := os.Stat(s.anyRefFile(r.Name)); !os.IsNotExist(e) {
				t.Fatalf("dispatch record written: %v", e)
			}
		})
	}
}

// (e) A refused dispatch holds with the refusal and notifies once.
func TestWatchAnyRefDispatchRefused(t *testing.T) {
	s, g, _, n, r := anyRefSetup(t, true)
	g.err = &github.APIError{Code: 403, Method: "POST", Path: "/repos/example/project/actions/workflows/ci-any-ref.yml/dispatches"}
	for i := 0; i < 3; i++ {
		got := lastWatch(t, s, r, Options{})
		if got.Action != "hold" || !strings.HasPrefix(got.Reason, "any-ref CI dispatch refused for "+anyU2[:7]) || !strings.Contains(got.Reason, "HTTP 403") {
			t.Fatalf("%+v", got)
		}
	}
	if len(g.dispatched) != 1 || n.count != 1 {
		t.Fatalf("dispatches=%v notifications=%d", g.dispatched, n.count)
	}
}

// (f) A dispatched sha with no result past the wait bound holds and notifies once.
func TestWatchAnyRefTimeout(t *testing.T) {
	s, g, _, n, r := anyRefSetup(t, true)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	if got := lastWatch(t, s, r, Options{}); got.Reason != reasonFor(anyU2) {
		t.Fatalf("%+v", got)
	}
	now = now.Add(44 * time.Minute)
	if got := lastWatch(t, s, r, Options{}); got.Reason != reasonFor(anyU2) {
		t.Fatalf("%+v", got)
	}
	now = now.Add(2 * time.Minute)
	for i := 0; i < 2; i++ {
		got := lastWatch(t, s, r, Options{})
		if got.Action != "hold" || got.Reason != "any-ref CI timed out on "+anyU2[:7] {
			t.Fatalf("%+v", got)
		}
	}
	if len(g.dispatched) != 1 || n.count != 1 {
		t.Fatalf("dispatches=%v notifications=%d", g.dispatched, n.count)
	}
}

// A new red head starts a new incident; a green head ends it.
func TestWatchAnyRefIncidentScope(t *testing.T) {
	s, g, _, _, r := anyRefSetup(t, true)
	g.err = &github.APIError{Code: 422}
	if got := lastWatch(t, s, r, Options{}); !strings.HasPrefix(got.Reason, "any-ref CI dispatch refused") {
		t.Fatalf("%+v", got)
	}
	// A new red head on top of R: the refusal no longer applies.
	g.err = nil
	head := strings.Repeat("6", 40)
	g.head = head
	g.parents[head] = anyR
	g.checks[head] = github.Red
	got := lastWatch(t, s, r, Options{})
	if got.AnyRefDispatch == "" || !strings.HasPrefix(got.Reason, "waiting for any-ref CI on ") {
		t.Fatalf("%+v", got)
	}
	g.checks[head] = github.Green
	if got = lastWatch(t, s, r, Options{}); got.Action != "healthy" {
		t.Fatalf("%+v", got)
	}
	if _, e := os.Stat(s.anyRefFile(r.Name)); !os.IsNotExist(e) {
		t.Fatalf("incident not ended: %v", e)
	}
}

func TestAnyRefLimit(t *testing.T) {
	for n, want := range map[int]int{1: 2, 2: 3, 3: 4, 4: 4, 5: 5, 8: 5, 9: 6} {
		if got := anyRefLimit(n); got != want {
			t.Errorf("anyRefLimit(%d)=%d want %d", n, got, want)
		}
	}
}
