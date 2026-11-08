package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/policy"
)

// suiteGH serves one any-ref workflow file and records every dispatch's inputs.
type suiteGH struct {
	*fakeGH
	workflow string
	inputs   []map[string]string
}

func (g *suiteGH) ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error) {
	if strings.HasSuffix(name, "/ci-any-ref.yml") {
		return []byte(g.workflow), nil
	}
	return g.fakeGH.ReadFile(ctx, repo, sha, name)
}

func (g *suiteGH) DispatchWorkflow(_ context.Context, _, _, _ string, inputs map[string]string) error {
	g.inputs = append(g.inputs, inputs)
	return nil
}

// suiteWorkflow declares a workflow_dispatch ref input and, when typ is set, a
// suite input of that type with the given options. options nil writes no list.
func suiteWorkflow(typ string, options []string) string {
	w := "on:\n  workflow_dispatch:\n    inputs:\n      ref:\n        type: string\n"
	if typ == "" && options == nil {
		return w
	}
	w += "      suite:\n"
	if typ != "" {
		w += "        type: " + typ + "\n"
	}
	if options != nil {
		w += "        options: [" + strings.Join(options, ", ") + "]\n"
	}
	return w
}

// suiteDispatch dispatches once for the given stream and returns the inputs sent.
func suiteDispatch(t *testing.T, workflow string, deploy bool) (map[string]string, error) {
	t.Helper()
	s, f, _, _, r := setup(t)
	g := &suiteGH{fakeGH: f, workflow: workflow}
	s.GH = g
	ws := &watchStream{Service: s, deploy: deploy, suites: true}
	p := policy.Policy{AnyRefWorkflow: "ci-any-ref.yml"}
	_, err := ws.dispatch(context.Background(), g, r, p, anyU2)
	if err != nil {
		return nil, err
	}
	if len(g.inputs) != 1 {
		t.Fatalf("dispatches=%d, want 1", len(g.inputs))
	}
	return g.inputs[0], nil
}

// (a) A choice input whose options hold main and deploy gets each stream's value.
func TestWatchAnyRefSuiteOptionsAccepted(t *testing.T) {
	wf := suiteWorkflow("choice", []string{"main", "deploy"})
	if in, err := suiteDispatch(t, wf, false); err != nil || in["suite"] != "main" {
		t.Fatalf("main: inputs=%v err=%v", in, err)
	}
	if in, err := suiteDispatch(t, wf, true); err != nil || in["suite"] != "deploy" {
		t.Fatalf("deploy: inputs=%v err=%v", in, err)
	}
}

// (b) A workflow whose options lack main gets no suite from the main stream,
// so it runs its own default; the deploy stream still sends deploy.
func TestWatchAnyRefSuiteMainOmittedWhenUnsupported(t *testing.T) {
	wf := suiteWorkflow("choice", []string{"all", "test", "vitest", "build", "deploy"})
	in, err := suiteDispatch(t, wf, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := in["suite"]; ok {
		t.Fatalf("main sent suite=%q, want none: %v", in["suite"], in)
	}
	if in, err := suiteDispatch(t, wf, true); err != nil || in["suite"] != "deploy" {
		t.Fatalf("deploy: inputs=%v err=%v", in, err)
	}
}

// (c) Deploy is refused without a dispatch when its options lack deploy; the
// main stream omits suite instead.
func TestWatchAnyRefSuiteDeployRefusedWhenUnsupported(t *testing.T) {
	wf := suiteWorkflow("choice", []string{"all", "test"})
	in, err := suiteDispatch(t, wf, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := in["suite"]; ok {
		t.Fatalf("main sent suite=%q, want none", in["suite"])
	}
	_, err = suiteDispatch(t, wf, true)
	var unsupported *anyRefSuiteError
	if !errors.As(err, &unsupported) {
		t.Fatalf("deploy err=%v, want anyRefSuiteError", err)
	}
	if got := strings.Join(unsupported.Options, ","); got != "all,test" {
		t.Fatalf("options=%q", got)
	}
}

// (c, continued) The deploy hold is recorded once, sends nothing, and stays
// unknown: a reason that is not a dispatch and notifies the desk.
func TestWatchAnyRefSuiteUnsupportedHoldsOnce(t *testing.T) {
	s, f, _, _, r := setup(t)
	g := &suiteGH{fakeGH: f, workflow: suiteWorkflow("choice", []string{"all", "test"})}
	s.GH = g
	var out strings.Builder
	s.Out = &out
	ws := &watchStream{Service: s, deploy: true, suites: true}
	p := policy.Policy{AnyRefWorkflow: "ci-any-ref.yml"}
	ctx := context.Background()

	reason, notify, err := ws.anyRef(ctx, r, p, anyR, anyU2, 2, true, &watchResult{})
	if err != nil {
		t.Fatal(err)
	}
	if !notify || !strings.Contains(reason, "deploy") || !strings.Contains(reason, "all, test") {
		t.Fatalf("reason=%q notify=%v", reason, notify)
	}
	again, notify2, err := ws.anyRef(ctx, r, p, anyR, anyU2, 2, true, &watchResult{})
	if err != nil {
		t.Fatal(err)
	}
	if again != reason || !notify2 {
		t.Fatalf("second pass reason=%q notify=%v, want %q", again, notify2, reason)
	}
	if len(g.inputs) != 0 {
		t.Fatalf("dispatched %v, want none", g.inputs)
	}
	if n := strings.Count(out.String(), `"command":"anyref-suite-unsupported"`); n != 1 {
		t.Fatalf("anyref-suite-unsupported rows=%d, want 1:\n%s", n, out.String())
	}
	if !strings.Contains(out.String(), `"stream":"deploy"`) {
		t.Fatalf("audit row lacks stream: %s", out.String())
	}
}

// (d) A string suite input with no options list takes each stream's value as before.
func TestWatchAnyRefSuiteFreeStringUnchanged(t *testing.T) {
	wf := suiteWorkflow("string", nil)
	if in, err := suiteDispatch(t, wf, false); err != nil || in["suite"] != "main" {
		t.Fatalf("main: inputs=%v err=%v", in, err)
	}
	if in, err := suiteDispatch(t, wf, true); err != nil || in["suite"] != "deploy" {
		t.Fatalf("deploy: inputs=%v err=%v", in, err)
	}
	// An unexpected options shape (a scalar, not a list) also takes the free-string rule.
	odd := "on:\n  workflow_dispatch:\n    inputs:\n      suite:\n        type: string\n        options: all\n"
	if in, err := suiteDispatch(t, odd, true); err != nil || in["suite"] != "deploy" {
		t.Fatalf("odd options: inputs=%v err=%v", in, err)
	}
}

// (e) A workflow with no suite input is dispatched with no suite, as before.
func TestWatchAnyRefNoSuiteInput(t *testing.T) {
	wf := suiteWorkflow("", nil)
	for _, deploy := range []bool{false, true} {
		in, err := suiteDispatch(t, wf, deploy)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := in["suite"]; ok {
			t.Fatalf("deploy=%v sent suite=%q, want none", deploy, in["suite"])
		}
	}
}
