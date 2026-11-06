package cipolicy

import (
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/policy"
)

const goodCI = `name: CI
on:
  push:
    branches: [main]
  pull_request:
concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true
jobs:
  test:
    runs-on: ubuntu-latest
    steps: [{run: go test ./...}]
`

const anyRef = `name: CI any ref
on:
  workflow_dispatch:
    inputs: {ref: {required: true}}
jobs:
  test:
    runs-on: ubuntu-latest
    steps: [{run: go test ./...}]
`

const releaseFalse = `name: Release
on:
  push:
    branches: [main]
concurrency:
  group: release
  cancel-in-progress: false
jobs:
  publish:
    runs-on: ubuntu-latest
    steps: [{run: ./release.sh}]
`

func pol(t *testing.T, text string) policy.Policy {
	t.Helper()
	var p policy.Policy
	if e := config.Decode(strings.NewReader(text), &p); e != nil {
		t.Fatal(e)
	}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	return p
}

func input(t *testing.T, policyText string, workflows map[string]string) Input {
	t.Helper()
	in := Input{Repo: "o/r", Branch: "main", Head: strings.Repeat("a", 40), PolicyPath: ".mergewake/policy.yaml", PolicyBlob: "p1", Policy: pol(t, policyText)}
	for name, text := range workflows {
		in.Workflows = append(in.Workflows, File{Path: WorkflowDir + "/" + name, Blob: "b-" + name, Data: []byte(text)})
	}
	return in
}

const polOK = "version: 1\nrequired_checks: [test]\nany_ref_workflow: ci-any-ref.yml\n"

func hasReason(r Report, sub string) bool {
	for _, x := range r.Reasons {
		if strings.Contains(x, sub) {
			return true
		}
	}
	return false
}

// (a) A compliant repo is ok.
func TestCIPolicyCompliantRepoIsOK(t *testing.T) {
	r := Check(input(t, polOK, map[string]string{"ci.yml": goodCI, "ci-any-ref.yml": anyRef}))
	if r.Verdict != OK || len(r.Reasons) != 0 {
		t.Fatalf("%+v", r)
	}
	if len(r.Workflows) != 1 || r.Workflows[0].File != "ci.yml" || r.Workflows[0].Produces != ProducesYes || r.Workflows[0].Concurrency.CancelInProgress != CancelTrue || !r.Workflows[0].Concurrency.PerRef {
		t.Fatalf("%+v", r.Workflows)
	}
}

// (b) A push-to-main CI without concurrency warns and names the workflow.
func TestCIPolicyMissingConcurrencyWarns(t *testing.T) {
	noConc := strings.Replace(goodCI, "concurrency:\n  group: ci-${{ github.ref }}\n  cancel-in-progress: true\n", "", 1)
	r := Check(input(t, polOK, map[string]string{"ci.yml": noConc, "ci-any-ref.yml": anyRef}))
	if r.Verdict != Warn || !hasReason(r, "workflow ci.yml: no concurrency block") {
		t.Fatalf("%+v", r)
	}
	for name, tt := range map[string]struct{ from, to, want string }{
		"group is not per ref":   {"ci-${{ github.ref }}", "ci", `group "ci" is not per ref`},
		"cancel false":           {"cancel-in-progress: true", "cancel-in-progress: false", "cancel-in-progress is false"},
		"cancel missing":         {"  cancel-in-progress: true\n", "", "cancel-in-progress is missing"},
		"concurrency is a group": {"group: ci-${{ github.ref }}\n  cancel-in-progress: true", "group: ci-${{ github.ref }}", "cancel-in-progress is missing"},
	} {
		t.Run(name, func(t *testing.T) {
			r := Check(input(t, polOK, map[string]string{"ci.yml": strings.Replace(goodCI, tt.from, tt.to, 1), "ci-any-ref.yml": anyRef}))
			if r.Verdict != Warn || !hasReason(r, tt.want) {
				t.Fatalf("%+v", r)
			}
		})
	}
}

// (c) cancel-in-progress: false on a release workflow is not a warning, while
// the same file warns once one of its jobs is a watched check.
func TestCIPolicyReleaseWorkflowKeepsFalse(t *testing.T) {
	files := map[string]string{"ci.yml": goodCI, "ci-any-ref.yml": anyRef, "release.yml": releaseFalse}
	r := Check(input(t, polOK, files))
	if r.Verdict != OK {
		t.Fatalf("%+v", r)
	}
	var rel Workflow
	for _, w := range r.Workflows {
		if w.File == "release.yml" {
			rel = w
		}
	}
	if rel.Produces != ProducesNo || !rel.Compliant || rel.Concurrency.CancelInProgress != CancelFalse {
		t.Fatalf("%+v", rel)
	}
	r = Check(input(t, "version: 1\nrequired_checks: [test, publish]\nany_ref_workflow: ci-any-ref.yml\n", files))
	if r.Verdict != Warn || !hasReason(r, "workflow release.yml: concurrency group \"release\" is not per ref; cancel-in-progress is false") {
		t.Fatalf("%+v", r)
	}
}

// (d) any_ref_workflow missing, or named but absent, warns.
func TestCIPolicyAnyRefWorkflow(t *testing.T) {
	files := map[string]string{"ci.yml": goodCI, "ci-any-ref.yml": anyRef}
	r := Check(input(t, "version: 1\nrequired_checks: [test]\n", files))
	if r.Verdict != Warn || r.AnyRef.Set || !hasReason(r, "any_ref_workflow is not set") {
		t.Fatalf("%+v", r)
	}
	r = Check(input(t, polOK, map[string]string{"ci.yml": goodCI}))
	if r.Verdict != Warn || !r.AnyRef.Set || r.AnyRef.Exists || !hasReason(r, "any_ref_workflow ci-any-ref.yml does not exist") {
		t.Fatalf("%+v", r)
	}
}

// (e) An expression value is reported as such and is not silently ok.
func TestCIPolicyExpressionIsNotOK(t *testing.T) {
	expr := strings.Replace(goodCI, "cancel-in-progress: true", "cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}", 1)
	r := Check(input(t, polOK, map[string]string{"ci.yml": expr, "ci-any-ref.yml": anyRef}))
	if r.Verdict != Warn || r.Workflows[0].Concurrency.CancelInProgress != CancelExpression || !hasReason(r, "expression") {
		t.Fatalf("%+v", r)
	}
}

// A job name the checker cannot resolve is reported as unknown, and a
// non-compliant workflow with one still warns.
func TestCIPolicyUnknownJobName(t *testing.T) {
	wf := strings.Replace(strings.Replace(goodCI, "test:", "test:\n    name: ${{ matrix.os }}", 1), "cancel-in-progress: true", "cancel-in-progress: false", 1)
	r := Check(input(t, polOK, map[string]string{"ci.yml": wf, "ci-any-ref.yml": anyRef}))
	if r.Verdict != Warn || r.Workflows[0].Produces != ProducesUnknown || !hasReason(r, "cannot tell whether it produces a watched check") {
		t.Fatalf("%+v", r)
	}
}

func TestCIPolicyTriggers(t *testing.T) {
	wf := func(on string) string {
		return "on: " + on + "\njobs:\n  test:\n    runs-on: x\n"
	}
	for _, tt := range []struct {
		on   string
		push bool
	}{
		{"push", true},
		{"[pull_request, push]", true},
		{"pull_request", false},
		{"workflow_dispatch", false},
		{"\n  push:\n    branches: [main]", true},
		{"\n  push:\n    branches: ['release/**']", false},
		{"\n  push:\n    branches: ['**', '!main']", false},
		{"\n  push:\n    branches-ignore: [main]", false},
		{"\n  push:\n    branches-ignore: [dev]", true},
		{"\n  push:\n    tags: ['v*']", false},
		{"\n  push:", true},
		{"\n  pull_request:\n  push:\n    paths: ['**.go']", true},
	} {
		t.Run(strings.TrimSpace(tt.on), func(t *testing.T) {
			r := Check(input(t, polOK, map[string]string{"w.yml": wf(tt.on), "ci-any-ref.yml": anyRef}))
			if got := len(r.Workflows) == 1; got != tt.push {
				t.Fatalf("push-to-main = %t, want %t: %+v", got, tt.push, r)
			}
		})
	}
}

// Matrix and reusable-workflow jobs produce suffixed check names.
func TestCIPolicyMatchesSuffixedCheckNames(t *testing.T) {
	wf := "on: push\njobs:\n  test:\n    strategy: {matrix: {go: [1, 2]}}\n    runs-on: x\n  lint:\n    uses: o/r/.github/workflows/l.yml@v1\n"
	for _, checks := range []string{"test (1)", "lint / golangci"} {
		r := Check(input(t, "version: 1\nrequired_checks: ["+checks+"]\nany_ref_workflow: ci-any-ref.yml\n", map[string]string{"w.yml": wf, "ci-any-ref.yml": anyRef}))
		if r.Workflows[0].Produces != ProducesYes {
			t.Fatalf("%s: %+v", checks, r.Workflows)
		}
	}
}

func TestCIPolicyKeyTracksBlobs(t *testing.T) {
	files := map[string]string{"ci.yml": goodCI, "ci-any-ref.yml": anyRef}
	a := input(t, polOK, files)
	b := input(t, polOK, files)
	if Check(a).Key != Check(b).Key {
		t.Fatal("same blobs, different key")
	}
	b.Workflows[0].Blob = "changed"
	if Check(a).Key == Check(b).Key {
		t.Fatal("workflow blob change kept the key")
	}
	c := input(t, polOK, files)
	c.PolicyBlob = "p2"
	if Check(a).Key == Check(c).Key {
		t.Fatal("policy blob change kept the key")
	}
}

func TestCIPolicyUnreadablePolicyWarns(t *testing.T) {
	in := Input{Repo: "o/r", Branch: "main", PolicyPath: ".mergewake/policy.yaml", PolicyErr: "file is missing", Workflows: []File{{Path: WorkflowDir + "/ci.yml", Blob: "b", Data: []byte(goodCI)}}}
	r := Check(in)
	if r.Verdict != Warn || !hasReason(r, "file is missing") || r.Workflows[0].Produces != ProducesUnknown {
		t.Fatalf("%+v", r)
	}
}

// A job named by an expression that quotes a watched check is taken as that
// check; one that quotes nothing watched stays unknown.
func TestCIPolicyExpressionJobNameQuotingWatchedCheck(t *testing.T) {
	wf := func(name string) string {
		return strings.Replace(goodCI, "test:", "test:\n    name: "+name, 1)
	}
	r := Check(input(t, polOK, map[string]string{"ci.yml": wf("${{ inputs.suite == 'all' && 'test' || 'other' }}"), "ci-any-ref.yml": anyRef}))
	if r.Workflows[0].Produces != ProducesYes || r.Verdict != OK {
		t.Fatalf("%+v", r)
	}
	r = Check(input(t, polOK, map[string]string{"ci.yml": wf("${{ matrix.os }}"), "ci-any-ref.yml": anyRef}))
	if r.Workflows[0].Produces != ProducesUnknown {
		t.Fatalf("%+v", r)
	}
}
