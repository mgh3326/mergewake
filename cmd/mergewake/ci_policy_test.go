package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/cipolicy"
	"github.com/mgh3326/mergewake/internal/github"
)

// fakeCISource serves each repo's files from a map; a repo not in it fails.
type fakeCISource map[string]map[string]string

func (f fakeCISource) Head(_ context.Context, repo, _ string) (string, error) {
	if _, ok := f[repo]; !ok {
		return "", &github.APIError{Code: 404, Method: "GET", Path: repo}
	}
	return strings.Repeat("a", 40), nil
}
func (f fakeCISource) ReadFile(_ context.Context, repo, _, name string) ([]byte, error) {
	return []byte(f[repo][name]), nil
}
func (f fakeCISource) PathBlob(_ context.Context, repo, _, name string) (string, error) {
	if _, ok := f[repo][name]; !ok {
		return "", nil
	}
	return "blob-" + name, nil
}
func (f fakeCISource) Dir(_ context.Context, repo, _, dir string) ([]github.TreeEntry, error) {
	var out []github.TreeEntry
	for name := range f[repo] {
		if strings.HasPrefix(name, dir+"/") {
			out = append(out, github.TreeEntry{Path: name, Type: "blob", SHA: "blob-" + name})
		}
	}
	return out, nil
}

const cliCI = "on:\n  push:\n    branches: [main]\nconcurrency:\n  group: ci-${{ github.ref }}\n  cancel-in-progress: CANCEL\njobs:\n  test:\n    runs-on: x\n"

func ciPolicyHost(t *testing.T, src cipolicy.Source) string {
	t.Helper()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	text := "version: 1\nhost: host-a\nstate_dir: " + filepath.Join(dir, "state") + "\nrepos:\n  - name: o/good\n  - name: o/bad\n"
	if e := os.WriteFile(cfg, []byte(text), 0o600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("GITHUB_TOKEN", "test-token")
	old := ciPolicySource
	ciPolicySource = func(string, io.Writer) cipolicy.Source { return src }
	t.Cleanup(func() { ciPolicySource = old })
	return cfg
}

func twoRepos() fakeCISource {
	return fakeCISource{
		"o/good": {
			".mergewake/policy.yaml":           "version: 1\nrequired_checks: [test]\nany_ref_workflow: any.yml\n",
			".github/workflows/ci.yml":         strings.Replace(cliCI, "CANCEL", "true", 1),
			".github/workflows/any.yml":        "on: workflow_dispatch\njobs:\n  test:\n    runs-on: x\n",
			".github/workflows/not-a-workflow": "ignored",
		},
		"o/bad": {
			".mergewake/policy.yaml":   "version: 1\nrequired_checks: [test]\n",
			".github/workflows/ci.yml": strings.Replace(cliCI, "CANCEL", "false", 1),
		},
	}
}

// (g) --json prints one report per configured repo; --repo narrows it to one.
func TestCIPolicyCLIJSONForTwoRepos(t *testing.T) {
	cfg := ciPolicyHost(t, twoRepos())
	var out, stderr bytes.Buffer
	if e := run(context.Background(), []string{"ci-policy", "--config", cfg, "--json"}, &out, &stderr); e != nil {
		t.Fatal(e, stderr.String())
	}
	var got []cipolicy.Report
	if e := json.Unmarshal(out.Bytes(), &got); e != nil {
		t.Fatalf("%v\n%s", e, out.String())
	}
	if len(got) != 2 || got[0].Repo != "o/good" || got[1].Repo != "o/bad" {
		t.Fatalf("%s", out.String())
	}
	if got[0].Verdict != "ok" || len(got[0].Reasons) != 0 || len(got[0].Workflows) != 1 || got[0].Workflows[0].Concurrency.CancelInProgress != "true" {
		t.Fatalf("good: %+v", got[0])
	}
	bad := got[1]
	if bad.Verdict != "warn" || len(bad.Reasons) != 2 || bad.Workflows[0].Concurrency.CancelInProgress != "false" || bad.AnyRef.Set {
		t.Fatalf("bad: %+v", bad)
	}
	for _, key := range []string{`"verdict"`, `"cancel_in_progress"`, `"any_ref"`, `"watched_checks"`} {
		if !strings.Contains(out.String(), key) {
			t.Fatalf("missing %s in\n%s", key, out.String())
		}
	}

	out.Reset()
	if e := run(context.Background(), []string{"ci-policy", "--config", cfg, "--repo", "o/bad", "--json"}, &out, &stderr); e != nil {
		t.Fatal(e)
	}
	got = nil
	if e := json.Unmarshal(out.Bytes(), &got); e != nil || len(got) != 1 || got[0].Repo != "o/bad" {
		t.Fatalf("%v\n%s", e, out.String())
	}
}

func TestCIPolicyCLIText(t *testing.T) {
	cfg := ciPolicyHost(t, twoRepos())
	var out, stderr bytes.Buffer
	if e := run(context.Background(), []string{"ci-policy", "--config", cfg}, &out, &stderr); e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"o/good main@aaaaaaa OK", "o/bad main@aaaaaaa WARN", "workflow ci.yml: cancel-in-progress is false", "any_ref_workflow is not set"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in\n%s", want, out.String())
		}
	}
}

// A repo that cannot be read is reported and fails the command; the others
// still print.
func TestCIPolicyCLIReportsUnreadableRepo(t *testing.T) {
	src := twoRepos()
	delete(src, "o/bad")
	cfg := ciPolicyHost(t, src)
	var out, stderr bytes.Buffer
	e := run(context.Background(), []string{"ci-policy", "--config", cfg}, &out, &stderr)
	if e == nil || !strings.Contains(e.Error(), "1 of 2 repos could not be read") {
		t.Fatalf("err %v", e)
	}
	if !strings.Contains(out.String(), "o/good main@aaaaaaa OK") || !strings.Contains(out.String(), "o/bad ERROR") {
		t.Fatalf("%s", out.String())
	}
}
