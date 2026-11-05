package promotion

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func gitT(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(cmd.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("git %v: %v\n%s", args, e, out)
	}
	return strings.TrimSpace(string(out))
}

// The consumer's reader over the real git protocol: ls-remote for the pointer
// and peeled tags, a tag-object-only fetch, and the raw tag parse.
func TestGitReaderAdmitsThroughRealGit(t *testing.T) {
	dir := t.TempDir()
	work := filepath.Join(dir, "work")
	gitT(t, dir, "init", "-q", "-b", "main", work)
	gitT(t, work, "commit", "-q", "--allow-empty", "-m", "G")
	g := gitT(t, work, "rev-parse", "HEAD")
	gitT(t, work, "commit", "-q", "--allow-empty", "-m", "R")
	r := gitT(t, work, "rev-parse", "HEAD")
	rec := Record{Schema: 1, Repo: repo, Seq: 1, SHA: g, Artifact: ArtifactRef{Image: "ghcr.io/mgh3326/brewdial", Platform: "linux/arm64", Tag: "sha-" + g[:7], Digest: digestOf("G")}, Policy: PolicyVersion, Controller: Controller{Identity: "app", Generation: 1}, Created: "2026-11-04T12:00:00Z"}
	gitT(t, work, "tag", "-a", "promotion/1", "-m", string(rec.Canonical()), g)
	gitT(t, work, "tag", "latest", r) // a mutable tag admission must ignore
	gitT(t, work, "branch", "deployable", g)

	reader := Git{URL: func(string) string { return work }, Cache: filepath.Join(dir, "cache"), Exec: OSExec{}}
	s := testSuite(t)
	a, e := Admit(context.Background(), reader, repo, s)
	if e != nil {
		t.Fatal(e)
	}
	if a.SHA != g || a.Digest != digestOf("G") || a.Seq != 1 {
		t.Fatalf("admitted %+v", a)
	}

	// Pointer moved to R with no record for R: refuse.
	gitT(t, work, "branch", "-f", "deployable", r)
	_, e = Admit(context.Background(), reader, repo, s)
	mustRefuse(t, e, "has no promotion record")

	// A lightweight promotion tag on R is not a record.
	gitT(t, work, "tag", "promotion/2", r)
	_, e = Admit(context.Background(), reader, repo, s)
	mustRefuse(t, e, "lightweight")

	// Unreachable remote: refuse.
	reader.URL = func(string) string { return filepath.Join(dir, "missing") }
	_, e = Admit(context.Background(), reader, repo, s)
	mustRefuse(t, e, "denied")
}
