package promotion

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/mgh3326/mergewake/internal/github"
)

// API is the GitHub REST implementation of GitHub, used by the promotion owner.
type API struct{ C *github.Client }

func (a API) Branch(ctx context.Context, repo, name string) (string, bool, error) {
	return a.C.BranchRef(ctx, repo, name)
}

func (a API) PromotionTags(ctx context.Context, repo string) ([]TagRef, error) {
	refs, e := a.C.MatchingRefs(ctx, repo, "tags/"+TagPrefix)
	if e != nil {
		return nil, e
	}
	var out []TagRef
	for _, r := range refs {
		name := strings.TrimPrefix(r.Ref, "refs/tags/")
		if r.Object.Type != "tag" {
			// A lightweight tag peels to itself; Admit refuses it if it matters.
			out = append(out, TagRef{Name: name, Object: r.Object.SHA, Target: r.Object.SHA})
			continue
		}
		t, e := a.C.GitTag(ctx, repo, r.Object.SHA)
		if e != nil {
			return nil, e
		}
		target := t.Object.SHA
		if t.Object.Type != "commit" {
			target = ""
		}
		out = append(out, TagRef{Name: name, Object: r.Object.SHA, Target: target})
	}
	return out, nil
}

func (a API) TagObject(ctx context.Context, repo string, ref TagRef) (Tag, error) {
	t, e := a.C.GitTag(ctx, repo, ref.Object)
	if e != nil {
		return Tag{}, e
	}
	return Tag{Name: t.Tag, Target: t.Object.SHA, TargetType: t.Object.Type, Message: t.Message}, nil
}

func (a API) Parents(ctx context.Context, repo, sha string) ([]string, error) {
	return a.C.CommitParents(ctx, repo, sha)
}

func (a API) PushRuns(ctx context.Context, repo, branch, sha string) ([]Run, error) {
	rs, e := a.C.PushRuns(ctx, repo, branch, sha)
	if e != nil {
		return nil, e
	}
	out := make([]Run, len(rs))
	for i, r := range rs {
		out[i] = Run{ID: r.ID, WorkflowID: r.WorkflowID, Path: r.Path, Event: r.Event, HeadBranch: r.HeadBranch, HeadSHA: r.HeadSHA, Attempt: r.RunAttempt, Status: r.Status, Conclusion: r.Conclusion}
	}
	return out, nil
}

func (a API) Jobs(ctx context.Context, repo string, run int64, attempt int) ([]Job, error) {
	js, e := a.C.AttemptJobs(ctx, repo, run, attempt)
	if e != nil {
		return nil, e
	}
	out := make([]Job, len(js))
	for i, j := range js {
		out[i] = Job{Name: j.Name, HeadSHA: j.HeadSHA, RunAttempt: j.RunAttempt, Status: j.Status, Conclusion: j.Conclusion}
	}
	return out, nil
}

func (a API) CreateRecord(ctx context.Context, repo, name, target, message string) error {
	return a.C.CreateAnnotatedTag(ctx, repo, name, target, message)
}

func (a API) CreateBranch(ctx context.Context, repo, name, sha string) error {
	return a.C.CreateRef(ctx, repo, name, sha)
}

func (a API) AdvanceBranch(ctx context.Context, repo, name, sha string) error {
	return a.C.FastForwardBranch(ctx, repo, name, sha)
}

// Git reads the pointer and records over the public git protocol, so the
// consumer needs no API token and spends no API rate limit. Cache is a bare
// repository it owns; objects are content-addressed, so a cached tag object
// is verified by its id.
type Git struct {
	// URL maps owner/name to a fetch URL; nil means https://github.com/<repo>.git.
	URL   func(repo string) string
	Cache string
	Exec  Commander
}

func (g Git) url(repo string) string {
	if g.URL != nil {
		return g.URL(repo)
	}
	return "https://github.com/" + repo + ".git"
}

func (g Git) git(ctx context.Context, args ...string) ([]byte, error) {
	out, code, e := g.Exec.Command(ctx, "git", args...)
	if e != nil {
		return nil, e
	}
	if code != 0 {
		return nil, fmt.Errorf("git %s exited %d", args[0], code)
	}
	return out, nil
}

func (g Git) lsRemote(ctx context.Context, repo string, patterns ...string) (map[string]string, error) {
	out, e := g.git(ctx, append([]string{"ls-remote", g.url(repo)}, patterns...)...)
	if e != nil {
		return nil, e
	}
	refs := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		sha, ref, ok := strings.Cut(line, "\t")
		if !ok || !fullSHA.MatchString(sha) {
			return nil, errors.New("unparseable ls-remote output")
		}
		refs[ref] = sha
	}
	return refs, nil
}

func (g Git) Branch(ctx context.Context, repo, name string) (string, bool, error) {
	refs, e := g.lsRemote(ctx, repo, "refs/heads/"+name)
	if e != nil {
		return "", false, e
	}
	sha, ok := refs["refs/heads/"+name]
	return sha, ok, nil
}

func (g Git) PromotionTags(ctx context.Context, repo string) ([]TagRef, error) {
	refs, e := g.lsRemote(ctx, repo, "refs/tags/"+TagPrefix+"*")
	if e != nil {
		return nil, e
	}
	var out []TagRef
	for ref, sha := range refs {
		if strings.HasSuffix(ref, "^{}") {
			continue
		}
		name := strings.TrimPrefix(ref, "refs/tags/")
		target, annotated := refs[ref+"^{}"]
		if !annotated {
			target = sha
		}
		out = append(out, TagRef{Name: name, Object: sha, Target: target})
	}
	return out, nil
}

func (g Git) TagObject(ctx context.Context, repo string, ref TagRef) (Tag, error) {
	if !fullSHA.MatchString(ref.Object) || SeqOf(ref.Name) == 0 {
		return Tag{}, errors.New("invalid tag ref")
	}
	if _, e := os.Stat(filepath.Join(g.Cache, "HEAD")); e != nil {
		if _, e := g.git(ctx, "init", "--bare", "--quiet", g.Cache); e != nil {
			return Tag{}, e
		}
	}
	if _, e := g.git(ctx, "-C", g.Cache, "cat-file", "-e", ref.Object); e != nil {
		// Fetch the tag object and its commit only; no trees or blobs.
		if _, e := g.git(ctx, "-C", g.Cache, "fetch", "--quiet", "--no-tags", "--depth=1", "--filter=tree:0", g.url(repo), "+refs/tags/"+ref.Name+":refs/tags/"+ref.Name); e != nil {
			return Tag{}, e
		}
	}
	raw, e := g.git(ctx, "-C", g.Cache, "cat-file", "tag", ref.Object)
	if e != nil {
		return Tag{}, e
	}
	return parseTag(string(raw))
}

// parseTag reads the raw tag object format: headers, a blank line, the message.
func parseTag(raw string) (Tag, error) {
	head, msg, ok := strings.Cut(raw, "\n\n")
	if !ok {
		return Tag{}, errors.New("tag object has no message")
	}
	var t Tag
	for _, line := range strings.Split(head, "\n") {
		k, v, _ := strings.Cut(line, " ")
		switch k {
		case "object":
			t.Target = v
		case "type":
			t.TargetType = v
		case "tag":
			t.Name = v
		}
	}
	// A signed tag appends its signature after the message.
	if i := strings.Index(msg, "-----BEGIN "); i >= 0 {
		msg = msg[:i]
	}
	t.Message = msg
	return t, nil
}

// FileLock holds an exclusive flock per repo for the life of one promotion. A
// second promotion of the same repo on this host fails instead of waiting.
type FileLock struct{ Dir string }

func (l FileLock) Lock(repo string) (func(), error) {
	if e := os.MkdirAll(l.Dir, 0o700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(l.Dir, "promote-"+strings.ReplaceAll(repo, "/", "_")+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if e != nil {
		return nil, e
	}
	if e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("another promotion of %s holds the lock", repo)
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}

// OSExec runs commands directly, without a shell.
type OSExec struct{}

func (OSExec) Command(ctx context.Context, name string, args ...string) ([]byte, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = os.Stderr
	out, e := cmd.Output()
	var x *exec.ExitError
	if errors.As(e, &x) {
		return out, x.ExitCode(), nil
	}
	if e != nil {
		return nil, -1, e
	}
	return out, 0, nil
}
