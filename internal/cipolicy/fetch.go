package cipolicy

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

// Source is the GitHub read side Fetch needs; *github.Client implements it.
type Source interface {
	Head(ctx context.Context, repo, branch string) (string, error)
	ReadFile(ctx context.Context, repo, sha, name string) ([]byte, error)
	PathBlob(ctx context.Context, repo, sha, name string) (string, error)
	Dir(ctx context.Context, repo, sha, dir string) ([]github.TreeEntry, error)
}

var _ Source = (*github.Client)(nil)

// Fetch reads the policy file and the workflow files of r at head. A missing
// or invalid policy file is reported in Input.PolicyErr, not as an error; an
// error means a GitHub read failed.
func Fetch(ctx context.Context, src Source, r config.Repo, head string) (Input, error) {
	in := Input{Repo: r.Name, Branch: r.Branch, Head: head, PolicyPath: r.Policy}
	blob, e := src.PathBlob(ctx, r.Name, head, r.Policy)
	if e != nil {
		return in, e
	}
	if blob == "" {
		in.PolicyErr = "file is missing"
	} else {
		in.PolicyBlob = blob
		b, e := src.ReadFile(ctx, r.Name, head, r.Policy)
		if e != nil {
			return in, e
		}
		if e = config.Decode(bytes.NewReader(b), &in.Policy); e == nil {
			e = in.Policy.Validate()
		}
		if e != nil {
			in.PolicyErr = e.Error()
		}
	}
	entries, e := src.Dir(ctx, r.Name, head, WorkflowDir)
	if e != nil {
		return in, e
	}
	for _, en := range entries {
		if !strings.HasSuffix(en.Path, ".yml") && !strings.HasSuffix(en.Path, ".yaml") {
			continue
		}
		b, e := src.ReadFile(ctx, r.Name, head, en.Path)
		if e != nil {
			return in, fmt.Errorf("read %s: %w", en.Path, e)
		}
		in.Workflows = append(in.Workflows, File{Path: en.Path, Blob: en.SHA, Data: b})
	}
	return in, nil
}
