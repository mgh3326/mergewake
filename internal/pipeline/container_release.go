package pipeline

import (
	"context"
	"errors"
	"strings"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
)

func (s *Service) containerRelease(ctx context.Context, r config.Repo) (manifest.Manifest, error) {
	source, ok := s.GH.(interface {
		ContainerReleaseHead(context.Context, string) (string, error)
	})
	if !ok {
		return manifest.Manifest{}, errors.New("container release adapter unavailable")
	}
	sha, e := source.ContainerReleaseHead(ctx, r.Name)
	if e != nil {
		return manifest.Manifest{}, e
	}
	if !manifest.SHA.MatchString(sha) {
		return manifest.Manifest{}, errors.New("invalid container release commit")
	}
	specs := append([]config.Install(nil), r.Install...)
	for i, spec := range specs {
		// The registry never chooses a commit: GitHub chooses it and CI is checked
		// by Rollout before a digest can reach the installer.
		out, e := s.Runner.Run(ctx, "docker", []string{"buildx", "imagetools", "inspect", spec.Image + ":" + sha, "--format", "{{.Manifest.Digest}}"}, "", "")
		if e != nil {
			return manifest.Manifest{}, e
		}
		digest := strings.TrimSpace(string(out))
		if !manifest.ContainerDigest.MatchString(digest) {
			return manifest.Manifest{}, errors.New("registry did not return an immutable digest")
		}
		specs[i].Digest = digest
	}
	return manifest.Build(r.Name, sha, specs, nil)
}
