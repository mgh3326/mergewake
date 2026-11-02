package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

type releaseGH struct{ *fakeGH }

func (g releaseGH) ContainerReleaseHead(context.Context, string) (string, error) { return shaB, nil }

type registryRunner struct {
	out  string
	err  error
	args []string
}

func (r *registryRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.args = args
	return []byte(r.out), r.err
}
func TestContainerReleaseRegistry(t *testing.T) {
	for _, tc := range []struct {
		out string
		err error
		bad bool
	}{
		{"sha256:" + strings.Repeat("b", 64), nil, false}, {"main", nil, true}, {"", errors.New("registry down"), true},
	} {
		s, g, _, _, r := setup(t)
		s.GH = releaseGH{g}
		r.Install = []config.Install{{Kind: "container", Target: "api", Image: "registry.example/api"}}
		registry := &registryRunner{out: tc.out, err: tc.err}
		s.Runner = registry
		m, e := s.containerRelease(context.Background(), r)
		if (e != nil) != tc.bad {
			t.Fatalf("%v %v", m, e)
		}
		if !tc.bad && (m.Commit != shaB || m.Commands[0].Digest != tc.out || registry.args[3] != "registry.example/api:"+shaB) {
			t.Fatalf("%v args=%v", m, registry.args)
		}
	}
}
