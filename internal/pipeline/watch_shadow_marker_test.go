package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/github"
)

// ambiguousHeadGH answers the head's check as the same-sha disagreement a
// re-run that passed beside the head's own failed run leaves behind.
type ambiguousHeadGH struct {
	GitHub
	sha string
}

func (g ambiguousHeadGH) Check(ctx context.Context, repo, sha string, names []string) (github.CI, error) {
	if sha == g.sha {
		return github.Pending, fmt.Errorf("%w named %q", github.ErrAmbiguousCheck, "CI")
	}
	return g.GitHub.Check(ctx, repo, sha, names)
}

// A shadow or dry pass never reads the confirmation marker, even on the
// ambiguous-check branch: it returns the ambiguous error it would return with
// no marker at all, and the marker bytes stay as the acting pass left them.
func TestWatchShadowAmbiguousNeverReadsMarker(t *testing.T) {
	for _, mode := range []struct {
		name string
		o    Options
	}{{"shadow flag", Options{Shadow: true}}, {"dry run", Options{DryRun: true}}} {
		t.Run(mode.name, func(t *testing.T) {
			s, g, _, out, _, r := headCulpritChain(t)
			g.blocking = true
			if e := s.Watch(context.Background(), r, Options{}); e != nil {
				t.Fatal(e)
			}
			path := (&watchStream{Service: s}).redSeenFile(r.Name)
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatalf("acting pass left no marker: %v", e)
			}
			s.GH = ambiguousHeadGH{GitHub: s.GH, sha: shaB}
			out.Reset()
			got := s.Watch(context.Background(), r, mode.o)
			if !errors.Is(got, github.ErrAmbiguousCheck) {
				t.Fatalf("err = %v, want today's ambiguous-check error", got)
			}
			if strings.Contains(out.String(), "mixed result") {
				t.Fatalf("shadow pass reported the mixed hold: %s", out.String())
			}
			after, e := os.ReadFile(path)
			if e != nil || string(after) != string(before) {
				t.Fatalf("marker changed: %q -> %q (%v)", before, after, e)
			}
		})
	}
}

// A shadow pass does not read the marker file at all, so a marker it cannot
// decode cannot fail it: the ambiguous-check error still comes back.
func TestWatchShadowAmbiguousIgnoresUnreadableMarker(t *testing.T) {
	s, g, _, _, _, r := headCulpritChain(t)
	g.blocking = true
	if e := s.Watch(context.Background(), r, Options{}); e != nil {
		t.Fatal(e)
	}
	path := (&watchStream{Service: s}).redSeenFile(r.Name)
	if e := os.WriteFile(path, []byte("{not json"), 0o600); e != nil {
		t.Fatal(e)
	}
	s.GH = ambiguousHeadGH{GitHub: s.GH, sha: shaB}
	got := s.Watch(context.Background(), r, Options{Shadow: true})
	if !errors.Is(got, github.ErrAmbiguousCheck) {
		t.Fatalf("err = %v, want the ambiguous-check error, not the marker read", got)
	}
}
