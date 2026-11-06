package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/mergewake/internal/cipolicy"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// ciPolicyDoc is where the rule and the warning are explained.
const ciPolicyDoc = "docs/ci-policy.md"

// ciPolicy checks, once per change of the policy or workflow blobs, that the
// repo follows the fleet main-CI default (see package cipolicy). A GitHub
// client that cannot list a directory, or a failed read, never changes the
// watch pass: the failure is only audited.
func (s *Service) ciPolicy(ctx context.Context, r config.Repo, head string, o Options) {
	src, ok := s.GH.(cipolicy.Source)
	if !ok {
		return
	}
	if e := s.ciPolicyPass(ctx, src, r, head, o); e != nil {
		_ = s.audit(r.Name, "ci-policy-error", map[string]any{"head": head, "error": integrations.Detail(e)}, o.DryRun)
	}
}

func (s *Service) ciPolicyPass(ctx context.Context, src cipolicy.Source, r config.Repo, head string, o Options) error {
	in, e := cipolicy.Fetch(ctx, src, r, head)
	if e != nil {
		return e
	}
	rep := cipolicy.Check(in)
	file := filepath.Join(s.state(r.Name), "ci-policy-key")
	last, e := os.ReadFile(file)
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if string(last) == rep.Key {
		return nil
	}
	if rep.Verdict == cipolicy.Warn {
		if e = s.audit(r.Name, "ci-policy", rep, o.DryRun); e != nil {
			return e
		}
		if !o.Reporting() {
			return nil
		}
		text := fmt.Sprintf("[mergewake] ci-policy warn %s@%s: %s | rule and fix: %s", r.Name, short7(head), strings.Join(rep.Reasons, "; "), ciPolicyDoc)
		reportCtx, cancel := reportContext(ctx)
		defer cancel()
		if e = s.Notifier.Notify(reportCtx, integrations.Event{ID: "mergewake-ci-policy-" + r.Name + "-" + rep.Key, Text: clip(text, findingsLaneText), Lane: r.Lane, Type: "info"}); e != nil {
			return e
		}
	}
	if o.DryRun {
		return nil
	}
	return fsutil.Atomic(file, []byte(rep.Key), 0600)
}
