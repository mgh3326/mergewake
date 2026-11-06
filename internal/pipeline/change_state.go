package pipeline

import (
	"context"
	"crypto/sha1"
	"fmt"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/changestate"
	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
)

type changeWriterProvider interface {
	ChangeWriter() changestate.Writer
}

func (s *Service) changes() changestate.Writer {
	if p, ok := s.GH.(changeWriterProvider); ok {
		return p.ChangeWriter()
	}
	return nil
}
func (s *Service) writeGate(_ context.Context, r config.Repo, pr github.Pull, base string, v gateVerdict) {
	w := s.changes()
	if w == nil {
		return
	}
	at := time.Now().UTC()
	// ReadFile is keyed by the immutable base and uses the GitHub object cache.
	// Failure to observe the policy revision never affects the decision.
	w.Async(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		body, err := s.GH.ReadFile(ctx, r.Name, base, r.Policy)
		revision := ""
		if err == nil {
			h := sha1.New()
			fmt.Fprintf(h, "blob %d\x00", len(body))
			_, _ = h.Write(body)
			revision = fmt.Sprintf("%x", h.Sum(nil))
		}

		reasons := make([]string, 0, min(len(v.Reasons), 32))
		for _, reason := range v.Reasons {
			if len(reasons) == 32 {
				break
			}
			reasons = append(reasons, changeReason(reason))
		}
		w.Queue(r.Name, pr.Number, "gate", changestate.Section{At: at, Value: map[string]any{"head_sha": pr.Head.SHA, "state": v.State, "reasons": reasons, "policy_revision": revision, "decided_at": at, "source": "review"}})
	})
}

// hk accepts lowercase reason codes, while gate details can contain names and spaces.
func changeReason(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if b.Len() >= 64 {
			break
		}
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:-", r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}
