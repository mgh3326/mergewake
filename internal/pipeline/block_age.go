package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type blockAge struct {
	First     time.Time `json:"first"`
	Heads     []string  `json:"heads"`
	Escalated bool      `json:"escalated"`
}

// Check failures have no review thread. Keep their age across head changes,
// clearing each clock as soon as that named check stops failing.
func (s *Service) ageCheckFailures(ctx context.Context, r config.Repo, pr github.Pull, reasons []string) error {
	file := filepath.Join(s.state(r.Name), fmt.Sprintf("pr-%d-check-age.json", pr.Number))
	clocks := map[string]blockAge{}
	if _, e := fsutil.ReadJSON(file, &clocks); e != nil {
		return e
	}
	next := map[string]blockAge{}
	for _, reason := range reasons {
		if !strings.HasPrefix(reason, "check_failed:") {
			continue
		}
		c, ok := clocks[reason]
		if !ok {
			c.First = s.now().UTC()
		}
		if !slices.Contains(c.Heads, pr.Head.SHA) {
			c.Heads = append(c.Heads, pr.Head.SHA)
		}
		if !c.Escalated && (s.now().Sub(c.First) >= s.Config.Daemon.PR.Findings.EscalationWindow() || (s.Config.Daemon.PR.Findings.EscalationRounds() > 0 && len(c.Heads) >= s.Config.Daemon.PR.Findings.EscalationRounds())) {
			rt, e := s.findingsRoute(ctx, r, pr)
			if e != nil {
				return e
			}
			if rt.owner != "" && rt.owner != rt.lane {
				nctx, cancel := reportContext(ctx)
				e := s.Notifier.Notify(nctx, integrations.Event{
					ID:   fmt.Sprintf("mergewake-check-aged-%s-%d-%s-%d", r.Name, pr.Number, reason, c.First.UnixNano()),
					Text: fmt.Sprintf("[mergewake] ESC %s#%d | %s still failing after %d head rounds and %s | author lane=%s", r.Name, pr.Number, reason, len(c.Heads), s.now().Sub(c.First).Round(time.Minute), rt.lane),
					Lane: rt.owner, Type: "decision",
				})
				cancel()
				if e != nil {
					return e
				}
			}
			c.Escalated = true
		}
		next[reason] = c
	}
	if len(next) == 0 && len(clocks) == 0 {
		return nil
	}
	return fsutil.WriteJSON(file, next)
}
