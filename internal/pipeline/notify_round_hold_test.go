package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/reap"
)

// scanFailRunner fails the author pane scan's herdr agent list on every host
// and passes every other command to the round-rule runner.
type scanFailRunner struct{ *roundRuleRunner }

func (p scanFailRunner) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if strings.Contains(strings.Join(args, " "), "herdr agent list") {
		return nil, errors.New("pane list failed")
	}
	return p.roundRuleRunner.Run(ctx, command, args, input, dir)
}

// A round hold whose verdict cannot finish (the author pane scan fails while a
// recent finding waits on the author) still sends its one decision event.
// NotifyPR returns the read error after the decision, and a later pass sends
// no second decision (hk 5536).
func TestNotifyRoundHoldSentWhenAuthorScanFails(t *testing.T) {
	s, g, n, sink, run, r := roundRuleSetup(t, 7)
	th := ownFinding("T1", "high", 10)
	th.Comments[0].CreatedAt = s.now().Add(-5 * time.Minute)
	g.feedback.Threads = []github.ReviewThread{th}
	sink.jobs = map[string]string{}
	s.Config.Hosts = []reap.Host{{Name: "remote", SSH: "remote"}}
	s.Runner = scanFailRunner{run}

	err := s.NotifyPR(context.Background(), r, 7, Options{})
	if err == nil || !strings.Contains(err.Error(), "pane list failed") {
		t.Fatalf("NotifyPR err = %v, want the author pane scan error", err)
	}
	decisions := func() (count int) {
		for _, e := range n.events {
			if e.Type == "decision" && e.Lane == "design-lane" {
				count++
			}
		}
		return count
	}
	if got := decisions(); got != 1 {
		t.Fatalf("first pass sent %d hold decisions, want 1: %+v", got, n.events)
	}
	if st := s.loadRoundRuleState(r.Name, 7); !st.eventSent() {
		t.Fatalf("round-rule state does not record the sent decision: %+v", st)
	}

	_ = s.NotifyPR(context.Background(), r, 7, Options{})
	if got := decisions(); got != 1 {
		t.Fatalf("later pass sent a second hold decision: got %d, want 1", got)
	}
}
