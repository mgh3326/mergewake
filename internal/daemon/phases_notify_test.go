package daemon

import (
	"testing"
	"time"
)

// The notify stage adds its time to notify_ms, and runs of it add up.
func TestNotifyStageTimeIsLogged(t *testing.T) {
	clock := time.Unix(1000, 0)
	p := newPhases(func() time.Time { return clock })
	stop := p.Notify()
	clock = clock.Add(3 * time.Second)
	stop()
	stop = p.Notify()
	clock = clock.Add(2 * time.Second)
	stop()
	attrs := p.attrs()
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i] == "notify_ms" {
			if got := attrs[i+1]; got != int64(5000) {
				t.Fatalf("notify_ms = %v, want 5000", got)
			}
			return
		}
	}
	t.Fatalf("attrs have no notify_ms: %v", attrs)
}
