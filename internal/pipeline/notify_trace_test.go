package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// A run that takes notifySlow or longer logs its steps by name; a quicker run
// logs nothing.
func TestNotifyTraceLogsSlowRunSteps(t *testing.T) {
	ctx, trace := withNotifyTrace(context.Background())
	end := notifyStart(ctx, "lane")
	end()
	end = notifyStart(ctx, "lane")
	end()
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))

	trace.logIfSlow(log, "o/r", 7, notifySlow-time.Millisecond)
	if logs.Len() != 0 {
		t.Fatalf("a run under the threshold logged %q", logs.String())
	}
	trace.logIfSlow(log, "o/r", 7, notifySlow)
	out := logs.String()
	if !strings.Contains(out, "notify slow") || !strings.Contains(out, "Name:lane Calls:2") {
		t.Fatalf("slow run log = %q, want the notify slow line with the lane step twice", out)
	}
}

// Steps outside a notify run record nothing and do not panic.
func TestNotifyStepOutsideRunIsNoop(t *testing.T) {
	if e := notifyStep(context.Background(), "lane", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
}
