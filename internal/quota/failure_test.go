package quota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

var noon = time.Date(2026, 10, 29, 9, 59, 0, 0, time.UTC)

func commandErr(stderr string) error {
	return fmt.Errorf("devin reviewer: %w", &integrations.CommandError{Command: "env", Err: errors.New("exit status 1"), Stderr: stderr})
}

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		quota  bool
		reset  string
		resets bool
	}{
		{"devin relative", commandErr("Agent error: Reached free model rate limit … Your limit will reset in 1 hour 2 minutes (at 11:01 UTC)"), true, "2026-10-29T11:01:00Z", true},
		{"minutes", commandErr("rate limit: reset in 45 minutes"), true, "2026-10-29T10:44:00Z", true},
		{"days and hours", commandErr("usage limit reached, reset in 1 day, 2 hours"), true, "2026-10-30T11:59:00Z", true},
		{"rfc3339", commandErr("429 Too Many Requests; quota will reset 2026-10-29T12:30:00Z"), true, "2026-10-29T12:30:00Z", true},
		{"http 429", commandErr("API Error: 429 {\"type\":\"rate_limit_error\"}"), true, "", false},
		{"resource exhausted", commandErr("RESOURCE_EXHAUSTED: quota exceeded"), true, "", false},
		{"codex usage", commandErr("You've hit your usage limit. Try again later."), true, "", false},
		{"generic exit", commandErr("panic: nil pointer"), false, "", false},
		{"empty stderr", commandErr(""), false, "", false},
		{"plain error", errors.New("devin reviewer: env failed: exit status 1"), false, "", false},
		{"nil", nil, false, "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := Classify(tt.err, noon)
			if f.Quota != tt.quota {
				t.Fatalf("quota=%v", f.Quota)
			}
			if tt.resets && f.Reset.Format(time.RFC3339) != tt.reset {
				t.Fatalf("reset=%v want %s", f.Reset, tt.reset)
			}
			if !tt.resets && !f.Reset.IsZero() {
				t.Fatalf("unexpected reset %v", f.Reset)
			}
		})
	}
	// A bare clock time is the next such moment in UTC.
	if got := Classify(commandErr("rate limit (at 11:01 UTC)"), noon).Reset.Format(time.RFC3339); got != "2026-10-29T11:01:00Z" {
		t.Fatal(got)
	}
	if got := Classify(commandErr("rate limit (at 09:00 UTC)"), noon).Reset.Format(time.RFC3339); got != "2026-10-30T09:00:00Z" {
		t.Fatal(got)
	}
}

type recorder struct{ args []string }

func (r *recorder) Run(_ context.Context, _ string, a []string, _, _ string) ([]byte, error) {
	r.args = a
	return nil, nil
}

func TestObserveDevinFreeLimitArguments(t *testing.T) {
	r := &recorder{}
	c := CLI{Runner: r, Command: "pacelever"}
	reset := time.Date(2026, 10, 29, 11, 1, 0, 0, time.UTC)
	if e := c.ObserveDevinFreeLimit(context.Background(), reset, noon, "swe-2", "mergewake:example/project#7"); e != nil {
		t.Fatal(e)
	}
	want := "observe devin-free-limit --reset 2026-10-29T11:01:00Z --observed 2026-10-29T09:59:00Z --rung swe-2 --source mergewake:example/project#7"
	if got := strings.Join(r.args, " "); got != want {
		t.Fatal(got)
	}
	if c.ObserveDevinFreeLimit(context.Background(), time.Time{}, noon, "swe-2", "x") == nil {
		t.Fatal("zero reset accepted")
	}
}
