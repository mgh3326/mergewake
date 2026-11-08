package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// inboxArgvRecorder records the argv of each panewire inbox call the findings
// lane reads and acks.
type inboxArgvRecorder struct {
	*panewireRunner
	calls [][]string
}

func (r *inboxArgvRecorder) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 0 && args[0] == "inbox" {
		r.calls = append(r.calls, slices.Clone(args))
	}
	return r.panewireRunner.Run(ctx, command, args, input, dir)
}

// The findings lane reads and acks its inbox with the reap.hub credential
// when reap.hub is set; the argv is unchanged when it is not.
func TestFindingsInboxCallsCarryTheReapHubCredential(t *testing.T) {
	const announcement = `{"id":9,"kind":"lane.event","event":{"event_id":"lbl-findings","text":"no attachment named here"}}`
	for _, tc := range []struct {
		name string
		hub  config.ReapHub
		flag string
	}{
		{"hub", config.ReapHub{URL: "https://hub.example.test", TokenFile: "/fake/svc-mergewake.env", CFFile: "/fake/cf.env"},
			" --hub-url https://hub.example.test --hub-token-env /fake/svc-mergewake.env --hub-cf-env /fake/cf.env"},
		{"hub without cf", config.ReapHub{URL: "https://hub.example.test", TokenFile: "/fake/svc-mergewake.env"},
			" --hub-url https://hub.example.test --hub-token-env /fake/svc-mergewake.env"},
		{"no hub", config.ReapHub{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &panewireRunner{inbox: []string{announcement}}
			s, _, _ := panewireSetup(t, runner)
			s.Config.Reap.Hub = tc.hub
			rec := &inboxArgvRecorder{panewireRunner: runner}
			s.Runner = rec
			// The announcement names no attachment, so it is consumed by its
			// ack and nothing is returned.
			if b, err := (&panewireExecutor{s: s}).announced(context.Background(), "w-7", "lbl"); err != nil || b != nil {
				t.Fatal(b, err)
			}
			want := []string{"inbox list --lane w-7 --json" + tc.flag, "inbox ack 9 --lane w-7 --json" + tc.flag}
			got := make([]string, 0, len(rec.calls))
			for _, c := range rec.calls {
				got = append(got, strings.Join(c, " "))
			}
			if !slices.Equal(got, want) {
				t.Fatalf("got  %q\nwant %q", got, want)
			}
		})
	}
}
