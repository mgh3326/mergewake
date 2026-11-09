package pipeline

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// inboxArgvRecorder records the argv of each panewire inbox call a review
// makes.
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

// A review makes no findings inbox call, with or without the reap.hub
// credential, even with a findings lane configured and the reviewer's
// findings announced on it: the announcement and the malformed one before it
// stay unread and unacknowledged.
func TestReviewMakesNoFindingsInboxCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		hub  config.ReapHub
	}{
		{"hub", config.ReapHub{URL: "https://hub.example.test", TokenFile: "/fake/svc-mergewake.env", CFFile: "/fake/cf.env"}},
		{"no hub", config.ReapHub{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := legacyPane("lane")
			s, g, r := panewireSetup(t, runner)
			s.Config.Review.Panewire.Lane = "w-7"
			s.Config.Reap.Hub = tc.hub
			rec := &inboxArgvRecorder{panewireRunner: runner}
			s.Runner = rec
			e := s.Review(context.Background(), r, 7, "", Options{})
			if !errors.Is(e, ErrReviewUnavailable) || g.postCount != 0 {
				t.Fatalf("err=%v posts=%d", e, g.postCount)
			}
			if len(runner.inbox) == 0 {
				t.Fatal("the pane announced nothing on the lane")
			}
			if len(rec.calls) != 0 || runner.sink.attachmentReads != 0 {
				t.Fatalf("inbox calls %q, attachment reads %d", rec.calls, runner.sink.attachmentReads)
			}
		})
	}
}
