package pipeline

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

// errPassPending is no failure: a pass that ends pending produced no result,
// so it must not count toward the failure count, the backoff or escalation.
// A real failure joined with it still counts.
func TestErrPassPendingCountsNoReviewFailure(t *testing.T) {
	for _, err := range []error{
		errPassPending,
		fmt.Errorf("review: %w", errPassPending),
		errors.Join(fmt.Errorf("first: %w", errPassPending), fmt.Errorf("second: %w", errPassPending)),
	} {
		var class reviewFailureClass
		class.add(err)
		if class.real || class.collection || len(class.quotas) > 0 || len(class.held) > 0 {
			t.Fatalf("%v was classified as a failure: %+v", err, class)
		}
	}
	var mixed reviewFailureClass
	mixed.add(errors.Join(fmt.Errorf("wait: %w", errPassPending), errors.New("read failed")))
	if !mixed.real {
		t.Fatalf("a real failure joined with a pending pass did not count: %+v", mixed)
	}
}

// A pending retry is no failure, backoff or escalation, however often it
// repeats while the reviewer runs.
func TestRetryPendingPassLeavesFailuresBackoffsEscalationUntouched(t *testing.T) {
	f := asyncReview(t)
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1m", Maximum: "30m", Failures: 1}
	for i := 1; i <= 2; i++ {
		if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); !errors.Is(err, ErrReviewPending) {
			t.Fatalf("run %d: err=%v, want ErrReviewPending", i, err)
		}
		st := retryStateOf(t, f.s, f.r)
		if st.Failures != 0 || st.Backoffs != 0 || st.Escalated {
			t.Fatalf("run %d: state=%+v, want no failure, backoff or escalation", i, st)
		}
	}
	if n := f.s.Notifier.(*fakeNotifier); n.count != 0 {
		t.Fatalf("a pending pass escalated: %v", n.events)
	}
}
