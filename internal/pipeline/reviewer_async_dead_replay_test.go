package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// asyncRequestID is the hub request the recorded receipt names, and
// asyncNextRequestID the one the replacement's receipt names.
const (
	asyncRequestID      = "6f2c1a34-8f0b-4c2d-9a51-2b7f0d3c9e11"
	asyncNextRequestID  = "0b9d47c6-2e51-4a83-8f07-6d1c5a2e9b40"
	asyncThirdRequestID = "e2a7c9d1-4b63-4f80-9c25-7a1d8e6b3f52"
)

// spawnStatusReads answers `panewire spawn status <id>` with the exit panewire
// answers for the request's state, and runs every other call through next. It
// counts the reads and the replays of one recorded label.
type spawnStatusReads struct {
	next    integrations.Runner
	exits   map[string]int    // request id → the exit `spawn status` answers
	outputs map[string]string // request id → explicit `--json` output
	empty   map[string]bool   // request ids whose status read produced no output
	replay  string            // the recorded label whose replay it counts
	reads   []string
	replays int
}

func (x *spawnStatusReads) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "spawn" && args[1] == "status" {
		id := ""
		if len(args) > 2 {
			id = args[2]
		}
		x.reads = append(x.reads, id)
		if !slices.Contains(args, "--json") {
			return nil, errors.New("spawn status missing --json")
		}
		code, ok := x.exits[id]
		if !ok {
			// An older panewire has no status subcommand: a usage refusal.
			return nil, panewireExit(panewireUsage)
		}
		output, exists := x.outputs[id]
		if exists {
			return []byte(output), panewireExit(code)
		}
		if x.empty[id] || code == panewireConfigRefused || code == panewireDuplicateState || code == panewireQuotaTimeout || code == panewireUsage {
			return nil, panewireExit(code)
		}
		state, rc := "failed", code
		switch code {
		case 0:
			state, rc = "landed", 0
		case 85:
			state, rc = "accepted", 0
		case 91:
			state, rc = "parked", 0
		case 75:
			state, rc = "deadline_exceeded", 0
		}
		body := fmt.Sprintf(`{"request":%q,"state":%q,"rc":%d}`, id, state, rc)
		return []byte(body), panewireExit(code)
	}
	if command == "panewire" && len(args) > 0 && args[0] == "spawn" && x.replay != "" && arg(args, "-l") == x.replay {
		x.replays++
	}
	return x.next.Run(ctx, command, args, input, dir)
}

// recordedRequest reviews PR 7 under a revisit, so its reviewer record is the
// asynchronous one the live loop left behind: it holds the hub's receipt and no
// wait, and hk holds no job record for it, as when the hub refused the request
// at placement.
func recordedRequest(t *testing.T) (*asyncFixture, reviewerJob) {
	t.Helper()
	f := asyncReview(t)
	if err := f.review(t); err != nil {
		t.Fatalf("place the reviewer: %v", err)
	}
	rj := readReviewerJob(t, f.s, f.r, 1)
	rj.Spawned = acceptedReceipt(rj.Label, asyncRequestID)
	if err := fsutil.WriteJSON(f.s.reviewerJobPath(f.r.Name, 7, 1), rj); err != nil {
		t.Fatalf("save the receipt: %v", err)
	}
	// The hub never started the request: hk holds no job record for it, and the
	// next evaluation resumes the record.
	f.runner.sink.missing = nil
	f.c.t = f.c.t.Add(2 * time.Minute)
	expireLease(f.g)
	return f, rj
}

// acceptedReceipt is what panewire prints for a placed asynchronous spawn: the
// hub holds the request, and no wait follows.
func acceptedReceipt(label, request string) string {
	return "ACCEPTED job=" + label + " request=" + request + "\n"
}

// (a) A resumed asynchronous record whose hub request already ended on a
// refusal that is not parked is dead, and the pass fails once instead of
// spawning a replacement no one counts: review-retry back offs, and the run
// past the back-off is what spawns the replacement.
func TestAsyncEndedRequestFailsThePass(t *testing.T) {
	f, rj := recordedRequest(t)
	status := &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{
		asyncRequestID: panewireQuotaEscalate,
	}}
	f.s.Runner = status
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1h", Maximum: "1h", Failures: 10}
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
		t.Fatal("an ended request must fail the pass")
	}
	if !slices.Equal(status.reads, []string{asyncRequestID}) || status.replays != 0 {
		t.Fatalf("status reads %v replays %d, want one read and no replay", status.reads, status.replays)
	}
	if len(f.runner.spawns) != 1 {
		t.Fatalf("spawns %d, want no replacement in the failing evaluation", len(f.runner.spawns))
	}
	dead := auditRows(t, f.s, f.r, "reviewer-spawn-dead")
	if len(dead) != 1 || dead[0]["label"] != rj.Label || dead[0]["exit"] != float64(panewireQuotaEscalate) {
		t.Fatalf("reviewer-spawn-dead rows %v", dead)
	}
	if reason, _ := dead[0]["reason"].(string); !strings.Contains(reason, asyncRequestID) || !strings.Contains(reason, "87") {
		t.Fatalf("reviewer-spawn-dead reason %q, want the request and its exit", reason)
	}
	reviewerJobGone(t, f.s, f.r, 1)
	if _, err := os.Stat(f.s.deadLabelPath(f.r.Name, 7, 1)); err != nil {
		t.Fatalf("dead-label marker: %v", err)
	}
	st := retryStateOf(t, f.s, f.r)
	if st.Failures != 1 || !st.Next.After(f.s.now()) {
		t.Fatalf("retry state %+v, want one counted failure and a backoff", st)
	}
	// Inside the backoff nothing reads or spawns.
	before := st.Next
	f.c.t = before.Add(-time.Minute)
	expireLease(f.g)
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err != nil {
		t.Fatalf("evaluation inside the backoff: %v", err)
	}
	if len(status.reads) != 1 || len(f.runner.spawns) != 1 {
		t.Fatalf("inside the backoff: reads %v spawns %d, want neither", status.reads, len(f.runner.spawns))
	}
}

// (b) The live loop, end to end: a dead request fails the pass, the run past
// the backoff spawns a replacement whose hub request is refused later, and that
// refusal fails the next pass instead of replacing the reviewer again in the
// same evaluation. Failures are counted and no spawn happens before the next
// backoff ends.
func TestAsyncReplacementRefusedLaterIsNoLoop(t *testing.T) {
	f, rj := recordedRequest(t)
	status := &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{
		asyncRequestID:     panewireQuotaEscalate,
		asyncNextRequestID: panewireQuotaEscalate,
	}}
	// The replacement's spawn is accepted: an asynchronous spawn returns as soon
	// as the hub holds the request, and its refusal arrives later.
	f.runner.spawnOut = acceptedReceipt("replacement", asyncNextRequestID)
	f.s.Runner = status
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1h", Maximum: "1h", Failures: 10}
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
		t.Fatal("the first ended request must fail the pass")
	}
	first := retryStateOf(t, f.s, f.r)
	if first.Failures != 1 || len(f.runner.spawns) != 1 {
		t.Fatalf("failures=%d spawns=%d, want one failure and no replacement", first.Failures, len(f.runner.spawns))
	}

	// Past the backoff the replacement spawns, accepted at once and pending.
	f.c.t = first.Next.Add(time.Second)
	expireLease(f.g)
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
		t.Fatalf("replacement evaluation: %v", err)
	}
	fresh := readReviewerJob(t, f.s, f.r, 1)
	if len(f.runner.spawns) != 2 || fresh.Label == rj.Label || fresh.Supersedes != rj.Label {
		t.Fatalf("spawns %d record %+v, want a fresh label superseding %s", len(f.runner.spawns), fresh, rj.Label)
	}
	if !strings.Contains(fresh.Spawned, asyncNextRequestID) {
		t.Fatalf("replacement receipt %q, want the request the hub holds", fresh.Spawned)
	}

	// The hub refused the replacement after its spawn: its status is read once,
	// the pass fails and counts, and nothing is spawned before the next backoff.
	f.c.t = f.c.t.Add(2 * time.Minute)
	expireLease(f.g)
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
		t.Fatal("the replacement's ended request must fail the pass")
	}
	if len(f.runner.spawns) != 2 {
		t.Fatalf("spawns %d, want no replacement in the failing evaluation", len(f.runner.spawns))
	}
	if st := retryStateOf(t, f.s, f.r); st.Failures != 2 || !st.Next.After(f.s.now()) {
		t.Fatalf("retry state %+v, want the second refusal counted on top of the first with a backoff", st)
	}
	dead := auditRows(t, f.s, f.r, "reviewer-spawn-dead")
	if len(dead) != 2 || dead[1]["label"] != fresh.Label || dead[1]["exit"] != float64(panewireQuotaEscalate) {
		t.Fatalf("reviewer-spawn-dead rows %v, want the replacement's request named too", dead)
	}
	reviewerJobGone(t, f.s, f.r, 1)
	if !slices.Equal(status.reads, []string{asyncRequestID, asyncNextRequestID}) {
		t.Fatalf("status reads %v, want each dead request read once", status.reads)
	}
	if status.replays != 0 {
		t.Fatalf("replays %d, want none: an ended request is never submitted again", status.replays)
	}
}

// (c) The other endings panewire reports for a request the hub will never run
// are as dead as the escalation: a role/seat refusal, an active-job refusal, a
// request that ended before it landed, and the placement refusals that leave no
// pane — no host admitted the seat, a remote preflight failed, mock isolation
// refused, local spawning is disabled, and the account binding is unusable.
func TestAsyncEndedRequestCodesAreDead(t *testing.T) {
	for name, code := range map[string]int{
		"role refused":     panewireRefused,
		"active job":       panewireHeld,
		"ended unlost":     panewireRequestEnded,
		"quota escalate":   panewireQuotaEscalate,
		"no host":          panewireNoHost,
		"preflight":        panewirePreflight,
		"mock isolation":   panewireMockIsolation,
		"local disabled":   panewireLocalDisabled,
		"auth unavailable": panewireAuthUnavailable,
	} {
		t.Run(name, func(t *testing.T) {
			f, rj := recordedRequest(t)
			f.s.Runner = &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{asyncRequestID: code}}
			if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err == nil {
				t.Fatal("an ended request must fail the pass")
			}
			dead := auditRows(t, f.s, f.r, "reviewer-spawn-dead")
			if len(dead) != 1 || dead[0]["label"] != rj.Label || dead[0]["exit"] != float64(code) {
				t.Fatalf("reviewer-spawn-dead rows %v, want exit %d", dead, code)
			}
			reviewerJobGone(t, f.s, f.r, 1)
			if len(f.runner.spawns) != 1 {
				t.Fatalf("spawns %d, want no replacement in the failing evaluation", len(f.runner.spawns))
			}
		})
	}
}

func TestAsyncStatusJSONDecidesEndedRequest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   int
		output string
		empty  bool
		ended  bool
	}{
		{name: "failed rc 70", code: 70, output: `{"request":"` + asyncRequestID + `","state":"failed","rc":70}`, ended: true},
		{name: "failed parked rc 86", code: 86, output: `{"request":"` + asyncRequestID + `","state":"failed","rc":86}`},
		{name: "retryable landing timeout rc 124", code: 124, output: `{"request":"` + asyncRequestID + `","state":"retryable","rc":124}`},
		{name: "landed", code: 0, output: `{"request":"` + asyncRequestID + `","state":"landed"}`},
		{name: "unparsable", code: 70, output: "not json"},
		{name: "empty", code: 70, empty: true},
		{name: "failed rc 87", code: 87, output: `{"request":"` + asyncRequestID + `","state":"failed","rc":87}`, ended: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, rj := recordedRequest(t)
			status := &spawnStatusReads{
				next: f.runner, replay: rj.Label,
				exits:   map[string]int{asyncRequestID: tc.code},
				outputs: map[string]string{asyncRequestID: tc.output},
				empty:   map[string]bool{asyncRequestID: tc.empty},
			}
			f.s.Runner = status
			err := f.s.Review(f.ctx, f.r, 7, "", Options{})
			if tc.ended {
				if err == nil || status.replays != 0 {
					t.Fatalf("review error=%v replays=%d, want ended request and no replay", err, status.replays)
				}
				reviewerJobGone(t, f.s, f.r, 1)
			} else {
				if err != nil || status.replays != 1 {
					t.Fatalf("review error=%v replays=%d, want kept record replayed", err, status.replays)
				}
				if got := readReviewerJob(t, f.s, f.r, 1).Label; got != rj.Label {
					t.Fatalf("record label=%s, want %s", got, rj.Label)
				}
			}
		})
	}
}

// (d) A request the hub parked waits for quota: the record stays and the spawn
// is replayed, as today.
func TestAsyncParkedRequestKeepsRecord(t *testing.T) {
	f, rj := recordedRequest(t)
	status := &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{asyncRequestID: 91}}
	f.s.Runner = status
	if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
		t.Fatalf("review: %v", err)
	}
	if !slices.Equal(status.reads, []string{asyncRequestID}) || status.replays != 1 {
		t.Fatalf("status reads %v replays %d, want one read and the replay", status.reads, status.replays)
	}
	if got := readReviewerJob(t, f.s, f.r, 1).Label; got != rj.Label {
		t.Fatalf("record label %s, want the kept %s", got, rj.Label)
	}
	if rows := auditRows(t, f.s, f.r, "reviewer-spawn-dead"); len(rows) != 0 {
		t.Fatalf("reviewer-spawn-dead rows %v", rows)
	}
}

// (e) A request the hub did not end, and a read that cannot answer, keep the
// record and replay it, as today: a landed request, a parked one, an in-flight
// or unknown one, a deadline the hub did not resolve, a landing timeout, a read
// that failed, and a panewire too old to read a status.
func TestAsyncUnendedRequestKeepsRecord(t *testing.T) {
	for name, code := range map[string]int{
		"landed":            0,
		"parked":            91,
		"unresolved":        panewireDuplicateState,
		"landing timeout":   panewireQuotaTimeout,
		"unknown state":     panewireWaitTimeout,
		"unreadable read":   panewireConfigRefused,
		"no status command": panewireUsage,
	} {
		t.Run(name, func(t *testing.T) {
			f, rj := recordedRequest(t)
			status := &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{asyncRequestID: code}}
			if name == "unreadable read" {
				status.empty = map[string]bool{asyncRequestID: true}
			}
			f.s.Runner = status
			if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
				t.Fatalf("review: %v", err)
			}
			if status.replays != 1 {
				t.Fatalf("replays %d, want the recorded spawn replayed", status.replays)
			}
			if got := readReviewerJob(t, f.s, f.r, 1).Label; got != rj.Label {
				t.Fatalf("record label %s, want the kept %s", got, rj.Label)
			}
			if rows := auditRows(t, f.s, f.r, "reviewer-spawn-dead"); len(rows) != 0 {
				t.Fatalf("reviewer-spawn-dead rows %v", rows)
			}
		})
	}
}

// spawnCycleReceipts gives each asynchronous spawn in turn the next receipt,
// so the loop's spawns hold hub requests of their own, and answers the status
// reads with spawnStatusReads.
type spawnCycleReceipts struct {
	*spawnStatusReads
	runner   *panewireRunner
	receipts []string
	spawns   int
}

func (x *spawnCycleReceipts) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "panewire" && len(args) > 1 && args[0] == "spawn" && args[1] != "status" && x.spawns < len(x.receipts) {
		x.runner.spawnOut = acceptedReceipt(arg(args, "-l"), x.receipts[x.spawns])
		x.spawns++
	}
	return x.spawnStatusReads.Run(ctx, command, args, input, dir)
}

// (f) A spawn the hub accepted and then refused is no landing: each refused
// request counts its own failure, so repeated ACCEPTED-then-87 cycles on one
// head reach the escalation threshold instead of zeroing the count every time
// (hk 5005).
func TestAsyncAcceptedThenRefusedCountsEveryPass(t *testing.T) {
	f, _ := recordedRequest(t)
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1h", Maximum: "1h", Failures: 3}
	status := &spawnCycleReceipts{
		spawnStatusReads: &spawnStatusReads{next: f.runner, exits: map[string]int{
			asyncRequestID:      panewireQuotaEscalate,
			asyncNextRequestID:  panewireQuotaEscalate,
			asyncThirdRequestID: panewireQuotaEscalate,
		}},
		runner:   f.runner.panewireRunner,
		receipts: []string{asyncNextRequestID, asyncThirdRequestID},
	}
	f.s.Runner = status
	// The recorded request was accepted and then refused: the pass fails once,
	// and no receipt resets the count.
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
		t.Fatal("a refused request must fail the pass")
	}
	failures := 1
	if st := retryStateOf(t, f.s, f.r); st.Failures != failures {
		t.Fatalf("failures=%d after the first refusal, want %d", st.Failures, failures)
	}
	// Two more cycles: past the backoff a replacement spawn is accepted and
	// returns pending, and the next evaluation finds that request refused too.
	for failures = 2; failures <= 3; failures++ {
		f.c.t = retryStateOf(t, f.s, f.r).Next
		expireLease(f.g)
		if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
			t.Fatalf("replacement spawn of cycle %d: %v", failures, err)
		}
		if got := retryStateOf(t, f.s, f.r).Failures; got != failures-1 {
			t.Fatalf("an accepted spawn reset the count: failures=%d, want %d", got, failures-1)
		}
		f.c.t = f.c.t.Add(2 * time.Minute)
		expireLease(f.g)
		if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
			t.Fatalf("cycle %d: a refused request must fail the pass", failures)
		}
		if st := retryStateOf(t, f.s, f.r); st.Failures != failures {
			t.Fatalf("failures=%d after refusal %d, want %d", st.Failures, failures, failures)
		}
	}
	if status.spawns != 2 || !slices.Contains(status.reads, asyncNextRequestID) || !slices.Contains(status.reads, asyncThirdRequestID) {
		t.Fatalf("spawns %d reads %v, want each cycle's request read once", status.spawns, status.reads)
	}
	if st := retryStateOf(t, f.s, f.r); !st.Escalated {
		t.Fatalf("state %+v, want the escalation at 3 failures", st)
	}
	if n := f.s.Notifier.(*fakeNotifier); n.count != 1 || !strings.Contains(n.events[0].Text, "review failed 3 times") {
		t.Fatalf("notices %d %v, want one escalation at 3", n.count, n.events)
	}
}

// (g) An asynchronous reviewer the hub really placed — an hk job record exists
// for its label — still starts a new attempt: the earlier count is reset when
// the landing is seen, and a reviewer lost after it counts as one failure
// again.
func TestAsyncLandedReviewerResetsTheCount(t *testing.T) {
	f := asyncReview(t)
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1h", Maximum: "1h", Failures: 10}
	f.runner.spawnOut = acceptedReceipt("reviewer", asyncRequestID)
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
		t.Fatalf("place the reviewer: %v", err)
	}
	// An earlier attempt failed: its count must not hold a landed reviewer.
	stale := reviewRetryState{Head: shaA, Failures: 4, Escalated: true, Backoffs: 4, Next: f.c.t.Add(-time.Minute)}
	if err := fsutil.WriteJSON(filepath.Join(f.s.state(f.r.Name), "review-retry-7.json"), stale); err != nil {
		t.Fatal(err)
	}
	f.c.t = f.c.t.Add(2 * time.Minute)
	expireLease(f.g)
	// The resumed run reads the reviewer's hk record: it landed, so the count
	// of the earlier attempt is reset.
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err != nil && !errors.Is(err, ErrReviewPending) {
		t.Fatalf("resumed run: %v", err)
	}
	if st := retryStateOf(t, f.s, f.r); st.Failures != 0 || st.Escalated {
		t.Fatalf("state %+v, want the landed reviewer to reset the count", st)
	}
	if rj := readReviewerJob(t, f.s, f.r, 1); !rj.Placed {
		t.Fatalf("record %+v, want the accounted landing", rj)
	}
	// The landed reviewer is lost: that is a failure of the new attempt.
	f.runner.sink.missing = &integrations.JobRecord{State: "lost"}
	f.c.t = f.c.t.Add(2 * time.Minute)
	expireLease(f.g)
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
		t.Fatal("a lost reviewer must fail the pass")
	}
	if st := retryStateOf(t, f.s, f.r); st.Failures != 1 || st.Escalated {
		t.Fatalf("state %+v, want one counted failure after the loss", st)
	}
}
