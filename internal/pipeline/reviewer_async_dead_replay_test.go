package pipeline

import (
	"context"
	"errors"
	"os"
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
	asyncRequestID     = "6f2c1a34-8f0b-4c2d-9a51-2b7f0d3c9e11"
	asyncNextRequestID = "0b9d47c6-2e51-4a83-8f07-6d1c5a2e9b40"
)

// spawnStatusReads answers `panewire spawn status <id>` with the exit panewire
// answers for the request's state, and runs every other call through next. It
// counts the reads and the replays of one recorded label.
type spawnStatusReads struct {
	next    integrations.Runner
	exits   map[string]int // request id → the exit `spawn status` answers
	replay  string         // the recorded label whose replay it counts
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
		code, ok := x.exits[id]
		if !ok {
			// An older panewire has no status subcommand: a usage refusal.
			return nil, panewireExit(panewireUsage)
		}
		if code == 0 {
			return []byte("request=" + id + " state=succeeded\n"), nil
		}
		return nil, panewireExit(code)
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
	if st := retryStateOf(t, f.s, f.r); st.Failures != 1 || !st.Next.After(f.s.now()) {
		t.Fatalf("retry state %+v, want the second refusal counted once with a backoff", st)
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
// are as dead as the escalation: a role/seat refusal, an active-job refusal and
// a request that ended before it landed.
func TestAsyncEndedRequestCodesAreDead(t *testing.T) {
	for name, code := range map[string]int{
		"role refused":   panewireRefused,
		"active job":     panewireHeld,
		"ended unlost":   panewireRequestEnded,
		"quota escalate": panewireQuotaEscalate,
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
// record and replay it, as today: a landed request, a deadline the hub did not
// resolve, a landing timeout, a state this client does not know, a read that
// failed, and a panewire too old to read a status.
func TestAsyncUnendedRequestKeepsRecord(t *testing.T) {
	for name, code := range map[string]int{
		"landed":            0,
		"unresolved":        panewireDuplicateState,
		"landing timeout":   panewireQuotaTimeout,
		"unknown state":     panewireWaitTimeout,
		"unreadable read":   panewireConfigRefused,
		"no status command": panewireUsage,
	} {
		t.Run(name, func(t *testing.T) {
			f, rj := recordedRequest(t)
			status := &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{asyncRequestID: code}}
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
