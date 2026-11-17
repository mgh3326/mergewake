package pipeline

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/fsutil"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// asyncRequestID is the hub request the recorded receipt names.
const asyncRequestID = "6f2c1a34-8f0b-4c2d-9a51-2b7f0d3c9e11"

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
	rj.Spawned = "ACCEPTED job=" + rj.Label + " request=" + asyncRequestID
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

// (a) A resumed asynchronous record whose hub request already ended on a
// refusal that is not parked is dead: the pass reads the operation once and a
// fresh reviewer replaces the dead one, exactly as after a replayed refusal.
// The request itself is never submitted again.
func TestAsyncEndedRequestReplacesDeadReviewer(t *testing.T) {
	f, rj := recordedRequest(t)
	status := &spawnStatusReads{next: f.runner, replay: rj.Label, exits: map[string]int{
		asyncRequestID: panewireQuotaEscalate,
	}}
	f.s.Runner = status
	// The replacement's own spawn is refused the same way, so the pass fails
	// once and review-retry backs off.
	f.runner.exits = []int{panewireQuotaEscalate}
	f.s.Config.Daemon.PR.ReviewRetry = config.ReviewRetry{Initial: "1h", Maximum: "1h", Failures: 10}
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err == nil {
		t.Fatal("the replacement's refusal must fail the pass")
	}
	if !slices.Equal(status.reads, []string{asyncRequestID}) || status.replays != 0 {
		t.Fatalf("status reads %v replays %d, want one read and no replay", status.reads, status.replays)
	}
	if len(f.runner.spawns) != 2 || arg(f.runner.spawns[1], "-l") == rj.Label {
		t.Fatalf("spawns %v, want the placement and one fresh reviewer", f.runner.spawns)
	}
	dead := auditRows(t, f.s, f.r, "reviewer-spawn-dead")
	if len(dead) != 1 || dead[0]["label"] != rj.Label || dead[0]["exit"] != float64(panewireQuotaEscalate) {
		t.Fatalf("reviewer-spawn-dead rows %v", dead)
	}
	if reason, _ := dead[0]["reason"].(string); !strings.Contains(reason, asyncRequestID) || !strings.Contains(reason, "87") {
		t.Fatalf("reviewer-spawn-dead reason %q, want the request and its exit", reason)
	}
	reviewerJobGone(t, f.s, f.r, 1)
	if st := retryStateOf(t, f.s, f.r); st.Failures != 1 {
		t.Fatalf("review-retry failures %d, want the replacement's refusal counted once", st.Failures)
	}
	// The next evaluation inside the backoff neither reads nor replays it.
	f.c.t = f.c.t.Add(time.Second)
	if err := f.s.RetryReview(f.ctx, f.r, f.g.pull, Options{}); err != nil {
		t.Fatalf("next evaluation: %v", err)
	}
	if len(status.reads) != 1 || status.replays != 0 || len(f.runner.spawns) != 2 {
		t.Fatalf("next evaluation reads %v replays %d spawns %d", status.reads, status.replays, len(f.runner.spawns))
	}
}

// (b) The other endings panewire reports for a request the hub will never run
// are as dead as the escalation: a role/seat refusal and a request that ended
// before it landed.
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
			if err := f.s.Review(f.ctx, f.r, 7, "", Options{}); err != nil {
				t.Fatalf("review: %v", err)
			}
			dead := auditRows(t, f.s, f.r, "reviewer-spawn-dead")
			if len(dead) != 1 || dead[0]["label"] != rj.Label || dead[0]["exit"] != float64(code) {
				t.Fatalf("reviewer-spawn-dead rows %v, want exit %d", dead, code)
			}
			fresh := readReviewerJob(t, f.s, f.r, 1)
			if fresh.Label == rj.Label {
				t.Fatalf("record label %s, want the fresh replacement", fresh.Label)
			}
		})
	}
}

// (c) A request the hub parked waits for quota: the record stays and the spawn
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

// (d) A request the hub did not end, and a read that cannot answer, keep the
// record and replay it, as today: a landed request, a deadline the hub did not
// resolve, a landing timeout, a state this client does not know, and a
// panewire too old to read a status.
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
