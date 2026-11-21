package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
)

// hostScanNow is the clock the host-scan gate tests read.
var hostScanNow = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// hostScanLandSetup is a PR whose head has one unanswered mergewake finding
// posted at posted. Host "healthy" answers with panes; host "unavailable"
// fails its pane list (setPartialAuthorScan). The PR's task 42 has no bound job
// unless sink binds one.
func hostScanLandSetup(t *testing.T, posted time.Time, sink *jobSink, panes ...aPane) (*Service, *fakeGH, config.Repo) {
	t.Helper()
	s, g, _, _, r := setup(t)
	s.Config.Daemon.PR.AuthorFixWait = "30m"
	s.Now = func() time.Time { return hostScanNow }
	g.enabled = true
	g.ci = github.Green
	g.policyExtra = "review_gate: {land: graded}\n"
	g.pull.User.Login = "dev"
	g.pull.Body = "Mergewake-Task: 42\n"
	markReviewed(g)
	comment := github.ReviewComment{Author: "dev", AuthorType: "User", Body: "<!-- mergewake-severity:medium -->\nfinding", CommitSHA: shaA, OriginalCommitSHA: shaA, CreatedAt: posted}
	g.feedback.Threads = []github.ReviewThread{{ID: "thread-1", Comments: []github.ReviewComment{comment}}}
	g.findings = 1
	s.Sink = sink
	run := authorHost(panes...)
	s.Runner = run
	setPartialAuthorScan(s, run)
	return s, g, r
}

// Gate (hk 5817): a host whose pane list fails does not fail the merge gate.
// A live author pane on a host that answered, or no pane at all while the
// finding is inside the author-fix window, keeps the head pending as
// author_fixing; once the window has passed the gate proceeds.
func TestLandHostScanDoesNotFailTheGate(t *testing.T) {
	inWindow := hostScanNow.Add(-time.Minute)
	pastWindow := hostScanNow.Add(-31 * time.Minute)
	authorPane := aPane{"w1:p1", "author-job", "/wt/a", "feature", projectOrigin}
	for _, tc := range []struct {
		name       string
		posted     time.Time
		panes      []aPane
		wantMerges int
		wantReason string
	}{
		{name: "author pane on answering host waits", posted: inWindow, panes: []aPane{authorPane}, wantReason: "author_fixing"},
		{name: "no author pane anywhere waits in window", posted: inWindow, wantReason: "author_fixing"},
		{name: "no author pane anywhere merges after window", posted: pastWindow, wantMerges: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &jobSink{jobs: map[string]string{}, recs: map[string]integrations.JobRecord{}}
			sink.recs["author-job"] = integrations.JobRecord{State: "running", Label: "author-job-lane"}
			s, g, r := hostScanLandSetup(t, tc.posted, sink, tc.panes...)
			if err := s.Land(context.Background(), r, 7, Options{Observed: true}); err != nil {
				t.Fatalf("Land returned %v for a host whose pane list failed", err)
			}
			if g.mergeCount != tc.wantMerges {
				t.Fatalf("merge count = %d, want %d", g.mergeCount, tc.wantMerges)
			}
			if tc.wantReason != "" && !strings.Contains(strings.Join(auditLines(t, s, r.Name), "\n"), tc.wantReason) {
				t.Fatalf("audit does not name %q", tc.wantReason)
			}
		})
	}
}

// Gate (hk 5817): a read that is not a host scan keeps failing the gate, so
// the pass retries as it always did.
func TestLandNonHostErrorStillFailsTheGate(t *testing.T) {
	jobs := &jobSink{jobs: map[string]string{"42": "author-job"}, recs: map[string]integrations.JobRecord{}}
	jobs.recs["author-job"] = integrations.JobRecord{State: "running", Label: "author-job-lane"}
	s, _, r := hostScanLandSetup(t, hostScanNow.Add(-time.Minute), jobs)
	s.Sink = &badJobSink{jobSink: jobs, bad: "author-job"}
	err := s.Land(context.Background(), r, 7, Options{Observed: true})
	if err == nil {
		t.Fatal("a failed hk job read did not fail the gate")
	}
	if hostScanOnly(err) {
		t.Fatalf("hk job read error %v is reported as a host scan error", err)
	}
}

// badJobSink fails the read of one job with a non-scan error.
type badJobSink struct {
	*jobSink
	bad string
}

func (s *badJobSink) Job(ctx context.Context, id string) (integrations.JobRecord, error) {
	if id == s.bad {
		return integrations.JobRecord{}, errors.New("hk job read failed")
	}
	return s.jobSink.Job(ctx, id)
}

// Notice and findings routing (hk 5817): with one host unreadable, a route
// that finds the author on a host that answered is delivered there, named as a
// partial scan, with no error.
func TestNotifyRoutesOnAnsweringHostWhenAHostScanFails(t *testing.T) {
	s, _, n, r := workerNotifySetup(t)
	delete(s.Sink.(*jobSink).jobs, "42")
	s.Sink.(*jobSink).recs["w-9"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-9", HubLane: "w-9-lane"}
	run := authorHost(paneB)
	s.Runner = run
	setPartialAuthorScan(s, run)
	s.GH.(*fakeGH).ci = github.Red
	if e := s.NotifyPR(context.Background(), r, 7, Options{}); e != nil {
		t.Fatalf("NotifyPR returned %v for a partial pane scan", e)
	}
	if n.count != 1 || n.events[0].Lane != "w-9-lane" {
		t.Fatalf("verdict went to %+v, want one event on the answering author's lane w-9-lane", n.events)
	}
	rows := prNotifyRows(t, s, r)
	if len(rows) != 1 || !strings.Contains(fmt.Sprint(rows[0]["via"]), "partial pane scan") {
		t.Fatalf("partial route missing from audit: %+v", rows)
	}
}

// SplitHostScan (hk 5817): the pr sweep counts only the errors that are not an
// unreadable host's pane scan, and names the unreadable hosts once each.
func TestSplitHostScan(t *testing.T) {
	scan := hostScanError{"host-a"}
	other := errors.New("hk unreachable")
	for _, tc := range []struct {
		name      string
		err       error
		wantRest  error
		wantHosts []string
	}{
		{name: "nil", err: nil},
		{name: "host scan alone is not a failure", err: scan, wantHosts: []string{"host-a"}},
		{name: "wrapped host scan is not a failure", err: fmt.Errorf("PR 7 outcomes: %w", scan), wantHosts: []string{"host-a"}},
		{name: "joined host scans name each host once", err: errors.Join(scan, hostScanError{"host-a"}, hostScanError{"host-b"}), wantHosts: []string{"host-a", "host-b"}},
		{name: "other error stays a failure", err: other, wantRest: other},
		{name: "host scan beside other error keeps the other", err: errors.Join(scan, other), wantRest: other, wantHosts: []string{"host-a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rest, hosts := SplitHostScan(tc.err)
			if !errors.Is(rest, tc.wantRest) || (rest == nil) != (tc.wantRest == nil) {
				t.Fatalf("rest = %v, want %v", rest, tc.wantRest)
			}
			if fmt.Sprint(hosts) != fmt.Sprint(tc.wantHosts) {
				t.Fatalf("hosts = %v, want %v", hosts, tc.wantHosts)
			}
		})
	}
}

// Sweep WARN (hk 5825): a host whose pane scan fails during a clean verdict (CI
// green, no finding delivery, no error returned) is still reported to the pass's
// sink, so the sweep can name it. The answering host is not reported.
func TestHostScanFailureReachesSinkOnCleanVerdict(t *testing.T) {
	s, g, n, r := workerNotifySetup(t)
	delete(s.Sink.(*jobSink).jobs, "42")
	s.Sink.(*jobSink).recs["w-9"] = integrations.JobRecord{State: "spawned", Role: "worker", Label: "w-9", HubLane: "w-9-lane"}
	run := authorHost(paneB)
	s.Runner = run
	setPartialAuthorScan(s, run)
	g.ci = github.Green
	var reported []string
	ctx := WithHostScanSink(context.Background(), func(host string) { reported = append(reported, host) })
	if e := s.NotifyPR(ctx, r, 7, Options{}); e != nil {
		t.Fatalf("NotifyPR returned %v for a clean verdict on a partial pane scan", e)
	}
	if n.count != 0 {
		t.Fatalf("clean verdict sent %d events, want none", n.count)
	}
	if !slices.Contains(reported, "unavailable") || slices.Contains(reported, "healthy") {
		t.Fatalf("sink reported %v, want the failed host \"unavailable\" and not the answering host", reported)
	}
}
