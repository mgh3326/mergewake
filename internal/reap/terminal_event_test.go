package reap

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// listRunner answers the events listing of one job dir and records the call.
type listRunner struct {
	listing string
	err     error
	calls   [][]string
}

func (r *listRunner) Run(_ context.Context, name string, args []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	if r.err != nil {
		return nil, r.err
	}
	return []byte(r.listing), nil
}

func TestNewestTerminalEvent(t *testing.T) {
	cases := map[string]struct {
		listing string
		want    string
	}{
		"no events":                {"", ""},
		"no terminal event":        {"00001-job.spawned.json\n00002-job.claim.json\n", ""},
		"one terminal":             {"00001-job.spawned.json\n00002-job.revoked.json\n", "job.revoked"},
		"newest terminal":          {"00003-job.lost.json\n00001-job.completed.json\n00002-job.reaped.json\n", "job.lost"},
		"revived after completion": {"00001-job.completed.json\n00002-job.reclaim.json\n", ""},
		"reprompted after reaped":  {"00001-job.reaped.json\n00002-job.reprompted.json\n", ""},
		"revived then ended again": {"00001-job.revoked.json\n00002-job.spawned.json\n00003-job.completed.json\n", "job.completed"},
		"names not ledger":         {"job.completed.json\n0002-job.completed.json\n00004-job.completed.txt\n", ""},
		"sequence not digits":      {"0000a-job.completed.json\n", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := newestTerminalEvent(tc.listing); got != tc.want {
				t.Fatalf("newestTerminalEvent=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestExecTerminalEventReadsJobEvents(t *testing.T) {
	r := &listRunner{listing: "00001-job.spawned.json\n00002-job.reaped.json\n"}
	e := Exec{Runner: r, Host: Host{Name: "node", SSH: "ops@node.example", Jobs: "/jobs"}}
	got, err := e.TerminalEvent(context.Background(), "mwr-12-aaaa111-p1-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "job.reaped" {
		t.Fatalf("TerminalEvent=%q, want job.reaped", got)
	}
	if len(r.calls) != 1 || r.calls[0][0] != "ssh" || !strings.Contains(r.calls[0][len(r.calls[0])-1], "/jobs/mwr-12-aaaa111-p1-1") {
		t.Fatalf("calls=%q, want one ssh listing of the job dir", r.calls)
	}
}

func TestExecTerminalEventUnreadable(t *testing.T) {
	r := &listRunner{err: errors.New("exit status 2")}
	local := Exec{Runner: r, Host: Host{Name: "local", Jobs: "/jobs"}}
	if _, err := local.TerminalEvent(context.Background(), "mwr-12-aaaa111-p1-1"); err == nil {
		t.Fatal("an unreadable events dir must be an error")
	}
	noJobs := Exec{Runner: r, Host: Host{Name: "local"}}
	if _, err := noJobs.TerminalEvent(context.Background(), "mwr-12-aaaa111-p1-1"); err == nil {
		t.Fatal("a host with no jobs dir must be an error, not an empty answer")
	}
	if len(r.calls) != 1 {
		t.Fatalf("a host with no jobs dir ran a command: %q", r.calls)
	}
}
