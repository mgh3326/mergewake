package integrations

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type call struct {
	command string
	args    []string
}
type fakeRunner struct {
	calls []call
	fail  bool
	err   error
	shown string // overrides the `tasks show` reply
}

func (r *fakeRunner) Run(_ context.Context, c string, a []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, call{c, a})
	if r.err != nil {
		return nil, r.err
	}
	if r.fail {
		return nil, errors.New("offline")
	}
	if len(a) > 1 && a[0] == "tasks" && a[1] == "show" {
		if r.shown != "" {
			return []byte(r.shown), nil
		}
		return []byte(`{"state":"merged","lane":"task-lane"}`), nil
	}
	return nil, nil
}
func TestAdaptersUseArgumentVectors(t *testing.T) {
	r := &fakeRunner{}
	p := Panewire{Runner: r, Command: "panewire", Lane: "operator-desk"}
	text := "literal $(no-command) `no-command`"
	if e := p.Notify(context.Background(), Event{ID: "event-a", Text: text}); e != nil {
		t.Fatal(e)
	}
	if r.calls[0].args[len(r.calls[0].args)-1] != text {
		t.Fatal("text changed")
	}
	h := Handoffkeep{Runner: r, Command: "handoffkeep", State: "reopened"}
	if e := h.Reopen(context.Background(), "42", "CI red"); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(strings.Join(r.calls[1].args, " "), "tasks show 42") || !strings.Contains(strings.Join(r.calls[3].args, " "), "tasks reopen 42 --reason CI red") {
		t.Fatal(r.calls)
	}
	before := len(r.calls)
	if e := h.Reopen(context.Background(), "42;cmd", "bad"); e == nil || len(r.calls) != before {
		t.Fatal("invalid task accepted")
	}
	if e := h.Record(context.Background(), "mergewake/report-a", []byte(`{"status":"success"}`)); e != nil {
		t.Fatal(e)
	}
	if r.calls[before].args[0] != "doc" {
		t.Fatal(r.calls)
	}
}
func TestNotifyLaneOverrideAndTaskLane(t *testing.T) {
	r := &fakeRunner{}
	p := Panewire{Runner: r, Command: "panewire", Lane: "config-lane"}
	if e := p.Notify(context.Background(), Event{ID: "event-a", Text: "text", Lane: "task-lane"}); e != nil {
		t.Fatal(e)
	}
	args := r.calls[0].args
	lane := ""
	for i, a := range args {
		if a == "--lane" {
			lane = args[i+1]
		}
	}
	if lane != "task-lane" {
		t.Fatalf("event lane ignored: %v", args)
	}
	if e := p.Notify(context.Background(), Event{ID: "event-b", Text: "text"}); e != nil {
		t.Fatal(e)
	}
	args = r.calls[1].args
	lane = ""
	for i, a := range args {
		if a == "--lane" {
			lane = args[i+1]
		}
	}
	if lane != "config-lane" {
		t.Fatalf("config lane not used: %v", args)
	}
	h := Handoffkeep{Runner: r, Command: "handoffkeep", State: "reopened"}
	lane, e := h.Lane(context.Background(), "42")
	if e != nil || lane != "task-lane" {
		t.Fatalf("lane=%q err=%v", lane, e)
	}
	if e := func() error { _, e := h.Lane(context.Background(), "x"); return e }(); e == nil {
		t.Fatal("invalid task accepted")
	}
}

func TestAdapterErrorsPropagate(t *testing.T) {
	r := &fakeRunner{fail: true}
	if (Panewire{r, "panewire", "lane-a"}).Notify(context.Background(), Event{}) == nil {
		t.Fatal("notify failure hidden")
	}
	h := Handoffkeep{Runner: r, Command: "handoffkeep", State: "in_progress"}
	if h.Reopen(context.Background(), "42", "note") == nil || len(r.calls) != 2 {
		t.Fatal("show failure did not stop transition")
	}
}

type oldServerRunner struct{ calls int }

func (r *oldServerRunner) Run(_ context.Context, _ string, args []string, _, _ string) ([]byte, error) {
	r.calls++
	if args[1] == "show" {
		return []byte(`{"state":"merged"}`), nil
	}
	if args[1] == "reopen" {
		return nil, errors.New("HTTP 404: unsupported operation")
	}
	return nil, nil
}
func TestReopenOlderServerFailsExplicitly(t *testing.T) {
	r := &oldServerRunner{}
	h := Handoffkeep{Runner: r, Command: "handoffkeep", State: "reopened"}
	err := h.Reopen(context.Background(), "42", "reverted")
	if err == nil || !strings.Contains(err.Error(), "comment recorded") || !strings.Contains(err.Error(), "lack explicit reopen support") || r.calls != 3 {
		t.Fatalf("calls=%d err=%v", r.calls, err)
	}
	before := r.calls
	if h.Reopen(context.Background(), "42", " ") == nil || r.calls != before {
		t.Fatal("empty reason sent requests")
	}
}

func TestPanewireDuplicateEventCountsAsDelivered(t *testing.T) {
	r := &fakeRunner{err: &CommandError{Command: "panewire", Err: errors.New("exit status 2"), Stderr: "emit: duplicate event_id"}}
	p := Panewire{Runner: r, Command: "panewire", Lane: "operator-desk"}
	if err := p.Notify(context.Background(), Event{ID: "mergewake-ready-a-b-1-sha", Text: "x"}); err != nil {
		t.Fatalf("duplicate event_id reported as failure: %v", err)
	}
	r.err = &CommandError{Command: "panewire", Err: errors.New("exit status 2"), Stderr: "emit: daemon unavailable"}
	if err := p.Notify(context.Background(), Event{ID: "x", Text: "x"}); err == nil {
		t.Fatal("other emit failure swallowed")
	}
}

func TestDetailAddsStderrTail(t *testing.T) {
	err := &CommandError{Command: "pacelever", Err: errors.New("exit status 2"), Stderr: "error: experiment_arm must be baseline or downshift\n"}
	got := Detail(err)
	if !strings.Contains(got, "exit status 2") || !strings.Contains(got, "experiment_arm") {
		t.Fatalf("detail=%q", got)
	}
	if Detail(errors.New("plain")) != "plain" {
		t.Fatal("non-command error changed")
	}
}

func TestFailedCommandKeepsStderrOutOfMessage(t *testing.T) {
	_, err := Exec{}.Run(context.Background(), "sh", []string{"-c", "echo 'rate limit hit' >&2; exit 1"}, "", "")
	var ce *CommandError
	if !errors.As(err, &ce) || !strings.Contains(ce.Stderr, "rate limit hit") {
		t.Fatalf("stderr not kept: %v", err)
	}
	if strings.Contains(err.Error(), "rate limit") || !strings.Contains(err.Error(), "exit status 1") {
		t.Fatalf("message changed: %v", err)
	}
}

func TestHandoffkeepAttachmentReadsBySHA(t *testing.T) {
	r := &fakeRunner{}
	h := Handoffkeep{Runner: r, Command: "handoffkeep"}
	sha := strings.Repeat("ab", 32)
	if _, e := h.Attachment(context.Background(), sha); e != nil {
		t.Fatal(e)
	}
	if got := strings.Join(r.calls[0].args, " "); got != "attach get "+sha {
		t.Fatal(got)
	}
	for _, bad := range []string{"", "-o", strings.Repeat("AB", 32), sha + "0"} {
		if _, e := h.Attachment(context.Background(), bad); e == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	if len(r.calls) != 1 {
		t.Fatal(r.calls)
	}
}

func TestTaskBodyReadsBodyThenDescription(t *testing.T) {
	for _, tc := range []struct{ shown, want string }{
		{`{"id":42,"body":"## Acceptance\n- x"}`, "## Acceptance\n- x"},
		{`{"id":42,"description":"from description"}`, "from description"},
		{`{"id":42,"title":"no text"}`, ""},
	} {
		h := Handoffkeep{Runner: &fakeRunner{shown: tc.shown}, Command: "handoffkeep"}
		got, e := h.TaskBody(context.Background(), "42")
		if e != nil || got != tc.want {
			t.Fatalf("%s: %q %v", tc.shown, got, e)
		}
	}
	if _, e := (Handoffkeep{Runner: &fakeRunner{}}).TaskBody(context.Background(), "x"); e == nil {
		t.Fatal("invalid task accepted")
	}
}
