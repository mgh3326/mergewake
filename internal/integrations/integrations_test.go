package integrations

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/config"
)

type call struct {
	command string
	args    []string
}
type fakeRunner struct {
	calls []call
	fail  bool
	err   error
}

func (r *fakeRunner) Run(_ context.Context, c string, a []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, call{c, a})
	if r.err != nil {
		return nil, r.err
	}
	if r.fail {
		return nil, errors.New("offline")
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
}

func TestAdapterErrorsPropagate(t *testing.T) {
	r := &fakeRunner{fail: true}
	if (Panewire{Runner: r, Command: "panewire", Lane: "lane-a"}).Notify(context.Background(), Event{}) == nil {
		t.Fatal("notify failure hidden")
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
	err := &CommandError{Command: "panewire", Err: errors.New("exit status 2"), Stderr: "error: experiment_arm must be baseline or downshift\n"}
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

// typeRunner refuses --type the way a panewire older than the type does, and
// serves an inbox list.
type typeRunner struct {
	calls  [][]string
	oldCLI bool
	inbox  string
}

func (r *typeRunner) Run(_ context.Context, _ string, a []string, _, _ string) ([]byte, error) {
	r.calls = append(r.calls, a)
	if a[0] == "inbox" {
		return []byte(r.inbox), nil
	}
	for i, arg := range a {
		if arg == "--type" && r.oldCLI {
			return nil, &CommandError{Command: "panewire", Err: errors.New("exit status 2"), Stderr: "emit: invalid --type " + a[i+1]}
		}
	}
	return nil, nil
}

func TestPanewireDeclaresTheEventType(t *testing.T) {
	r := &typeRunner{}
	p := Panewire{Runner: r, Command: "panewire", Lane: "desk"}
	if err := p.Notify(context.Background(), Event{ID: "e1", Text: "x", Lane: "w-7", Type: "findings"}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.calls[0], " "); got != "emit --kind lane.event --lane w-7 --event-id e1 --text x --type findings" {
		t.Fatal(got)
	}
	if err := p.Notify(context.Background(), Event{ID: "e2", Text: "x"}); err != nil || strings.Contains(strings.Join(r.calls[1], " "), "--type") {
		t.Fatalf("untyped event declared a type: %v %v", err, r.calls[1])
	}
	// A panewire older than the type refuses it before recording anything;
	// the event is sent again untyped.
	old := &typeRunner{oldCLI: true}
	p.Runner = old
	if err := p.Notify(context.Background(), Event{ID: "e3", Text: "x", Type: "blocked"}); err != nil {
		t.Fatal(err)
	}
	if len(old.calls) != 2 || strings.Contains(strings.Join(old.calls[1], " "), "--type") {
		t.Fatalf("calls %v", old.calls)
	}
}

func TestPanewireAckedReadsTheLaneInbox(t *testing.T) {
	r := &typeRunner{inbox: `[{"id":4,"acked_at_ms":1700,"event":{"event_id":"done"}},{"id":3,"event":{"event_id":"open"}}]`}
	p := Panewire{Runner: r, Command: "panewire"}
	got, err := p.AckedEvents(context.Background(), "w-7")
	if err != nil || len(got) != 2 || !got["done"] || got["open"] {
		t.Fatalf("%v %v", got, err)
	}
	if _, ok := got["absent"]; ok {
		t.Fatal("absent event reported")
	}
	if got := strings.Join(r.calls[0], " "); got != "inbox list --lane w-7 --json" {
		t.Fatal(got)
	}
}

// inboxNotifier builds the panewire notifier the daemon builds from config,
// with the reap.hub credential set as given. The paths are fakes; the fake
// Runner never reads them.
func inboxNotifier(t *testing.T, r Runner, hub config.ReapHub) Panewire {
	t.Helper()
	var c config.Config
	c.Notifier = config.Notifier{Type: "panewire", Command: "panewire", Lane: "desk"}
	c.Reap.Hub = hub
	_, n := New(c, r, io.Discard)
	p, ok := n.(Panewire)
	if !ok {
		t.Fatalf("notifier is %T, not Panewire", n)
	}
	return p
}

var fakeHub = config.ReapHub{URL: "https://hub.example.test", TokenFile: "/fake/svc-mergewake.env", CFFile: "/fake/cf.env"}

const fakeHubFlags = " --hub-url https://hub.example.test --hub-token-env /fake/svc-mergewake.env"

func TestInboxReadCarriesTheReapHubCredential(t *testing.T) {
	r := &typeRunner{inbox: `[{"id":4,"acked_at_ms":1700,"event":{"event_id":"done"}}]`}
	if _, err := inboxNotifier(t, r, fakeHub).AckedEvents(context.Background(), "w-7"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(r.calls[0], " "), "inbox list --lane w-7 --json"+fakeHubFlags+" --hub-cf-env /fake/cf.env"; got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestInboxReadWithoutCFFileOmitsItsFlag(t *testing.T) {
	r := &typeRunner{inbox: `[]`}
	hub := fakeHub
	hub.CFFile = ""
	if _, err := inboxNotifier(t, r, hub).AckedEvents(context.Background(), "w-7"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(r.calls[0], " "), "inbox list --lane w-7 --json"+fakeHubFlags; got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

// Either credential path unset leaves the inbox argv as it was before the
// reap.hub credential was passed.
func TestInboxReadWithoutTheHubKeepsTheBareArgv(t *testing.T) {
	for _, hub := range []config.ReapHub{
		{},
		{URL: "https://hub.example.test"},
		{TokenFile: "/fake/svc-mergewake.env", CFFile: "/fake/cf.env"},
	} {
		r := &typeRunner{inbox: `[]`}
		if _, err := inboxNotifier(t, r, hub).AckedEvents(context.Background(), "w-7"); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(r.calls[0], " "); got != "inbox list --lane w-7 --json" {
			t.Fatalf("hub %+v: got %q", hub, got)
		}
	}
}

// emit (the lane event) never carries the inbox credential.
func TestEmitIgnoresTheReapHubCredential(t *testing.T) {
	r := &typeRunner{}
	if err := inboxNotifier(t, r, fakeHub).Notify(context.Background(), Event{ID: "e1", Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.calls[0], " "); got != "emit --kind lane.event --lane desk --event-id e1 --text x" {
		t.Fatal(got)
	}
}

// HubArgv appends the reap.hub credential paths only when the URL and token
// file are both set, and never writes into the slice it was given.
func TestHubArgvAppendsTheCredentialOnlyWhenSet(t *testing.T) {
	for _, tt := range []struct {
		name string
		hub  config.ReapHub
		want string
	}{
		{"hub", fakeHub, "job close w-7" + fakeHubFlags + " --hub-cf-env /fake/cf.env"},
		{"hub without cf", config.ReapHub{URL: fakeHub.URL, TokenFile: fakeHub.TokenFile}, "job close w-7" + fakeHubFlags},
		{"no hub", config.ReapHub{}, "job close w-7"},
		{"url only", config.ReapHub{URL: fakeHub.URL}, "job close w-7"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := strings.Join(HubArgv(tt.hub, "job", "close", "w-7"), " "); got != tt.want {
				t.Fatalf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestHubArgvDoesNotWriteIntoItsInput(t *testing.T) {
	base := append(make([]string, 0, 16), "job", "close", "w-7")
	_ = HubArgv(fakeHub, base...)
	if got := base[:cap(base)][3]; got != "" {
		t.Fatalf("input slice was written past its length: %q", got)
	}
}
