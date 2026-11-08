// Package integrations adapts external CLIs and services without importing
// their internals; hk is reached through its public client package.
package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/mergewake/internal/config"
)

type Runner interface {
	Run(context.Context, string, []string, string, string) ([]byte, error)
}

// Exec runs each command as its own process group. ReapGroup kills the group
// after every exit (one-shot reviewers); Grace bounds SIGTERM before SIGKILL.
type Exec struct {
	ReapGroup bool
	Grace     time.Duration
}
type limited struct {
	bytes.Buffer
	overflow bool
}

func (b *limited) Write(p []byte) (int, error) {
	n := len(p)
	if b.Len()+n > 4<<20 {
		b.overflow = true
		return n, nil
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
func (x Exec) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	return x.RunEnv(ctx, command, args, input, dir, nil)
}

// RunEnv applies per-process overrides without changing the daemon environment.
func (x Exec) RunEnv(ctx context.Context, command string, args []string, input, dir string, env []string) ([]byte, error) {
	if command == "" {
		return nil, errors.New("empty command")
	}
	c := groupCommand(ctx, command, args, x.Grace, x.ReapGroup)
	c.Dir = dir
	if env != nil {
		c.Env = env
	}
	var out, stderr limited
	var e error
	if x.ReapGroup {
		e = runReaped(ctx, c.Cmd, input, &out, &stderr, x.Grace)
	} else {
		c.Stdin = strings.NewReader(input)
		c.Stdout = &out
		c.Stderr = &stderr
		e = c.Run()
	}
	c.finish(ctx, x.ReapGroup)
	if e != nil {
		e = stopped(ctx, command, e)
		if ctx.Err() == nil && stderr.Len() > 0 {
			e = &CommandError{Command: command, Err: errors.Unwrap(e), Stderr: tail(stderr.String(), stderrTail)}
		}
		return nil, e
	}
	if out.overflow || stderr.overflow {
		return nil, errors.New("command output exceeds limit")
	}
	return out.Bytes(), nil
}

// stderrTail bounds the stderr kept on a failed command.
const stderrTail = 4 << 10

// CommandError is a command that exited unsuccessfully. Its message is the
// bare "<command> failed: <exit>"; Stderr keeps the tail of what it wrote so
// callers can classify the failure (a rate limit, say) without logging it.
type CommandError struct {
	Command string
	Err     error
	Stderr  string
}

func (e *CommandError) Error() string { return e.Command + " failed: " + e.Err.Error() }
func (e *CommandError) Unwrap() error { return e.Err }

func tail(s string, n int) string {
	if len(s) > n {
		s = s[len(s)-n:]
	}
	return s
}

// Detail describes an error for an operator-facing failure report: a failed
// command's message plus the tail of its captured stderr. CommandError keeps
// stderr out of its own message; callers add it back only at reporting
// boundaries such as a daemon's last_error.
func Detail(err error) string {
	if err == nil {
		return ""
	}
	var ce *CommandError
	if errors.As(err, &ce) && strings.TrimSpace(ce.Stderr) != "" {
		return err.Error() + ": " + tail(strings.TrimSpace(ce.Stderr), reportTail)
	}
	return err.Error()
}

// reportTail bounds the stderr shown in an error report.
const reportTail = 512

// Event is one lane event. Type is the declared panewire notification type
// (`emit --type`); ready, blocked, decision and findings stay unread on the
// receiving lane until it runs `panewire inbox ack`.
type Event struct {
	ID   string `json:"id"`
	Text string `json:"text"`
	Lane string `json:"lane,omitempty"`
	Type string `json:"type,omitempty"`
}
type Notifier interface {
	Notify(context.Context, Event) error
}
type Sink interface {
	Reopen(context.Context, string, string) error
	Record(context.Context, string, []byte) error
}
type Noop struct{}

func (Noop) Notify(context.Context, Event) error          { return nil }
func (Noop) Reopen(context.Context, string, string) error { return nil }
func (Noop) Record(context.Context, string, []byte) error { return nil }

type Stdout struct{ Out io.Writer }

func (s Stdout) Notify(_ context.Context, e Event) error {
	_, err := fmt.Fprintf(s.Out, "%s %s\n", e.ID, e.Text)
	return err
}

type Panewire struct {
	Runner        Runner
	Command, Lane string
	// Hub is reap.hub: the inbox reads and acks run with its service
	// credential (see InboxArgv).
	Hub config.ReapHub
}

func (p Panewire) Notify(ctx context.Context, e Event) error {
	lane := e.Lane
	if lane == "" {
		lane = p.Lane
	}
	args := []string{"emit", "--kind", "lane.event", "--lane", lane, "--event-id", e.ID, "--text", e.Text}
	if e.Type != "" {
		args = append(args, "--type", e.Type)
	}
	_, err := p.Runner.Run(ctx, p.Command, args, "", "")
	var ce *CommandError
	if err != nil && e.Type != "" && errors.As(err, &ce) && strings.Contains(ce.Stderr, "invalid --type") {
		// A panewire older than this type refuses it as a usage error before
		// recording anything; the untyped event still reaches the lane, by
		// its text tags, as before.
		_, err = p.Runner.Run(ctx, p.Command, args[:len(args)-2], "", "")
	}
	if err != nil && errors.As(err, &ce) && strings.Contains(ce.Stderr, "duplicate event_id") {
		// The event reached the store under this ID before; delivery is done.
		return nil
	}
	return err
}

// InboxArgv is the argv of one `panewire inbox` call; sub is the subcommand
// and its arguments. The call carries reap.hub's service credential (see
// HubArgv), so the scoped service reads and acks the lane.
func InboxArgv(hub config.ReapHub, sub ...string) []string {
	return HubArgv(hub, append([]string{"inbox"}, sub...)...)
}

// HubArgv is the argv of one panewire call that reaches the hub. When reap.hub's
// URL and token file are both set, it appends the hub's service credential
// paths (the cf file too, when set), so the scoped service runs the call.
// Otherwise argv is returned as given. Only paths are passed; the files are
// never read here. The result never shares argv's backing array.
func HubArgv(hub config.ReapHub, argv ...string) []string {
	if hub.URL == "" || hub.TokenFile == "" {
		return argv
	}
	out := append(argv[:len(argv):len(argv)], "--hub-url", hub.URL, "--hub-token-env", hub.TokenFile)
	if hub.CFFile != "" {
		out = append(out, "--hub-cf-env", hub.CFFile)
	}
	return out
}

// AckedEvents reads lane's inbox and maps each lane event's id to whether its
// receiver acknowledged it (`panewire inbox ack`). An id the hub has no item
// for is absent.
func (p Panewire) AckedEvents(ctx context.Context, lane string) (map[string]bool, error) {
	raw, err := p.Runner.Run(ctx, p.Command, InboxArgv(p.Hub, "list", "--lane", lane, "--json"), "", "")
	if err != nil {
		return nil, err
	}
	var items []struct {
		AckedAt int64 `json:"acked_at_ms"`
		Event   struct {
			EventID string `json:"event_id"`
		} `json:"event"`
	}
	if err = json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("inbox list: %v", err)
	}
	out := map[string]bool{}
	for _, it := range items {
		if it.Event.EventID != "" {
			out[it.Event.EventID] = out[it.Event.EventID] || it.AckedAt > 0
		}
	}
	return out, nil
}

// Handoffkeep is the hk task sink. Its task, comment, reopen, decision,
// attachment and document calls go to hk over HTTP through the hk public
// client with mergewake's own svc-mergewake credential (hkClient), never as
// hk CLI subprocesses (hk 3549, hk 3555). No hk call uses Runner any more;
// Command (sink.command) is the CLI AttachmentCommand names for a reviewer
// on its own host.
type Handoffkeep struct {
	Runner         Runner
	Command, State string
	// Jobs, when set, reads job records (tests inject it); nil resolves the
	// handoffkeep HTTP endpoint from the svc-mergewake environment.
	Jobs JobReader
	// EventAttempts, EventBackoff and EventTimeout tune PostTaskEvent: the
	// tries in total, the first retry's wait (doubling after), and each
	// try's HTTP timeout. Zero uses the Default* values.
	EventAttempts int
	EventBackoff  time.Duration
	EventTimeout  time.Duration
}

var TaskID = regexp.MustCompile(`^[1-9][0-9]*$`)

// Reopen reads the task, records note as a comment, then reopens a merged or
// dropped task with note as the reason. An already reopened task only gets
// the comment; any other state is refused after the comment.
func (h Handoffkeep) Reopen(ctx context.Context, task, note string) error {
	if strings.TrimSpace(note) == "" {
		return errors.New("reopen requires a reason")
	}
	c, id, e := taskClient(task)
	if e != nil {
		return e
	}
	current, e := getTask(ctx, c, id)
	if e != nil {
		return e
	}
	if _, e = c.CommentTask(ctx, id, note); e != nil {
		return e
	}
	if current.State == "reopened" {
		return nil
	}

	if current.State != "merged" && current.State != "dropped" {
		return fmt.Errorf("recovery comment recorded; task %s is %q, explicit reopen requires merged or dropped", task, current.State)
	}
	if _, e = c.ReopenTask(ctx, id, note); e != nil {
		return fmt.Errorf("recovery comment recorded, but task %s could not reopen from %q to reopened; server may lack explicit reopen support: %w", task, current.State, e)
	}
	return nil
}

// Lane reads the task's owning lane; an absent field means the task has no
// routing lane and the caller falls back, never guessing.
func (h Handoffkeep) Lane(ctx context.Context, task string) (string, error) {
	x, e := h.task(ctx, task)
	return x.Lane, e
}

// TaskBody reads the task's free text. hk's task answer has never carried a
// body or description field (a task's body is the document its body_doc
// names), so the `tasks show` read this replaces always found "" — and so
// does this one. The read is kept so an unknown task or an unreachable hk
// is still an error.
func (h Handoffkeep) TaskBody(ctx context.Context, task string) (string, error) {
	_, e := h.task(ctx, task)
	return "", e
}

// TaskJob reads the panewire job bound to a task (`refs.job_id`) and the task
// title, which carries the task's grade and T marker.
func (h Handoffkeep) TaskJob(ctx context.Context, task string) (string, string, error) {
	x, e := h.task(ctx, task)
	if e != nil {
		return "", "", e
	}
	return x.Refs.JobID, x.Title, nil
}

// TaskPriority reads the task's priority: 1 is urgent and 4 low; 0 means the
// task has none.
func (h Handoffkeep) TaskPriority(ctx context.Context, task string) (int, error) {
	x, e := h.task(ctx, task)
	return x.Priority, e
}

// Attachment reads one handoffkeep attachment by its sha256 id.
func (h Handoffkeep) Attachment(ctx context.Context, sha string) ([]byte, error) {
	if !attachmentID.MatchString(sha) {
		return nil, errors.New("attachment ID must be a sha256 hex digest")
	}
	return readAttachment(ctx, sha)
}

// AttachmentCommand names the CLI reviewers use to retrieve an attachment.
func (h Handoffkeep) AttachmentCommand() string { return h.Command }

// PutAttachment publishes a file for a reviewer on any host, named by its
// base name.
func (h Handoffkeep) PutAttachment(ctx context.Context, path string) (string, error) {
	if _, e := hkClient(); e != nil {
		return "", e
	}
	body, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	return h.UploadAttachment(ctx, filepath.Base(path), body)
}

var attachmentID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Record stores data as the hk note document key.
func (h Handoffkeep) Record(ctx context.Context, key string, data []byte) error {
	c, e := hkClient()
	if e != nil {
		return e
	}
	_, _, e = c.PutDocument(ctx, client.Document{Key: key, Kind: "note", Body: string(data)})
	return e
}
func New(c config.Config, r Runner, out io.Writer) (Sink, Notifier) {
	var s Sink = Noop{}
	var n Notifier = Noop{}
	if c.Sink.Type == "handoffkeep" {
		cmd := c.Sink.Command
		if cmd == "" {
			cmd = "handoffkeep"
		}
		s = Handoffkeep{Runner: r, Command: cmd, State: c.Sink.ReopenState}
	}
	switch c.Notifier.Type {
	case "stdout":
		n = Stdout{out}
	case "panewire":
		cmd := c.Notifier.Command
		if cmd == "" {
			cmd = "panewire"
		}
		n = Panewire{r, cmd, c.Notifier.Lane, c.Reap.Hub}
	}
	return s, n
}

// RunDuplex owns one foreground protocol session and always reaps its process
// group after the single exchange, including errors and cancellation.
func (x Exec) RunDuplex(ctx context.Context, command string, args []string, dir string, exchange func(io.Reader, io.Writer) ([]byte, error)) ([]byte, error) {
	c := groupCommand(ctx, command, args, x.Grace, true)
	c.Dir = dir
	var stderr limited
	c.Stderr = &stderr
	r, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	w, err := c.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err = c.Start(); err != nil {
		return nil, err
	}
	b, exchangeErr := exchange(r, w)
	_ = w.Close()
	// The exchange is complete or failed: the server gets no grace period.
	_ = c.signal(syscall.SIGKILL)
	_ = c.Wait()
	c.finish(ctx, true)
	if ctx.Err() != nil {
		return nil, stopped(ctx, command, ctx.Err())
	}
	if exchangeErr != nil {
		return nil, exchangeErr
	}
	if stderr.overflow {
		return nil, errors.New("protocol output exceeds limit")
	}
	return b, nil
}

func (Exec) Available(command string) error { _, err := exec.LookPath(command); return err }

func (x Exec) RunReview(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	return (Exec{ReapGroup: true, Grace: x.Grace}).Run(ctx, command, args, input, dir)
}
