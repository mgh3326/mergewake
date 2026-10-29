// Package integrations adapts external CLIs without importing their internals.
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
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

type Runner interface {
	Run(context.Context, string, []string, string, string) ([]byte, error)
}
type Exec struct{ ReapGroup bool }
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
	c := exec.CommandContext(ctx, command, args...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error { return syscall.Kill(-c.Process.Pid, syscall.SIGKILL) }
	c.Dir = dir
	if env != nil {
		c.Env = env
	}
	c.Stdin = strings.NewReader(input)
	c.WaitDelay = 2 * time.Second
	var out, stderr limited
	c.Stdout = &out
	c.Stderr = &stderr
	e := c.Run()
	if x.ReapGroup && c.Process != nil {
		_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
	}
	if e != nil {
		return nil, fmt.Errorf("%s failed: %w", command, e)
	}
	if out.overflow || stderr.overflow {
		return nil, errors.New("command output exceeds limit")
	}
	return out.Bytes(), nil
}

type Event struct {
	ID   string `json:"id"`
	Text string `json:"text"`
	Lane string `json:"lane,omitempty"`
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
}

func (p Panewire) Notify(ctx context.Context, e Event) error {
	lane := e.Lane
	if lane == "" {
		lane = p.Lane
	}
	_, err := p.Runner.Run(ctx, p.Command, []string{"emit", "--kind", "lane.event", "--lane", lane, "--event-id", e.ID, "--text", e.Text}, "", "")
	return err
}

type Handoffkeep struct {
	Runner         Runner
	Command, State string
}

var TaskID = regexp.MustCompile(`^[1-9][0-9]*$`)

func (h Handoffkeep) Reopen(ctx context.Context, task, note string) error {
	if strings.TrimSpace(note) == "" {
		return errors.New("reopen requires a reason")
	}
	if !TaskID.MatchString(task) {
		return errors.New("task ID must be a positive integer")
	}

	shown, e := h.Runner.Run(ctx, h.Command, []string{"tasks", "show", task}, "", "")
	if e != nil {
		return e
	}
	if _, e = h.Runner.Run(ctx, h.Command, []string{"tasks", "comment", task, "--body", note}, "", ""); e != nil {
		return e
	}
	var current struct {
		State string `json:"state"`
	}
	_ = json.Unmarshal(shown, &current)
	if current.State == "reopened" {
		return nil
	}

	if current.State != "merged" && current.State != "dropped" {
		return fmt.Errorf("recovery comment recorded; task %s is %q, explicit reopen requires merged or dropped", task, current.State)
	}
	_, e = h.Runner.Run(ctx, h.Command, []string{"tasks", "reopen", task, "--reason", note}, "", "")
	if e != nil {
		return fmt.Errorf("recovery comment recorded, but task %s could not reopen from %q to reopened; server/CLI may lack explicit reopen support: %w", task, current.State, e)
	}
	return nil
}
// Lane reads the owning lane from `tasks show`; an absent field means the task
// has no routing lane and the caller falls back, never guessing.
func (h Handoffkeep) Lane(ctx context.Context, task string) (string, error) {
	if !TaskID.MatchString(task) {
		return "", errors.New("task ID must be a positive integer")
	}
	out, e := h.Runner.Run(ctx, h.Command, []string{"tasks", "show", task}, "", "")
	if e != nil {
		return "", e
	}
	var shown struct {
		Lane string `json:"lane"`
	}
	if e = json.Unmarshal(out, &shown); e != nil {
		return "", e
	}
	return shown.Lane, nil
}
func (h Handoffkeep) Record(ctx context.Context, key string, data []byte) error {
	f, e := os.CreateTemp("", "mergewake-record-*.json")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(data); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	_, e = h.Runner.Run(ctx, h.Command, []string{"doc", "put", "--key", key, "--kind", "note", "--file", f.Name()}, "", "")
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
		s = Handoffkeep{r, cmd, c.Sink.ReopenState}
	}
	switch c.Notifier.Type {
	case "stdout":
		n = Stdout{out}
	case "panewire":
		cmd := c.Notifier.Command
		if cmd == "" {
			cmd = "panewire"
		}
		n = Panewire{r, cmd, c.Notifier.Lane}
	}
	return s, n
}

// RunDuplex owns one foreground protocol session and always reaps its process
// group after the single exchange, including errors and cancellation.
func (Exec) RunDuplex(ctx context.Context, command string, args []string, dir string, exchange func(io.Reader, io.Writer) ([]byte, error)) ([]byte, error) {
	c := exec.CommandContext(ctx, command, args...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error { return syscall.Kill(-c.Process.Pid, syscall.SIGKILL) }
	c.WaitDelay = 2 * time.Second
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
	_ = c.Cancel()
	_ = c.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
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

func (Exec) RunReview(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	return (Exec{ReapGroup: true}).Run(ctx, command, args, input, dir)
}
