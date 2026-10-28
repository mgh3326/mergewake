// Package integrations adapts external CLIs without importing their internals.
package integrations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
)

type Runner interface {
	Run(context.Context, string, []string, string, string) ([]byte, error)
}
type Exec struct{}
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
func (Exec) Run(ctx context.Context, command string, args []string, input, dir string) ([]byte, error) {
	if command == "" {
		return nil, errors.New("empty command")
	}
	c := exec.CommandContext(ctx, command, args...)
	c.Dir = dir
	c.Stdin = strings.NewReader(input)
	c.WaitDelay = 2 * time.Second
	var out, stderr limited
	c.Stdout = &out
	c.Stderr = &stderr
	e := c.Run()
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
	_, err := p.Runner.Run(ctx, p.Command, []string{"emit", "--kind", "lane.event", "--lane", p.Lane, "--event-id", e.ID, "--text", e.Text}, "", "")
	return err
}

type Handoffkeep struct {
	Runner         Runner
	Command, State string
}

var TaskID = regexp.MustCompile(`^[1-9][0-9]*$`)

func (h Handoffkeep) Reopen(ctx context.Context, task, note string) error {
	if !TaskID.MatchString(task) {
		return errors.New("task ID must be a positive integer")
	}
	if _, e := h.Runner.Run(ctx, h.Command, []string{"tasks", "show", task}, "", ""); e != nil {
		return e
	}
	_, e := h.Runner.Run(ctx, h.Command, []string{"tasks", "transition", task, "--to", h.State, "--note", note}, "", "")
	return e
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
