package integrations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// DefaultGrace is how long a cancelled process group gets between SIGTERM and
// SIGKILL.
const DefaultGrace = 5 * time.Second

// group is one command running as the leader of its own process group.
type group struct {
	*exec.Cmd
	mu     sync.Mutex
	reaped bool
	kill   *time.Timer
}

// groupCommand starts command as the leader of its own process group. On
// cancellation (timeout, caller cancel, or the parent's own SIGINT/SIGTERM/SIGHUP
// via the main context) the whole group receives SIGTERM, and a timer SIGKILLs
// it after grace, independently of Wait: a caller still blocked reading the
// process's output (RunDuplex) is released too. reap marks one-shot review
// processes, whose descendants must never outlive the call.
func groupCommand(ctx context.Context, command string, args []string, grace time.Duration, reap bool) *group {
	if grace <= 0 {
		grace = DefaultGrace
	}
	g := &group{Cmd: exec.CommandContext(ctx, command, args...)}
	g.SysProcAttr = groupAttr(reap)
	g.Cancel = func() error {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.reaped {
			return nil
		}
		if g.kill == nil {
			g.kill = time.AfterFunc(grace, func() { _ = g.signal(syscall.SIGKILL) })
		}
		return signalGroup(g.Cmd, syscall.SIGTERM)
	}
	// exec's own escalation (Kill of the leader, closing our pipe ends) is only
	// a backstop for a descendant that escaped the group; the timer acts first.
	g.WaitDelay = 2 * grace
	return g
}

// signal reaches the group only while its leader is unreaped; the zombie
// leader keeps the group ID from being reused before then.
func (g *group) signal(sig syscall.Signal) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reaped {
		return nil
	}
	return signalGroup(g.Cmd, sig)
}

// finish runs after Wait has reaped the leader: it disarms the escalation
// timer, and a cancelled or one-shot group loses every remaining member, so no
// child is left behind as an orphan.
func (g *group) finish(ctx context.Context, reap bool) {
	g.mu.Lock()
	g.reaped = true
	if g.kill != nil {
		g.kill.Stop()
	}
	g.mu.Unlock()
	if reap || ctx.Err() != nil {
		_ = signalGroup(g.Cmd, syscall.SIGKILL)
	}
}

func signalGroup(c *exec.Cmd, sig syscall.Signal) error {
	if c.Process == nil {
		return nil
	}
	err := syscall.Kill(-c.Process.Pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// stopped names why a context-bound process ended, instead of exec's bare
// "signal: killed".
func stopped(ctx context.Context, command string, err error) error {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("%s timed out; process group terminated: %w", command, ctx.Err())
	case ctx.Err() != nil:
		return fmt.Errorf("%s cancelled; process group terminated: %w", command, ctx.Err())
	}
	return fmt.Errorf("%s failed: %w", command, err)
}

// runReaped is Run for one-shot groups. Using our own pipes, Wait returns when
// the leader exits instead of when every descendant closes stdout; the group is
// then killed and the pipes drained. A descendant that escaped the group with
// setsid only delays draining by grace before the pipes are closed under it.
func runReaped(ctx context.Context, c *exec.Cmd, input string, stdout, stderr io.Writer, grace time.Duration) error {
	if grace <= 0 {
		grace = DefaultGrace
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		return err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		return err
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		outR.Close()
		outW.Close()
		return err
	}
	c.Stdin, c.Stdout, c.Stderr = inR, outW, errW
	err = c.Start()
	inR.Close()
	outW.Close()
	errW.Close()
	if err != nil {
		inW.Close()
		outR.Close()
		errR.Close()
		return err
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _, _ = io.WriteString(inW, input); inW.Close() }()
	go func() { defer wg.Done(); _, _ = io.Copy(stdout, outR) }()
	go func() { defer wg.Done(); _, _ = io.Copy(stderr, errR) }()
	err = c.Wait()
	_ = signalGroup(c, syscall.SIGKILL)
	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(grace):
		inW.Close()
		outR.Close()
		errR.Close()
		<-drained
	}
	outR.Close()
	errR.Close()
	return err
}
