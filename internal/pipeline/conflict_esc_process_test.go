package pipeline

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/integrations"
)

// conflictEscChildEnv names the shared state dir of a TestHelperProcessConflictEsc
// child; unset, that test does nothing.
const conflictEscChildEnv = "MERGEWAKE_CONFLICT_ESC_CHILD"

// conflictEscChildren is how many processes observe the head at once.
const conflictEscChildren = 4

// appendNotifier is a panewire shared by processes: every ESC it is offered
// is accepted and appended as one line to a file, so the lines count the
// accepted sends of all processes. It holds each send long enough that a
// process not serialised by the state file's lock reads the state before any
// receipt is written.
type appendNotifier struct {
	file string
	hold time.Duration
}

func (n appendNotifier) Notify(_ context.Context, e integrations.Event) error {
	time.Sleep(n.hold)
	f, err := os.OpenFile(n.file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(e.Lane + " " + e.ID + "\n")
	return err
}

// TestHelperProcessConflictEsc is one process of
// TestConflictEscCrossProcessSendsOnce. It says it is ready on fd 3, waits for
// its stdin to close, then observes the conflicting head once against the
// shared state dir.
func TestHelperProcessConflictEsc(t *testing.T) {
	dir := os.Getenv(conflictEscChildEnv)
	if dir == "" {
		return
	}
	rig := newEscRig(t)
	rig.g.pull.Body = "Mergewake-Notify-Lane: design-lane\n"
	rig.s.Config.StateDir = filepath.Join(dir, "state")
	rig.s.Notifier = appendNotifier{file: filepath.Join(dir, "sends"), hold: 500 * time.Millisecond}
	ready := os.NewFile(3, "ready")
	if _, e := ready.Write([]byte{1}); e != nil {
		t.Fatal(e)
	}
	ready.Close()
	if _, e := io.Copy(io.Discard, os.Stdin); e != nil {
		t.Fatal(e)
	}
	if e := rig.s.EscalateConflict(context.Background(), rig.r, rig.g.pull, Options{}); e != nil {
		t.Fatal(e)
	}
}

// (P4, mergewake#247 r1) Separate processes observe the same conflicting head
// at the same moment against one state dir: exactly one of them sends the ESC.
// Only the state file's lock serialises them; the in-process path lock does
// not reach across processes.
func TestConflictEscCrossProcessSendsOnce(t *testing.T) {
	dir := t.TempDir()
	readyR, readyW, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer readyR.Close()
	goR, goW, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	type child struct {
		cmd *exec.Cmd
		out *bytes.Buffer
	}
	var children []child
	for i := 0; i < conflictEscChildren; i++ {
		out := &bytes.Buffer{}
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessConflictEsc$", "-test.count=1")
		cmd.Env = append(os.Environ(), conflictEscChildEnv+"="+dir)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = goR, out, out
		cmd.ExtraFiles = []*os.File{readyW}
		if e := cmd.Start(); e != nil {
			t.Fatal(e)
		}
		children = append(children, child{cmd, out})
	}
	readyW.Close()
	goR.Close()
	// Every child is past its start-up before any is let go.
	if _, e := io.ReadFull(readyR, make([]byte, conflictEscChildren)); e != nil {
		t.Errorf("a child exited before it was ready: %v", e)
	}
	goW.Close()
	for i, c := range children {
		if e := c.cmd.Wait(); e != nil {
			t.Errorf("child %d: %v\n%s", i, e, c.out)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	b, e := os.ReadFile(filepath.Join(dir, "sends"))
	if e != nil {
		t.Fatal(e)
	}
	sends := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(sends) != 1 || !strings.HasPrefix(sends[0], "design-lane mergewake-esc-example/project-7-conflict-"+shaA) {
		t.Fatalf("%d ESCs sent by %d processes, want 1: %q", len(sends), conflictEscChildren, sends)
	}
}
