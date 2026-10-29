package reviewer

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

func TestAgyStreamJSONStdinAndNoTools(t *testing.T) {
	for _, tt := range []struct {
		name, steps, want string
	}{
		{"review", `["user_input","agent_response"]`, ""},
		{"tool step", `["user_input","agent_response","tool","agent_response"]`, "tool call"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			work := filepath.Join(root, "work")
			control := filepath.Join(root, "control")
			for _, d := range []string{work, control} {
				if err := os.Mkdir(d, 0700); err != nil {
					t.Fatal(err)
				}
			}
			command := filepath.Join(root, "agy")
			script := `#!/usr/bin/env python3
import json,os,sys
assert not os.listdir(os.getcwd())
assert 'GITHUB_TOKEN' not in os.environ
args=sys.argv[1:]
assert '--print=' in args and args[args.index('--input-format')+1]=='stream-json'
assert 'command(*)' in json.loads(args[args.index('--settings')+1])['permissions']['deny']
assert 'untrusted' not in ' '.join(args)
lines=sys.stdin.read().splitlines()
assert len(lines)==1
m=json.loads(lines[0])
assert m['event']=='user' and m['message']['content']=='untrusted diff $(inert)'
print(json.dumps({'event':'init','init':{'model':'test-model'}}))
for i,s in enumerate(` + tt.steps + `):
 print(json.dumps({'event':'step_update','step_update':{'step_index':i,'state':'DONE','step_type':s}}))
print(json.dumps({'event':'result','result':{'status':'SUCCESS','response':'{"findings":[]}\n','num_turns':1}}))
`
			if err := os.WriteFile(command, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GITHUB_TOKEN", "fixture")
			inv, err := Prepare(config.Reviewer{Command: command, Args: []string{"--model", "test-model", "--effort", "high"}}, control)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Run(context.Background(), integrations.Exec{}, inv, "untrusted diff $(inert)", work, "")
			if tt.want != "" {
				if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.HasPrefix(err.Error(), "agy reviewer:") {
					t.Fatal(string(b), err)
				}
				return
			}
			if err != nil || string(b) != "{\"findings\":[]}\n" {
				t.Fatal(string(b), err)
			}
		})
	}
}

func TestAgyOutputFailsClosed(t *testing.T) {
	for _, out := range []string{
		``,
		`not json`,
		`{"event":"result","result":{"status":"ERROR","response":"x","num_turns":1}}`,
		`{"event":"result","result":{"status":"SUCCESS","response":"x","num_turns":2}}`,
		`{"event":"result","result":{"status":"SUCCESS","response":"  ","num_turns":1}}`,
		`{"event":"step_update","step_update":{"step_type":"browser"}}` + "\n" + `{"event":"result","result":{"status":"SUCCESS","response":"x","num_turns":1}}`,
	} {
		if _, err := agyOutput([]byte(out)); err == nil {
			t.Fatalf("accepted %q", out)
		}
	}
}

func TestSweepScratchRemovesOnlyStaleOwnDirs(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	old := filepath.Join(root, "mergewake-review-old")
	fresh := filepath.Join(root, "mergewake-review-fresh")
	other := filepath.Join(root, "unrelated-old")
	for _, d := range []string{old, fresh, other} {
		if err := os.MkdirAll(filepath.Join(d, "control", "kimi"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "mergewake-review-link")
	if err := os.Symlink(other, link); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{old, other, link} {
		_ = os.Chtimes(d, now.Add(-2*time.Hour), now.Add(-2*time.Hour))
	}
	if err := SweepScratch(root, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]bool{old: false, fresh: true, other: true, link: true} {
		if _, err := os.Lstat(path); (err == nil) != want {
			t.Fatalf("%s exists=%v want %v", path, err == nil, want)
		}
	}
}

// agedScratch builds a lock-bearing scratch dir the way NewScratch does, aged.
func agedScratch(t *testing.T, root, name string, age time.Duration, now time.Time) string {
	t.Helper()
	d := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(d, "control", "kimi"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, scratchLock), []byte(strconv.Itoa(os.Getpid())+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(d, now.Add(-age), now.Add(-age)); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSweepScratchKeepsLiveOwner(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	root := os.TempDir()
	live, release, err := NewScratch()
	if err != nil {
		t.Fatal(err)
	}
	// Review A works inside work/, so the top-level mtime stays old while a
	// later review B sweeps.
	if err := os.Mkdir(filepath.Join(live, "work"), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := os.Chtimes(live, now.Add(-3*time.Hour), now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	dead := agedScratch(t, root, "mergewake-review-dead", 3*time.Hour, now)
	if err := SweepScratch(root, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(live, "work")); err != nil {
		t.Fatalf("live scratch swept: %v", err)
	}
	if _, err := os.Lstat(dead); err == nil {
		t.Fatal("dead owner's scratch kept")
	}
	release()
	if _, err := os.Lstat(live); err == nil {
		t.Fatal("release left scratch behind")
	}
}

func TestScratchOwnerHelper(t *testing.T) {
	if os.Getenv("MERGEWAKE_SCRATCH_HELPER") == "" {
		t.Skip("helper process only")
	}
	path, _, err := NewScratch()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(path)
	time.Sleep(time.Minute)
}

func TestSweepScratchAfterOwnerKilled(t *testing.T) {
	tmp := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestScratchOwnerHelper$")
	cmd.Env = append(os.Environ(), "MERGEWAKE_SCRATCH_HELPER=1", "TMPDIR="+tmp)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	path, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	path = strings.TrimSpace(path)
	now := time.Now()
	_ = os.Chtimes(path, now.Add(-3*time.Hour), now.Add(-3*time.Hour))
	if err := SweepScratch(tmp, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("running owner's scratch swept: %v", err)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if err := SweepScratch(tmp, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err == nil {
		t.Fatal("killed owner's scratch kept")
	}
}

func TestSweepScratchIgnoresRecordedPid(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	// The lock file names this very (live) process, as a reused pid would, but
	// nothing holds the lock: the owner is gone.
	d := agedScratch(t, root, "mergewake-review-reused", 3*time.Hour, now)
	if err := SweepScratch(root, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(d); err == nil {
		t.Fatal("unlocked scratch kept because its recorded pid is alive")
	}
}

func TestSweepScratchKeepsFreshUnlocked(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	d := agedScratch(t, root, "mergewake-review-young", time.Minute, now)
	if err := SweepScratch(root, time.Hour, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(d); err != nil {
		t.Fatal("fresh scratch removed")
	}
}
