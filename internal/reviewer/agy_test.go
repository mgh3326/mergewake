package reviewer

import (
	"context"
	"os"
	"path/filepath"
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
