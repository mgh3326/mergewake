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

func TestFakeCLIsReceiveOnlyStdin(t *testing.T) {
	for _, kind := range []string{"claude", "codex", "devin", "grok", "pi", "cmd"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			work := filepath.Join(root, "work")
			control := filepath.Join(root, "control")
			capture := filepath.Join(root, "input")
			for _, d := range []string{work, control} {
				if err := os.Mkdir(d, 0700); err != nil {
					t.Fatal(err)
				}
			}
			command := filepath.Join(root, kind)
			script := `#!/bin/sh
set -eu
[ -z "${GITHUB_TOKEN+x}" ]
[ -z "${GH_TOKEN+x}" ]
[ -z "${CUSTOM_GH_SECRET+x}" ]
[ -z "$(ls -A)" ]
printf '%s\n' "$@" > "$CAPTURE_ARGS"
cat > "$CAPTURE_INPUT"
printf '%s' '{"findings":[]}'
`
			if err := os.WriteFile(command, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CAPTURE_INPUT", capture)
			t.Setenv("CAPTURE_ARGS", filepath.Join(root, "args"))
			t.Setenv("GITHUB_TOKEN", "fixture")
			t.Setenv("GH_TOKEN", "fixture")
			t.Setenv("CUSTOM_GH_SECRET", "fixture")
			inv, err := Prepare(config.Reviewer{Command: command, Args: []string{"--model", "test-model"}}, control)
			if err != nil {
				t.Fatal(err)
			}
			input := "diff\n$(this is inert) `also inert`\n"
			b, err := Run(context.Background(), integrations.Exec{}, inv, input, work, "CUSTOM_GH_SECRET")
			if err != nil || string(b) != `{"findings":[]}` {
				t.Fatal(string(b), err)
			}
			got, _ := os.ReadFile(capture)
			if string(got) != input {
				t.Fatal("stdin changed")
			}
			args, _ := os.ReadFile(filepath.Join(root, "args"))
			if strings.Contains(string(args), "this is inert") {
				t.Fatal("diff leaked into argv")
			}
			expected := map[string]string{"claude": "--tools\n\n", "codex": "features.shell_tool=false", "devin": "--prompt-file\n/dev/stdin", "grok": "MCPTool(*)", "pi": "--no-tools", "cmd": "permissions.deny"}[kind]
			if !strings.Contains(string(args), expected) {
				t.Fatal("missing isolation control", string(args))
			}
		})
	}
}
func TestKimiACPStdinAndNoTools(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "work")
	control := filepath.Join(root, "control")
	source := filepath.Join(root, "source")
	for _, d := range []string{work, control, source} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "config.toml"), []byte("default_model = 'test-model'\n[hooks]\nmalicious = 'not copied'\n[tools]\ndisabled = []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KIMI_CODE_HOME", source)
	command := filepath.Join(root, "kimi")
	script := `#!/usr/bin/env python3
import json,os,sys
assert not os.listdir(os.getcwd())
assert 'GITHUB_TOKEN' not in os.environ
config=open(os.path.join(os.environ['KIMI_CODE_HOME'],'config.toml')).read()
assert '[hooks]' not in config and 'enabled = [' in config and '*' in config
for line in sys.stdin:
 m=json.loads(line); method=m['method']; result={}
 if method=='initialize': assert m['params']['clientCapabilities']['terminal']==False
 elif method=='session/new':
  assert m['params']['mcpServers']==[]
  result={'sessionId':'test-session'}
 elif method=='session/set_model': assert m['params']['modelId']=='test-model'
 elif method=='session/prompt':
  assert m['params']['prompt'][0]['text']=='untrusted diff'
  print(json.dumps({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'test-session','update':{'sessionUpdate':'agent_message_chunk','content':{'type':'text','text':'{"findings":[]}'}}}}),flush=True)
  result={'stopReason':'end_turn'}
 print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':result}),flush=True)
`
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	inv, err := Prepare(config.Reviewer{Command: command, Args: []string{"--model", "test-model"}}, control)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	b, err := Run(ctx, integrations.Exec{}, inv, "untrusted diff", work, "")
	if err != nil || string(b) != `{"findings":[]}` {
		t.Fatal(string(b), err)
	}
}
