package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestHelpAndFlagValidation(t *testing.T) {
	for _, cmd := range []string{"land", "review", "watch", "release", "rollout", "agent", "daemon", "status", "superfan", "reap"} {
		t.Run(cmd, func(t *testing.T) {
			var out bytes.Buffer
			if e := run(context.Background(), []string{cmd, "--help"}, &out, &out); e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(out.String(), "dry-run") {
				t.Fatal("missing dry-run flag")
			}
		})
	}
	for _, args := range [][]string{{"unknown"}, {"land", "--unknown"}, {"superfan", "--launch"}} {
		if e := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); e == nil {
			t.Fatal(args)
		}
	}
}
func TestSuperfanDryLaunch(t *testing.T) {
	var out bytes.Buffer
	if e := run(context.Background(), []string{"superfan", "--name", "ci", "--launch", "--runner", "does-not-exist", "--dry-run"}, &out, &out); e != nil {
		t.Fatal(e)
	}
	var spec map[string]any
	if e := json.Unmarshal(out.Bytes(), &spec); e != nil {
		t.Fatal(e)
	}
	if spec["name"] != "ci" || len(spec["allowed_actions"].([]any)) == 0 {
		t.Fatal(spec)
	}
}
