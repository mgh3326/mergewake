package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestReapLegacyFlagValidation(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"reap", "--legacy", "--pr", "7"}, "does not take --pr"},
		{[]string{"reap", "--legacy", "--apply", "--dry-run"}, "exclusive"},
		{[]string{"reap", "--host", "h1", "--repo", "example/project"}, "--host needs --legacy"},
	} {
		e := run(context.Background(), tc.args, &bytes.Buffer{}, &bytes.Buffer{})
		if e == nil || !strings.Contains(e.Error(), tc.want) {
			t.Errorf("%v: %v", tc.args, e)
		}
	}
	var out bytes.Buffer
	if e := run(context.Background(), []string{"reap", "--help"}, &out, &out); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "-legacy") || !strings.Contains(out.String(), "-host") {
		t.Fatalf("help:\n%s", out.String())
	}
}
