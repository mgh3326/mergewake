package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestServerStoreBoundary(t *testing.T) {
	t.Setenv("MERGEWAKE_MYSQL_DSN", "")
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	for _, args := range [][]string{{"server"}, {"server", "serve"}, {"server", "migrate", "unexpected"}, {"server", "check"}} {
		var out, errout bytes.Buffer
		err := run(context.Background(), args, &out, &errout)
		if err == nil || out.Len() != 0 {
			t.Fatal(args, err, out.String())
		}
		if strings.Contains(err.Error(), "GitHub") {
			t.Fatal("store path entered legacy pipeline")
		}
	}
}

func TestServerStoreHelpPrintsUsageWithoutStore(t *testing.T) {
	t.Setenv("MERGEWAKE_MYSQL_DSN", "")
	t.Setenv("MERGEWAKE_MYSQL_DSN_FILE", "")
	for _, args := range [][]string{{"--help"}, {"help"}, {"migrate", "--help"}, {"check", "-h"}} {
		var out, errout bytes.Buffer
		if err := runServerStore(context.Background(), args, &out, &errout); err != nil {
			t.Fatal(args, err)
		}
		if args[0] == "--help" || args[0] == "help" {
			if !strings.Contains(out.String(), "usage: server") {
				t.Fatal(args, out.String())
			}
		}
	}
}
