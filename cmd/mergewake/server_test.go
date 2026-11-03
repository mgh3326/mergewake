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
