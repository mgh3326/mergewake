package github

import (
	"context"
	"strings"
	"testing"
)

// Dir lists the blobs of one directory with full paths and blob SHAs, PathBlob
// names a file's blob, and both treat a missing path as empty, not an error.
func TestDirAndPathBlob(t *testing.T) {
	var srv repoServer
	c := srv.client(t, []byte("payload"))
	ctx := context.Background()
	got, e := c.Dir(ctx, "example/project", testSHA, "a")
	if e != nil || len(got) != 2 || got[0].Path != "a/b.txt" || got[0].SHA != testBlob || got[1].Path != "a/c.txt" || got[1].SHA != strings.Repeat("e", 40) {
		t.Fatalf("%+v %v", got, e)
	}
	if got, e = c.Dir(ctx, "example/project", testSHA, "nope"); e != nil || len(got) != 0 {
		t.Fatalf("missing dir: %+v %v", got, e)
	}
	if got, e = c.Dir(ctx, "example/project", testSHA, "a/b.txt"); e != nil || len(got) != 0 {
		t.Fatalf("file as dir: %+v %v", got, e)
	}
	if b, e := c.PathBlob(ctx, "example/project", testSHA, "a/b.txt"); e != nil || b != testBlob {
		t.Fatalf("%q %v", b, e)
	}
	if b, e := c.PathBlob(ctx, "example/project", testSHA, "a/zzz"); e != nil || b != "" {
		t.Fatalf("missing file: %q %v", b, e)
	}
	if b, e := c.PathBlob(ctx, "example/project", testSHA, "a"); e != nil || b != "" {
		t.Fatalf("directory: %q %v", b, e)
	}
}
