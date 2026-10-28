package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/manifest"
)

func TestPinnedReleaseBinaryAndChecksum(t *testing.T) {
	sha := strings.Repeat("a", 40)
	data := []byte("binary payload")
	bad := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example/project/releases":
			fmt.Fprintf(w, `[{"tag_name":"v1.0.0","assets":[{"id":1,"name":%q,"size":%d},{"id":2,"name":"SHA256SUMS","size":100}]}]`, binaryName(), len(data))
		case "/repos/example/project/commits/v1.0.0":
			fmt.Fprintf(w, `{"sha":%q}`, sha)
		case "/repos/example/project/releases/assets/1":
			if bad {
				w.Write([]byte("corrupt"))
			} else {
				w.Write(data)
			}
		case "/repos/example/project/releases/assets/2":
			fmt.Fprintf(w, "%s  %s\n", manifest.Sum(data), binaryName())
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	got, e := c.BinaryHead(context.Background(), "example/project")
	if e != nil || got != sha {
		t.Fatalf("head %s %v", got, e)
	}
	b, e := c.BinaryAtCommit(context.Background(), "example/project", sha)
	if e != nil || string(b) != string(data) {
		t.Fatalf("binary %s %v", b, e)
	}
	bad = true
	if _, e = c.BinaryAtCommit(context.Background(), "example/project", sha); e == nil {
		t.Fatal("corrupt binary accepted")
	}
	if _, e = c.BinaryAtCommit(context.Background(), "example/project", strings.Repeat("b", 40)); e == nil {
		t.Fatal("unpinned release accepted")
	}
}
