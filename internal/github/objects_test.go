package github

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type fakeCache struct {
	mu             sync.Mutex
	objects        map[string][]byte
	gets, puts     int
	getErr, putErr error
}

func (f *fakeCache) GetObject(_ context.Context, repo, kind, sha, path string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	b, ok := f.objects[repo+"|"+kind+"|"+sha+"|"+path]
	return b, ok, nil
}

func (f *fakeCache) PutObject(_ context.Context, repo, kind, sha, path string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.putErr != nil {
		return f.putErr
	}
	if f.objects == nil {
		f.objects = map[string][]byte{}
	}
	f.objects[repo+"|"+kind+"|"+sha+"|"+path] = append([]byte(nil), body...)
	return nil
}

func (f *fakeCache) has(kind string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.objects {
		if strings.Contains(k, "|"+kind+"|") {
			return true
		}
	}
	return false
}

var (
	testTree = strings.Repeat("b", 40)
	testBlob = strings.Repeat("c", 40)
)

// repoServer serves one commit holding file "a/b.txt" and counts requests by
// route. The blob holds content.
type repoServer struct {
	mu    sync.Mutex
	count map[string]int
}

func (r *repoServer) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.count {
		n += c
	}
	return n
}

func (r *repoServer) client(t *testing.T, content []byte) *Client {
	r.count = map[string]int{}
	return api(t, func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.count[req.Method+" "+req.URL.Path]++
		r.mu.Unlock()
		switch {
		case strings.Contains(req.URL.Path, "/commits/"):
			reply(w, map[string]any{"sha": testSHA, "commit": map[string]any{"tree": Ref{SHA: testTree}}})
		case strings.HasSuffix(req.URL.Path, "/git/trees/"+testTree):
			reply(w, map[string]any{"tree": []TreeEntry{{Path: "a", Type: "tree", Mode: "040000", SHA: strings.Repeat("d", 40)}}})
		case strings.HasSuffix(req.URL.Path, "/git/trees/"+strings.Repeat("d", 40)):
			reply(w, map[string]any{"tree": []TreeEntry{{Path: "b.txt", Type: "blob", Mode: "100644", SHA: testBlob}, {Path: "c.txt", Type: "blob", Mode: "100644", SHA: strings.Repeat("e", 40)}}})
		case strings.HasSuffix(req.URL.Path, "/git/blobs/"+testBlob), strings.HasSuffix(req.URL.Path, "/git/blobs/"+strings.Repeat("e", 40)):
			reply(w, map[string]any{"encoding": "base64", "size": len(content), "content": base64.StdEncoding.EncodeToString(content)})
		default:
			http.NotFound(w, req)
		}
	})
}

func TestReadFileServedFromObjectCache(t *testing.T) {
	var srv repoServer
	c := srv.client(t, []byte("payload"))
	cache := &fakeCache{}
	c.Objects = cache
	ctx, counts := CountRequests(context.Background(), "pr")
	got, e := c.ReadFile(ctx, "example/project", testSHA, "a/b.txt")
	if e != nil || string(got) != "payload" {
		t.Fatalf("%q %v", got, e)
	}
	// commit, two trees, blob.
	if first := srv.total(); first != 4 {
		t.Fatalf("first read made %d requests", first)
	}
	got, e = c.ReadFile(ctx, "example/project", testSHA, "a/b.txt")
	if e != nil || string(got) != "payload" || srv.total() != 4 {
		t.Fatalf("second read: %q %v, %d requests", got, e, srv.total())
	}
	for _, kind := range []string{kindCommit, kindTree, kindBlob, kindFile} {
		if !cache.has(kind) {
			t.Errorf("no %s stored", kind)
		}
	}
	if s := counts.CacheStats(); s.Hits != 1 || s.Misses < 1 {
		t.Fatalf("stats %+v", s)
	}
	// A new process (empty client state) sharing the cache makes no request.
	var other repoServer
	c2 := other.client(t, []byte("different"))
	c2.Objects = cache
	if got, e = c2.ReadFile(context.Background(), "example/project", testSHA, "a/b.txt"); e != nil || string(got) != "payload" || other.total() != 0 {
		t.Fatalf("shared cache: %q %v, %d requests", got, e, other.total())
	}
	// Another file of the same commit reuses its commit and trees: one blob read.
	if got, e = c.ReadFile(ctx, "example/project", testSHA, "a/c.txt"); e != nil || string(got) != "payload" || srv.total() != 5 {
		t.Fatalf("sibling file: %q %v, %d requests", got, e, srv.total())
	}
}

func TestObjectCacheErrorsFallThroughToGitHub(t *testing.T) {
	for name, cache := range map[string]*fakeCache{
		"get":  {getErr: errors.New("get failed")},
		"put":  {putErr: errors.New("put failed")},
		"both": {getErr: errors.New("get failed"), putErr: errors.New("put failed")},
	} {
		t.Run(name, func(t *testing.T) {
			var srv repoServer
			c := srv.client(t, []byte("payload"))
			var log bytes.Buffer
			c.Stderr = &log
			c.Objects = cache
			ctx, _ := CountRequests(context.Background(), "pr")
			var calls int
			for i := 0; i < 2; i++ {
				got, e := c.ReadFile(ctx, "example/project", testSHA, "a/b.txt")
				if e != nil || string(got) != "payload" {
					t.Fatalf("read %d: %q %v", i, got, e)
				}
				if i == 0 {
					calls = cache.gets + cache.puts
				}
			}
			if lines := strings.Count(log.String(), "object cache unavailable"); lines != 1 {
				t.Fatalf("%d log lines in one pass: %q", lines, log.String())
			}
			if cache.gets+cache.puts != calls {
				t.Fatalf("cache kept being called after it failed: %d gets, %d puts", cache.gets, cache.puts)
			}
			// A later pass tries the cache again and logs again.
			ctx, _ = CountRequests(context.Background(), "pr")
			if _, e := c.ReadFile(ctx, "example/project", testSHA, "a/b.txt"); e != nil {
				t.Fatal(e)
			}
			if lines := strings.Count(log.String(), "object cache unavailable"); lines != 2 {
				t.Fatalf("%d log lines over two passes", lines)
			}
		})
	}
}

func TestNoObjectCacheReadsGitHubEveryTime(t *testing.T) {
	var srv repoServer
	c := srv.client(t, []byte("payload"))
	if c.Objects != nil {
		t.Fatal("a client has a cache by default")
	}
	for i := 0; i < 2; i++ {
		if got, e := c.ReadFile(context.Background(), "example/project", testSHA, "a/b.txt"); e != nil || string(got) != "payload" {
			t.Fatalf("%q %v", got, e)
		}
	}
	if srv.total() != 8 {
		t.Fatalf("%d requests for two uncached reads", srv.total())
	}
}

func TestObjectsOverTheBoundAreNotStored(t *testing.T) {
	big := bytes.Repeat([]byte("x"), MaxCachedObject+1)
	var srv repoServer
	c := srv.client(t, big)
	cache := &fakeCache{}
	c.Objects = cache
	if got, e := c.ReadFile(context.Background(), "example/project", testSHA, "a/b.txt"); e != nil || len(got) != len(big) {
		t.Fatalf("%d %v", len(got), e)
	}
	for k, v := range cache.objects {
		if len(v) > MaxCachedObject {
			t.Errorf("%s stored %d bytes", k, len(v))
		}
	}
	if cache.has(kindFile) || cache.has(kindBlob) {
		t.Fatal("oversized file or blob stored")
	}
	if !cache.has(kindCommit) || !cache.has(kindTree) {
		t.Fatal("small objects of the same read were not stored")
	}
}

func TestOnlyShaAddressedObjectsAreCached(t *testing.T) {
	var srv repoServer
	c := srv.client(t, []byte("payload"))
	cache := &fakeCache{}
	c.Objects = cache
	// A branch name is mutable: it never reaches the cache.
	if _, e := c.Commit(context.Background(), "example/project", "main"); e != nil {
		t.Fatal(e)
	}
	if cache.gets != 0 || cache.puts != 0 {
		t.Fatalf("branch name reached the cache: %d gets, %d puts", cache.gets, cache.puts)
	}
	if cacheable(testSHA, "dir/é.txt") || cacheable(testSHA, strings.Repeat("a", 256)) || !cacheable(strings.ToUpper(testSHA), "dir/a.txt") {
		t.Fatal("cacheable key rules")
	}
}
