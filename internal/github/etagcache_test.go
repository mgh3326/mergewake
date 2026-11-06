package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// etagServer serves every GET path with a stable ETag and answers a matching
// If-None-Match with 304, as GitHub does for an unchanged resource.
func etagServer(ok, notModified *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tag := fmt.Sprintf(`"%s"`, r.URL.Path)
		w.Header().Set("ETag", tag)
		if r.Header.Get("If-None-Match") == tag {
			notModified.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		ok.Add(1)
		fmt.Fprintf(w, `{"path":%q}`, r.URL.Path)
	}))
}

// passOver reads keys distinct routes once.
func passOver(t *testing.T, c *Client, keys int) {
	t.Helper()
	for i := 0; i < keys; i++ {
		if _, e := c.request(context.Background(), "GET", fmt.Sprintf("/repos/o/r/pulls/%d", i), "application/vnd.github+json", nil); e != nil {
			t.Fatal(e)
		}
	}
}

// One pass reads far more keys than the cache once held (512). The validators
// of the first pass must still answer the second: nearly all of its reads are
// 304s. With wipe-everything eviction the cache was emptied at the 513th key,
// so a second pass over 1,500 keys got 304s for only the keys after the wipe.
func TestValidatorsSurviveAPassLargerThanTheOldCache(t *testing.T) {
	const keys = 1500
	var ok, notModified atomic.Int32
	server := etagServer(&ok, &notModified)
	defer server.Close()
	c := New("")
	c.BaseURL = server.URL
	c.Polling = true
	passOver(t, c, keys)
	first200, first304 := ok.Load(), notModified.Load()
	passOver(t, c, keys)
	second200, second304 := ok.Load()-first200, notModified.Load()-first304
	t.Logf("pass 1: %d×200 %d×304; pass 2: %d×200 %d×304", first200, first304, second200, second304)
	if first200 != keys || first304 != 0 {
		t.Fatalf("first pass: %d×200 %d×304", first200, first304)
	}
	if second304 != keys || second200 != 0 {
		t.Fatalf("second pass over %d keys: %d×200 %d×304, want all 304", keys, second200, second304)
	}
}

// The cache stays bounded and evicts the least recently used key, not all of
// them: a key read again keeps its validator while the coldest one goes.
func TestResponseCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := newResponseCache(3, 1<<20)
	for _, k := range []string{"a", "b", "c"} {
		cache.put(k, cachedResponse{ETag: k, Body: []byte(k)})
	}
	cache.get("a")
	evicted := cache.put("d", cachedResponse{ETag: "d", Body: []byte("d")})
	if len(evicted) != 1 || evicted[0] != "b" {
		t.Fatalf("evicted %v, want [b]", evicted)
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := cache.get(k); !ok {
			t.Fatalf("%s was evicted", k)
		}
	}
	if cache.len() != 3 || cache.bytes != 3 {
		t.Fatalf("len %d bytes %d", cache.len(), cache.bytes)
	}
}

func TestResponseCacheBoundsBytes(t *testing.T) {
	cache := newResponseCache(100, 10)
	for _, k := range []string{"a", "b", "c"} {
		cache.put(k, cachedResponse{ETag: k, Body: make([]byte, 4)})
	}
	if _, ok := cache.get("a"); ok || cache.len() != 2 || cache.bytes != 8 {
		t.Fatalf("len %d bytes %d, oldest kept: %v", cache.len(), cache.bytes, ok)
	}
	cache.put("b", cachedResponse{ETag: "b2", Body: make([]byte, 6)})
	if cache.bytes != 10 || cache.len() != 2 {
		t.Fatalf("replacing a body miscounts: len %d bytes %d", cache.len(), cache.bytes)
	}
}
