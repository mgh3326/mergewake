package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	appTestUserToken = "user-token-placeholder"
	appTestAppID     = 4242
	appTestInstall   = 77
)

var (
	appKeyOnce sync.Once
	appKey     *rsa.PrivateKey
)

// testAppKey is generated here and never written anywhere.
func testAppKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	appKeyOnce.Do(func() {
		k, e := rsa.GenerateKey(rand.Reader, 2048)
		if e != nil {
			panic(e)
		}
		appKey = k
	})
	return appKey
}

type seen struct {
	method, path, auth string
	header             http.Header
}

// fakeGitHub answers App endpoints and every repository route, recording who
// asked with which credential.
type fakeGitHub struct {
	mu        sync.Mutex
	requests  []seen
	mints     atomic.Int32
	lookups   atomic.Int32
	noInstall map[string]bool
	denyRead  map[string]int // repo -> status a read with an installation token gets
	expiresIn time.Duration
	clock     func() time.Time
	remaining func(auth string) (int, time.Time, bool)
}

func (f *fakeGitHub) token() string { return "inst-token-" + strconv.Itoa(int(f.mints.Load())) }

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	f.mu.Lock()
	f.requests = append(f.requests, seen{r.Method, r.URL.Path, auth, r.Header.Clone()})
	f.mu.Unlock()
	switch {
	case strings.HasSuffix(r.URL.Path, "/installation") && r.Method == "GET":
		f.lookups.Add(1)
		parts := strings.Split(r.URL.Path, "/")
		if f.noInstall[parts[2]+"/"+parts[3]] {
			http.NotFound(w, r)
			return
		}
		reply(w, map[string]any{"id": appTestInstall})
		return
	case strings.HasPrefix(r.URL.Path, "/app/installations/") && r.Method == "POST":
		f.mints.Add(1)
		w.WriteHeader(http.StatusCreated)
		reply(w, map[string]any{"token": f.token(), "expires_at": f.clock().Add(f.expiresIn).UTC().Format(time.RFC3339)})
		return
	}
	if code := f.denyRead[memoRepo(r.URL.Path)]; code != 0 && strings.HasPrefix(auth, "Bearer inst-token-") {
		w.WriteHeader(code)
		return
	}
	if f.remaining != nil {
		if n, reset, ok := f.remaining(auth); ok {
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(n))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
		}
	}
	if r.URL.Path == "/graphql" {
		reply(w, map[string]any{"data": map[string]any{}})
		return
	}
	reply(w, map[string]any{"merged": true})
}

func (f *fakeGitHub) last() seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func (f *fakeGitHub) repoRequests() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []seen
	for _, s := range f.requests {
		if !strings.HasPrefix(s.path, "/app/") && !strings.HasSuffix(s.path, "/installation") {
			out = append(out, s)
		}
	}
	return out
}

type appHarness struct {
	*Client
	fake   *fakeGitHub
	stderr *bytes.Buffer
	now    *atomic.Int64
}

func newAppHarness(t *testing.T, withApp bool) appHarness {
	t.Helper()
	h := appHarness{fake: &fakeGitHub{noInstall: map[string]bool{}, denyRead: map[string]int{}, expiresIn: time.Hour}, stderr: &bytes.Buffer{}, now: &atomic.Int64{}}
	h.now.Store(time.Now().Unix())
	clock := func() time.Time { return time.Unix(h.now.Load(), 0) }
	h.fake.clock = clock
	server := httptest.NewServer(h.fake)
	t.Cleanup(server.Close)
	h.Client = New(appTestUserToken)
	h.BaseURL, h.UploadURL, h.HTTP = server.URL, server.URL, server.Client()
	h.Stderr = h.stderr
	h.Now = clock
	if withApp {
		h.EnableApp(appTestAppID, testAppKey(t))
	}
	return h
}

// exercise sends two reads and three writes about one repository.
func exercise(t *testing.T, c *Client) {
	t.Helper()
	ctx := context.Background()
	if e := c.json(ctx, "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
		t.Fatal(e)
	}
	if e := c.graphql(ctx, "o/r", `query($n:Int!){repository(owner:"o",name:"r"){pullRequest(number:$n){id}}}`, map[string]any{"n": 1}, nil); e != nil {
		t.Fatal(e)
	}
	if e := c.json(ctx, "POST", "/repos/o/r/pulls/1/reviews", map[string]string{"event": "COMMENT"}, nil); e != nil {
		t.Fatal(e)
	}
	if e := c.Merge(ctx, "o/r", 1, testSHA); e != nil {
		t.Fatal(e)
	}
	if e := c.graphql(ctx, "o/r", `mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{id}}}`, map[string]any{"id": "T"}, nil); e != nil {
		t.Fatal(e)
	}
}

func TestAppReadsUseInstallationTokenAndWritesUseUserToken(t *testing.T) {
	h := newAppHarness(t, true)
	exercise(t, h.Client)
	want := []struct{ what, method, auth string }{
		{"GET", "GET", "Bearer inst-token-1"},
		{"GraphQL query", "POST", "Bearer inst-token-1"},
		{"POST review", "POST", "Bearer " + appTestUserToken},
		{"PUT merge", "PUT", "Bearer " + appTestUserToken},
		{"GraphQL mutation", "POST", "Bearer " + appTestUserToken},
	}
	got := h.fake.repoRequests()
	if len(got) != len(want) {
		t.Fatalf("got %d repository requests, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].method != w.method || got[i].auth != w.auth {
			t.Errorf("%s: %s with %q, want %s with %q", w.what, got[i].method, got[i].auth, w.method, w.auth)
		}
	}
	if h.fake.lookups.Load() != 1 || h.fake.mints.Load() != 1 {
		t.Errorf("lookups=%d mints=%d, want one each for one repository", h.fake.lookups.Load(), h.fake.mints.Load())
	}
}

func TestAppJWTClaimsAndSignature(t *testing.T) {
	h := newAppHarness(t, true)
	if e := h.json(context.Background(), "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
		t.Fatal(e)
	}
	var jwt string
	for _, s := range h.fake.requests {
		if strings.HasSuffix(s.path, "/installation") {
			jwt = strings.TrimPrefix(s.auth, "Bearer ")
		}
	}
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWT: %d parts", len(parts))
	}
	header, _ := base64.RawURLEncoding.DecodeString(parts[0])
	if string(header) != `{"alg":"RS256","typ":"JWT"}` {
		t.Errorf("header %s", header)
	}
	var claims struct {
		Iss      string
		Iat, Exp int64
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	if e := json.Unmarshal(raw, &claims); e != nil {
		t.Fatal(e)
	}
	now := h.now.Load()
	if claims.Iss != strconv.Itoa(appTestAppID) || claims.Iat != now-60 || claims.Exp-now > 600 || claims.Exp <= now {
		t.Errorf("claims %+v at %d", claims, now)
	}
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if e := rsa.VerifyPKCS1v15(&testAppKey(t).PublicKey, crypto.SHA256, sum[:], sig); e != nil {
		t.Errorf("signature: %v", e)
	}
}

func TestWithoutAppEveryRequestUsesUserToken(t *testing.T) {
	h := newAppHarness(t, false)
	exercise(t, h.Client)
	got := h.fake.requests
	if len(got) != 5 {
		t.Fatalf("got %d requests, want 5 and no App calls: %+v", len(got), got)
	}
	for i, s := range got {
		want := http.Header{
			"Accept":               {"application/vnd.github+json"},
			"Authorization":        {"Bearer " + appTestUserToken},
			"User-Agent":           {"mergewake"},
			"X-Github-Api-Version": {"2026-03-10"},
		}
		if s.method != "GET" {
			want.Set("Content-Type", "application/json")
		}
		for k, v := range want {
			if strings.Join(s.header[k], ",") != strings.Join(v, ",") {
				t.Errorf("request %d header %s = %q, want %q", i, k, s.header[k], v)
			}
		}
		for k := range s.header {
			if _, ok := want[k]; !ok && k != "Accept-Encoding" && k != "Content-Length" {
				t.Errorf("request %d has unexpected header %s", i, k)
			}
		}
	}
}

func TestInstallationTokenIsReusedThenRefreshedOnce(t *testing.T) {
	h := newAppHarness(t, true)
	ctx := context.Background()
	read := func() {
		if e := h.json(ctx, "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
			t.Error(e)
		}
	}
	for range 5 {
		read()
	}
	if h.fake.mints.Load() != 1 {
		t.Fatalf("minted %d tokens for repeated reads, want 1", h.fake.mints.Load())
	}
	h.now.Add(int64((54 * time.Minute) / time.Second)) // 6 minutes left: still valid
	read()
	if h.fake.mints.Load() != 1 {
		t.Fatalf("refreshed with %d tokens minted, want reuse until 5 minutes before expiry", h.fake.mints.Load())
	}
	h.now.Add(int64((2 * time.Minute) / time.Second)) // 4 minutes left: inside the margin
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() { defer wg.Done(); read() }()
	}
	wg.Wait()
	if h.fake.mints.Load() != 2 {
		t.Fatalf("minted %d tokens, want exactly one refresh for concurrent callers", h.fake.mints.Load())
	}
	if h.fake.lookups.Load() != 1 {
		t.Fatalf("looked up the installation %d times, want 1", h.fake.lookups.Load())
	}
	if got := h.fake.last().auth; got != "Bearer inst-token-2" {
		t.Errorf("last read used %q after refresh", got)
	}
}

func fallbackLines(buf *bytes.Buffer, repo string) int {
	n := 0
	for line := range strings.SplitSeq(buf.String(), "\n") {
		if strings.Contains(line, "GitHub App not used for "+repo+":") {
			n++
		}
	}
	return n
}

func TestRepoWithoutInstallationFallsBackToUserTokenAndLogsOnce(t *testing.T) {
	h := newAppHarness(t, true)
	h.fake.noInstall["o/none"] = true
	for range 3 {
		if e := h.json(context.Background(), "GET", "/repos/o/none/pulls/1", nil, nil); e != nil {
			t.Fatal(e)
		}
	}
	for i, s := range h.fake.repoRequests() {
		if s.auth != "Bearer "+appTestUserToken {
			t.Errorf("request %d used %q, want the user token", i, s.auth)
		}
	}
	if h.fake.lookups.Load() != 1 {
		t.Errorf("looked up %d times; the 404 should hold for the fallback window", h.fake.lookups.Load())
	}
	if n := fallbackLines(h.stderr, "o/none"); n != 1 {
		t.Errorf("logged %d times, want once: %q", n, h.stderr.String())
	}
	// A repository with an installation is unaffected.
	if e := h.json(context.Background(), "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
		t.Fatal(e)
	}
	if got := h.fake.last().auth; !strings.HasPrefix(got, "Bearer inst-token-") {
		t.Errorf("other repo used %q", got)
	}
}

func TestAppRefusalFallsBackToUserTokenAndLogsOnce(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusUnauthorized} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			h := newAppHarness(t, true)
			h.fake.denyRead["o/r"] = code
			for range 3 {
				if e := h.json(context.Background(), "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
					t.Fatalf("a refused App read must succeed on the user token: %v", e)
				}
			}
			reads := h.fake.repoRequests()
			if len(reads) != 4 {
				t.Fatalf("got %d reads, want the refused App read plus 3 user reads: %+v", len(reads), reads)
			}
			if !strings.HasPrefix(reads[0].auth, "Bearer inst-token-") {
				t.Errorf("first read used %q", reads[0].auth)
			}
			for _, s := range reads[1:] {
				if s.auth != "Bearer "+appTestUserToken {
					t.Errorf("after refusal a read used %q", s.auth)
				}
			}
			if n := fallbackLines(h.stderr, "o/r"); n != 1 {
				t.Errorf("logged %d times, want once: %q", n, h.stderr.String())
			}
			// The App is tried again once the fallback window ends.
			h.now.Add(int64((appFallbackTTL + time.Second) / time.Second))
			delete(h.fake.denyRead, "o/r")
			if e := h.json(context.Background(), "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
				t.Fatal(e)
			}
			if got := h.fake.last().auth; !strings.HasPrefix(got, "Bearer inst-token-") {
				t.Errorf("after the window a read used %q", got)
			}
			if n := fallbackLines(h.stderr, "o/r"); n != 1 {
				t.Errorf("still logged %d times", n)
			}
		})
	}
}

func TestRateLimitsAreTrackedPerCredential(t *testing.T) {
	h := newAppHarness(t, true)
	reset := time.Now().Add(time.Hour)
	h.fake.remaining = func(auth string) (int, time.Time, bool) {
		if auth == "Bearer "+appTestUserToken {
			return 4000, reset, true
		}
		return 0, reset, true // the App installation is out of budget
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e := h.json(ctx, "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
		t.Fatal(e)
	}
	// The App ran dry: a write on the user token must not wait for it.
	start := time.Now()
	if e := h.json(ctx, "POST", "/repos/o/r/pulls/1/reviews", map[string]string{"event": "COMMENT"}, nil); e != nil {
		t.Fatal(e)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("a user-token write waited %s on the App budget", time.Since(start))
	}
	if got := h.RateRemaining(); got["app"] != 0 || got["user"] != 4000 {
		t.Fatalf("remaining %v, want app 0 and user 4000", got)
	}
	// A read for the dry App waits for the App's budget, not the user's.
	short, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	if e := h.json(short, "GET", "/repos/o/r/pulls/1", nil, nil); e == nil {
		t.Fatal("a read on the exhausted App budget was not held back")
	}

	// And the other way round: user budget gone, App reads go through.
	h = newAppHarness(t, true)
	h.fake.remaining = func(auth string) (int, time.Time, bool) {
		if auth == "Bearer "+appTestUserToken {
			return 0, reset, true
		}
		return 4900, reset, true
	}
	if e := h.json(ctx, "POST", "/repos/o/r/pulls/1/reviews", map[string]string{"event": "COMMENT"}, nil); e != nil {
		t.Fatal(e)
	}
	start = time.Now()
	if e := h.json(ctx, "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
		t.Fatal(e)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("an App read waited %s on the user budget", time.Since(start))
	}
	if got := h.RateRemaining(); got["user"] != 0 || got["app"] != 4900 {
		t.Fatalf("remaining %v, want user 0 and app 4900", got)
	}
}

func TestRequestCountsSplitByCredential(t *testing.T) {
	h := newAppHarness(t, true)
	ctx, counts := CountRequests(context.Background(), "test")
	if e := h.json(ctx, "GET", "/repos/o/r/pulls/1", nil, nil); e != nil {
		t.Fatal(e)
	}
	if e := h.json(ctx, "POST", "/repos/o/r/pulls/1/reviews", map[string]string{"event": "COMMENT"}, nil); e != nil {
		t.Fatal(e)
	}
	// lookup and token mint count as App requests, the GET too; the write is the user's.
	got := counts.ByCredential()
	if got["app"] != 3 || got["user"] != 1 {
		t.Fatalf("by credential %v, want app 3 (lookup, token, read) and user 1", got)
	}
}

func TestGraphQLDocumentKind(t *testing.T) {
	ctx := context.WithValue(context.Background(), graphqlRepoKey{}, "o/r")
	for _, tt := range []struct {
		name, body, want string
	}{
		{"query", `{"query":"query($n:Int!){repository(owner:\"o\",name:\"r\"){id}}"}`, "o/r"},
		{"anonymous query", `{"query":"{viewer{login}}"}`, "o/r"},
		{"mutation", `{"query":"mutation{addStar(input:{}){clientMutationId}}"}`, ""},
		{"mutation after a query", `{"query":"query A{viewer{login}} mutation B{x}"}`, ""},
		{"unparseable", `not json`, ""},
		{"empty", `{"query":""}`, ""},
	} {
		if got := readRepo(ctx, "POST", "/graphql", []byte(tt.body)); got != tt.want {
			t.Errorf("%s: repo %q, want %q", tt.name, got, tt.want)
		}
	}
	if got := readRepo(context.Background(), "POST", "/graphql", []byte(`{"query":"{viewer{login}}"}`)); got != "" {
		t.Errorf("a query without a repository must use the user token, got %q", got)
	}
	for _, m := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		if got := readRepo(ctx, m, "/repos/o/r/pulls/1/merge", nil); got != "" {
			t.Errorf("%s routed to the App for %q", m, got)
		}
	}
	for _, route := range []string{"/user", "/events", "/notifications"} {
		if got := readRepo(ctx, "GET", route, nil); got != "" {
			t.Errorf("GET %s has no repository but routed to %q", route, got)
		}
	}
}
