package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Reads go out with a GitHub App installation token, writes with the user
// token. The App side is: sign a short-lived App JWT, resolve the installation
// of one repository, exchange it for an installation token, and keep that token
// until shortly before it expires. A repository the App cannot serve (no
// installation, or a 401/403 on a read) uses the user token for a while.

const (
	credUser = "user"
	credApp  = "app"

	// appTokenRefreshMargin refreshes an installation token this long before
	// GitHub expires it, so a request never starts with a token about to lapse.
	appTokenRefreshMargin = 5 * time.Minute
	// appFallbackTTL is how long a repository whose App access was refused
	// reads with the user token before the App is tried again.
	appFallbackTTL = 10 * time.Minute
	// appTransientTTL is the same for a lookup that failed for a reason that
	// may pass on its own (network, 5xx).
	appTransientTTL = time.Minute
)

// credential is the token one request carries. id names its rate-limit budget:
// "user", or "app:<installation id>".
type credential struct {
	id, kind, token, repo string
	installation          int64
}

func userCredential(token string) credential {
	return credential{id: credUser, kind: credUser, token: token}
}

type installToken struct {
	token   string
	expires time.Time
}

type appAuth struct {
	id  int64
	key *rsa.PrivateKey

	mu        sync.Mutex
	installs  map[string]int64
	tokens    map[int64]installToken
	fallback  map[string]time.Time
	announced map[string]bool
	// permissions records the installations whose permission set was logged.
	permissions map[int64]bool
	// fallbackUntil is the latest end of any repository's fallback window.
	fallbackUntil atomic.Int64

	// pause waits between delivery-log pages; nil means a real timer. maxPages
	// bounds the walk; zero means appDeliveriesMaxPages. Tests set both.
	pause    func(context.Context, time.Duration) error
	maxPages int
}

// EnableApp makes the client read through installation tokens of the App with
// this id, signed by key. Without it every request uses Token.
func (c *Client) EnableApp(appID int64, key *rsa.PrivateKey) {
	c.app = &appAuth{id: appID, key: key, installs: map[string]int64{}, tokens: map[int64]installToken{}, fallback: map[string]time.Time{}, announced: map[string]bool{}, permissions: map[int64]bool{}}
}

var graphqlMutation = regexp.MustCompile(`\b(mutation|subscription)\b`)

// graphqlRepoKey carries the repository a GraphQL request is about; the
// /graphql route itself names none.
type graphqlRepoKey struct{}

// readRepo returns the repository whose installation token should serve this
// request, or "" when the request must use the user token: any write, any
// GraphQL document that is not provably a query, and any route that names no
// repository.
func readRepo(ctx context.Context, method, route string, data []byte) string {
	switch {
	case method == http.MethodGet:
		return memoRepo(strings.SplitN(route, "?", 2)[0])
	case method == http.MethodPost && route == "/graphql":
		var doc struct {
			Query string `json:"query"`
		}
		if json.Unmarshal(data, &doc) != nil || doc.Query == "" || graphqlMutation.MatchString(doc.Query) {
			return ""
		}
		repo, _ := ctx.Value(graphqlRepoKey{}).(string)
		return repo
	}
	return ""
}

// credentialFor picks the credential for a request.
func (c *Client) credentialFor(ctx context.Context, method, route string, data []byte) credential {
	if c.app != nil {
		if repo := readRepo(ctx, method, route, data); repo != "" {
			if cred, ok := c.app.credential(ctx, c, repo); ok {
				return cred
			}
		}
	}
	return userCredential(c.Token)
}

func (a *appAuth) credential(ctx context.Context, c *Client, repo string) (credential, bool) {
	// One lock covers lookup and refresh: concurrent callers wait for the
	// first and then read its token instead of each fetching their own.
	a.mu.Lock()
	defer a.mu.Unlock()
	now := c.now()
	if until, ok := a.fallback[repo]; ok {
		if now.Before(until) {
			return credential{}, false
		}
		delete(a.fallback, repo)
	}
	id, ok := a.installs[repo]
	if !ok {
		var perms map[string]string
		var code int
		var e error
		id, perms, code, e = a.lookup(ctx, c, repo)
		if e != nil {
			ttl, reason := appTransientTTL, "installation lookup failed"
			if code == http.StatusNotFound {
				ttl, reason = appFallbackTTL, "no installation of the App"
			}
			a.deny(c, repo, now, ttl, reason)
			return credential{}, false
		}
		a.installs[repo] = id
		a.announcePermissions(c, id, perms)
	}
	t := a.tokens[id]
	if t.token == "" || !now.Before(t.expires.Add(-appTokenRefreshMargin)) {
		var perms map[string]string
		var e error
		t, perms, e = a.mint(ctx, c, id)
		if e != nil {
			a.deny(c, repo, now, appTransientTTL, "installation token request failed")
			return credential{}, false
		}
		a.tokens[id] = t
		a.announcePermissions(c, id, perms)
	}
	return credential{id: credApp + ":" + strconv.FormatInt(id, 10), kind: credApp, token: t.token, repo: repo, installation: id}, true
}

// AppToken returns the App installation token that serves reads of repo, or
// false when the App cannot serve it. It never returns the user token: a
// caller that needs App-only access (a mirror fetch) must not fetch at all.
func (c *Client) AppToken(ctx context.Context, repo string) (string, bool) {
	if c.app == nil {
		return "", false
	}
	cred, ok := c.app.credential(ctx, c, repo)
	if !ok || cred.kind != credApp || cred.token == "" {
		return "", false
	}
	return cred.token, true
}

// fallingBack reports whether any repository currently reads with the user
// token because the App could not serve it. It never waits on a token fetch.
func (a *appAuth) fallingBack(now time.Time) bool {
	return now.UnixNano() < a.fallbackUntil.Load()
}

// refuse records that an installation token was refused on a read (401: the
// token is no longer valid, 403: a permission the App lacks): the repository
// reads with the user token for appFallbackTTL.
func (a *appAuth) refuse(c *Client, cred credential, code int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if code == http.StatusUnauthorized {
		delete(a.tokens, cred.installation)
	}
	a.deny(c, cred.repo, c.now(), appFallbackTTL, fmt.Sprintf("App request refused (HTTP %d)", code))
}

// deny sends repo to the user token until now+ttl and says so once per
// repository per process. The caller holds a.mu.
func (a *appAuth) deny(c *Client, repo string, now time.Time, ttl time.Duration, reason string) {
	a.fallback[repo] = now.Add(ttl)
	if end := now.Add(ttl).UnixNano(); end > a.fallbackUntil.Load() {
		a.fallbackUntil.Store(end)
	}
	if a.announced[repo] {
		return
	}
	a.announced[repo] = true
	out := c.Stderr
	if out == nil {
		out = os.Stderr
	}
	fmt.Fprintf(out, "mergewake: GitHub App not used for %s: %s; reads use the user token\n", repo, reason)
}

// call sends one request authenticated as the App itself (a JWT). These
// calls never carry the user token and sit outside the request gate: they are
// few, and the App has its own rate limit.
func (a *appAuth) call(ctx context.Context, c *Client, method, route string, out any) (int, error) {
	code, _, e := a.send(ctx, c, method, c.BaseURL+route, route, out)
	return code, e
}

// send is call for a full URL, and also returns the response headers (the
// delivery log pages by Link header). route names the request in counts and
// errors, so a cursor in target never reaches them.
func (a *appAuth) send(ctx context.Context, c *Client, method, target, route string, out any) (int, http.Header, error) {
	jwt, e := signAppJWT(a.id, a.key, c.now())
	if e != nil {
		return 0, nil, e
	}
	req, e := http.NewRequestWithContext(ctx, method, target, nil)
	if e != nil {
		return 0, nil, e
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	req.Header.Set("User-Agent", "mergewake")
	req.Header.Set("Authorization", "Bearer "+jwt)
	requestStarted := time.Now()
	resp, e := c.HTTP.Do(req)
	requestTime := time.Since(requestStarted)
	if e != nil {
		recordRequest(ctx, method, route, 0, credApp, requestTime)
		if cause := context.Cause(ctx); cause != nil {
			e = cause
		}
		return 0, nil, fmt.Errorf("GitHub App %s request failed: %w", method, e)
	}
	defer resp.Body.Close()
	recordRequest(ctx, method, route, resp.StatusCode, credApp, requestTime)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, resp.Header, &APIError{Code: resp.StatusCode, Method: method, Path: route, Message: readAPIMessage(resp.Body)}
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if e != nil {
		return resp.StatusCode, resp.Header, e
	}
	return resp.StatusCode, resp.Header, json.Unmarshal(b, out)
}

func (a *appAuth) lookup(ctx context.Context, c *Client, repo string) (int64, map[string]string, int, error) {
	var out struct {
		ID          int64             `json:"id"`
		Permissions map[string]string `json:"permissions"`
	}
	code, e := a.call(ctx, c, http.MethodGet, base(repo)+"/installation", &out)
	if e == nil && out.ID <= 0 {
		e = errors.New("GitHub App installation has no id")
	}
	return out.ID, out.Permissions, code, e
}

func (a *appAuth) mint(ctx context.Context, c *Client, installation int64) (installToken, map[string]string, error) {
	var out struct {
		Token       string            `json:"token"`
		ExpiresAt   time.Time         `json:"expires_at"`
		Permissions map[string]string `json:"permissions"`
	}
	if _, e := a.call(ctx, c, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", installation), &out); e != nil {
		return installToken{}, nil, e
	}
	if out.Token == "" || out.ExpiresAt.IsZero() {
		return installToken{}, nil, errors.New("GitHub App token response is incomplete")
	}
	return installToken{out.Token, out.ExpiresAt}, out.Permissions, nil
}

// announcePermissions logs the permission names and levels GitHub reported
// for an installation, once per installation per process. The token is never
// part of the line. The caller holds a.mu.
func (a *appAuth) announcePermissions(c *Client, installation int64, perms map[string]string) {
	if len(perms) == 0 || a.permissions[installation] {
		return
	}
	a.permissions[installation] = true
	out := c.Stderr
	if out == nil {
		out = os.Stderr
	}
	fmt.Fprintf(out, "mergewake: app installation permissions: %s\n", permissionList(perms))
}

// permissionList renders a permissions object as "name=level" in name order.
func permissionList(perms map[string]string) string {
	names := make([]string, 0, len(perms))
	for name := range perms {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = name + "=" + perms[name]
	}
	return strings.Join(parts, ", ")
}

// signAppJWT builds the RS256 token GitHub expects from an App: issued a
// minute in the past to absorb clock skew, valid for nine minutes (the limit
// is ten).
func signAppJWT(appID int64, key *rsa.PrivateKey, now time.Time) (string, error) {
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"iss": strconv.FormatInt(appID, 10), "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix()})
	signing := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, e := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if e != nil {
		return "", errors.New("cannot sign GitHub App token")
	}
	return signing + "." + enc.EncodeToString(sig), nil
}
