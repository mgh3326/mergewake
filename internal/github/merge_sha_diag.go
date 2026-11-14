package github

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"os"
	"slices"
	"strconv"
	"sync"
)

// Why a merged PR decodes without merge_commit_sha (hk 4666). A pull the client
// decodes as merged with an empty merge_commit_sha is logged once per
// repository and number in this client, with where its body came from, the
// response status and the top-level keys of the raw object. Keys only: no
// value, header or token is ever logged. It names the cause; it changes no
// answer.
//
// Only a body send hands back reaches a pull decode. The other ways a read can
// answer carry no body and so no keys: a pass-memo hit returns the value it
// kept, and stored webhook state serves open PRs only (storedPull), never a
// merged one. A pull route is not a polling endpoint, so no poll window ever
// replays it. The code that reads a pull is therefore the live response and the
// ETag replay of it.

// sourceNotModified names a body served from the cache after GitHub answered a
// conditional read 304.
const sourceNotModified = "http 304 replay"

// decodedBody is where the last body one read returned came from. A read that
// serves no body (a pass-memo hit) leaves it empty.
type decodedBody struct {
	mu                 sync.Mutex
	source, credential string
	status             int
	body               []byte
}

type decodedBodyKey struct{}

// withDecodedBody returns a ctx whose reads report the provenance of the body
// they return to the returned record.
func withDecodedBody(ctx context.Context) (context.Context, *decodedBody) {
	b := &decodedBody{}
	return context.WithValue(ctx, decodedBodyKey{}, b), b
}

// noteDecodedBody records where the body a read returns came from. send calls
// it for every body it hands back, so on a retry the last body wins.
func noteDecodedBody(ctx context.Context, body []byte, status int, source, credential string) {
	b, _ := ctx.Value(decodedBodyKey{}).(*decodedBody)
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.source, b.status, b.credential, b.body = source, status, credential, body
}

// snapshot is the recorded provenance, empty when the read served no body.
func (b *decodedBody) snapshot() (source string, status int, credential string, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.source, b.status, b.credential, b.body
}

// decodedFirst is the first body a pull read decoded, taken before any fresh
// re-read replaces the record. Its pull is that body's decode, before any
// adoption of re-read fields.
type decodedFirst struct {
	pull               Pull
	source, credential string
	status             int
	raw                []byte
}

// first takes the provenance of the body the record holds now, for a pull
// decoded from it.
func (b *decodedBody) first(p Pull) decodedFirst {
	source, status, credential, raw := b.snapshot()
	return decodedFirst{pull: p, source: source, status: status, credential: credential, raw: raw}
}

// Outcomes of the fresh re-read a merged pull without merge_commit_sha gets.
const (
	refetchNotTried     = "not_tried"
	refetchFilled       = "filled"
	refetchStillMissing = "still_missing"
)

// refetchOutcome says what the re-read did: none was tried, it supplied the
// sha, or the pull still lacks it.
func refetchOutcome(tried bool, p Pull) string {
	switch {
	case !tried:
		return refetchNotTried
	case p.MergeCommitSHA != "":
		return refetchFilled
	default:
		return refetchStillMissing
	}
}

// httpBodySource names a body a live response carried: "http 200".
func httpBodySource(status int) string {
	return "http " + strconv.Itoa(status)
}

// mergeSHAFacts is what a raw pull object says about merge_commit_sha.
type mergeSHAFacts struct {
	KeyPresent bool
	ValueNull  bool
	Keys       []string
}

// factsOf reads the sorted top-level keys of a raw object and whether
// merge_commit_sha is among them and null. It returns no keys for a body that
// is not an object.
func factsOf(raw []byte) mergeSHAFacts {
	f := mergeSHAFacts{Keys: []string{}}
	var obj map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &obj) != nil {
		return f
	}
	f.Keys = slices.Sorted(maps.Keys(obj))
	v, ok := obj["merge_commit_sha"]
	f.KeyPresent = ok
	f.ValueNull = ok && bytes.Equal(bytes.TrimSpace(v), []byte("null"))
	return f
}

// mergeSHAMissing is the log line. It carries no pull title, body or sha.
type mergeSHAMissing struct {
	Level      string   `json:"level"`
	Msg        string   `json:"msg"`
	Repo       string   `json:"repo"`
	PR         int      `json:"pr"`
	Route      string   `json:"route"`
	Credential string   `json:"credential"`
	Source     string   `json:"source"`
	Status     int      `json:"status"`
	KeyPresent bool     `json:"key_present"`
	ValueNull  bool     `json:"value_null"`
	Keys       []string `json:"keys"`
	Refetch    string   `json:"refetch"`
}

// mergeSHALog reports, once per repository and number, a merged decode without
// merge_commit_sha. The client is one process's decoder, so a daemon logs a PR
// once however often the pass reads it.
type mergeSHALog struct {
	mu   sync.Mutex
	seen map[string]bool
}

// noteMergeSHA logs a pull whose first decoded body was merged without
// merge_commit_sha, from that body (taken before any fresh re-read), and what
// the re-read did about it. It logs even when the re-read then supplies the
// sha. raw is the object to read keys from: the body itself for a single read,
// one element of a list body for MergedPulls. A decode that served no body (a
// pass-memo hit) is not logged: the read that produced the memoized value was
// logged, or was fine, when it happened.
func (c *Client) noteMergeSHA(repo string, n int, route string, first decodedFirst, refetch string) {
	if first.pull.MergeCommitSHA != "" || !(first.pull.Merged || first.pull.MergedAt != nil) {
		return
	}
	if first.source == "" {
		return
	}
	key := normalizeMemoRepo(repo) + "\x00" + strconv.Itoa(n)
	s := &c.mergeSHASeen
	s.mu.Lock()
	if s.seen == nil {
		s.seen = map[string]bool{}
	}
	if s.seen[key] {
		s.mu.Unlock()
		return
	}
	s.seen[key] = true
	s.mu.Unlock()
	f := factsOf(first.raw)
	out := c.Stderr
	if out == nil {
		out = os.Stderr
	}
	_ = json.NewEncoder(out).Encode(mergeSHAMissing{
		Level: "INFO", Msg: "merge_sha_missing", Repo: repo, PR: n, Route: route,
		Credential: first.credential, Source: first.source, Status: first.status,
		KeyPresent: f.KeyPresent, ValueNull: f.ValueNull, Keys: f.Keys,
		Refetch: refetch,
	})
}
