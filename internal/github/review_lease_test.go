package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestReviewLeaseAcquisition(t *testing.T) {
	for _, tt := range []struct {
		name            string
		present, legacy bool
		age             time.Duration
		status          int
		want            bool
		fail            bool
	}{
		{name: "first claim", want: true},
		{name: "live claim", present: true, age: time.Second},
		{name: "expired claim", present: true, age: time.Hour, want: true},
		{name: "legacy expired claim", present: true, legacy: true, age: time.Hour, want: true},
		{name: "legacy live claim", present: true, legacy: true, age: time.Second},
		{name: "reclaim race", present: true, age: time.Hour, status: 422},
		{name: "first claim race", status: 422},
		{name: "API error", present: true, age: time.Hour, status: 500, fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			writes := 0
			branch := "mergewake/reviews/7/" + testSHA
			old := strings.Repeat("b", 40)
			next := strings.Repeat("c", 40)
			started := time.Now().UTC().Add(-tt.age)
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "GET" && strings.Contains(r.URL.Path, "/git/ref/"):
					if !tt.present {
						http.NotFound(w, r)
						return
					}
					reply(w, map[string]any{"object": Ref{SHA: old}})
				case r.Method == "GET" && strings.Contains(r.URL.Path, "/commits/"):
					message := leasePrefix + started.Format(time.RFC3339Nano)
					if tt.legacy {
						message = "legacy commit"
					}
					reply(w, map[string]any{"sha": old, "commit": map[string]any{"tree": Ref{SHA: testSHA}, "message": message, "committer": map[string]any{"date": started}}})
				case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/git/commits"):
					var body struct {
						Message string
						Tree    string
						Parents []string
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					parent := testSHA
					if tt.present {
						parent = old
					}
					if len(body.Parents) != 1 || body.Parents[0] != parent || body.Tree != testSHA {
						t.Fatalf("%+v", body)
					}
					if _, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(body.Message, leasePrefix)); err != nil {
						t.Fatal(err)
					}
					reply(w, Ref{SHA: next})
				case r.Method == "PATCH" || r.Method == "POST":
					writes++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					if tt.present && (r.Method != "PATCH" || body["force"] != false) {
						t.Fatal("reclaim must fast-forward", body)
					}
					if body["sha"] != next {
						t.Fatal(body)
					}
					if tt.status != 0 {
						w.WriteHeader(tt.status)
						return
					}
					reply(w, map[string]any{})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			})
			leaseSHA, got, err := c.ClaimReviewWithSHA(context.Background(), "example/project", branch, testSHA, time.Minute)
			if got != tt.want || (err != nil) != tt.fail {
				t.Fatalf("got=%v err=%v", got, err)
			}
			if (tt.want && leaseSHA != next) || (!tt.want && leaseSHA != "") {
				t.Fatalf("lease SHA = %q", leaseSHA)
			}
			if tt.present && tt.age < time.Minute && writes != 0 {
				t.Fatal("live claim changed")
			}
		})
	}
}

func TestReviewLeaseUnknownTimestampFailsClosed(t *testing.T) {
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/git/ref/") {
			reply(w, map[string]any{"object": Ref{SHA: testSHA}})
			return
		}
		fmt.Fprint(w, `{"commit":{"message":"mergewake review lease: invalid"}}`)
	})
	if _, _, err := c.ReviewLease(context.Background(), "example/project", "claim", time.Minute); err == nil {
		t.Fatal("unknown lease accepted")
	}
}

func TestRenewReviewLeasePreservesOwnership(t *testing.T) {
	old, next := strings.Repeat("b", 40), strings.Repeat("c", 40)
	writes := 0
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/git/ref/"):
			reply(w, map[string]any{"object": Ref{SHA: old}})
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/commits/"):
			reply(w, map[string]any{"commit": map[string]any{"tree": Ref{SHA: testSHA}}})
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/git/commits"):
			var body struct {
				Parents []string
				Tree    string
				Message string
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Parents) != 1 || body.Parents[0] != old || body.Tree != testSHA || !strings.HasPrefix(body.Message, leasePrefix) {
				t.Fatalf("commit=%+v", body)
			}
			reply(w, Ref{SHA: next})
		case r.Method == "PATCH":
			writes++
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["sha"] != next || body["force"] != false {
				t.Fatalf("patch=%v", body)
			}
			reply(w, map[string]any{})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	})
	if sha, ok, err := c.RenewReviewLease(context.Background(), "example/project", "claim", "wrong"); err != nil || ok || sha != "" || writes != 0 {
		t.Fatalf("mismatch sha=%q ok=%v err=%v writes=%d", sha, ok, err, writes)
	}
	if sha, ok, err := c.RenewReviewLease(context.Background(), "example/project", "claim", old); err != nil || !ok || sha != next || writes != 1 {
		t.Fatalf("sha=%q ok=%v err=%v writes=%d", sha, ok, err, writes)
	}
}

// releaseServer is a ref that moves only by fast-forward, as GitHub's does for a
// PATCH without force.
type releaseServer struct {
	head    string            // the lease commit the branch points at
	parents map[string]string // created commit → its parent
	patches []string          // PATCH request URIs
	message string            // message of the last commit created
}

func (s *releaseServer) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/commits/"):
			if strings.HasSuffix(r.URL.Path, "/gone") {
				http.NotFound(w, r)
				return
			}
			reply(w, map[string]any{"commit": map[string]any{"tree": Ref{SHA: testSHA}}})
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/git/commits"):
			var body struct {
				Message string
				Parents []string
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			s.message = body.Message
			sha := strings.Repeat("d", 40)
			s.parents[sha] = body.Parents[0]
			reply(w, Ref{SHA: sha})
		case r.Method == "PATCH":
			s.patches = append(s.patches, r.RequestURI)
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			sha, _ := body["sha"].(string)
			if body["force"] != false || s.parents[sha] != s.head {
				w.WriteHeader(http.StatusUnprocessableEntity)
				reply(w, map[string]any{"message": "Update is not a fast forward"})
				return
			}
			s.head = sha
			reply(w, map[string]any{})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}
}

func TestReleaseReviewLeaseIsACompareAndSwap(t *testing.T) {
	own, other := strings.Repeat("b", 40), strings.Repeat("c", 40)
	for _, tt := range []struct {
		name     string
		head     string // where the branch points when the release lands
		expected string
		want     bool
	}{
		{name: "own lease", head: own, expected: own, want: true},
		{name: "renewed by another host", head: other, expected: own},
		{name: "lease commit gone", head: own, expected: "gone"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := &releaseServer{head: tt.head, parents: map[string]string{}}
			c := api(t, srv.handler(t))
			got, err := c.ReleaseReviewLease(context.Background(), "example/project", "mergewake/reviews/7/x", tt.expected)
			if err != nil || got != tt.want {
				t.Fatalf("released=%v err=%v", got, err)
			}
			if tt.want {
				if srv.head == tt.expected || srv.message != leasePrefix+"1970-01-01T00:00:00Z" {
					t.Fatalf("head=%s message=%q", srv.head, srv.message)
				}
			} else if srv.head != tt.head {
				t.Fatalf("a renewed or missing lease moved: head=%s", srv.head)
			}
		})
	}
}

// The lease branch has slashes: the ref is addressed by its plain path, never
// with %2F, which GitHub does not take as a separator.
func TestReleaseReviewLeaseUsesPlainRefPath(t *testing.T) {
	own := strings.Repeat("b", 40)
	branch := "mergewake/reviews/55/" + testSHA + "-p2"
	srv := &releaseServer{head: own, parents: map[string]string{}}
	c := api(t, srv.handler(t))
	got, err := c.ReleaseReviewLease(context.Background(), "example/project", branch, own)
	want := "/repos/example/project/git/refs/heads/" + branch
	if err != nil || !got || len(srv.patches) != 1 || srv.patches[0] != want {
		t.Fatalf("released=%v err=%v patches=%q want %q", got, err, srv.patches, want)
	}
}
