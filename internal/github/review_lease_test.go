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
