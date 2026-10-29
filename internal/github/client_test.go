package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/manifest"
)

var testSHA = strings.Repeat("a", 40)

func api(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	c := New("placeholder")
	c.BaseURL = server.URL
	c.UploadURL = server.URL
	c.HTTP = server.Client()
	return c
}
func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func TestCheckEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, runs, statuses string
		want                 CI
		fail                 bool
	}{
		{"success", `[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]`, `[]`, Green, false},
		{"failure", `[{"id":1,"name":"CI","status":"completed","conclusion":"failure"}]`, `[]`, Red, false},
		{"missing", `[]`, `[]`, Pending, false},
		{"cancelled", `[{"id":1,"name":"CI","status":"completed","conclusion":"cancelled"}]`, `[]`, Pending, false},
		{"skipped", `[{"id":1,"name":"CI","status":"completed","conclusion":"skipped"}]`, `[]`, Pending, false},
		{"pending", `[{"id":1,"name":"CI","status":"in_progress"}]`, `[]`, Pending, false},
		{"latest status", `[]`, `[{"id":2,"context":"CI","state":"success"},{"id":1,"context":"CI","state":"failure"}]`, Green, false},
		{"ambiguous", `[{"id":1,"name":"CI","status":"completed","conclusion":"success"}]`, `[{"id":2,"context":"CI","state":"failure"}]`, Pending, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer placeholder" {
					t.Error("missing auth")
				}
				if strings.HasSuffix(r.URL.Path, "/check-runs") {
					fmt.Fprint(w, `{"check_runs":`+tt.runs+`}`)
				} else if strings.HasSuffix(r.URL.Path, "/statuses") {
					fmt.Fprint(w, tt.statuses)
				} else {
					http.NotFound(w, r)
				}
			})
			got, e := c.Check(context.Background(), "example/project", testSHA, []string{"CI"})
			if (e != nil) != tt.fail || got != tt.want {
				t.Fatalf("%s %v", got, e)
			}
		})
	}
}
func TestMergeUsesSHAAndSquash(t *testing.T) {
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Fatal(r.Method)
		}
		var body map[string]any
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Fatal(e)
		}
		if body["sha"] != testSHA || body["merge_method"] != "squash" {
			t.Fatal(body)
		}
		reply(w, map[string]bool{"merged": true})
	})
	if e := c.Merge(context.Background(), "example/project", 7, testSHA); e != nil {
		t.Fatal(e)
	}
}
func TestReviewResolutionFailsClosed(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		want       int
		fail       bool
	}{
		{"resolved", `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":true}],"pageInfo":{"hasNextPage":false}}}}}}`, 0, false},
		{"unresolved", `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":false}],"pageInfo":{"hasNextPage":false}}}}}}`, 1, false},
		{"api errors", `{"errors":[{"message":"unavailable"}]}`, 0, true},
		{"missing repository", `{"data":{"repository":null}}`, 0, true},
		{"missing pr", `{"data":{"repository":{"pullRequest":null}}}`, 0, true},
		{"missing fields", `{}`, 0, true},
		{"missing state", `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{}],"pageInfo":{"hasNextPage":false}}}}}}`, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/reviews") {
					fmt.Fprint(w, `[]`)
				} else {
					fmt.Fprint(w, tt.body)
				}
			})
			got, e := c.Unresolved(context.Background(), "example/project", 7)
			if (e != nil) != tt.fail || got != tt.want {
				t.Fatalf("%d %v", got, e)
			}
		})
	}
}
func TestReviewPaginationAndChangesRequested(t *testing.T) {
	pages := 0
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/reviews") {
			fmt.Fprint(w, `[{"id":1,"state":"CHANGES_REQUESTED","user":{"login":"reviewer"}},{"id":2,"state":"COMMENTED","user":{"login":"reviewer"}}]`)
			return
		}
		pages++
		var request struct {
			Variables map[string]any `json:"variables"`
		}
		if e := json.NewDecoder(r.Body).Decode(&request); e != nil {
			t.Fatal(e)
		}
		if pages == 1 {
			if request.Variables["cursor"] != nil {
				t.Fatal(request)
			}
			fmt.Fprint(w, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"isResolved":false}],"pageInfo":{"hasNextPage":true,"endCursor":"next"}}}}}}`)
		} else {
			if request.Variables["cursor"] != "next" {
				t.Fatal(request)
			}
			fmt.Fprint(w, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}}}`)
		}
	})
	count, e := c.Unresolved(context.Background(), "example/project", 7)
	if e != nil || count != 2 || pages != 2 {
		t.Fatalf("%d %d %v", count, pages, e)
	}
}
func TestReviewedRequiresTrustedLoginAndExactHead(t *testing.T) {
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		reply(w, []map[string]any{{"body": Marker(testSHA) + "\ncomplete", "commit_id": testSHA, "state": "COMMENTED", "user": map[string]string{"login": "review-bot"}}})
	})
	for _, tt := range []struct {
		sha, login string
		want       bool
	}{{testSHA, "review-bot", true}, {testSHA, "someone-else", false}, {strings.Repeat("b", 40), "review-bot", false}} {
		got, e := c.Reviewed(context.Background(), "example/project", 7, tt.sha, tt.login)
		if e != nil || got != tt.want {
			t.Fatalf("%t %v", got, e)
		}
	}
}
func TestPublishDraftAndIdempotency(t *testing.T) {
	m, e := manifest.Build("example/project", testSHA, []config.Install{{Source: "tool", Target: "bin/tool"}}, func(string) ([]byte, error) { return []byte("payload"), nil })
	if e != nil {
		t.Fatal(e)
	}
	tagged, published := false, false
	uploads, mutations := 0, 0
	var asset []byte
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.Method != "GET" {
			mutations++
		}
		switch {
		case strings.Contains(p, "/git/ref/tags/"):
			if !tagged {
				http.NotFound(w, r)
			} else {
				reply(w, map[string]any{"object": Ref{SHA: testSHA}})
			}
		case strings.HasSuffix(p, "/git/refs"):
			tagged = true
			reply(w, Ref{SHA: testSHA})
		case strings.Contains(p, "/releases/tags/"):
			if !published {
				http.NotFound(w, r)
			} else {
				reply(w, Release{ID: 1, Tag: Tag(testSHA), Assets: []Asset{{ID: 2, Name: AssetName, Size: int64(len(asset))}}})
			}
		case strings.HasSuffix(p, "/releases") && r.Method == "POST":
			reply(w, Release{ID: 1, Tag: Tag(testSHA), Draft: true})
		case strings.HasSuffix(p, "/releases/1/assets"):
			uploads++
			var v manifest.Manifest
			if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
				t.Fatal(e)
			}
			asset, _ = json.MarshalIndent(v, "", "  ")
			reply(w, Asset{ID: 2})
		case strings.HasSuffix(p, "/releases/1") && r.Method == "PATCH":
			if uploads != 1 {
				t.Error("published without asset")
			}
			published = true
			reply(w, Release{ID: 1})
		case strings.HasSuffix(p, "/releases/assets/2"):
			_, _ = w.Write(asset)
		default:
			t.Error("unexpected route", r.Method, p)
			http.NotFound(w, r)
		}
	})
	if e = c.Publish(context.Background(), m); e != nil {
		t.Fatal(e)
	}
	first := mutations
	if e = c.Publish(context.Background(), m); e != nil {
		t.Fatal(e)
	}
	if uploads != 1 || mutations != first {
		t.Fatal("idempotent publish mutated release")
	}
}
func TestReadFileRejectsSymlink(t *testing.T) {
	for _, mode := range []string{"100644", "120000"} {
		t.Run(mode, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.Contains(r.URL.Path, "/commits/"):
					reply(w, map[string]any{"sha": testSHA, "commit": map[string]any{"tree": Ref{SHA: "tree"}}})
				case strings.Contains(r.URL.Path, "/git/trees/"):
					reply(w, map[string]any{"tree": []TreeEntry{{Path: "tool", Type: "blob", Mode: mode, SHA: "blob"}}})
				case strings.Contains(r.URL.Path, "/git/blobs/"):
					reply(w, map[string]any{"encoding": "base64", "size": 7, "content": base64.StdEncoding.EncodeToString([]byte("payload"))})
				default:
					http.NotFound(w, r)
				}
			})
			b, e := c.ReadFile(context.Background(), "example/project", testSHA, "tool")
			if mode == "120000" {
				if e == nil {
					t.Fatal("symlink accepted")
				}
			} else if e != nil || string(b) != "payload" {
				t.Fatalf("%q %v", b, e)
			}
		})
	}
}
func TestLatestRejectsWrongTagCommit(t *testing.T) {
	m, _ := manifest.Build("example/project", testSHA, []config.Install{{Source: "tool", Target: "tool"}}, func(string) ([]byte, error) { return []byte("x"), nil })
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/releases"):
			reply(w, []Release{{ID: 1, Tag: Tag(testSHA), Assets: []Asset{{ID: 2, Name: AssetName, Size: 1000}}}})
		case strings.Contains(r.URL.Path, "/assets/"):
			reply(w, m)
		case strings.Contains(r.URL.Path, "/commits/"):
			reply(w, Ref{SHA: strings.Repeat("b", 40)})
		default:
			http.NotFound(w, r)
		}
	})
	if _, e := c.Latest(context.Background(), "example/project"); e == nil {
		t.Fatal("tag mismatch accepted")
	}
}
func TestRevertOverlapAndCASLatch(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprint(overlap), func(t *testing.T) {
			latched := false
			prs, trees := 0, 0
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				p := r.URL.Path
				switch {
				case strings.Contains(p, "/commits/"):
					sha := p[strings.LastIndex(p, "/")+1:]
					parent := strings.Repeat("a", 40)
					tree := "before"
					if sha == strings.Repeat("b", 40) {
						tree = "after"
					}
					if sha == strings.Repeat("c", 40) {
						tree = "current"
					}
					reply(w, map[string]any{"sha": sha, "parents": []Ref{{SHA: parent}}, "commit": map[string]any{"tree": Ref{SHA: tree}}})
				case strings.HasSuffix(p, "/pulls/7/files"):
					fmt.Fprint(w, `[{"filename":"tool","status":"modified"}]`)
				case strings.HasSuffix(p, "/pulls/7"):
					reply(w, Pull{ChangedFiles: 1})
				case strings.Contains(p, "/git/trees/"):
					blob := "new"
					if strings.HasSuffix(p, "/before") {
						blob = "old"
					}
					if overlap && strings.HasSuffix(p, "/current") {
						blob = "later"
					}
					reply(w, map[string]any{"tree": []TreeEntry{{Path: "tool", Mode: "100755", Type: "blob", SHA: blob}}})
				case strings.HasSuffix(p, "/git/trees") && r.Method == "POST":
					trees++
					var v struct {
						Tree []TreeEntry `json:"tree"`
					}
					if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
						t.Fatal(e)
					}
					if len(v.Tree) != 1 || v.Tree[0].SHA != "old" {
						t.Fatal(v)
					}
					reply(w, Ref{SHA: "revert-tree"})
				case strings.HasSuffix(p, "/git/commits"):
					reply(w, Ref{SHA: strings.Repeat("d", 40)})
				case strings.HasSuffix(p, "/git/refs"):
					var v map[string]string
					if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
						t.Fatal(e)
					}
					if v["ref"] == "refs/tags/"+RecoveryTag {
						if latched {
							w.WriteHeader(422)
							return
						}
						latched = true
					} else if v["ref"] != "refs/heads/"+RecoveryBranch || !latched {
						t.Fatal("branch before latch", v)
					}
					reply(w, Ref{SHA: strings.Repeat("d", 40)})
				case strings.HasSuffix(p, "/pulls") && r.Method == "POST":
					prs++
					reply(w, map[string]string{"html_url": "https://github.com/example/project/pull/8"})
				default:
					t.Error("unexpected", r.Method, p)
					http.NotFound(w, r)
				}
			})
			_, e := c.Revert(context.Background(), "example/project", "main", strings.Repeat("c", 40), strings.Repeat("b", 40), 7)
			if overlap {
				if e == nil || trees != 0 || prs != 0 {
					t.Fatalf("overlap wrote: %d %d %v", trees, prs, e)
				}
			} else {
				if e != nil || prs != 1 {
					t.Fatal(e)
				}
				if _, e = c.Revert(context.Background(), "example/project", "main", strings.Repeat("c", 40), strings.Repeat("b", 40), 7); e == nil || prs != 1 {
					t.Fatal("second recovery escaped latch")
				}
			}
		})
	}
}

func TestRecoveryTagSurvivesBranchDeletion(t *testing.T) {
	for _, tt := range []struct {
		name         string
		tag, branch  bool
		status       int
		active, fail bool
	}{
		{name: "persistent tag with deleted branch", tag: true, active: true}, {name: "legacy branch alone", branch: true, active: true}, {name: "no recovery", active: false}, {name: "API refusal", status: 403, fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			branchReads := 0
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				if tt.status != 0 {
					w.WriteHeader(tt.status)
					return
				}
				if strings.Contains(r.URL.Path, "/git/ref/tags/") {
					if tt.tag {
						reply(w, map[string]any{"object": Ref{SHA: testSHA}})
					} else {
						http.NotFound(w, r)
					}
					return
				}
				branchReads++
				if tt.branch {
					reply(w, map[string]any{"object": Ref{SHA: testSHA}})
				} else {
					http.NotFound(w, r)
				}
			})
			active, e := c.RecoveryActive(context.Background(), "example/project")
			if (e != nil) != tt.fail || active != tt.active {
				t.Fatalf("%t %v", active, e)
			}
			if tt.tag && branchReads != 0 {
				t.Fatal("tag latch depended on branch")
			}
		})
	}
}
func TestMergedPullsFiltersBaseAndBoundsScan(t *testing.T) {
	recent := time.Now().UTC().Format(time.RFC3339)
	old := time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339)
	pages := 0
	c := api(t, func(w http.ResponseWriter, r *http.Request) {
		pages++
		if pages > 1 {
			t.Fatal("scan did not stop at the first old updated_at")
		}
		reply(w, []Pull{
			{Number: 1, State: "closed", UpdatedAt: recent, Merged: true, Base: Ref{Ref: "main"}},
			{Number: 2, State: "closed", UpdatedAt: recent, Base: Ref{Ref: "main"}},
			{Number: 3, State: "closed", UpdatedAt: recent, Merged: true, Base: Ref{Ref: "release"}},
			{Number: 4, State: "closed", UpdatedAt: old, Merged: true, Base: Ref{Ref: "main"}},
		})
	})
	pulls, e := c.MergedPulls(context.Background(), "example/project", "main", time.Now().Add(-time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if len(pulls) != 2 || pulls[0].Number != 1 || pulls[1].Number != 4 {
		t.Fatalf("%+v", pulls)
	}
}
