package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/mgh3326/mergewake/internal/policy"
)

func TestFeedbackActorTypesAndRechecks(t *testing.T) {
	for _, current := range []bool{false, true} {
		t.Run(fmt.Sprint(current), func(t *testing.T) {
			reviewHead := strings.Repeat("b", 40)
			if current {
				reviewHead = testSHA
			}
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Query string `json:"query"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				for _, field := range []string{"author{login __typename}", "resolvedBy{login __typename}", "headRefOid", "commit{oid}"} {
					if !strings.Contains(req.Query, field) {
						t.Errorf("missing GraphQL field %s", field)
					}
				}
				fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"author":{"login":"pr-author","__typename":"User"},"headRefOid":%q,"reviews":{"nodes":[{"author":{"login":"coderabbitai","__typename":"Bot"},"state":"COMMENTED","commit":{"oid":%q}}]},"reviewThreads":{"nodes":[{"id":"one","path":"tool","line":1,"isResolved":true,"isOutdated":true,"resolvedBy":{"login":"coderabbitai","__typename":"Bot"},"comments":{"nodes":[{"author":{"login":"coderabbitai","__typename":"Bot"},"body":"Critical bug"},{"author":{"login":"github-actions","__typename":"Bot"},"body":"ok"}],"pageInfo":{"hasNextPage":false}}}],"pageInfo":{"hasNextPage":false}}}}}}`, testSHA, reviewHead)
			})
			fb, err := c.ReviewFeedback(context.Background(), "example/project", 7)
			if err != nil {
				t.Fatal(err)
			}
			fs := fb.Findings((policy.ReviewGate{}).GateAuthors(), "review-bot")
			if len(fs) != 1 || fs[0].Answered || !fs[0].ReviewerResolved || fs[0].Rechecked != current {
				t.Fatalf("%+v", fs)
			}
			if fb.PRAuthor != "pr-author" || fb.ReviewAuthors["coderabbitai"] != map[bool]int{false: 0, true: 1}[current] {
				t.Fatalf("%+v", fb)
			}
		})
	}
}

func TestOnlyNonAuthorHumansAnswer(t *testing.T) {
	for _, tt := range []struct {
		name, login, kind string
		want              bool
	}{
		{"PR author", "pr-author", "User", false},
		{"GraphQL bot without suffix", "github-actions", "Bot", false},
		{"copilot without suffix", "copilot", "Bot", false},
		{"unknown actor", "maybe-human", "", false},
		{"ghost", "", "", false},
		{"human", "maintainer", "User", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fb := ReviewFeedback{PRAuthor: "pr-author", Threads: []ReviewThread{{Path: "tool", Comments: []ReviewComment{{Author: "coderabbitai", AuthorType: "Bot", Body: "Major problem"}, {Author: tt.login, AuthorType: tt.kind, Body: "ok"}}}}}
			fs := fb.Findings((policy.ReviewGate{}).GateAuthors(), "review-bot")
			if len(fs) != 1 || fs[0].Answered != tt.want {
				t.Fatalf("%+v", fs)
			}
		})
	}
}

func TestResolverMustBeReviewingBot(t *testing.T) {
	for _, who := range []string{"pr-author", "maintainer", "other-bot", "coderabbitai"} {
		t.Run(who, func(t *testing.T) {
			fb := ReviewFeedback{Threads: []ReviewThread{{IsResolved: true, ResolvedBy: who, ResolvedByType: "Bot", Comments: []ReviewComment{{Author: "coderabbitai", Body: "Critical"}}}}}
			f := fb.Findings((policy.ReviewGate{}).GateAuthors(), "review-bot")[0]
			v := (policy.ReviewGate{}).Evaluate([]policy.ReviewFinding{f}, "routine")
			if (len(v.Blocking) == 0) != (who == "coderabbitai") {
				t.Fatalf("resolver=%s verdict=%+v", who, v)
			}
		})
	}
}

func TestResolverTypeAndReservedIdentities(t *testing.T) {
	for _, tt := range []struct {
		name, kind, prAuthor, ownLogin string
		want                           bool
	}{
		{"reviewing Bot", "Bot", "pr-author", "review-bot", true},
		{"User in gate bot list", "User", "pr-author", "review-bot", false},
		{"PR author even if Bot", "Bot", "coderabbitai", "review-bot", false},
		{"review login even if Bot", "Bot", "pr-author", "coderabbitai", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fb := ReviewFeedback{PRAuthor: tt.prAuthor, Threads: []ReviewThread{{IsResolved: true, ResolvedBy: "coderabbitai", ResolvedByType: tt.kind, Comments: []ReviewComment{{Author: "coderabbitai", Body: "Critical finding", CommitSHA: testSHA}}}}}
			f := fb.Findings((policy.ReviewGate{}).GateAuthors(), tt.ownLogin)[0]
			v := (policy.ReviewGate{}).Evaluate([]policy.ReviewFinding{f}, "routine")
			if f.ReviewerResolved != tt.want || (len(v.Blocking) == 0) != tt.want {
				t.Fatalf("%+v %+v", f, v)
			}
		})
	}
}

func TestThreadCommitAndReviewHeadBinding(t *testing.T) {
	old := strings.Repeat("b", 40)
	for _, current := range []bool{false, true} {
		t.Run(fmt.Sprint(current), func(t *testing.T) {
			head := old
			if current {
				head = testSHA
			}
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				var req struct{ Query string }
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(req.Query, "body commit{oid} originalCommit{oid}") {
					t.Fatal("thread commit not queried")
				}
				fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"headRefOid":%q,"reviews":{"nodes":[{"author":{"login":"coderabbitai","__typename":"Bot"},"state":"COMMENTED","commit":{"oid":%q}}]},"reviewThreads":{"nodes":[{"comments":{"nodes":[{"author":{"login":"coderabbitai","__typename":"Bot"},"body":"Minor","commit":{"oid":%q}}]}}],"pageInfo":{"hasNextPage":false}}}}}}`, testSHA, head, head)
			})
			fb, err := c.ReviewFeedback(context.Background(), "example/project", 7)
			if err != nil {
				t.Fatal(err)
			}
			fs := fb.Findings((policy.ReviewGate{}).GateAuthors(), "review-bot")
			if fs[0].CommitSHA != head {
				t.Fatalf("%+v", fs)
			}
			gate := policy.ReviewGate{RequiredBots: []string{"coderabbitai"}}
			if missing := gate.MissingRequiredBots(fs, fb.ReviewAuthors, testSHA); (len(missing) == 0) != current {
				t.Fatalf("current=%v missing=%v reviews=%v", current, missing, fb.ReviewAuthors)
			}
		})
	}
}

// A reviewer's latest decisive review decides; a later approval or dismissal
// withdraws an earlier request for changes, and COMMENTED reviews never do.
func TestReviewFeedbackChangesRequested(t *testing.T) {
	review := func(login, state string) string {
		return fmt.Sprintf(`{"author":{"login":%q,"__typename":"User"},"state":%q}`, login, state)
	}
	for _, tt := range []struct {
		name, reviews, decision string
		want                    int
	}{
		{"none", review("a", "COMMENTED"), "", 0},
		{"one reviewer", review("a", "CHANGES_REQUESTED"), "", 1},
		{"commented after does not withdraw", review("a", "CHANGES_REQUESTED") + "," + review("a", "COMMENTED"), "", 1},
		{"approved after withdraws", review("a", "CHANGES_REQUESTED") + "," + review("a", "APPROVED"), "", 0},
		{"two reviewers", review("a", "CHANGES_REQUESTED") + "," + review("b", "CHANGES_REQUESTED"), "", 2},
		{"decision alone counts once", review("a", "COMMENTED"), "CHANGES_REQUESTED", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := api(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"data":{"repository":{"pullRequest":{"headRefOid":%q,"reviewDecision":%q,"reviews":{"nodes":[%s]},"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}}}`, testSHA, tt.decision, tt.reviews)
			})
			fb, err := c.ReviewFeedback(context.Background(), "example/project", 7)
			if err != nil || fb.ChangesRequested != tt.want {
				t.Fatalf("err=%v changes_requested=%d", err, fb.ChangesRequested)
			}
		})
	}
}
