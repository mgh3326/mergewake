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
			fs := fb.Findings((policy.ReviewGate{}).GateAuthors())
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
			fs := fb.Findings((policy.ReviewGate{}).GateAuthors())
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
			f := fb.Findings((policy.ReviewGate{}).GateAuthors())[0]
			v := (policy.ReviewGate{}).Evaluate([]policy.ReviewFinding{f}, "routine")
			if (len(v.Blocking) == 0) != (who == "coderabbitai") {
				t.Fatalf("resolver=%s verdict=%+v", who, v)
			}
		})
	}
}
