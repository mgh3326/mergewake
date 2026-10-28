package github

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/mgh3326/mergewake/internal/policy"
)

type ReviewComment struct {
	Author string `json:"author"`
	Body   string `json:"body"`
}
type ReviewThread struct {
	ID         string          `json:"id"`
	Path       string          `json:"path"`
	Line       int             `json:"line"`
	IsResolved bool            `json:"resolved"`
	IsOutdated bool            `json:"outdated"`
	Comments   []ReviewComment `json:"comments"`
}

// ReviewFeedback carries raw thread state plus who submitted reviews, so the
// land gate can grade findings and record bot coverage gaps.
type ReviewFeedback struct {
	Threads       []ReviewThread `json:"threads"`
	ReviewAuthors map[string]int `json:"review_authors"`
}

func (c *Client) ReviewFeedback(ctx context.Context, repo string, n int) (ReviewFeedback, error) {
	var fb ReviewFeedback
	fb.ReviewAuthors = map[string]int{}
	parts := strings.Split(repo, "/")
	cursor := (*string)(nil)
	for page := 0; page < 100; page++ {
		var result struct {
			Errors []json.RawMessage `json:"errors"`
			Data   *struct {
				Repository *struct {
					PullRequest *struct {
						Reviews *struct {
							Nodes []struct {
								Author *struct {
									Login string `json:"login"`
								} `json:"author"`
							} `json:"nodes"`
						} `json:"reviews"`
						ReviewThreads *struct {
							Nodes []struct {
								ID         string `json:"id"`
								Path       string `json:"path"`
								Line       int    `json:"line"`
								IsResolved bool   `json:"isResolved"`
								IsOutdated bool   `json:"isOutdated"`
								Comments   *struct {
									Nodes []struct {
										Author *struct {
											Login string `json:"login"`
										} `json:"author"`
										Body string `json:"body"`
									} `json:"nodes"`
									PageInfo *struct {
										HasNextPage bool `json:"hasNextPage"`
									} `json:"pageInfo"`
								} `json:"comments"`
							} `json:"nodes"`
							PageInfo *struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
						} `json:"reviewThreads"`
					} `json:"pullRequest"`
				} `json:"repository"`
			} `json:"data"`
		}
		query := `query($owner:String!,$name:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$name){pullRequest(number:$number){reviews(last:100){nodes{author{login}}} reviewThreads(first:100,after:$cursor){nodes{id path line isResolved isOutdated comments(first:50){nodes{author{login} body} pageInfo{hasNextPage}}} pageInfo{hasNextPage endCursor}}}}}`
		if e := c.json(ctx, "POST", "/graphql", map[string]any{"query": query, "variables": map[string]any{"owner": parts[0], "name": parts[1], "number": n, "cursor": cursor}}, &result); e != nil {
			return fb, e
		}
		if len(result.Errors) > 0 || result.Data == nil || result.Data.Repository == nil || result.Data.Repository.PullRequest == nil {
			return fb, errors.New("GitHub review thread query failed")
		}
		pr := result.Data.Repository.PullRequest
		if pr.ReviewThreads == nil || pr.ReviewThreads.PageInfo == nil {
			return fb, errors.New("GitHub returned incomplete review threads")
		}
		if page == 0 && pr.Reviews != nil {
			for _, r := range pr.Reviews.Nodes {
				if r.Author != nil {
					fb.ReviewAuthors[policy.NormalizeAuthor(r.Author.Login)]++
				}
			}
		}
		for _, t := range pr.ReviewThreads.Nodes {
			thread := ReviewThread{ID: t.ID, Path: t.Path, Line: t.Line, IsResolved: t.IsResolved, IsOutdated: t.IsOutdated}
			if t.Comments == nil || len(t.Comments.Nodes) == 0 {
				return fb, errors.New("review thread is missing its finding comment")
			}
			// Comments beyond the first page stay unseen; the thread then counts
			// as unanswered, which fails closed rather than inventing a reply.
			for _, cm := range t.Comments.Nodes {
				c := ReviewComment{Body: cm.Body}
				if cm.Author != nil {
					c.Author = cm.Author.Login
				}
				thread.Comments = append(thread.Comments, c)
			}
			fb.Threads = append(fb.Threads, thread)
		}
		if !pr.ReviewThreads.PageInfo.HasNextPage {
			return fb, nil
		}
		next := pr.ReviewThreads.PageInfo.EndCursor
		if next == "" {
			return fb, errors.New("missing review thread cursor")
		}
		cursor = &next
	}
	return fb, errors.New("review thread pagination limit")
}

var severityMarker = regexp.MustCompile(`<!-- mergewake-severity:([a-z]+) -->`)
var codeRabbitSeverity = regexp.MustCompile(`(?i)\b(critical|major|minor|nitpick)\b`)
var codexSeverity = regexp.MustCompile(`(?i)\bP([123])\b`)

// ThreadSeverity reads the machine or prose severity label from a finding's
// first comment. Unknown authors yield an empty severity, which no gate rule
// matches.
func ThreadSeverity(author, body string) string {
	if len(body) > 400 {
		body = body[:400]
	}
	if m := severityMarker.FindStringSubmatch(body); len(m) == 2 {
		return m[1]
	}
	switch policy.NormalizeAuthor(author) {
	case "coderabbitai":
		if m := codeRabbitSeverity.FindStringSubmatch(body); len(m) == 2 {
			return strings.ToLower(m[1])
		}
	case "chatgpt-codex-connector":
		if m := codexSeverity.FindStringSubmatch(body); len(m) == 2 {
			return "p" + strings.ToLower(m[1])
		}
	}
	return ""
}

// Findings grades raw threads into gate findings. bots are normalized logins
// whose replies never count as a human answer; a raw login ending in [bot] or
// the thread's own author is also treated as a bot.
func (f ReviewFeedback) Findings(bots map[string]bool) []policy.ReviewFinding {
	out := []policy.ReviewFinding{}
	for _, t := range f.Threads {
		if len(t.Comments) == 0 {
			continue
		}
		finding := policy.ReviewFinding{
			Author:   policy.NormalizeAuthor(t.Comments[0].Author),
			Severity: ThreadSeverity(t.Comments[0].Author, t.Comments[0].Body),
			Path:     t.Path,
			Line:     t.Line,
			Resolved: t.IsResolved,
			Outdated: t.IsOutdated,
		}
		for _, c := range t.Comments[1:] {
			author := policy.NormalizeAuthor(c.Author)
			if author != "" && author != finding.Author && !bots[author] && !strings.HasSuffix(c.Author, "[bot]") {
				finding.Answered = true
				break
			}
		}
		out = append(out, finding)
	}
	return out
}
