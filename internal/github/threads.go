package github

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/mgh3326/mergewake/internal/policy"
)

type reviewActor struct {
	Login string `json:"login"`
	Type  string `json:"__typename"`
}

type ReviewComment struct {
	AuthorType string `json:"author_type"`
	Author     string `json:"author"`
	Body       string `json:"body"`
	CommitSHA  string `json:"commit_sha"`
	// OriginalCommitSHA is the head the comment was posted on. CommitSHA
	// follows the PR head while the commented line is unchanged, so it cannot
	// tell a finding posted on this head from an older one that still applies.
	OriginalCommitSHA string `json:"original_commit_sha,omitempty"`
}
type ReviewThread struct {
	ID             string          `json:"id"`
	Path           string          `json:"path"`
	Line           int             `json:"line"`
	IsResolved     bool            `json:"resolved"`
	IsOutdated     bool            `json:"outdated"`
	ResolvedBy     string          `json:"resolved_by"`
	ResolvedByType string          `json:"resolved_by_type"`
	Comments       []ReviewComment `json:"comments"`
}

// ReviewFeedback carries raw thread state plus who submitted reviews, so the
// land gate can grade current-head findings and check explicitly required bots.
type ReviewFeedback struct {
	HeadSHA          string          `json:"head_sha"`
	Threads          []ReviewThread  `json:"threads"`
	ReviewAuthors    map[string]int  `json:"review_authors"`
	RecheckedAuthors map[string]bool `json:"rechecked_authors"`
	PRAuthor         string          `json:"pr_author"`
	// ChangesRequested counts reviewers whose latest decisive review requests
	// changes, or one when GitHub's overall decision does. The graded land gate
	// treats each as a blocking finding, as the raw thread count does.
	ChangesRequested int `json:"changes_requested"`
}

func (c *Client) ReviewFeedback(ctx context.Context, repo string, n int) (ReviewFeedback, error) {
	var fb ReviewFeedback
	fb.ReviewAuthors = map[string]int{}
	fb.RecheckedAuthors = map[string]bool{}
	parts := strings.Split(repo, "/")
	cursor := (*string)(nil)
	decisive := map[string]string{}
	decision := false
	for page := 0; page < 100; page++ {
		var result struct {
			Errors []json.RawMessage `json:"errors"`
			Data   *struct {
				Repository *struct {
					PullRequest *struct {
						Author         *reviewActor `json:"author"`
						HeadRefOid     string       `json:"headRefOid"`
						ReviewDecision string       `json:"reviewDecision"`
						Reviews        *struct {
							Nodes []struct {
								Author *reviewActor `json:"author"`
								State  string       `json:"state"`
								Commit *struct {
									OID string `json:"oid"`
								} `json:"commit"`
							} `json:"nodes"`
						} `json:"reviews"`
						ReviewThreads *struct {
							Nodes []struct {
								ID         string       `json:"id"`
								Path       string       `json:"path"`
								Line       int          `json:"line"`
								IsResolved bool         `json:"isResolved"`
								IsOutdated bool         `json:"isOutdated"`
								ResolvedBy *reviewActor `json:"resolvedBy"`
								Comments   *struct {
									Nodes []struct {
										Author *reviewActor `json:"author"`
										Body   string       `json:"body"`
										Commit *struct {
											OID string `json:"oid"`
										} `json:"commit"`
										OriginalCommit *struct {
											OID string `json:"oid"`
										} `json:"originalCommit"`
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
		query := `query($owner:String!,$name:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$name){pullRequest(number:$number){author{login __typename} headRefOid reviewDecision reviews(last:100){nodes{author{login __typename} state commit{oid}}} reviewThreads(first:100,after:$cursor){nodes{id path line isResolved isOutdated resolvedBy{login __typename} comments(first:50){nodes{author{login __typename} body commit{oid} originalCommit{oid}} pageInfo{hasNextPage}}} pageInfo{hasNextPage endCursor}}}}}`
		if e := c.json(ctx, "POST", "/graphql", map[string]any{"query": query, "variables": map[string]any{"owner": parts[0], "name": parts[1], "number": n, "cursor": cursor}}, &result); e != nil {
			return fb, e
		}
		if len(result.Errors) > 0 || result.Data == nil || result.Data.Repository == nil || result.Data.Repository.PullRequest == nil {
			return fb, errors.New("GitHub review thread query failed")
		}
		pr := result.Data.Repository.PullRequest
		if pr.HeadRefOid == "" || (fb.HeadSHA != "" && fb.HeadSHA != pr.HeadRefOid) {
			return fb, errors.New("missing or changed PR head in review feedback")
		}
		fb.HeadSHA = pr.HeadRefOid
		if page == 0 && pr.ReviewDecision == "CHANGES_REQUESTED" {
			decision = true
		}
		if pr.Author != nil {
			fb.PRAuthor = pr.Author.Login
		}
		if pr.ReviewThreads == nil || pr.ReviewThreads.PageInfo == nil {
			return fb, errors.New("GitHub returned incomplete review threads")
		}
		if page == 0 && pr.Reviews != nil {
			for _, r := range pr.Reviews.Nodes {
				if r.Author != nil {
					switch r.State {
					case "APPROVED", "CHANGES_REQUESTED", "DISMISSED":
						decisive[strings.ToLower(r.Author.Login)] = r.State
					}
					if r.State == "COMMENTED" || r.State == "APPROVED" || r.State == "CHANGES_REQUESTED" {
						author := policy.NormalizeAuthor(r.Author.Login)
						if r.Commit != nil && r.Commit.OID != "" && r.Commit.OID == pr.HeadRefOid {
							fb.ReviewAuthors[author]++
							fb.RecheckedAuthors[author] = true
						}
					}
				}
			}
		}
		for _, t := range pr.ReviewThreads.Nodes {
			thread := ReviewThread{ID: t.ID, Path: t.Path, Line: t.Line, IsResolved: t.IsResolved, IsOutdated: t.IsOutdated}
			if t.ResolvedBy != nil {
				thread.ResolvedBy = t.ResolvedBy.Login
				thread.ResolvedByType = t.ResolvedBy.Type
			}
			if t.Comments == nil || len(t.Comments.Nodes) == 0 {
				return fb, errors.New("review thread is missing its finding comment")
			}
			// Comments beyond the first page stay unseen; the thread then counts
			// as unanswered, which fails closed rather than inventing a reply.
			for _, cm := range t.Comments.Nodes {
				c := ReviewComment{Body: cm.Body}
				if cm.Commit != nil {
					c.CommitSHA = cm.Commit.OID
				}
				if cm.OriginalCommit != nil {
					c.OriginalCommitSHA = cm.OriginalCommit.OID
				}
				if cm.Author != nil {
					c.Author = cm.Author.Login
					c.AuthorType = cm.Author.Type
				}
				thread.Comments = append(thread.Comments, c)
			}
			fb.Threads = append(fb.Threads, thread)
		}
		if !pr.ReviewThreads.PageInfo.HasNextPage {
			for _, state := range decisive {
				if state == "CHANGES_REQUESTED" {
					fb.ChangesRequested++
				}
			}
			if fb.ChangesRequested == 0 && decision {
				fb.ChangesRequested = 1
			}
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
// first comment. Unknown severities stay empty and require triage on blocking paths.
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
// whose replies never count as human answers. GraphQL actor types and the PR
// author are checked independently of login suffixes.
func (f ReviewFeedback) Findings(bots map[string]bool, ownLogin string) []policy.ReviewFinding {
	ownLogin = policy.NormalizeAuthor(ownLogin)
	out := []policy.ReviewFinding{}
	for _, t := range f.Threads {
		if len(t.Comments) == 0 {
			continue
		}
		finding := policy.ReviewFinding{
			Author:    policy.NormalizeAuthor(t.Comments[0].Author),
			CommitSHA: t.Comments[0].CommitSHA,
			Severity:  ThreadSeverity(t.Comments[0].Author, t.Comments[0].Body),
			Path:      t.Path,
			Line:      t.Line,
			Resolved:  t.IsResolved,
			Outdated:  t.IsOutdated,
		}
		finding.OwnReview = ownLogin != "" && finding.Author == ownLogin
		finding.Rechecked = !finding.OwnReview && f.RecheckedAuthors[finding.Author]
		finding.ReviewerResolved = finding.Author != "" && t.ResolvedByType == "Bot" && policy.NormalizeAuthor(t.ResolvedBy) == finding.Author && finding.Author != policy.NormalizeAuthor(f.PRAuthor) && finding.Author != ownLogin
		for _, c := range t.Comments[1:] {
			author := policy.NormalizeAuthor(c.Author)
			if f.PRAuthor != "" && c.AuthorType == "User" && author != policy.NormalizeAuthor(f.PRAuthor) && author != "" && author != finding.Author && !bots[author] && !strings.HasSuffix(c.Author, "[bot]") {
				finding.Answered = true
				break
			}
		}
		out = append(out, finding)
	}
	return out
}
