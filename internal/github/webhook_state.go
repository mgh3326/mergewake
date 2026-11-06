package github

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/mergewake/internal/github/webhookstate"
)

// Stored webhook state is read before the API (docs/webhook-state.md). The
// land-time reads run under FreshEvidence and mergeability, review threads and
// every repository without a Webhook store never use it.

func (c *Client) distrustWebhookState(repo string) {
	if c.Webhook != nil && repo != "" {
		c.Webhook.Distrust(repo)
	}
}

// serving reports whether a read under ctx may be answered from stored state.
func (c *Client) serving(ctx context.Context) bool {
	return c.Webhook != nil && c.serveWebhookState && !isFresh(ctx)
}

func pullToStored(p Pull) webhookstate.PR {
	u, _ := time.Parse(time.RFC3339, p.UpdatedAt)
	out := webhookstate.PR{Number: p.Number, State: p.State, Draft: p.Draft, Merged: p.Merged || p.MergedAt != nil, Title: p.Title, Body: p.Body, HTMLURL: p.HTMLURL,
		UserLogin: p.User.Login, UserType: p.User.Type, HeadSHA: p.Head.SHA, HeadRef: p.Head.Ref, HeadRepo: p.Head.Repo.FullName,
		BaseSHA: p.Base.SHA, BaseRef: p.Base.Ref, BaseRepo: p.Base.Repo.FullName, MergeCommitSHA: p.MergeCommitSHA, ChangedFiles: p.ChangedFiles,
		Additions: p.Additions, Deletions: p.Deletions, CreatedAt: p.CreatedAt, ClosedAt: p.ClosedAt, MergedAt: p.MergedAt, UpdatedAt: u}
	for _, l := range p.Labels {
		out.Labels = append(out.Labels, l.Name)
	}
	return out
}

// storedToPull rebuilds a Pull from stored state. Mergeable and MergeableState
// stay empty: GitHub computes them on demand and a delivery does not carry them,
// so a caller that needs them reads under FreshEvidence.
func storedToPull(s webhookstate.PR) Pull {
	p := Pull{CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt.UTC().Format(time.RFC3339), ClosedAt: s.ClosedAt, HTMLURL: s.HTMLURL, MergedAt: s.MergedAt, Number: s.Number,
		State: s.State, Draft: s.Draft, Merged: s.Merged, Title: s.Title, Body: s.Body, MergeCommitSHA: s.MergeCommitSHA,
		ChangedFiles: s.ChangedFiles, Additions: s.Additions, Deletions: s.Deletions, User: PullUser{Login: s.UserLogin, Type: s.UserType}}
	p.Head.SHA, p.Head.Ref, p.Head.Repo.FullName = s.HeadSHA, s.HeadRef, s.HeadRepo
	p.Base.SHA, p.Base.Ref, p.Base.Repo.FullName = s.BaseSHA, s.BaseRef, s.BaseRepo
	for _, l := range s.Labels {
		p.Labels = append(p.Labels, PullLabel{Name: l})
	}
	return p
}

func (c *Client) storedPull(ctx context.Context, repo string, n int) (Pull, bool) {
	if !c.serving(ctx) {
		return Pull{}, false
	}
	s, ok := c.Webhook.UsablePR(repo, n)
	if !ok || s.State != "open" {
		return Pull{}, false
	}
	c.Webhook.Hit()
	return storedToPull(s), true
}

// shadowPull compares the stored PR with the API's, then records the API read.
// A stored PR newer than the API's came in during the read and is not a mismatch.
func (c *Client) shadowPull(repo string, n int, api Pull) {
	if c.Webhook == nil {
		return
	}
	a := pullToStored(api)
	if s, ok := c.Webhook.PR(repo, n); !ok {
		c.Webhook.Miss()
	} else {
		c.Webhook.Hit()
		if !s.UpdatedAt.After(a.UpdatedAt) {
			c.Webhook.LogDiff(repo, n, a.HeadSHA, webhookstate.DiffPR(s, a))
		}
	}
	c.Webhook.SeedPR(repo, a)
}

func runsToStored(runs []checkRun) []webhookstate.Run {
	out := make([]webhookstate.Run, 0, len(runs))
	for _, r := range runs {
		out = append(out, webhookstate.Run{ID: r.ID, SuiteID: r.CheckSuite.ID, Name: r.Name, Status: r.Status, Conclusion: r.Conclusion,
			DetailsURL: r.DetailsURL, App: r.App.Slug, StartedAt: r.StartedAt, CompletedAt: r.CompletedAt})
	}
	return out
}

func storedChecks(h webhookstate.Head) ([]checkRun, []status) {
	var runs []checkRun
	for _, r := range h.Runs {
		cr := checkRun{ID: r.ID, Name: r.Name, Status: r.Status, Conclusion: r.Conclusion, CompletedAt: r.CompletedAt, StartedAt: r.StartedAt, DetailsURL: r.DetailsURL}
		cr.CheckSuite.ID, cr.App.Slug = r.SuiteID, r.App
		runs = append(runs, cr)
	}
	slices.SortFunc(runs, func(a, b checkRun) int { return cmp.Or(cmp.Compare(a.ID, b.ID), strings.Compare(a.Name, b.Name)) })
	var statuses []status
	for _, s := range h.Statuses {
		statuses = append(statuses, status{ID: s.ID, Context: s.Context, State: s.State, TargetURL: s.TargetURL, UpdatedAt: s.UpdatedAt})
	}
	slices.SortFunc(statuses, func(a, b status) int { return cmp.Compare(a.ID, b.ID) })
	return runs, statuses
}

// storedHeadChecks answers a head-check read from stored state when it holds a
// complete, recent list for the head. It serves only the gate's per-check read
// (headChecks); Check, the aggregate the merge path calls, always asks the API.
func (c *Client) storedHeadChecks(ctx context.Context, repo, sha string) (checkEvidence, bool) {
	if !c.serving(ctx) {
		return checkEvidence{}, false
	}
	h, ok, usable := c.Webhook.Head(repo, sha)
	if !ok || !usable {
		return checkEvidence{}, false
	}
	runs, statuses := storedChecks(h)
	ev, e := foldChecks(runs, statuses, true)
	if e != nil {
		return checkEvidence{}, false
	}
	c.Webhook.Hit()
	return ev, true
}

// shadowChecks compares the stored checks of sha with what the API gave, then
// records the API read.
func (c *Client) shadowChecks(repo, sha string, runs []checkRun, statuses []status, preserve bool, api checkEvidence, apiErr error) {
	if c.Webhook == nil || apiErr != nil {
		return
	}
	if h, ok, _ := c.Webhook.Head(repo, sha); !ok || h.Synced.IsZero() && len(h.Runs)+len(h.Statuses) == 0 {
		c.Webhook.Miss()
	} else {
		c.Webhook.Hit()
		sr, ss := storedChecks(h)
		stored, e := foldChecks(sr, ss, preserve)
		var diffs []webhookstate.Diff
		if e != nil {
			diffs = append(diffs, webhookstate.Diff{Field: "checks", Stored: e.Error(), API: "ok"})
		} else {
			for _, name := range slices.Sorted(maps.Keys(maps.Collect(joinKeys(stored.states, api.states)))) {
				s, sok := stored.states[name]
				a, aok := api.states[name]
				switch {
				case sok != aok:
					diffs = append(diffs, webhookstate.Diff{Field: "check " + name, Stored: presence(sok, string(s)), API: presence(aok, string(a))})
				case s != a:
					diffs = append(diffs, webhookstate.Diff{Field: "check " + name, Stored: string(s), API: string(a)})
				}
			}
		}
		c.Webhook.LogDiff(repo, c.Webhook.PRForHead(repo, sha), sha, diffs)
	}
	c.Webhook.SeedChecks(repo, sha, runsToStored(runs), statusesToStored(statuses))
}

func joinKeys(a, b map[string]CI) func(yield func(string, struct{}) bool) {
	return func(yield func(string, struct{}) bool) {
		for k := range a {
			if !yield(k, struct{}{}) {
				return
			}
		}
		for k := range b {
			if !yield(k, struct{}{}) {
				return
			}
		}
	}
}

func presence(ok bool, v string) string {
	if !ok {
		return "absent"
	}
	return v
}

func statusesToStored(statuses []status) []webhookstate.Status {
	out := make([]webhookstate.Status, 0, len(statuses))
	for _, s := range statuses {
		out = append(out, webhookstate.Status{ID: s.ID, Context: s.Context, State: s.State, TargetURL: s.TargetURL, UpdatedAt: s.UpdatedAt})
	}
	return out
}

func reviewsToStored(reviews []Review) []webhookstate.Review {
	out := make([]webhookstate.Review, 0, len(reviews))
	for _, r := range reviews {
		v := webhookstate.Review{ID: r.ID, Login: r.User.Login, State: r.State, CommitID: r.CommitID, Body: r.Body}
		if r.SubmittedAt != nil {
			v.SubmittedAt = *r.SubmittedAt
		}
		out = append(out, v)
	}
	return out
}

func (c *Client) storedReviews(ctx context.Context, repo string, n int) ([]Review, bool) {
	if !c.serving(ctx) {
		return nil, false
	}
	stored, ok, usable := c.Webhook.Reviews(repo, n)
	if !ok || !usable {
		return nil, false
	}
	c.Webhook.Hit()
	out := make([]Review, 0, len(stored))
	for _, s := range stored {
		r := Review{ID: s.ID, Body: s.Body, CommitID: s.CommitID, State: s.State}
		r.User.Login = s.Login
		if !s.SubmittedAt.IsZero() {
			t := s.SubmittedAt
			r.SubmittedAt = &t
		}
		out = append(out, r)
	}
	return out, true
}

// shadowReviews compares the stored reviews of PR n with the API's, then
// records the API read.
func (c *Client) shadowReviews(repo string, n int, api []Review) {
	if c.Webhook == nil {
		return
	}
	a := reviewsToStored(api)
	if stored, ok, _ := c.Webhook.Reviews(repo, n); !ok {
		c.Webhook.Miss()
	} else {
		c.Webhook.Hit()
		pr, _ := c.Webhook.PR(repo, n)
		c.Webhook.LogDiff(repo, n, pr.HeadSHA, webhookstate.DiffReviews(stored, a))
	}
	c.Webhook.SeedReviews(repo, n, a)
}
