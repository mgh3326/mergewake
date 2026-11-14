package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/mergewake/internal/changestate"
)

func (c *Client) ChangeWriter() changestate.Writer { return c.Changes }
func (c *Client) ChangeCounts() changestate.Counts {
	var counts changestate.Counts
	if c.Changes != nil {
		counts = c.Changes.Counts()
	}
	counts.Skipped = c.mergedStateSkipped.Load()
	return counts
}
func (c *Client) WaitChanges(ctx context.Context) {
	if c.Changes != nil {
		c.Changes.Wait(ctx)
	}
}
func changeSource(ctx context.Context) string {
	if isFresh(ctx) {
		return "fresh"
	}
	return "poll"
}
func (c *Client) writePull(ctx context.Context, repo string, p Pull, at time.Time, source, delivery string) {
	c.writePullFresh(ctx, repo, p, at, source, delivery, Pull{}, false)
}

// writePullFresh is writePull with a fresh PR read the caller already made
// past the cache. freshTried reports that the read happened even when it
// failed, so the state writer does not fetch the same PR twice.
func (c *Client) writePullFresh(ctx context.Context, repo string, p Pull, at time.Time, source, delivery string, fresh Pull, freshTried bool) {
	if c.Changes == nil {
		return
	}
	state := p.State
	merged := p.Merged || p.MergedAt != nil
	if merged {
		state = "merged"
		// hk keeps the newest observation per section, so a merged write without
		// merge_sha or merged_at would erase what an earlier write held. A source
		// that lacks either (a cached or replayed body, a webhook payload) is read
		// once past the cache; if GitHub still lacks it, no write is made and a
		// later one fills it.
		if p.MergeCommitSHA == "" || p.MergedAt == nil {
			if !freshTried {
				freshTried = true
				fresh, _ = c.freshPull(ctx, repo, p.Number)
			}
			if fresh.MergeCommitSHA != "" && fresh.MergedAt != nil {
				p.MergeCommitSHA, p.MergedAt = fresh.MergeCommitSHA, fresh.MergedAt
			}
		}
		if p.MergeCommitSHA == "" || p.MergedAt == nil {
			c.mergedStateSkipped.Add(1)
			return
		}
	}
	v := map[string]any{"head_sha": p.Head.SHA, "base_sha": p.Base.SHA, "base_ref": p.Base.Ref, "state": state, "draft": p.Draft, "title": p.Title, "author": p.User.Login, "observed_at": at, "source": source}
	if merged {
		v["merge_sha"] = p.MergeCommitSHA
		v["merged_at"] = *p.MergedAt
	}
	if delivery != "" {
		v["delivery_id"] = delivery
	}
	c.Changes.Queue(repo, p.Number, "state", changestate.Section{At: at, Value: v})
}
func limitRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}
func (c *Client) writeThreads(ctx context.Context, repo string, n int, fb ReviewFeedback) {
	if c.Changes == nil || len(fb.Threads) > 300 {
		return
	}
	at := time.Now().UTC()
	threads := make([]map[string]any, 0, len(fb.Threads))
	for _, t := range fb.Threads {
		item := map[string]any{"thread_id": t.ID, "path": t.Path, "resolved": t.IsResolved, "outdated": t.IsOutdated, "comment_count": len(t.Comments), "author_kind": "human"}
		if t.Line > 0 {
			item["line"] = t.Line
		}
		if len(t.Comments) > 0 {
			first := t.Comments[0]
			if last := t.Comments[len(t.Comments)-1].CreatedAt; !last.IsZero() {
				item["last_comment_at"] = last
			}
			item["author"] = first.Author
			item["url"] = first.URL
			item["excerpt"] = limitRunes(first.Body, 2000)
			if strings.EqualFold(first.Author, c.ChangeLogin) && c.ChangeLogin != "" {
				item["author_kind"] = "mergewake"
			} else if strings.EqualFold(first.AuthorType, "Bot") {
				item["author_kind"] = "bot"
			}
			if sev := hkSeverity(ThreadSeverity(first.Author, first.Body)); sev != "" {
				item["severity"] = sev
			}
			for _, comment := range t.Comments[1:] {
				if strings.EqualFold(comment.Author, fb.PRAuthor) && fb.PRAuthor != "" {
					item["replied"] = true
				} else if strings.EqualFold(comment.Author, first.Author) {
					item["replied"] = false
				}
			}
		}
		threads = append(threads, item)
	}
	c.Changes.Queue(repo, n, "threads", changestate.Section{At: at, Value: map[string]any{"threads": threads, "observed_at": at, "source": changeSource(ctx), "complete": true}})
}

// WebhookDelivery records an applied delivery after the webhook store orders it.
func (c *Client) WebhookDelivery(repo, event, delivery string, body []byte) {
	if c.Changes == nil || c.Webhook == nil {
		return
	}
	var p struct {
		PullRequest Pull      `json:"pull_request"`
		SHA         string    `json:"sha"`
		UpdatedAt   time.Time `json:"updated_at"`
		CheckRun    struct {
			HeadSHA      string    `json:"head_sha"`
			StartedAt    time.Time `json:"started_at"`
			CompletedAt  time.Time `json:"completed_at"`
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"check_run"`
		CheckSuite struct {
			HeadSHA      string    `json:"head_sha"`
			UpdatedAt    time.Time `json:"updated_at"`
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"check_suite"`
	}
	if json.Unmarshal(body, &p) != nil {
		return
	}
	if event == "pull_request" {
		at, e := time.Parse(time.RFC3339, p.PullRequest.UpdatedAt)
		if e == nil {
			c.writePull(context.Background(), repo, p.PullRequest, at, "webhook", delivery)
		}
		return
	}
	sha, at := p.SHA, p.UpdatedAt
	switch event {
	case "check_run":
		sha = p.CheckRun.HeadSHA
		at = p.CheckRun.StartedAt
		if p.CheckRun.CompletedAt.After(at) {
			at = p.CheckRun.CompletedAt
		}
	case "check_suite":
		sha = p.CheckSuite.HeadSHA
		at = p.CheckSuite.UpdatedAt
	}
	if sha == "" || at.IsZero() {
		return
	}
	numbers := map[int]bool{}
	for _, n := range c.Webhook.PRsForHead(repo, sha) {
		numbers[n] = true
	}
	for _, pr := range p.CheckRun.PullRequests {
		if pr.Number > 0 {
			numbers[pr.Number] = true
		}
	}
	for _, pr := range p.CheckSuite.PullRequests {
		if pr.Number > 0 {
			numbers[pr.Number] = true
		}
	}
	if len(numbers) == 0 {
		return
	}
	h, ok, _ := c.Webhook.Head(repo, sha)
	if !ok {
		return
	}
	for _, r := range h.Runs {
		if r.StartedAt.After(at) {
			at = r.StartedAt
		}
		if r.CompletedAt.After(at) {
			at = r.CompletedAt
		}
	}
	for _, st := range h.Statuses {
		if st.UpdatedAt.After(at) {
			at = st.UpdatedAt
		}
	}
	for _, su := range h.Suites {
		if su.UpdatedAt.After(at) {
			at = su.UpdatedAt
		}
	}
	checks := map[string]map[string]any{}
	runIDs := map[string]int64{}
	for _, run := range h.Runs {
		if id, ok := runIDs[run.Name]; ok && id > run.ID {
			continue
		}
		runIDs[run.Name] = run.ID
		item := map[string]any{"name": run.Name, "state": run.Status}
		if run.Conclusion != "" {
			item["conclusion"] = run.Conclusion
		}
		if run.DetailsURL != "" {
			item["url"] = run.DetailsURL
		}
		if !run.StartedAt.IsZero() {
			item["started_at"] = run.StartedAt
		}
		if !run.CompletedAt.IsZero() {
			item["completed_at"] = run.CompletedAt
		}
		checks[run.Name] = item
	}
	for _, st := range h.Statuses {
		if _, ok := checks[st.Context]; ok {
			continue
		}
		checks[st.Context] = map[string]any{"name": st.Context, "state": st.State, "url": st.TargetURL}
	}
	list := make([]map[string]any, 0, len(checks))
	for _, v := range checks {
		list = append(list, v)
	}
	for n := range numbers {
		v := map[string]any{"head_sha": sha, "checks": list, "observed_at": at, "source": "webhook", "complete": false}
		// hk refuses a delivery_id over 128 characters, and with it the write.
		if delivery != "" && len(delivery) <= 128 {
			v["delivery_id"] = delivery
		}
		c.Changes.Queue(repo, n, "checks", changestate.Section{At: at, Value: v})
	}
}

func limitBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
func (c *Client) writePostedFindings(repo string, n int, sha string, findings []Finding, meta PassReceipt) {
	if c.Changes == nil || len(findings) > 200 {
		return
	}
	at := time.Now().UTC()
	findings = append([]Finding(nil), findings...)
	key := fmt.Sprintf("%s#%d", repo, n)
	c.changeJobsMu.Lock()
	if c.changeJobs == nil {
		c.changeJobs = map[string]chan struct{}{}
	}
	previous := c.changeJobs[key]
	done := make(chan struct{})
	c.changeJobs[key] = done
	c.changeJobsMu.Unlock()
	c.Changes.Async(func() {
		defer func() {
			close(done)
			c.changeJobsMu.Lock()
			if c.changeJobs[key] == done {
				delete(c.changeJobs, key)
			}
			c.changeJobsMu.Unlock()
		}()
		if previous != nil {
			<-previous
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var fb ReviewFeedback
		var err error
		for {
			fb, err = c.reviewFeedback(ctx, repo, n)
			if err == nil && postedThreadsPresent(fb, findings) {
				break
			}
			select {
			case <-ctx.Done():
				goto observed
			case <-time.After(150 * time.Millisecond):
			}
		}
	observed:
		if err == nil {
			c.writeThreads(ctx, repo, n, fb)
		}
		used := map[string]bool{}
		out := make([]map[string]any, 0, len(findings))
		for i, f := range findings {
			id := fmt.Sprintf("%s-%d-%d-%s-%d", sha, meta.Round, meta.Pass, meta.Lens, i)
			item := map[string]any{"id": id, "location": fmt.Sprintf("%s:%d", f.Path, f.Line), "blocking": !f.Advisory && (f.Severity == "critical" || f.Severity == "high"), "body": limitBytes(f.Body, 8192)}
			if sev := hkSeverity(f.Severity); sev != "" {
				item["severity"] = sev
			}
			line, _, _ := strings.Cut(strings.TrimSpace(f.Body), "\n")
			item["summary"] = limitRunes(line, 200)
			if f.Anchor != AnchorOutside && err == nil {
				for _, t := range fb.Threads {
					if used[t.ID] || t.Path != f.Path || len(t.Comments) == 0 {
						continue
					}
					comment := t.Comments[0]
					if !strings.HasPrefix(comment.Body, FindingBody(f)) {
						continue
					}
					used[t.ID] = true
					item["id"], item["thread_id"] = t.ID, t.ID
					if comment.URL != "" {
						item["url"] = comment.URL
					}
					break
				}
			}
			out = append(out, item)
		}
		c.Changes.Queue(repo, n, "findings", changestate.Section{At: at, Value: map[string]any{"reviewed_head": sha, "round": meta.Round, "pass": meta.Pass, "lens": meta.Lens, "findings": out, "observed_at": at, "source": "review"}})
	})
}

type requiredChecksKey struct{}

func (c *Client) writeChecks(ctx context.Context, repo, sha string, runs []checkRun, statuses []status) {
	if c.Changes == nil {
		return
	}
	required, _ := ctx.Value(requiredChecksKey{}).([]string)
	requiredSet := map[string]bool{}
	for _, name := range required {
		requiredSet[name] = true
	}
	type selected struct {
		id   int64
		item map[string]any
	}
	latest := map[string]selected{}
	runNames := map[string]bool{}
	for _, r := range runs {
		runNames[r.Name] = true
		if old, ok := latest[r.Name]; ok && old.id > r.ID {
			continue
		}
		item := map[string]any{"name": r.Name, "state": r.Status, "required": requiredSet[r.Name]}
		if r.Conclusion != "" {
			item["conclusion"] = r.Conclusion
		}
		if r.DetailsURL != "" {
			item["url"] = r.DetailsURL
		}
		if !r.StartedAt.IsZero() {
			item["started_at"] = r.StartedAt
		}
		if !r.CompletedAt.IsZero() {
			item["completed_at"] = r.CompletedAt
		}
		latest[r.Name] = selected{r.ID, item}
	}
	for _, st := range statuses {
		if runNames[st.Context] {
			continue
		}
		if old, ok := latest[st.Context]; ok && old.id >= st.ID {
			continue
		}
		latest[st.Context] = selected{st.ID, map[string]any{"name": st.Context, "state": st.State, "url": st.TargetURL, "required": requiredSet[st.Context]}}
	}
	checks := make([]map[string]any, 0, len(latest))
	for _, v := range latest {
		checks = append(checks, v.item)
	}
	complete := len(checks) <= 500
	if !complete {
		checks = checks[:500]
	}
	at := time.Now().UTC()
	prefix := repo + "\x00"
	c.changeHeads.Range(func(k, v any) bool {
		key := k.(string)
		if !strings.HasPrefix(key, prefix) || v.(string) != sha {
			return true
		}
		n, e := strconv.Atoi(strings.TrimPrefix(key, prefix))
		if e == nil {
			c.Changes.Queue(repo, n, "checks", changestate.Section{At: at, Value: map[string]any{"head_sha": sha, "checks": checks, "observed_at": at, "source": changeSource(ctx), "complete": complete}})
		}
		return true
	})
}

func hkSeverity(s string) string {
	switch strings.ToLower(s) {
	case "critical", "high", "medium", "low":
		return strings.ToLower(s)
	default:
		return ""
	}
}

func postedThreadsPresent(fb ReviewFeedback, findings []Finding) bool {
	used := map[string]bool{}
	for _, f := range findings {
		if f.Anchor == AnchorOutside {
			continue
		}
		found := false
		for _, t := range fb.Threads {
			if !used[t.ID] && t.Path == f.Path && len(t.Comments) > 0 && strings.HasPrefix(t.Comments[0].Body, FindingBody(f)) {
				used[t.ID] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
