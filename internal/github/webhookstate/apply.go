package webhookstate

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

type payloadPR struct {
	Number         int     `json:"number"`
	State          string  `json:"state"`
	Draft          bool    `json:"draft"`
	Merged         bool    `json:"merged"`
	Title          string  `json:"title"`
	Body           string  `json:"body"`
	HTMLURL        string  `json:"html_url"`
	MergeCommitSHA string  `json:"merge_commit_sha"`
	ChangedFiles   int     `json:"changed_files"`
	Additions      int     `json:"additions"`
	Deletions      int     `json:"deletions"`
	CreatedAt      string  `json:"created_at"`
	ClosedAt       *string `json:"closed_at"`
	MergedAt       *string `json:"merged_at"`
	UpdatedAt      string  `json:"updated_at"`
	User           struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"user"`
	Head   payloadRef `json:"head"`
	Base   payloadRef `json:"base"`
	Labels []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

type payloadRef struct {
	SHA  string `json:"sha"`
	Ref  string `json:"ref"`
	Repo struct {
		FullName string `json:"full_name"`
	} `json:"repo"`
}

type payloadApp struct {
	Slug string `json:"slug"`
}

type payloadRun struct {
	ID          int64              `json:"id"`
	Name        string             `json:"name"`
	HeadSHA     string             `json:"head_sha"`
	Status      string             `json:"status"`
	Conclusion  string             `json:"conclusion"`
	DetailsURL  string             `json:"details_url"`
	StartedAt   time.Time          `json:"started_at"`
	CompletedAt time.Time          `json:"completed_at"`
	CheckSuite  struct{ ID int64 } `json:"check_suite"`
	App         payloadApp         `json:"app"`
}

type payload struct {
	Action      string     `json:"action"`
	Number      int        `json:"number"`
	PullRequest payloadPR  `json:"pull_request"`
	CheckRun    payloadRun `json:"check_run"`
	CheckSuite  struct {
		ID         int64      `json:"id"`
		HeadSHA    string     `json:"head_sha"`
		Status     string     `json:"status"`
		Conclusion string     `json:"conclusion"`
		UpdatedAt  time.Time  `json:"updated_at"`
		App        payloadApp `json:"app"`
	} `json:"check_suite"`
	// status events
	ID        int64     `json:"id"`
	SHA       string    `json:"sha"`
	State     string    `json:"state"`
	Context   string    `json:"context"`
	TargetURL string    `json:"target_url"`
	UpdatedAt time.Time `json:"updated_at"`
	Review    struct {
		ID          int64     `json:"id"`
		State       string    `json:"state"`
		Body        string    `json:"body"`
		CommitID    string    `json:"commit_id"`
		SubmittedAt time.Time `json:"submitted_at"`
		User        struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"review"`
}

// Handles reports whether Apply keeps anything for event.
func Handles(event string) bool {
	switch event {
	case "pull_request", "check_run", "check_suite", "status", "pull_request_review":
		return true
	}
	return false
}

// Apply folds one verified delivery for repo into the state. An event that is
// not kept is ignored without error; a payload that does not parse returns an
// error and leaves the state as it was. A delivery id that was applied before
// is a no-op.
func (s *Store) Apply(repo, event, delivery string, body []byte) error {
	_, err := s.ApplyObserved(repo, event, delivery, body)
	return err
}

// ApplyObserved reports whether the delivery advanced state or checks.
func (s *Store) ApplyObserved(repo, event, delivery string, body []byte) (bool, error) {
	if !Handles(event) {
		return false, nil
	}
	var p payload
	if e := json.Unmarshal(body, &p); e != nil {
		return false, fmt.Errorf("parse %s payload: %w", event, e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil {
		return false, e
	}
	if delivery != "" {
		if slices.Contains(r.Deliveries, delivery) {
			return false, nil
		}
		r.Deliveries = append(r.Deliveries, delivery)
		if len(r.Deliveries) > maxDeliveries {
			r.Deliveries = slices.Clone(r.Deliveries[len(r.Deliveries)-maxDeliveries:])
		}
		r.dirty = true
	}
	now := s.now()
	applied := false
	switch event {
	case "pull_request":
		if p.PullRequest.Number == 0 {
			return false, fmt.Errorf("parse pull_request payload: no pull request number")
		}
		pr, e := prFromPayload(p.PullRequest)
		if e != nil {
			return false, e
		}
		applied = r.putPR(pr, now, false)
	case "check_run":
		c := p.CheckRun
		if c.HeadSHA == "" || c.Name == "" {
			return false, fmt.Errorf("parse check_run payload: no head_sha or name")
		}
		applied = r.head(c.HeadSHA, now).putRun(Run{ID: c.ID, SuiteID: c.CheckSuite.ID, Name: c.Name, Status: c.Status, Conclusion: c.Conclusion,
			DetailsURL: c.DetailsURL, App: c.App.Slug, StartedAt: c.StartedAt, CompletedAt: c.CompletedAt}, false)
		r.trimHeads()
	case "check_suite":
		c := p.CheckSuite
		if c.HeadSHA == "" || c.ID == 0 {
			return false, fmt.Errorf("parse check_suite payload: no head_sha or id")
		}
		applied = r.head(c.HeadSHA, now).putSuite(Suite{ID: c.ID, Status: c.Status, Conclusion: c.Conclusion, App: c.App.Slug, UpdatedAt: c.UpdatedAt})
		r.trimHeads()
	case "status":
		if p.SHA == "" || p.Context == "" {
			return false, fmt.Errorf("parse status payload: no sha or context")
		}
		applied = r.head(p.SHA, now).putStatus(Status{ID: p.ID, Context: p.Context, State: p.State, TargetURL: p.TargetURL, UpdatedAt: p.UpdatedAt}, false)
		r.trimHeads()
	case "pull_request_review":
		n := cmp.Or(p.PullRequest.Number, p.Number)
		if n == 0 || p.Review.ID == 0 {
			return false, fmt.Errorf("parse pull_request_review payload: no pull request number or review id")
		}
		rv := Review{ID: p.Review.ID, Login: p.Review.User.Login, State: strings.ToUpper(p.Review.State), CommitID: p.Review.CommitID,
			Body: BoundBody(p.Review.Body, true), SubmittedAt: p.Review.SubmittedAt}
		r.putReview(n, rv, p.Action)
	}
	r.dirty = true
	return applied, nil
}

func prFromPayload(p payloadPR) (PR, error) {
	updated, e := time.Parse(time.RFC3339, p.UpdatedAt)
	if e != nil {
		return PR{}, fmt.Errorf("parse pull_request payload: updated_at: %w", e)
	}
	pr := PR{Number: p.Number, State: p.State, Draft: p.Draft, Merged: p.Merged || p.MergedAt != nil, Title: p.Title, Body: BoundBody(p.Body, false), HTMLURL: p.HTMLURL,
		UserLogin: p.User.Login, UserType: p.User.Type, HeadSHA: p.Head.SHA, HeadRef: p.Head.Ref, HeadRepo: p.Head.Repo.FullName,
		BaseSHA: p.Base.SHA, BaseRef: p.Base.Ref, BaseRepo: p.Base.Repo.FullName, MergeCommitSHA: p.MergeCommitSHA, ChangedFiles: p.ChangedFiles,
		Additions: p.Additions, Deletions: p.Deletions, CreatedAt: p.CreatedAt, ClosedAt: p.ClosedAt, MergedAt: p.MergedAt, UpdatedAt: updated}
	for _, l := range p.Labels {
		pr.Labels = append(pr.Labels, l.Name)
	}
	return pr, nil
}

// putPR stores pr when it is newer than what is held (api reads also replace an
// equal one: the API is the truth for ties). A closed PR is pruned and
// remembered, so a straggler cannot bring it back; a newer delivery reopens it.
func (r *repoState) putPR(pr PR, now time.Time, api bool) bool {
	if gone, ok := r.Gone[pr.Number]; ok && !pr.UpdatedAt.After(gone) {
		return false
	}
	old := r.PRs[pr.Number]
	if old != nil && (pr.UpdatedAt.Before(old.UpdatedAt) || (!api && pr.UpdatedAt.Equal(old.UpdatedAt))) {
		return false
	}
	if pr.State == "closed" || pr.Merged {
		r.closePR(pr, old)
		return true
	}
	delete(r.Gone, pr.Number)
	pr.FirstSeen = now
	if old != nil {
		pr.FirstSeen = old.FirstSeen
	}
	if api {
		pr.SyncedAt = now
	} else if old != nil {
		pr.SyncedAt = old.SyncedAt
	}
	r.PRs[pr.Number] = &pr
	r.dirty = true
	if len(r.PRs) > maxPRs {
		r.evictPR()
	}

	return true
}

func (r *repoState) closePR(pr PR, old *PR) {
	delete(r.PRs, pr.Number)
	delete(r.Reviews, pr.Number)
	for _, sha := range []string{pr.HeadSHA, func() string {
		if old != nil {
			return old.HeadSHA
		}
		return ""
	}()} {
		if sha != "" && !r.headOfOpenPR(sha) {
			delete(r.Heads, sha)
		}
	}
	r.Gone[pr.Number] = pr.UpdatedAt
	if len(r.Gone) > maxGone {
		oldest, at := 0, time.Time{}
		for n, t := range r.Gone {
			if at.IsZero() || t.Before(at) {
				oldest, at = n, t
			}
		}
		delete(r.Gone, oldest)
	}
	r.dirty = true
}

func (r *repoState) headOfOpenPR(sha string) bool {
	for _, p := range r.PRs {
		if p.HeadSHA == sha {
			return true
		}
	}
	return false
}

func (r *repoState) evictPR() {
	oldest, at := 0, time.Time{}
	for n, p := range r.PRs {
		if at.IsZero() || p.UpdatedAt.Before(at) {
			oldest, at = n, p.UpdatedAt
		}
	}
	delete(r.PRs, oldest)
	delete(r.Reviews, oldest)
}

func (h *Head) initMaps() {
	if h.Runs == nil {
		h.Runs = map[string]Run{}
	}
	if h.Statuses == nil {
		h.Statuses = map[string]Status{}
	}
	if h.Suites == nil {
		h.Suites = map[int64]Suite{}
	}
}

func (r *repoState) head(sha string, now time.Time) *Head {
	h := r.Heads[sha]
	if h == nil {
		h = &Head{}
		h.initMaps()
		r.Heads[sha] = h
	}
	h.Touched = now
	return h
}

// trimHeads drops the heads untouched longest once there are too many.
func (r *repoState) trimHeads() {
	for len(r.Heads) > maxHeads {
		oldest, at := "", time.Time{}
		for sha, h := range r.Heads {
			if at.IsZero() || h.Touched.Before(at) {
				oldest, at = sha, h.Touched
			}
		}
		delete(r.Heads, oldest)
	}
}

func rank(status string) int {
	switch status {
	case "completed":
		return 2
	case "in_progress":
		return 1
	}
	return 0
}

func (r Run) at() time.Time {
	if r.CompletedAt.After(r.StartedAt) {
		return r.CompletedAt
	}
	return r.StartedAt
}

// newerThan orders two runs of one (suite, name): a higher id is a rerun; for
// one id the later event time wins, and without times the further status does.
// Equal is not newer.
func (r Run) newerThan(o Run) bool {
	if r.ID != o.ID {
		return r.ID > o.ID
	}
	if a, b := r.at(), o.at(); !a.Equal(b) {
		return a.After(b)
	}
	return r.at().IsZero() && rank(r.Status) > rank(o.Status)
}

func (s Status) newerThan(o Status) bool {
	if s.ID != o.ID {
		return s.ID > o.ID
	}
	return s.UpdatedAt.After(o.UpdatedAt)
}

func (h *Head) putRun(run Run, api bool) bool {
	if h.Runs == nil {
		h.Runs = map[string]Run{}
	}
	key := fmt.Sprintf("%d/%s", run.SuiteID, run.Name)
	old, ok := h.Runs[key]
	if ok && (old.newerThan(run) || (!api && !run.newerThan(old))) {
		return false
	}
	if !ok && len(h.Runs) >= maxRunsPerHead {
		return false
	}
	h.Runs[key] = run
	return true
}

func (h *Head) putStatus(st Status, api bool) bool {
	if h.Statuses == nil {
		h.Statuses = map[string]Status{}
	}
	old, ok := h.Statuses[st.Context]
	if ok && (old.newerThan(st) || (!api && !st.newerThan(old))) {
		return false
	}
	h.Statuses[st.Context] = st
	return true
}

func (h *Head) putSuite(su Suite) bool {
	if h.Suites == nil {
		h.Suites = map[int64]Suite{}
	}
	if old, ok := h.Suites[su.ID]; ok && !su.UpdatedAt.After(old.UpdatedAt) {
		return false
	}
	if len(h.Suites) < maxRunsPerHead {
		h.Suites[su.ID] = su
		return true
	}
	return false
}

// reviewSet returns the reviews of PR n, creating the set; nil for a closed PR
// or when the repository already holds as many sets as PRs it keeps.
func (r *repoState) reviewSet(n int) *reviewSet {
	if set := r.Reviews[n]; set != nil {
		return set
	}
	if _, gone := r.Gone[n]; gone && r.PRs[n] == nil || len(r.Reviews) >= maxPRs {
		return nil
	}
	set := &reviewSet{Reviews: map[int64]Review{}}
	r.Reviews[n] = set
	return set
}

// putReview applies a pull_request_review delivery. A submitted review never
// replaces one already held; an edit or a dismissal replaces it unless it is
// older than the one held.
func (r *repoState) putReview(n int, rv Review, action string) {
	set := r.reviewSet(n)
	if set == nil {
		return
	}
	old, ok := set.Reviews[rv.ID]
	switch {
	case ok && (action == "submitted" || rv.SubmittedAt.Before(old.SubmittedAt)):
		return
	case ok && action == "dismissed":
		rv.State = "DISMISSED"
	}
	if !ok && len(set.Reviews) >= maxReviews {
		return
	}
	set.Reviews[rv.ID] = rv
}

// SeedPR records an API read of a PR. The API is the truth for a tie, and the
// entry counts as confirmed at the time of the read.
func (s *Store) SeedPR(repo string, pr PR) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, e := s.repo(repo); e == nil {
		pr.Body = BoundBody(pr.Body, false)
		r.putPR(pr, s.now(), true)
	}
}

// SeedChecks records an API read of every check of a head.
func (s *Store) SeedChecks(repo, sha string, runs []Run, statuses []Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil {
		return
	}
	h := r.head(sha, s.now())
	for _, run := range runs {
		h.putRun(run, true)
	}
	for _, st := range statuses {
		h.putStatus(st, true)
	}
	h.Synced = s.now()
	r.trimHeads()
	r.dirty = true
}

// SeedReviews records an API read of every review of a PR.
func (s *Store) SeedReviews(repo string, n int, reviews []Review) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.repo(repo)
	if e != nil {
		return
	}
	set := r.reviewSet(n)
	if set == nil {
		return
	}
	for _, rv := range reviews {
		rv.Body = BoundBody(rv.Body, true)
		if old, ok := set.Reviews[rv.ID]; ok && rv.SubmittedAt.Before(old.SubmittedAt) {
			continue
		}
		if _, ok := set.Reviews[rv.ID]; ok || len(set.Reviews) < maxReviews {
			set.Reviews[rv.ID] = rv
		}
	}
	set.Synced = s.now()
	r.dirty = true
}
