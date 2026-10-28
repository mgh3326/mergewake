package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/github"
	"github.com/mgh3326/mergewake/internal/integrations"
	"github.com/mgh3326/mergewake/internal/rollout"
)

var taskTrailer = regexp.MustCompile(`(?m)^Mergewake-Task: ([1-9][0-9]*)\s*$`)

type watchResult struct {
	Head    string `json:"head"`
	Action  string `json:"action"`
	Shadow  bool   `json:"shadow"`
	Culprit string `json:"culprit,omitempty"`
	PR      int    `json:"pr,omitempty"`
	URL     string `json:"url,omitempty"`
}

// Bisect returns the first red candidate following a known green baseline.
// Missing/pending evidence stops the search rather than assigning blame.
func Bisect(shas []string, check func(string) (github.CI, error)) (int, error) {
	if len(shas) < 2 {
		return 0, errors.New("bisect needs a green baseline and red head")
	}
	lo, hi := 0, len(shas)-1
	state, e := check(shas[lo])
	if e != nil {
		return 0, e
	}
	if state != github.Green {
		return 0, errors.New("bisect baseline is not green")
	}
	state, e = check(shas[hi])
	if e != nil {
		return 0, e
	}
	if state != github.Red {
		return 0, errors.New("bisect head is not red")
	}
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		state, e = check(shas[mid])
		if e != nil {
			return 0, e
		}
		switch state {
		case github.Green:
			lo = mid
		case github.Red:
			hi = mid
		default:
			return 0, errors.New("bisect has missing or pending CI; operator or any-ref CI required")
		}
	}
	return hi, nil
}
func (s *Service) Watch(ctx context.Context, r config.Repo, o Options) error {
	head, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	p, e := s.Policy(ctx, r, head)
	if e != nil {
		return e
	}
	ci, e := s.GH.Check(ctx, r.Name, head, p.RequiredChecks)
	if e != nil {
		return e
	}
	result := watchResult{Head: head, Action: "waiting-for-ci", Shadow: !p.Enabled || o.Shadow}
	checkpoint := filepath.Join(s.state(r.Name), "last-green")
	if ci == github.Green {
		result.Action = "healthy"
		if !o.DryRun {
			if e = rollout.Atomic(checkpoint, []byte(head), 0600); e != nil {
				return e
			}
		}
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if ci != github.Red {
		return s.audit(r.Name, "watch", result, o.DryRun)
	}
	if _, e = s.GH.Head(ctx, r.Name, github.RecoveryBranch); e == nil {
		result.Action = "operator-required-recovery-latch"
		return s.audit(r.Name, "watch", result, o.DryRun)
	} else if !github.NotFound(e) {
		return e
	}
	baselineBytes, e := os.ReadFile(checkpoint)
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	baseline := string(baselineBytes)
	chain := []string{head}
	cache := map[string]github.CI{head: ci}
	current := head
	found := false
	check := func(sha string) (github.CI, error) {
		if v, ok := cache[sha]; ok {
			return v, nil
		}
		v, e := s.GH.Check(ctx, r.Name, sha, p.RequiredChecks)
		if e == nil {
			cache[sha] = v
		}
		return v, e
	}
	for len(chain) < s.Config.WatchLimit {
		c, e := s.GH.Commit(ctx, r.Name, current)
		if e != nil {
			return e
		}
		if len(c.Parents) != 1 {
			return errors.New("main history is not linear squash history; operator required")
		}
		current = c.Parents[0].SHA
		chain = append(chain, current)
		if baseline != "" {
			if current == baseline {
				found = true
				break
			}
		} else {
			v, e := check(current)
			if e != nil {
				return e
			}
			if v == github.Green {
				found = true
				break
			}
		}
	}
	if !found {
		return errors.New("no green ancestor within watch_limit; operator required")
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	// A later green commit supersedes an older checkpoint. Inspect the interval
	// before bisecting so an independent later regression cannot blame an older PR.
	for i := len(chain) - 2; i >= 0; i-- {
		v, e := check(chain[i])
		if e != nil {
			return e
		}
		if v == github.Green {
			chain = chain[i:]
			break
		}
		if v != github.Red {
			return errors.New("incomplete CI evidence in recovery interval")
		}
	}
	index, e := Bisect(chain, check)
	if e != nil {
		return e
	}
	culprit := chain[index]
	pulls, e := s.GH.PRsForCommit(ctx, r.Name, culprit)
	if e != nil {
		return e
	}
	var origin *github.Pull
	for i := range pulls {
		v := &pulls[i]
		if (v.Merged || v.MergedAt != nil) && v.Base.Ref == r.Branch && v.MergeCommitSHA == culprit {
			if origin != nil {
				return errors.New("ambiguous culprit PR")
			}
			origin = v
		}
	}
	if origin == nil {
		return errors.New("culprit is not an identifiable squash-merged PR")
	}
	// The confirmed boundary must be one independently green parent and one red PR.
	parent, e := s.GH.Commit(ctx, r.Name, culprit)
	if e != nil {
		return e
	}
	if len(parent.Parents) != 1 || parent.Parents[0].SHA != chain[index-1] {
		return errors.New("culprit parent mismatch")
	}
	task := r.Task
	if m := taskTrailer.FindStringSubmatch(origin.Body); len(m) == 2 {
		task = m[1]
	}
	if s.Config.Sink.Type == "handoffkeep" && !integrations.TaskID.MatchString(task) {
		return errors.New("origin task missing; add Mergewake-Task trailer or repo task")
	}
	result.Action = "open-revert-pr"
	result.Culprit = culprit
	result.PR = origin.Number
	if e = s.audit(r.Name, "watch", result, o.DryRun); e != nil {
		return e
	}
	if result.Shadow || o.DryRun {
		return nil
	}
	fresh, e := s.GH.Head(ctx, r.Name, r.Branch)
	if e != nil {
		return e
	}
	if fresh != head {
		return errors.New("main changed during diagnosis; rerun watch")
	}
	ci, e = s.GH.Check(ctx, r.Name, head, p.RequiredChecks)
	if e != nil {
		return e
	}
	if ci != github.Red {
		return errors.New("main CI changed during diagnosis")
	}
	result.URL, e = s.GH.Revert(ctx, r.Name, r.Branch, head, culprit, origin.Number)
	if e != nil {
		return e
	}
	result.Action = "opened-revert-pr"
	data, _ := json.Marshal(result)
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	note := fmt.Sprintf("mergewake: main CI failed after PR #%d; revert PR %s", origin.Number, result.URL)
	var reopen error
	if task != "" {
		reopen = s.Sink.Reopen(reportCtx, task, note)
	}
	if reopen != nil {
		note += "; task reopen failed (recovery comment retained): " + reopen.Error()
	}
	return errors.Join(s.audit(r.Name, "watch", result, false), reopen, s.Sink.Record(reportCtx, "mergewake/recovery/"+culprit, data), s.Notifier.Notify(reportCtx, integrations.Event{ID: "mergewake-revert-" + culprit, Text: note}))
}
