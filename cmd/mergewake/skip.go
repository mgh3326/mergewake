package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/mergewake/internal/daemon"
	"github.com/mgh3326/mergewake/internal/github"
)

// skipMaxAge caps how long a PR is skipped. It is the safety net for a webhook
// that never arrived (a resolved review thread has no other cheap signal), not
// a TTL on webhook data.
const skipMaxAge = 60 * time.Minute

// prEvaluation is what GitHub looked like when a PR was last evaluated in full.
type prEvaluation struct {
	validators github.Validators
	// base is the head of the branch the PR targets: the merge gate reads it.
	base string
	// seq is the webhook position before the evaluation read anything.
	seq uint64
	at  time.Time
}

// prSkips decides which PRs the pr pass can leave alone. A PR is unchanged when
// the ETags of its PR, review list and head checks match its last clean
// evaluation, its head and its base branch's head are the same, no webhook has
// named it since, and that evaluation is under skipMaxAge old. Anything else
// evaluates the PR in full. A nil *prSkips skips nothing.
type prSkips struct {
	// Dirty is fed by the webhook receiver; the daemon builds prSkips only when
	// it runs one, since without it nothing would tell a change from a quiet PR.
	Dirty *daemon.Dirty
	Log   *slog.Logger
	// Now is the clock for evaluation age; nil means time.Now.
	Now func() time.Time

	mu   sync.Mutex
	last map[prKey]prEvaluation
}

func (k *prSkips) now() time.Time {
	if k.Now != nil {
		return k.Now()
	}
	return time.Now()
}

type prKey struct {
	repo string
	n    int
}

// skipPlan is the decision for one PR in one pass.
type skipPlan struct {
	skip bool
	// reasons names every check that failed; empty when skip is true or the PR
	// is not tracked.
	reasons []string
	// track says the evaluation may be recorded when it ends clean.
	track bool
	eval  prEvaluation
}

// plan revalidates PR n and decides. base is the target branch's head; a
// non-nil baseErr (its read failed) evaluates the PR.
func (k *prSkips) plan(ctx context.Context, gh *github.Client, repo string, n int, base string, baseErr error) skipPlan {
	seq, at := k.Dirty.Seq(), k.now()
	v, err := gh.PullValidators(ctx, repo, n)
	if err != nil || baseErr != nil || !v.Known() {
		return skipPlan{reasons: []string{"unvalidated"}}
	}
	eval := prEvaluation{validators: v, base: base, seq: seq, at: at}
	k.mu.Lock()
	prior, ok := k.last[prKey{repo, n}]
	k.mu.Unlock()
	if !ok {
		return skipPlan{reasons: []string{"first"}, track: true, eval: eval}
	}
	var reasons []string
	if k.Dirty.Since(prior.seq, repo, n, v.Head) {
		reasons = append(reasons, "webhook")
	}
	reasons = append(reasons, v.Changed(prior.validators)...)
	if base != prior.base {
		reasons = append(reasons, "base")
	}
	if at.Sub(prior.at) >= skipMaxAge {
		reasons = append(reasons, "age")
	}
	return skipPlan{skip: len(reasons) == 0, reasons: reasons, track: true, eval: eval}
}

// record keeps a plan's evaluation when it ended clean and the PR is settled
// (a draft, or a head whose review is done); otherwise the PR is evaluated
// again next pass.
func (k *prSkips) record(repo string, n int, p skipPlan, settled bool) {
	if k == nil || !p.track {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.last == nil {
		k.last = map[prKey]prEvaluation{}
	}
	if p.skip {
		return
	}
	if settled {
		k.last[prKey{repo, n}] = p.eval
	} else {
		delete(k.last, prKey{repo, n})
	}
}

// forget drops the memory of PRs no longer open.
func (k *prSkips) forget(repo string, open map[int]bool) {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for key := range k.last {
		if key.repo == repo && !open[key.n] {
			delete(k.last, key)
		}
	}
}

// report logs one repository's pass: how many PRs it skipped and evaluated, and
// why each evaluated PR was.
func (k *prSkips) report(repo string, prs []int, plans []skipPlan) {
	if k == nil || k.Log == nil {
		return
	}
	skipped, evaluated := 0, 0
	reasons := map[string]int{}
	var evaluatedPRs []string
	for i, p := range plans {
		if p.skip {
			skipped++
			continue
		}
		evaluated++
		for _, r := range p.reasons {
			reasons[r]++
		}
		if len(p.reasons) > 0 {
			evaluatedPRs = append(evaluatedPRs, fmt.Sprintf("%d:%s", prs[i], strings.Join(p.reasons, "+")))
		}
	}
	slices.Sort(evaluatedPRs)
	k.Log.Info("pr pass", "repo", repo, "prs_skipped", skipped, "prs_evaluated", evaluated, "evaluated_reasons", reasons, "evaluated", evaluatedPRs[:min(len(evaluatedPRs), 30)])
}
