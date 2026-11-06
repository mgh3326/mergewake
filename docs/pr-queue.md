# PR event queue

The `pr` role evaluates one PR when something happens to it. It does not wait
for a whole pass over the repository. A slow review holds its own PR and no
other.

## What the queue holds

The queue holds items of the form (repository, PR). An item enters it from one
of five sources. The `source` field of the `pr evaluated` log line names them.

| Source | What puts the PR on the queue |
| --- | --- |
| `webhook` | A signed delivery that names the PR: a `pull_request*` event by PR number, or a `check_run`, `check_suite` or `status` event by the PR numbers it lists or by its head commit. A commit that no known open PR has as its head names nothing. |
| `reviewer` | A reviewer job of the PR finished (the reviewer watch). |
| `base` | A push to the repository's base branch (`repos[].branch`). Every open PR of the repository is queued, because its merge gate reads the base. |
| `status` | A fresh `GET /v1/pr-status?fresh=1` read found the PR changed. |
| `sweep` | The periodic pass, below. |

The `webhook` and `base` sources need `daemon.webhook`. The `reviewer` source
needs a recorded reviewer job. The queue and the sweep run whenever the `pr`
role is enabled.

An item that is already waiting absorbs a repeat: five deliveries for one PR
produce one evaluation, and its `source` lists each source once. An item that
is being evaluated runs once more after it ends, however many events arrived
meanwhile, so no event is lost and a PR is never evaluated twice at once.

## One evaluation

One evaluation is what one PR got in a pass before: read the PR, apply the
[skip rule](config.md#skipping-unchanged-prs), record the outcome, review the
head, then deliver findings, notify and land. It never waits for another PR.

- Different PRs run at once. `daemon.pr.review_parallel` bounds the evaluations
  of one repository that run at once. Unset or `0` means no bound. A priority-1
  PR may start past an explicit bound, up to two at once per repository.
- Lands stay one at a time per repository. The settle step of each PR (findings,
  notice, land) takes the repository's settle lock, so each land sees the base
  the previous one left.
- An event-driven evaluation reads the PR past the GitHub poll windows and
  caches. A sweep evaluation reads it as the pass did.

When more PRs wait than a bound lets run, the daemon looks up their hk
priorities, waits for the lookups at most two seconds, and starts the PRs in
priority order (1 first, none last, ties in arrival order). With no bound
nothing waits, so no lookup is made.

## The sweep

The `pr` role still wakes every `daemon.pr.interval`, and still wakes early on a
webhook or a finished reviewer job. Its pass no longer evaluates PRs. It lists
the open PRs, posts the Discord feed, puts each PR on the queue as source
`sweep`, reconciles outcomes of PRs that left the open list, and returns the
errors of the evaluations that ended since its last run. The interval therefore
bounds how long a missed delivery goes unnoticed. It does not bound how soon a
PR is looked at.

## What it logs

| Line | Fields |
| --- | --- |
| `pr evaluated` | One per evaluation: `repo`, `pr`, `source`, `queue_wait_ms` (event to start), `eval_ms`. |
| `pr pass` | One per repository after each sweep, as before (`prs_skipped`, `prs_evaluated`, `evaluated_reasons`, `reviews_concurrent_max`), now counting every evaluation since the last line, plus `queue_depth` (items waiting), `queue_wait_max` and `merge_overdue` (the overdue notices delivered since the last line, see `docs/config.md`). |
| `github requests` (role `pr`) | The sweep's own requests, plus `queue_depth`, `queue_wait_max` and `eval_requests` (requests the evaluations made since the last line). |

Each evaluation is its own `pr pass` Sentry transaction; the sweep is another.
An evaluation's errors are kept and returned by the next sweep of its
repository, so the role's failure count, backoff and health ping behave as they
did.

A restart drops the queue. The first sweep after it queues every open PR.
