# PR event queue

The `pr` role evaluates one PR when something happens to it. It does not wait
for a whole pass over the repository. A slow review holds its own PR and no
other.

## What the queue holds

The queue holds items of the form (repository, PR). An item enters it from one
of seven sources. The `source` field of the `pr evaluated` log line names them.

| Source | What puts the PR on the queue |
| --- | --- |
| `webhook` | A signed `pull_request*` delivery that names the PR, by PR number and head commit. |
| `check` | A signed `check_run` or `check_suite` delivery that completed, or a `status` delivery, naming the PR by the numbers it lists or by its head commit. A commit that no known open PR has as its head names nothing. A check that has not completed enqueues nothing: it has no result to evaluate, and its completed delivery follows. |
| `reviewer` | A reviewer job of the PR finished (the reviewer watch). |
| `base` | A push to the repository's base branch (`repos[].branch`). Every open PR of the repository is queued, because its merge gate reads the base. |
| `status` | A fresh `GET /v1/pr-status?fresh=1` read found the PR changed. |
| `timer` | A reviewer's deadline: the PR is evaluated once its reviewer is past `review.timeout`, so a reviewer that never finishes is replaced. |
| `sweep` | The periodic pass, below. |

The `webhook`, `check` and `base` sources need `daemon.webhook`. The `reviewer`
source needs a recorded reviewer job. The queue and the sweep run whenever the
`pr` role is enabled.

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
- An evaluation that only check deliveries queued reads the PR without forcing
  fresh evidence when the check ran on the head the queue knows for it: the
  check result is the new fact, not the PR. Any other source, and a check that
  named another head, reads the PR past the GitHub poll windows and caches, as
  every event-driven evaluation did. A sweep evaluation reads it as the pass
  did.

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

Each repository's sweep has a five-minute deadline. A timeout returns an error
for that repository naming the step (`open_pulls`, `bot_policy`, `feed` or
`reconcile`), while the other repositories finish their results and the pass
records its finish. The deadline is fixed in the package; there is no config
key. If a sweep is still running after its deadline because an operation did
not return on cancellation, later passes skip that repository and log `pr
sweep still running` with its repo, step and age. The sweep is never started a
second time for that repository while it remains in flight. Once it returns, a
later pass sweeps the repository normally.

## What it logs

| Line | Fields |
| --- | --- |
| `pr evaluated` | One per evaluation: `repo`, `pr`, `source`, `queue_wait_ms` (event to start), `eval_ms`, and the phase fields below. |
| `review-start latency` | One per reviewer pass whose spawn panewire accepted, see below. |
| `pr pass` | One per repository after each sweep, as before (`prs_skipped`, `prs_evaluated`, `evaluated_reasons`, `reviews_concurrent_max`), now counting every evaluation since the last line, plus `queue_depth` (items waiting), `queue_wait_max` and `merge_overdue` (the overdue notices delivered since the last line, see `docs/config.md`). |
| `github requests` (role `pr`) | The sweep's own requests, plus `queue_depth`, `queue_wait_max`, `eval_requests` (requests the evaluations made since the last line), `eval_routes` (those requests broken down by route, method and status, busiest ten first, as `routes` does for the sweep) and `gate_wait_ms` (the longest wait one of them spent queued for its credential's gate). |

### Where an evaluation spends its time

The phases are disjoint and add up to `eval_ms` (the few milliseconds between
them are not counted). All values are milliseconds.

| Field | What it covers |
| --- | --- |
| `read_ms` | The first reads: the PR, the bot policy and the skip plan. |
| `observe_ms` | Recording the PR's outcomes. |
| `review_ms` | The review claim check, the priority lookup and the review call, spawn included. It ends when the evaluation starts to wait for the settle lock. |
| `lock_wait_ms` | Waiting for the repository's settle lock. |
| `settle_ms` | Holding that lock: findings, notice and land. |

Two more fields sit inside those phases and are not added to the sum:

| Field | What it covers |
| --- | --- |
| `spawn_ms` | The longest time one review pass spent in its panewire spawn calls, in `review_ms`. A daemon that cannot read hk job records waits for the reviewer in that call, up to 30 seconds, so it reads as that wait. |
| `gate_ms` | The merge gate, in `settle_ms`. A land that merges runs it twice. |

`settling` is present only when an evaluation held something back while
GitHub's evidence caught up, and lists the reasons joined by `+`:
`review_files_settling` (the review did not start: the PR's `changed_files` and
its file list disagree just after a push), `head_moving` and `files_settling`
(the merge gate's verdicts for the same two causes). Nothing queues the PR
again after such an evaluation; the next delivery or the next sweep does.

### Head to reviewer

`review-start latency` answers how long a pushed head waited for its reviewer.
The fields are `repo`, `pr`, `head`, `pass`, `first_seen` (RFC 3339), `first_seen_by`
(`webhook` or `eval`), `evals` and the steps in milliseconds:

| Field | What it measures |
| --- | --- |
| `seen_to_first_eval_ms` | First seen to the start of the first evaluation that read the head. |
| `first_eval_to_eval_ms` | That start to the start of the evaluation that spawned the reviewer; `0` when the first evaluation spawned it. |
| `eval_to_accepted_ms` | The start of the spawning evaluation to the moment panewire accepted the spawn. |
| `total_ms` | The three steps added: first seen to spawn accepted. |

A head is first seen when the first delivery that names its PR and its commit
arrives (`webhook`), or when its first evaluation starts (`eval`), whichever is
earlier. The time is kept per head in `head-seen.json` in the repository's state
directory, so a restart does not reset it, and entries older than 48 hours are
dropped. `evals` counts the evaluations that read the head since the daemon
started. A pass that re-attaches a reviewer an earlier evaluation placed logs
nothing.

Each evaluation is its own `pr pass` Sentry transaction; the sweep is another.
An evaluation's errors are kept and returned by the next sweep of its
repository, so the role's failure count, backoff and health ping behave as they
did.

A restart drops the queue. The first sweep after it queues every open PR.
