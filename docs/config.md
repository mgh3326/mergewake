# Configuration reference

Each host has one operator-owned YAML file, normally
`~/.config/mergewake/config.yaml`. Each repository has a policy at
`.mergewake/policy.yaml`. Unknown keys, duplicate YAML keys, and multiple YAML
documents are errors. There is no environment interpolation except expansion of
a leading `~/` in local config and state
paths. Credentials belong in the environment, the external CLI's own credential
store, or a mode-0600 file the config names (`reap.hub.token_file`,
`discord.webhooks_file`).

Use [examples/host.yaml](../examples/host.yaml) and
[examples/policy.yaml](../examples/policy.yaml) as starting points. Their repo,
host, model IDs, commands, and task ID are placeholders. None is a discovery rule.

## Host file

| Key | Required / default | Meaning |
| --- | --- | --- |
| `version` | Required: `1` | Host-config schema version. |
| `host` | Required | Operator-supplied host label, used in receipts and notifications. Use a public-safe placeholder in shared configs. |
| `state_dir` | `~/.local/state/mergewake` | Private local audit, checkpoints, receipts, locks, and journals. Keep it outside Git and install roots. |
| `token_env` | `GITHUB_TOKEN` | Name of the environment variable containing the GitHub credential. Its value is never a config key. |
| `github_app` | Absent | Serve GitHub reads with a GitHub App installation token; see [GitHub App reads](#github-app-reads). Absent means the `token_env` credential serves every request. |
| `poll_interval` | `60s` | Positive Go duration between completed `watch` iterations. |
| `watch_limit` | `100` | Maximum commit interval inspected during recovery, from 2 through 1000. |
| `repos` | Empty list | Repositories this host may operate on. Names must be unique. |
| `hosts` | Empty list | Session hosts scanned by `reap`; see below. |
| `reap` | Disabled | Merged-PR session/worktree reaping; see below. |
| `daemon` | All roles disabled | Role config and explicit acting opt-in; see below. |
| `review` | Empty object | Independent-review configuration described below. |
| `notifier` | No-op | Notification adapter described below. |
| `sink` | No-op | Task and report adapter described below. |
| `liveness` | Optional | Daily made-against-done liveness checks across automatic mechanisms; see below. |

### `repos[]`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `name` | Required | GitHub `owner/name`; commands cannot select an unconfigured repo. |
| `branch` | `main` | Base branch for landing, CI watching, and recovery PRs. |
| `policy` | `.mergewake/policy.yaml` | Clean relative policy path inside the repository. Loaded through GitHub at a trusted base commit. |
| `root` | Empty | Local checkout of the repository. Review builds its local diff and detached PR-head worktrees from it, so a repository that is reviewed needs one. Leading `~/` expands. |
| `task` | Empty | Optional fallback numeric handoffkeep task ID when the originating PR has no `Mergewake-Task` trailer. |
| `lane` | Empty | The repo's design-session (owner) lane: receives this repo's PR events when neither the task trailer nor a `Mergewake-Notify-Lane` trailer names one, and the human-judgment notices and holds (review failures, lost review results, review blocks, aged check failures, and out-of-scope or unacknowledged findings handed to the owner). Empty falls back to the desk (`notifier.fallback_lane`, else `notifier.lane`). |
 | `hk_task_events` | `true` | With the `handoffkeep` sink, post a task event to hk's `POST /v1/task-events` for each PR with a `Mergewake-Task` trailer: `mergewake.pr` when an open, ready PR is first observed, `mergewake.review` when a review run claims a pass, `mergewake.merge` after mergewake's own merge, and `mergewake.revert` when a watch revert PR is merged (a revert merged more than seven days before mergewake first sees it is not posted). Dry runs and shadow mode post nothing. A failed post never fails the action; each post is an `hk-task-event` row in `decisions.jsonl`. Set `false` for a repo whose hk predates the endpoint (handoffkeep#136). |
| `review.checkout_exclude` | Empty | Sparse reviewer checkout (hk 5099): git non-cone patterns excluded from a spec-placed reviewer's node worktree, relative to the repo root, without a leading `!`, `/` or `-`. Empty, the default, means today's full checkout. At most 64 patterns, each at most 4096 bytes, with no `..`, NUL or newline and no leading `-` (argv safety); invalid config fails at load. A PR path that would read as a flag (leading `-` or a control character) takes the full checkout for that spawn. A non-empty list adds one `--checkout-exclude` per pattern and one `--checkout-include` per PR-changed path (added, modified or renamed; deleted files excluded; at most 500 paths). More than 500 changed paths takes the full checkout and writes a `reviewer-checkout-sparse-skipped` audit row. An older hub or CLI that refuses the new flags falls back to a full spec spawn for 10 minutes (`reviewer-checkout-sparse-refused` row); a node with git older than 2.37 falls back to a full checkout with `checkout_mode=full`. The spawn answer and job record carry `checkout_mode=sparse\|full`, recorded on the `reviewer-spawn` audit row when present. The reviewer brief names the patterns and tells the reviewer to run `git sparse-checkout list` (or check `core.sparseCheckout`); absent files under those patterns are not findings, read one with `git show HEAD:<path>`. |
| `reap.regenerable` | Empty | Per-repository additions to `reap.regenerable`, validated with the same rule (a trailing `/` is an explicit directory approval, matching a wholly ignored directory of that name at any depth). Additive to the global list and used only for this repo's passes, so panewire alone can approve its `.local/` test state dir without exposing other repos. Empty, the default, means the global list. |

### `hosts[]`

Session hosts where agent panes may run, used only by `reap`.

| Key | Required / default | Meaning |
| --- | --- | --- |
| `name` | Required | Unique label for receipts; same alphabet as `host`. |
| `ssh` | Empty / `local` | Empty or `local` (with no `machine`) runs commands on this machine. Any other value is a single `ssh(1)` destination such as `ops@node.example`, used by the ssh transport; options, whitespace and leading `-` are refused. |
| `machine` | `name` | The panewire machine id a remote host is reached as under the panewire transport. Setting it makes the host remote; under the ssh transport such a host also needs `ssh`. A job-bound lead's worktree is looked for only on the host whose `name` or `machine` equals its hk job record's `host` (panewire's `HUB_MACHINE_ID`), so name a local host after its machine id. |
| `jobs` | Empty | Base directory holding one job directory per named pane (`<jobs>/<pane name>`; `<jobs>/<job id>` for a job-bound lead). Reap's archive stage enumerates and archives these dirs; leading `~/` expands for local hosts only, ssh hosts must give an absolute path because `~` does not expand through the quoting used. Unused through the hub, where the node uses its own jobs inbox. |
| `work_root` | Empty | The node's work root: the directory panewire prepares a spec-placed job's checkout in, as `<work_root>/<label>-checkout`. The reviewer sweep builds that path when no record names a reviewer worktree, so without it such a label can only end with a terminal `kept` row. Absolute; leading `~/` expands. |

### `reap`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Gates the `reap` command and daemon role. |
| `act` | `false` | Reap's own acting opt-in, separate from `daemon.act`. Without it every mode — including `--apply` — stays observational. Requires `enabled`. |
| `worktree_after` | `0` | Optional extra delay before a merged PR's worktree may be removed, added to the watch verdict below. `0`, the default, means the verdict alone decides: the worktree goes as soon as main's CI after the merge is green, never at the merge. A positive value keeps the worktree that long after the merge even once the verdict is healthy. |
| `repo_budget` | `3m` | Time limit for one repository's daemon reap pass. At expiry, deadline-aware reads and commands are canceled and Reap stops at the next safe boundary. A guarded worktree removal or branch deletion already in flight uses its own 1-minute bound, independent of the repository budget, while daemon shutdown still cancels it. The daemon logs the remaining stage or in-flight operation and continues with the next repository. Within one daemon reap pass each host is scanned once, and the later repositories reuse that scan; a host whose scan failed stays failed for the pass, and every removal still re-reads the host's panes first. Remaining work is retried from discovery on the next pass; receipts suppress already completed work. The worker-worktree stage runs on two thirds of the budget the pass has left, so the reviewer sweep and what follows keep the rest; a worker stage the budget cuts resumes after its last candidate on the next pass. Three minutes gives each repository about 11% of the observed 27.5 minute median whole pass, preventing one repository from consuming most of the serial pass. |
| Reap pass log fields | — | The daemon emits one `repository reap completed`, `repository reap failed` or `repository reap stopped on budget` JSON line per repository. `repo` identifies the repository; `total_ms` is wall time; `git_ms`, `github_ms`, `hosts_ms`, `panewire_ms` and `hk_ms` are accumulated call times; matching `*_calls` fields count calls. GitHub time is HTTP request time, `git_ms` is local git command time, `panewire_ms` counts Panewire CLI calls, `hosts_ms` counts SSH/host commands and remote hub operations, and `hk_ms` counts task/job lookup calls. A failure line includes `error`; a budget line has one `left` field identifying either the next stage or the bounded in-flight operation, and includes `error` when independent failures occurred. |
| `scan_after` | `720h` | Merged-PR listing window: only PRs merged within this span are discovered each pass. Older work still arrives via prior `reap.jsonl` rows. |
| `legacy_every` | `24h` | How often the daemon's reap role runs the legacy sweep for a repo; `0` or `off` disables it. The last attempt per repo is kept in the state dir, so a restart inside the interval waits. |
| `balance_min_created` | `20` | Daily per-host created count at or above which the reviewer balance may notice: quiet hosts never alert. |
| `balance_max_removed_pct` | `70` | Removed share of created, as a percent, below which the reviewer balance notices a stalled host. |
| `balance_window_days` | `7` | Trailing window, in days, the reviewer balance cumulates the open balance over. |
| `balance_lane` | Desk lane | Lane the reviewer-balance stall notice goes to; empty means the desk (notifier.fallback_lane, else notifier.lane). |
| `transport` | `panewire` when `hub.url` is set, else `ssh` | How remote hosts are reached: `panewire` sends every remote operation through the hub's node commands; `ssh` runs `herdr`/`git` over `ssh(1)`. Local hosts always run locally. |
| `hub.url` | Empty | Panewire hub origin, `https://` (plain `http://` only on loopback), with no path, query or userinfo. Required by `transport: panewire`. |
| `hub.token_file` | Required by `panewire` | Absolute path (leading `~/` expands) to the mode-0600 env file of a scoped hub service credential: `HUB_MACHINE_ID=svc-mergewake` and its `HUB_TOKEN` (see "Hub credential" below). A file whose `HUB_MACHINE_ID` is `operator`, or that has none, is refused when the config loads and again when each pass reads it; a file other users can read is refused. |
| `hub.cf_file` | Empty | Optional mode-0600 env file with `CF_ACCESS_CLIENT_ID` and `CF_ACCESS_CLIENT_SECRET` for a hub behind Cloudflare Access. |
| `archive` | Disabled | Job-dir archiving to a synced Google Drive folder plus hk; see `reap.archive` below. |
| `regenerable` | `.venv`, `node_modules`, `__pycache__`, `.pytest_cache`, `dist`, `build`, `target`, `obj`, `bin`, `Library`, `Temp`, `Logs`, `UserSettings`, `.venv/`, `node_modules/`, `__pycache__/`, `.pytest_cache/`, `Library/`, `Temp/` | Gitignored path globs that are expendable (build/cache output), so a worktree holding only them may still be removed. The list goes to the node as configured: a slash-free name approves ignored entries of that name, and only a pattern ending in `/` approves a wholly ignored directory — as a content-blind, whole-directory approval, exactly as the node documents it. A slash-free `name/` matches a wholly ignored directory named `name` at any depth, so `node_modules/` also approves `web/node_modules/`, while a pattern with an inner slash (`dotnet/*/bin/`) matches the whole path only. A leading `/` (such as `/build`) is refused at load, so anchor a path with an inner slash instead. That is why the directory form goes only to tool-owned caches (`.venv/`, `node_modules/`, `__pycache__/`, `.pytest_cache/`) and generated directories (`Library/`, `Temp/`): a pattern ending in `/` deletes the whole directory with whatever it holds, so list only directories a tool owns. Globs match as the node's path.Match does: `*` stays within one path segment, and `**` is not a recursive wildcard (`vendor/**` reaches `vendor/mod` but not `vendor/mod/x.go`). An ignored file inside a tracked `bin/` or `Logs/` dir is not regenerable just because an ancestor shares the name. Wildcard-only patterns (`*`, `/`) are refused at load, as the node refuses them. Any other ignored path — `.env`, notes, unknown output — refuses removal. A non-empty configured list replaces the defaults, directory forms included; `repos[].reap.regenerable` adds per-repo entries on top. |
| `disposable` | `report.draft.md` | Untracked files a worker worktree may still hold when it is removed, as worktree-relative paths matched exactly. A worker's report draft lives in hk, so a leftover copy inside the checkout is expendable; the pass never removes a worktree holding any other untracked file. An empty list means the default; entries must be relative and must not contain `..`. The list goes to the hub as `worktree.remove`'s `disposable` argument; an older node refuses the request, mergewake retries without the argument, and the node's own dirty check then keeps the worktree with a `skipped | dirty` row until a node accepts the list. |
| `worktree_drain.per_host` | `20` | Maximum guarded removals the worker-worktree pass asks of one host in one pass; the host's remaining worktrees wait for the next pass. Zero means the default. It caps removals only, so one host's backlog never stalls the others. |
| `worktree_drain.host_order` | Config host order | Host names in the order the worker-worktree pass visits them, first to last; hosts it does not name follow in `hosts[]` order. With `per_host`, this decides which host's backlog drains first. |
| `reviewer_drain.per_host` | `20` | Maximum hub guarded removals the reviewer sweep asks of one host in one pass; the host's remaining labels wait for the next pass. Zero means the default. It does not cap pane or job reads, only removals, so one drained host never stalls the others; a label left pending for a missing `work_root` spends none of it. |
| `reviewer_drain.host_order` | Config host order | Host names in the order the reviewer sweep visits them, first to last; hosts it does not name follow in `hosts[]` order. With `per_host`, this decides which host's backlog drains first. |

**Legacy sweep.** `mergewake reap --legacy [--repo owner/name] [--host H] [--dry-run|--apply]`
runs one pass by hand; the daemon's reap role also runs the sweep for every
configured repo, over all hosts at once, on `reap.legacy_every`. Both cover
worktrees whose PR merged at least 14 days ago, however long ago (the daemon
only sees merges inside `scan_after`; this ignores that window). A worker
worktree (`wk/<job>`) is out of its scope: the worker-worktree pass above owns
it on every reap pass, so this sweep's 14-day floor never delays or blocks
one. Candidates come from two sources merged
by worktree path. On each host the sweep takes the common git dirs of the
worktrees reap already inspects (pane cwds and job cwds) whose origin is the
configured repo, and pages the node's `worktree.list` for each; every listed
worktree except the main checkout and `mwr-*` checkouts is a candidate. It also
reads every closed PR of the repo and follows `Mergewake-Task` → hk task →
bound job → the job record's host and cwd. A listed dirty or untracked worktree
is kept `dirty` and one whose status could not be read is kept `status-error`,
before any GitHub call. The sweep asks the node about each worktree
(`worktree.inspect`) and removes the worktree only
through the node's guarded `worktree.remove` (`detached:false`), and only when
the PR is merged into the base at least 14 days ago, the head is the merged
head or inside the merged work, no pane or process is in it, and the node
finds it clean. A worktree whose branch has no PR, or whose newest closed PR
did not merge, is removed the same way only when hk positively names its task
`dropped` or `duplicate`, last updated at least 14 days ago (hk exposes no
separate close time). The task comes from the PR's `Mergewake-Task` trailer
and from the job records whose cwd or recorded worktree is the path; every
identification must agree on one task, a task read that fails or a task in any
other state keeps the worktree (`no-task`, `task-read-error`, `task-<state>`,
`task-too-young`), and the same ancestry, pane and node-guard checks apply.
The node decides cleanliness at removal; a plan never probes
it, so a plan row says `would-remove` with reason `node guard decides
cleanliness`. Worktrees of other repos, branches with an open
PR, detached worktrees and anything unclassifiable are kept with
the reason; `mwr-*` reviewer checkouts, attached or detached, are left to the
reviewer sweep and get no row. A final node refusal (`ignored-files`, `dirty`, `worktree-changed` and similar)
is recorded as `refused` and never retried by a rerun; a refusal that says try
again (`busy`, `pane-in-use`, `pane-scan-failed`, `git_too_old`) is recorded as
`kept` and asked again on the next run. Just before each removal the sweep
re-reads open PRs and re-inspects the worktree, and keeps it if the branch has
an open PR or the checkout changed. Only PRs whose head is in the repo itself
count: a fork's branch of the same name neither blocks nor selects a worktree.
The newest closed PR of the branch decides, bound to a job or not. Every worktree gets one `legacy-sweep` row in `reap.jsonl` (plans too),
except a row that repeats the path's newest outcome for the same branch and head: a replacement checkout at that path — another
branch, or the same branch at another head — is always written, so the newest row names the checkout that was removed. A row for a
path an earlier sweep finished (removed or refused), reached again only through a stale job cwd and reported gone by inspection, is
tallied as already-removed and not written again. Each host ends with a summary of counts by result and reason, plus `listed`,
`candidates`, `would_remove`, `kept_by_reason`, `commons` and `commons_not_found`
(commons the node refused to list). PRs with no
trailer, no bound job or no job record cannot be reached this way and are only
counted. Only panewire-hub hosts can be swept. The daemon's reap ignores
`legacy-sweep` rows.

The daemon runs the sweep from its reap role, at most once per repo per
`reap.legacy_every` (default `24h`; `0` or `off` turns it off). The last
attempt per repo is written to `<state_dir>/<repo>/legacy-sweep.json` before the
sweep starts, so a restart inside the interval does not sweep again and a
failing sweep waits for the next interval instead of running on every reap
pass. A corrupt `legacy-sweep.json` is treated like a missing record — the
sweep runs and the stamp write repairs the file — while a read failure that is
not a decode error (permissions, say) stays not-due, since the write would fail
the same way; both are audited as `reap-legacy-error`. It acts under the same
gates as the pass it rides on: `reap.act` decides whether a removal is real,
and daemon shadow mode plans only, writing `would-remove` rows. Each repo's
sweep covers all its hosts at once, keeping one set of per-sweep caches. A
failure is audited as `reap-legacy-error` and logged with the daemon's other
errors; the remaining repos and the regular reap pass still run.
`mergewake reap --legacy` runs one pass by hand and does not move the daemon's
schedule.

**Reviewer balance.** Once a day, the first whole-repo reap pass after the UTC
day boundary counts reviewer checkouts created and removed per host across all
watched repos and writes one `reviewer-balance-daily` audit row per host into
the first configured repo's `decisions.jsonl`: `day`, `host`, `created`,
`removed`, the open balance (`open_balance`, created minus removed cumulated
over `reap.balance_window_days`) and `window_days`. Checkouts with no host in
any record are tallied as `unattributed` and reported but never notice.
Created counts reviewer spawns: each pass audits one `reviewer-spawn` row with
the placed label and host (older labels count by their label timestamp once
they left checkout evidence). Removed counts distinct checkout labels with a
`reviewer-cleanup` or `reviewer-reap` row with result `removed` that day, with
the host joined from the reap receipt when the audit row names none. When a
host created at least `reap.balance_min_created` checkouts and removed less
than `reap.balance_max_removed_pct` percent of that (defaults 20 and 70),
mergewake sends one notice per host per day to `reap.balance_lane` (or the
desk lane when unset) through the lane-event notify path, naming the host and
the counts; the day-plus-host sent key suppresses repeats and a
`reviewer-balance-notice` row records the delivery. `mergewake reap-balance
[--repo owner/name] [--day YYYY-MM-DD]` prints the same numbers on demand
without writing or noticing. The balance
runs only when the reap acts (not in shadow or dry-run) and advances one day
at a time under the state dir's `reviewer-balance.json` lock, so a crash
between days repeats at most that day. A retried day skips the daily rows it
already wrote and reuses the delivered-notice keys, so neither audit rows nor
notices duplicate.

**Loop liveness.** Once a day, alongside the reviewer balance, the first
whole-repo reap pass after the UTC day boundary runs table-driven made-against-done
liveness checks across mergewake's automatic mechanisms (review passes, reviewer
landing, checkout removal, merges, CI watch reverts, task events, outcome publication,
legacy sweep, finding dispatch, and review retries). See [`liveness`](#liveness) below.

`mergewake reap --repo owner/name [--pr N] [--dry-run|--apply]` and the `reap`
daemon role clean up merged-PR worktrees once they age out. Reap never closes
a pane: panewire's node closes an ended job's pane on pane-id + harness-session
proof (reviewer and consult jobs at completion or loss, implementer jobs when
the task is merged, deployed, dropped or handed over, `keep=true` panes 24 h
after completion), so a worktree still in use simply waits as `pane-in-use`
until that close lands. On deploy, panes of ended jobs close within an hour
via panewire; rpi is offline and still runs an older panewire without the
node close, so its panes wait for hk 4716.

For each merged PR on a configured repo's base branch, every host is scanned
with `herdr agent list` — remote hosts through the panewire hub (one
`GET /v1/nodes/agents` per pass, see "Panewire transport" below) or, under
the ssh transport, over `ssh <ssh> herdr agent list`. A pane matches only when its `cwd` is a linked git worktree, its
current branch equals the PR head branch, its checked-out commit equals the
PR head SHA, and the worktree's origin remote names the configured
repository. Titles and names are never used for matching; a reused branch
whose tip moved on does not match.

The head commit is read once per PR per daemon process, shared with the task
lookup below; a worktree that matches the head exactly costs no extra call.

#### Reviewer checkouts

A review pass creates one checkout per reviewer and never removes it itself
while the pane may still run: a detached worktree at the PR head under
`review.panewire.checkouts/<label>` on this machine, or, for a spec-placed
reviewer, the node's own worktree named `<label>-checkout` under that host's
`hosts[].work_root` (panewire's spec placement; the pass never holds its path).
Every pass end writes one `reviewer-cleanup` receipt for the label, naming the
worktree it holds, and leaves the pane for panewire's node to close.

The reviewer sweep removes a checkout once no pane uses it and the label is
older than `review.timeout` plus one minute — never because a pane is still
listed, and never earlier: the pane that spawned the label may still be
reviewing. Candidates come from those receipts, from a live pane named
`<label>`, and from the reviewer rows in `decisions.jsonl`, which name labels
whose receipts predate the label field; that journal is read incrementally, by
byte offset, in bounded windows, so the leaked backlog drains without a hand
step and no row is passed over. A remote worktree goes only through the hub's
guarded `worktree.remove` (`detached: true`), which refuses a paned, dirty or
different worktree; a local checkout is removed only when it is clean, detached
at the reviewed head and no local pane runs in it. A refusal is a
`pending-cleanup` row with the guard's reason and is asked again after a day,
three times, then final. When no record names the worktree's path, the sweep
builds `<hosts[].work_root>/<label>-checkout`; a host that names no work root
leaves the row pending with `hosts[<name>].work_root` named, retried every pass
and never counted against the drain cap, so configuring the key releases the
label. Only a label no record names a host for ends with a terminal `kept` row. `reap.reviewer_drain.per_host` caps the guarded removals
one host takes in one pass (default 20) and `reap.reviewer_drain.host_order`
fixes which hosts are visited first; a host's remaining labels wait for the
next pass.

#### Job-bound leads

A merged PR whose body or head commit carries `Mergewake-Task: <id>` is
reaped through its job as well as its worktree: the same binding the findings
router uses — hk task → bound job (`refs.job_id`) → the job record panewire
wrote at spawn (`host`, `cwd`). The binding is read once per merged PR per
daemon process (a task with no job yet is asked again each pass) and names a
third worktree source beside live panes and prior `reap.jsonl` rows: the
record's host (mapped to a configured host by `name` or `machine`) and cwd,
which outlive any pane. A PR without the trailer, or whose task has no bound
job, keeps the worktree match; an hk read error only loses that source
that pass.

Enabling reap (the desk): run `mergewake reap --repo <repo> --dry-run`
once per configured repo and compare the planned `would-remove` rows with the
worktrees you expect, then set `reap.act: true`.
Rollback: `reap.act: false`.

A merged PR's worktree is removed once the watch verdict for its merge commit
on main is healthy: the last green main head recorded by the watch role is the
merge commit or a descendant of it. `reap.worktree_after`, when positive, adds
an optional delay on top. A revert changes the rule: while its recovery PR is
open the worktree stays, and a reverted PR's worktree stays until the revert's
own merge is covered by a healthy verdict. A revert is opened from main through
GitHub's API (`internal/github/recovery.go`), never from the worker's
worktree, so the worktree is not needed to undo a merge. Removal only runs for
PRs that name a candidate worktree on some configured host (a matching pane
cwd, a prior `reap.jsonl` row, or the job record of the task the PR carries)
or for a `wk/<job>` worktree the host's own worktree list names; a merged PR
with nothing to reap produces no rows and no follow-up GitHub calls. The third source reads the record of the task's bound
job — the binding the job path above already made, so no extra task read and no
GitHub call — and takes its `host` and `cwd`, mapped to a configured host by
name or machine id. It reaches a worktree whose pane panewire's node closed
before reap looked at it; the `remove-worktree` row's `source`
says which of the three named it. Only the task's bound job is read: a
hand-over's earlier attempts are not, so their worktrees still need a pane or a
receipt. A record that cannot be read yields no candidate and is reported, and
never counts as a missing job; the pass summary's `job_records` counts what the
source reached (`prs`, `worktrees`) and what it could not (`no-task-trailer`,
`no-bound-job`, `no-job-record`, `job-without-cwd`, `job-host-unconfigured`,
`binding-error`, `job-read-error`), for merged PRs past `worktree_after`.
The API version the client sends carries no `merge_commit_sha` on REST pull
objects, so when candidates do exist it is resolved from GraphQL for that PR
before the revert check. The
revert knowledge comes from the watch role's recovery-PR convention: a
recovery merged within seven days of the PR's merge, or one still open, skips
cleanup. Removal also requires the worktree to still exist in `git worktree
list`, to be clean, and to hold no gitignored files outside the
`reap.regenerable` allowlist (`git status --porcelain --ignored`: regenerable
build and cache output is expendable, anything else — `.env`, notes, unknown
output — refuses removal, since `worktree remove` would delete it silently;
the refusing paths land in the receipt's `files` field, the reason is a
count-plus-hash `ignored-files:N-xxxxxxxx`), to have no live pane (agent or
plain shell, from `herdr pane list`) using it — the pane list is re-read
immediately before each removal — and to have no unpushed commits: the branch
must merge
into the base branch (remote-tracking `origin/<branch>` first, then local) or
be fully pushed upstream. Anything dirty, unknown or ambiguous is skipped with
the reason recorded; removal is `git worktree remove` without `--force` and the
local branch is deleted with `git branch -d`, never `-D`. The remote branch
stays. Worktrees are found from the panes, from prior `reap.jsonl` rows and from
the job record of the PR's task, so a worktree whose pane panewire already
closed still gets cleaned. A `wk/` worktree is not this path's: it is skipped
here and left to the worker-worktree pass below. On a local or ssh host this
path checks the machine's processes immediately before its `git worktree
remove` and keeps a checkout a process holds (a process scan that cannot read
every process keeps it too); under the hub transport the node's guard decides.

#### Worker worktrees

A worker's checkout is recognised by its branch: `wk/<job>`, where `<job>` is
the hk job that worked it. This pass is the only code that removes a `wk/`
checkout: the older merged-author cleanup above skips it, in whole-repo and
PR-scoped passes alike. The pass runs on every whole-repo `reap` pass and
discovers candidates through the hub's node commands only. Each clone's
registered worktrees are listed by its git common dir: the common dirs reap
already knows from panes, and the configured clone
`<hosts[].work_root>/<repo>` when that host names a work root. A clone guessed
from that path is admitted only after a registered worktree inspects as the
configured repository and the same common dir; a stale registration is
inconclusive and the next candidate is tried, a verified mismatch refuses the
clone, and none proven keeps it out. A host with no node mapping or no guarded
removal never falls back to ssh or local commands: a `wk/` checkout a pane
already named stays, with one `hub-required` row. The main checkout and
`mwr-*` reviewer checkouts are never candidates.

A candidate goes when its work is on GitHub:

- A merged PR's worktree goes once the watch verdict for its merge commit on
  main is healthy (the last green main head is the merge commit or a
  descendant), so a merge that broke main waits while its revert, if any, is
  open and until that revert's own merge is green. The head branch is never
  deleted on the remote, and a reverted PR's work stays fetchable from GitHub.
- A worktree whose newest PR closed unmerged, or that has no PR at all, goes
  once the hk job record encoded by `wk/<job>` on that host is terminal
  (`completed`, `abandoned`, `escalated` or `lost`). A live job, an unknown
  job, a record naming another host, or a PR binding a different job
  (`job-conflict`) keeps it.

Removal goes only through the node's guarded `worktree.remove`, with the
disposable list. The node must find the worktree registered and unlocked, at
the inspected commit, with no modified tracked file and no untracked file
outside `reap.disposable`; gitignored files must match `reap.regenerable` (so
Unity's `Library/`, `bin/` and `obj/` count as expendable when listed there, as
the defaults do); the branch must be released — its origin head equal to the
checkout, or, when the remote ref is gone as delete-branch-on-merge leaves it,
the checkout's commit in the base the node proves — and no live pane (agent or
plain shell) may use it. Immediately before the call the pass revalidates the
whole candidate through the node — same origin, common dir, path, branch and
head — and keeps a checkout that changed. The pass deletes nothing itself, so a
refused removal leaves the checkout and its report draft as they were. Anything
refused stays and gets one `remove-worktree` row with its precise reason —
`unpushed`, `dirty`, `pane-in-use`, `locked`, `ignored-files:<count>-<hash>`, a
guarded-removal refusal, or a rule keeping it (`watch-pending`,
`recovery-open`, `reverted`, `job-active:<state>`, `job-conflict`, `open-pr`).
A checkout whose only extra file is a disposable entry stays `skipped | dirty`
until the node accepts `reap.disposable` (see the `disposable` key above). The
row's `source` is `worktree-list` for a candidate only the listing reached.

#### Orphan worktrees

`reap.orphan_worktrees` adds a shadow-first pass for registered linked
worktrees one directory below each configured `hosts[].work_root`. It never
considers the main clone, unregistered directories, `wk/` worktrees owned by
the worker reap, or paths recorded by the reviewer reap.

```yaml
reap:
  orphan_worktrees:
    enabled: false
    act: false
    min_age: 14d
    per_host: 20
```

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Runs the orphan worktree listing and inspection stage. When false, the stage does no work and uses no budget. |
| `act` | `false` | Enables removal for candidates that pass every guard. It also requires the top-level `reap.act: true` and an acting reap invocation. |
| `min_age` | `14d` | Minimum age based on the newest mtime among the linked worktree's git index, `HEAD`, `logs/HEAD`, and top-level directory. Accepts Go durations and whole days. |
| `per_host` | `20` | Maximum candidates considered per host and repo pass. A persisted rotating cursor advances between passes. |

When enabled, this stage runs after the worker, reviewer, orphan-checkout, and archive stages. It receives one quarter of the repo budget remaining at that point; each candidate checks the stage budget before inspection.

Before a worktree is planned or removed, the pass requires all four checks:

1. `git --no-optional-locks status --porcelain --ignored` reports no modified
   or untracked files, and every ignored path matches `reap.regenerable`.
2. Its `HEAD` is an ancestor of the fetch-free `origin/<default branch>` ref,
   or its configured upstream ref resolves to the exact `HEAD` commit.
3. The newest of the gitdir `index`, `HEAD`, `logs/HEAD`, and worktree root
   mtimes is older than `min_age`.
4. A fresh full pane-cwd scan finds no pane using the path; acting passes
   repeat that scan immediately before `git worktree remove`.

Unknown evidence keeps the worktree with a reason. Each considered path writes
an `orphan-worktree` row with `would-remove` or `kept`; the `kind` is `exp`,
`detached`, `branch`, `wk-no-pr`, or `other`. The CLI also prints counts by
host and kind for routing-policy review before `act` is enabled. On panewire
hosts the current `worktree.list` response does not provide the no-lock status
paths, filesystem mtimes, or upstream refs needed for these checks, so those
paths are recorded as kept with `host-evidence-unavailable` until the node
command exposes that evidence. No host is enabled by this config default.

#### Panewire transport

Under `transport: panewire` no remote operation uses ssh, including archive
on a host that also has `ssh`. Each remote host maps to panewire machine
`machine` (default `name`) and goes through the hub's node commands (panewire
`docs/node-commands.md`):

- discovery reads that machine's row of `GET /v1/nodes/agents`; a machine
  the hub does not know, is not `connected` (even with an empty list and no
  error code) or does not answer fails that host for the pass, never reads
  as a host with no panes;
- worktree matching uses `worktree.inspect`, which returns the same branch,
  head and origin the ssh path reads with git;
- pane presence uses identity-checked `pane.read` and the agent list, read-only:
  reap never captures a screen, never checks a composer and never closes a
  pane — panewire's node closes an ended job's pane itself;
- the hub host's aged-out worktree is removed through node command
  `worktree.remove`, with the inspected PR head, base branch and configured
  `reap.regenerable` list. Its refusal outcomes become the same receipt reasons
  as the ssh path (`dirty`, `pane-in-use`, `unmerged-or-unpushed`, etc.);
  ignored paths retain the `ignored-files:<count-hash>` reason and `files` list.
  An unrecognized node outcome is skipped as `cleanup-unknown:<outcome>`.
  Shadow mode records `would-remove` without calling the removal command.

The hub path never falls back to ssh. Hosts with
empty `ssh` and no `machine` keep running every step locally.

##### Hub credential

Reap authenticates to the hub as the scoped service client `svc-mergewake`,
never with the operator token (panewire `docs/hub-scopes.md`). It sends
`X-Panewire-Machine-ID: <HUB_MACHINE_ID>` with `Authorization: Bearer
<HUB_TOKEN>`. The hub's auth file grants it exactly the reap scopes, on every
machine (operator decision D1):

```sh
HUB_SCOPES_svc-mergewake=nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:worktree.remove@*
```

| Scope | Reap uses it for |
| --- | --- |
| `nodes.agents.read` | discovery, `GET /v1/nodes/agents` |
| `node.command:agents.list@*` | not sent by reap today (discovery uses the fleet-wide list); granted with D1 for a per-machine re-read |
| `node.command:worktree.inspect@*` | worktree matching and the aged-out worktree check |
| `node.command:worktree.remove@*` | guarded cleanup of merged PR worktrees |

Reap sends no other node command: it never captures a screen, never classifies
a composer and never closes a pane. Panewire's node closes an ended job's pane
itself, so no close scopes are needed. A credential minted before this change
keeps working; the operator can drop the `pane.*` and `job.done` scopes at the
next mint.

The operator mints the credential on the hub with
`deploy/hub-token-mint.sh` (panewire) and copies the client env file, mode
0600, to the daemon host. reap reads it through `reap.hub.token_file`:

```yaml
reap:
  enabled: true
  transport: panewire
  hub:
    url: https://hub.example.invalid
    token_file: ~/.config/mergewake/svc-mergewake.env   # HUB_MACHINE_ID=svc-mergewake, HUB_TOKEN=...
    cf_file: ~/.config/mergewake/hub-cf.env             # optional, behind Cloudflare Access
hosts:
  - {name: node-a, machine: node-a}
```

A hub answer of `401` (unknown credential) or `403` (`scope_forbidden`: the
scopes do not cover the request) is a scope/auth error for that host. On
discovery or `worktree.inspect` it fails the host for the pass. The error reads
`hub scope/auth error ... for client svc-mergewake on <route>`, and a refused
host is never read as a host with no panes.

Every decision is appended to `<state_dir>/<repo>/reap.jsonl` — including
shadow `would-*` rows, which is how a later pass still finds a worktree it
planned earlier. Dry-run prints rows without writing them — the one
exception is the `check-pr` stamp, which records a GitHub call that really
happened so repeated dry-run polls stay bounded too. A receipt-named
PR is refetched only while it still has pending work and at most once a day —
each attempt appends a `check-pr` row whose timestamp bounds the next try —
so once every worktree its rows named reached `removed` or `worktree-gone` it
is skipped outright, and a persistent skip stops polling GitHub every pass.
A single PR's fetch failure never aborts the pass.

### `reap.archive`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Gates the reap archive stage and `mergewake archive-jobs`. |
| `drive_dir` | Required when enabled | Locally synced Google Drive folder on the archive host. Absolute; a leading `~/` expands only when the archive host is this machine. Never commit the real path. |
| `archive_host` | Empty / `local` | Where `drive_dir` lives: empty or `local` is this machine, anything else a single `ssh(1)` destination reachable from this machine (options, whitespace and leading `-` are refused). |
| `text_max_bytes` | `1048576` | Maximum size of `*.md` and `*.json` files inspected locally for hk guard patterns; at most 3 MiB. It does not control Drive copying. |
| `drive_max_file_bytes` | `0` | Maximum size of one regular file copied to Drive. `0` disables the cap; a file larger than a positive cap is listed as oversized and is deleted with the job dir after a successful archive. Oversized files are not uploaded to hk. |
| `min_age` | `24h` | How long after reap first sights an ended job whose pane is gone its job dir is archived; also the `archive-jobs --older-than` default. Accepts Go durations and whole days (`14d`). |
| `max_jobs_per_run` | `0` | Maximum jobs selected for archive or resume by one `archive-jobs` run; `0` means no limit. Dry-run and shadow use the same cap and selection as apply. Oldest jobs are taken first and the summary reports eligible jobs left. |
| `exclude` | `['pw-rollout-*', 'pw-hk*', 'events-lane']` | Shell globs matched against job directory names and skipped before per-job host inspection. An explicit empty list disables exclusions; invalid globs are configuration errors. |
| `cache_paths` | Built-in Go, Node, Python, and tool cache directories | Directory paths matched by complete path segments inside the job folder. Matching files are excluded from Drive and listed once per matched cache root with reason `cache` and a file count. An explicit empty list disables cache exclusions; entries must be relative directory paths ending in `/` (maximum 64, each at most 4096 bytes). |

The built-in `cache_paths` are `pkg/mod/`, `go-build/`, `.gocache/`,
`gocache/`, `node_modules/`, `.npm/`, `.pnpm-store/`, `.yarn/cache/`,
`__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.tox/`,
`.venv/`, `.cache/`, `.gradle/caches/`, `.m2/repository/`,
`.cargo/registry/`, `.cargo/git/`, and `Library/Caches/`.

A panewire job dir (`<jobs>/<job>` on a `hosts[]` entry) is archived as one
unit. Directories containing a `.git` file or directory are named as
`git-tracked` exclusions and none of their files are copied. The source job dir
is deleted after a successful archive even when a nested checkout is dirty,
has a stash or unpushed commits, or cannot be checked. Each such checkout's
`git-tracked` exclusion records its state (`dirty`, `stash`, `unpushed`, or
`git-error`) in the manifest and hk note. Ignored files do not make a checkout
dirty. Cache roots configured by `cache_paths` are excluded by directory path
segments and recorded once per root with reason `cache` and a file count.
Archiving runs these steps in order:

1. **Plan** (read-only): the jobs dir is resolved on its own host — a
   leading `~/` is expanded with that host's `$HOME`, since quoted paths
  never tilde-expand, and a relative one is refused as
  `jobs-dir-not-absolute`. In the host-wide `archive-jobs` scan, one listing
  supplies each job's root kind, events directory and entry metadata; symlink,
  missing, unfinished and too-new jobs are skipped before per-job
  planning. The shared listing returns one compact summary per job, so its
  output does not grow with the number of files. Candidates that pass those
  checks are refreshed per job and the completion and age gates are repeated
  immediately before remaining planning.
  A job dir that is itself a symlink is refused as
  `job-dir-symlink` (checked again right before the delete); otherwise
  list every non-directory entry. A job is eligible only when its `events/`
   contains `job.completed` or `job.lost` and no later `job.claim`, `job.spawned`, `job.joined` or
  `job.placement` event (a reused name has started again); bookkeeping events
  do not affect that decision. An `events` entry that is present but is not a
  readable directory is treated as unfinished; only an absent `events/` uses
  newest-file age as its completion signal. Every listed entry, including excluded entries,
  contributes to the newest-entry time used by `--older-than`.
   `reap.archive.exclude` is a list of shell globs matched against job names;
   it defaults to `["pw-rollout-*", "pw-hk*", "events-lane"]`, while an
   explicit empty list disables name exclusions. Matching jobs are skipped as
   `excluded-name` before per-job host inspection. Credential-like paths — any
   path segment matching `.env*`, `*token*`, `*.pem` or `*key*`,
   case-insensitively — are excluded from Drive and hk and named in the
   record. A regular file larger than a positive `drive_max_file_bytes` is
   listed as oversized instead of being moved; it is not counted in the
   archived file or byte totals and is not uploaded to hk, even when it is a
   text file. Oversized contents are fingerprinted on the job host, recorded in
   the manifest, and checked again before deletion; they are never hashed on
   Drive. Symlinks and special files are not copied (following a link could
   archive a secret from outside the job dir). A regular file whose base name
   starts with `._` is macOS AppleDouble metadata: it is not copied to Drive
   or uploaded to hk, and is named in the manifest's `appledouble` list.
   It is deleted with the job dir. The hk task id comes from the
  job's panewire events (`events/*-job.task_link.json`, falling back to
  `*-job.claim.json`). The
   destination is `<drive_dir>/tasks/<task-id>/<job>/`.
  A job without an hk task is stored at
  `<drive_dir>/no-task/<UTC YYYY-MM>/<host>-<job>/` and recorded as
  `archive/no-task/<host>-<job>` without a task comment. A destination that already holds a `manifest.json` is
   a finished archive: when that manifest names the same job, host and task
   with the same archived files, oversized paths, sizes and fingerprints, and
   the same excluded, skipped and AppleDouble lists, the plan is a delete-only
   `resume` (contents must still match the manifest);
   otherwise it is skipped as
   `drive-dir-exists`. Publication is serialized with an owner/timestamp lock
   under the archive's `.locks` directory, outside task/job names; a lock older
   than 24 hours is reclaimable, and a live lock is reported as
   `archive-lock-busy`. A guard (`<lock>.guard`) held by a claim is never broken by
   age; a claim that finds one held past 24 hours fails with an error naming the
   guard path, and the lock is left alone. Remove that guard by hand, with
   `rm -r`, once no archive run is live: it is a directory holding an `owner`
   file. If hosts present the same task/job key, a stale or
   different-host plan remains `drive-dir-exists` and cannot overwrite the
   completed copy. A job with its own top-level `manifest.json` is
   skipped as `manifest-collision`.
2. **Copy**: the remaining regular files are streamed as an uncompressed tar
   from the job host and unpacked straight into the destination on the
   archive host, relayed through this machine (`ssh` on either side as
   configured); inspected text files are copied from the exact snapshot scanned
   during planning. If a text source changes before deletion, the job is kept.
   The Drive copy is a plain directory, not an archive file. A
   job with nothing but excluded, oversized or non-regular entries copies
   nothing.
3. **Verify**: every archived file's sha256 is computed on both sides and must
   match; oversized file fingerprints are computed on the job host only. A
   missing, duplicate or unexpected hash record fails the archive. A path
   that cannot be hashed on the Drive side is named in the error (redacted),
   so the row shows which file the copy lost.
4. **Filter**: `text_max_bytes` bounds local secret-pattern inspection for
   regular `*.md` and `*.json` files. The nine patterns are pinned from
   `handoffkeep@8673725 internal/guard/guard.go:20-41`. Matches are excluded
   by pattern name only (`secret-like:<name>`); matched bytes are never
   recorded or sent to hk. A directory containing a `.git` entry is excluded
   as one `git-tracked` entry with its file count. Credential paths, non-regular
   entries, AppleDouble files and oversized files retain their existing rules.
   Cache paths from `reap.archive.cache_paths` are excluded as directory roots;
   the built-in list is documented above and an explicit empty list disables it.
5. **Record**: no file bytes are uploaded to hk. The Drive `manifest.json`
   keeps the existing fields and meanings (`job`, `host`, `task`, `source`,
   `files`, `total_bytes`, `oversized`, `excluded`, `skipped`, `appledouble`)
   and adds `excluded_entries`, with path, reason and optional file count.
   Reasons include `credential-path`, `git-tracked`, `secret-like:<pattern>`,
   `appledouble`, `oversized`, and `non-regular`. One hk note document per
   job (`archive/<task-id>/<job>`) contains counts, bytes, excluded names and
   reasons, Drive path and manifest sha256. Unsafe checkouts record their state
   on the corresponding `git-tracked` exclusion. A short task comment names that
   document and Drive path. Replays after the published manifest resume only
   the delete and do not rewrite either hk record.
6. **Manifest**: after both hk records succeed, `manifest.json` is written
   beside the files as the final publication marker; a failed record leaves
   no manifest and the job is retried.
7. **Delete**: the job dir is listed again and must be unchanged outside
   nested checkouts — a file added or modified since the plan was never
   archived, so the job is kept as `job-changed`, including oversized files.
   Checkout state is read for the manifest, but does not block source deletion.
   The job dir is removed with `rm -rf`. If that first removal fails, owner write and search
   permissions are restored on directories and regular files inside the job
   dir (without following symlinks), then removal is retried.
   Excluded credential and oversized files go with it; their names and sizes
   stay in the record.

The uploads and the comment go to hk over HTTP with mergewake's own
[hk HTTP credential](#hk-http-credential), whatever the sink; no hk CLI runs.
Unset, or with hk unreachable, the archive fails as an `error` row, keeps
the job dir and is retried an hour later. hk refuses an upload over its own
attachment limit (`HK_ATTACH_MAX_BYTES`, default 50 MiB) the same way. The
job host needs `find`, `chmod`, `stat`, `tar`, `xargs` and
`sha256sum` or `shasum`; the archive host `tar` and the same hashing tools;
this machine `bash`. A job dir whose listing exceeds the 4 MiB command
output limit fails and is kept.

In the reap role the stage runs after the PR loop, over each reachable host's
job dirs — it needs no GitHub call and no pane close. A job is eligible only
when no pane uses it anymore (neither by name nor by cwd), its hk job record
is terminal (`completed`, `escalated` or `lost`), and its own events say it
finished. A job the hk record does not name waits as `job-unknown`. The first pass that sees all three writes
a sighting row (`skipped` `awaiting-min-age`); a later pass archives once that
row is older than `min_age`, so the age counts from when reap first saw the
gone state, not from any file time. Acting needs `reap.act` and `--apply`
like every reap mutation; otherwise the plan — what moves to Drive, what goes
to hk, what is excluded and what would be deleted — is printed as an
`archive-plan` line with a `would-archive` row (one shadow plan per job; an
acting pass still archives once its own sighting ages out). A job whose name
is a live pane again is skipped as `pane-live`, and an hk record that is
still live as `job-active`; a job mid-run is `not-finished`. Each outcome is
an `archive-job` row in `reap.jsonl` with the Drive directory, hk ids and
excluded names; a new job dir under a reused name restarts every clock, since
its files are newer than the earlier job's final row — the restart writes a
fresh sighting row even when the earlier row is itself a sighting.
`archived` and every refusal other than a live pane, a missing pane scan, an
active or unfinished job, an unknown job, a `job-gone` race, or a fresh
sighting are final, and a failed attempt is retried at most once an hour, by
re-attempting the archive directly rather than starting a fresh sighting. A
full (non-`--pr`) pass runs the stage; a `--pr` run scopes to one PR's cleanup
and skips it. Old `close-pane` rows still load but no longer feed the stage.

`mergewake archive-jobs [--older-than 14d] [--host NAME] [--dry-run|--apply]`
runs the same plan and apply over a host's existing backlog: every job dir in
`hosts[].jobs` whose newest entry is older than `--older-than` (default
`min_age`), skipping names that are a live pane or hold a pane's cwd — a host
whose pane list cannot be read is skipped whole. Without `--apply` it only
plans; rows go to `<state_dir>/archive-jobs.jsonl` unless `--dry-run`. It
needs no GitHub token.
When `max_jobs_per_run` is positive, the oldest eligible jobs are processed up
to that limit and the summary reports `eligible_left` for a later run.

A run's `reap.archive.max_run` deadline bounds pane scans, SSH listings, job reads,
and Handoffkeep event reads, and is checked before an archive apply starts. An
apply already in progress finishes its publish/delete boundary before the run
writes its summary, so that single apply can extend the deadline. The summary
has `stopped_on_budget: true` and names unscanned hosts in
`unscanned_hosts`. Each host phase writes an `archive-phase` row with its
duration in milliseconds. A caller-cancelled run writes its summary and returns
the cancellation error. A pane scan failure skips that host, records its reason
in `unscanned_host_reasons`, and does not by itself make the command fail; errors
from jobs on hosts that were scanned do make the command fail. The command exits
zero when the run completes or stops on `max_run` without other errors, and
nonzero for job, configuration, state-write, or caller-cancellation errors.
The hosts are then walked from the one after the last host
it entered, so a host late in `hosts` is not starved by the earlier ones; the
next full run starts there, and a run that finishes every host returns to
config order. A `--host` run neither reads nor writes that state. A `--dry-run` run reads it, so its preview lists hosts in the order the next apply run will use, and never writes it. The summary
lists hosts it did not reach in `unscanned_hosts`; `eligible_left` counts
eligible jobs left unprocessed on hosts whose pane scan and jobs listing
completed. This includes the interrupted job and later jobs on a partly
processed host. Hosts whose pane scan or jobs listing did not complete, and
hosts the run never reached, are not counted. Unscanned hosts are not scanned
just to be counted.

### `liveness`

Automatic daily liveness checks evaluate made-against-done progress for all
core mergewake loops once per UTC day during the first acting whole-repo reap pass.
Daily evaluations write one `liveness-daily` row per mechanism into the primary
configured repository's `decisions.jsonl`, tracking `day`, `mechanism`, `made`,
`done`, `ratio`, and boolean `stalled`. The last processed day is tracked in
`<state_dir>/liveness.json`.

| Key | Required / default | Meaning |
| --- | --- | --- |
| `lane` | Desk lane | Panewire lane receiving stall notices. When unset, falls back to `reap.balance_lane`, then to the desk (`notifier.fallback_lane`, else `notifier.lane`). |
| `thresholds` | Default thresholds | Optional threshold overrides configured as flat fields on `liveness.thresholds`; see below. |

#### Mechanisms and thresholds

Every mechanism evaluates progress made against expected completions over the UTC day. A mechanism is considered `stalled` when its stall condition is met; quiet days below minimum attempts never stall.

| # | Mechanism name | Made predicate | Done predicate | Default threshold config |
|---|----------------|----------------|----------------|--------------------------|
| 1 | `review-pass` | Distinct PR heads claimed for review (`review-start`). | Distinct PR heads completing review with a verdict (`review` or `review-skipped`). | `review_pass_min_made: 10`<br>`review_pass_min_done_pct: 50` |
| 2 | `reviewer-landing` | Reviewer placement requests created (`review-brief-hk` with label). | Reviewer jobs landed on an execution host (`reviewer-spawn`). | `reviewer_landing_min_made: 10`<br>`reviewer_landing_min_landed_pct: 50` |
| 3 | `findings-delivery` | Distinct PR heads with critical or high review findings (`review-round-finding`). | Heads whose findings were delivered or acknowledged (`findings-delivered` or `findings-acked` carrying a `head`). A `findings-acked` row without a `head` (written before the ack carried one) stays uncounted. | `findings_delivery_min_made: 3`<br>(Stalls if made >= 3 and done == 0) |
| 4 | `merge` | Approved and CI-ready PRs eligible to merge (`land` action `merge`). | PRs successfully merged (`pr-notify` state `merged` or `hk-task-event` with source `mergewake.merge`). | `merge_max_unmerged: 3`<br>(Stalls if unmerged >= 3 and made >= 3) |
| 5 | `reviewer-checkout-removal` | Distinct checkouts created on or before the day (`reviewer-spawn`). | Distinct checkouts removed that day (`reviewer-cleanup` or `reviewer-reap` with result `removed`). | Inherits from `reap`:<br>`reap.balance_min_created: 20`<br>`reap.balance_max_removed_pct: 70` |
| 6 | `author-worktree-sweep` | Candidate author worktrees inspected for removal in `reap.jsonl` (`remove-worktree`). | Worktrees removed or found already gone (`result: "removed"` or `reason: "worktree-gone"`). | `author_sweep_min_made: 5`<br>`author_sweep_min_done_pct: 30` |
| 7 | `watch-revert` | Merges observed on base branch (`pr-notify` state `merged` or `land` action `merge`). | Base branch heads actively observed by CI watch (`watch`). | `watch_max_unverdicted_hours: 6`<br>(Stalls if repo has merges but zero watch observations) |
| 8 | `lane-notices` | Attempted lane notifications (`pr-notify`, `review-blocked`, `conflict-esc`). | Successful notifications (attempts minus notification errors). | `lane_notices_min_attempts: 5`<br>`lane_notices_max_error_pct: 50` |
| 9 | `task-events` | Attempted handoffkeep task event posts (`hk-task-event`). A `task-event-fields-refused` row for an event that posted successfully is the same event, not a second attempt; only a refusal with no successful post counts. | Successful task event posts (`hk-task-event` carrying `result` and no `error`). | `task_events_min_attempts: 10`<br>`task_events_max_refusal_pct: 20` |
| 10 | `outcome-publish` | Eligible outcomes published (`outcome-published` in `outcomes.jsonl`) plus errors. | Outcomes successfully published. | `outcome_publish_min_errors: 5`<br>(Stalls if errors >= 5 and published == 0) |
| 11 | `review-retry` | Review passes deferred for retry up to the evaluated day (`review-retry-deferred`). | Deferred review passes resolved (retried, superseded via `reviewer-head-moved`, or PR closed/merged) or within grace window. | `review_retry_max_deferred_hours: 24`<br>(Stalls if >= 2 unresolved deferrals exceed threshold; single miss does not alert) |
| 12 | `worker-worktrees` | Worker worktrees the sweeps took custody of per host: the first `reap.jsonl` row (`remove-worktree` or `legacy-sweep`) naming the host's worktree. No spawn rows exist for workers, so first sighting is made. | Worktrees removed that day per host (`remove-worktree` with `result: "removed"` or `"already-gone"`, or a gone proof: `reason: "worktree-gone"`, `"already-gone"` or `"gone"`). Reviewer (`mwr-*`) and consult (`consult-*`) checkouts never count; hostless rows are unattributed, never stalling. | `worker_worktrees_min_made: 10`<br>`worker_worktrees_max_removed_pct: 50`<br>`worker_worktrees_window_days: 7`<br>(Stalls if any host reaches made and its removed share falls below the threshold; the trailing window feeds the per-host open balance) |
| 13 | `notices` | Reviewer-problem events recorded that day, by cause (`notice-cause` rows; a `gave-up` row is an event too). | Events that became a notice (`sent: true`). | Stalls if a fix-needed cause (`spawn:auth`, `spawn:workspace`, `spawn:node-upgrade`, `spawn:host-unconfigured`, `spawn:repo-root`, `spawn:brief-upload`) appears at all that day, or a cause passes `notifier.cause_threshold` in one repo that day. Its `reason` names each cause's counts. |

#### Stall notices and repeat rule

When a mechanism evaluates as stalled:
* One notice per stalled mechanism per day is dispatched to `liveness.lane` (or fallback lanes) via panewire lane event.
* A notice the notifier refuses is recorded as a `liveness-notice-error` row and does not halt the day loop: the day still completes and advances, and the next pass retries the notice. Only a delivered notice writes `liveness-notice` and its sent key, so a notice is never delivered twice.
* The notice repeats daily while the stall holds.
* An operator can acknowledge a stall:
  ```bash
  mergewake liveness ack <mechanism> --reason "<text>"
  ```
  The acknowledgment records a `liveness-ack` row in `decisions.jsonl`. While acknowledged, repeat notices for that mechanism are suppressed. An ack is accepted only while the mechanism's latest daily row is stalled; otherwise it is refused. A late ack, written after recovery, can never silence a later, unrelated stall.
* When a recovery daily row (`stalled: false`) is written after an acknowledgment, the ack is automatically cleared. Any future stall on that mechanism will alert again.

#### Inspecting liveness

To inspect daily mechanism status and audit history without modifying state:
```bash
mergewake liveness [--day YYYY-MM-DD] [--mechanism M]
```
With `--mechanism worker-worktrees` the result carries one entry per host with
its made, removed and trailing open counts plus the `kept` list: every worker
worktree staying past the removal rules with its reason, PR and repo. The
daily pass also writes one `worker-worktrees-daily` audit row per host with
the same counts and kept list.

### `review`

| Key | Default | Meaning |
| --- | --- | --- |
| `login` | Required | GitHub identity posting the independent exact-head review. |
| `fleet_authors` | Empty | Additional fleet accounts. Their replies count under the same fix and dismissal checks as `login`. A PR that `login` or one of these accounts opened must carry a task under `executor: panewire` (see [Blocked fleet reviews](#blocked-fleet-reviews)). |
| `timeout` | `10m` | Bounds reviewer placement, execution and submission. A live reviewer record is never declared lost or replaced before this budget is spent; it must outlast panewire's ten-minute carry window for a quota-stopped reviewer, which the default does. |
| `default_grade` | `B` | Fallback grade shown in review plans when the PR has no `Mergewake-Grade: A` body trailer. Accepted grades, weakest to strongest: `C`, `B`, `A`, `A+`, `S`, `S+`, `SS`. Panewire placement uses the task title grade and tier. Outcomes never use this fallback. |
| `first_round_grade` | `{B: A, C: A}` | Map from PR grade to the grade asked for every reviewer pass of the PR's first review round, so the round that reads the whole diff gets a stronger reviewer than the rounds that read only the delta. The PR grade is the `Mergewake-Grade` trailer, else `default_grade`; a grade not in the map (A and above) is asked at the PR grade, as is every later round. Round one is the PR's first review round: no patch of an earlier head was reviewed (the same count the merge gate uses for rounds), so a later whole-diff review of the same PR, such as a delta that fell back to the whole diff or a head after `base_stale`, is not round one. A raised grade reaches panewire as `spawn --grade`; an unraised pass sends no `--grade` and panewire uses the task title grade. Keys and values must be accepted grades or the config does not load; an empty map (`first_round_grade: {}`) turns the raise off. Each `review` audit row records `pr_grade`, `grade_asked`, `first_round` and the `seat` that ran. |
| `default_family` | Empty | Reviewer family used when the PR has no author-family trailer and the head commit's Co-Authored-By/author identities hold no known model alias. Must name a configured profile family; reviewed heads record the inference. |
| `budget_rounds` | `3` | Review rounds a PR gets before later rounds report only critical and high findings; negative turns the budget off. See "Review rounds, contract and budget". |
| `select` | `pace` | Accepted for compatibility; unused. |
| `pairings` | Empty map | Accepted for compatibility; unused for reviewer selection. |
| `profiles` | Empty map | Profile families and `args` model values support author and reviewer family and model lookup; `gate_profile` can match a panewire seat to its family. `command` and `gate_effort` are accepted but unused; other `args` are unused. |
| `executor` | `panewire` | Panewire spawns a reviewer pane for each pass. `headless` is retired and rejected. See [Panewire executor](#panewire-executor). |
| `panewire.command` | `panewire` | Single executable for spawn and inbox calls. |
| `panewire.workspace` | Empty | Optional herdr workspace (`-w`) for reviewer panes. When empty, panewire uses the executing host's configured workspace. |
| `panewire.owner` | Empty | Owner lane (`--owner`); empty inherits the author job's owner. |
| `panewire.host` | `auto` | `--host`; anything but `auto` is passed with a recorded override reason. |
| `panewire.jobs` | `~/work/herdr-inbox/jobs` | panewire's jobs root on this host; the brief lives in `<jobs>/<label>/`, and so do the findings and report of a pane that runs here. mergewake never reads those findings from there: they come back on the job record, or from the host through `panewire node job-result` (step 5). |
| `panewire.checkouts` | `<state_dir>/review-checkouts` | Parent of the detached PR-head worktrees created from the repo's `root`; panewire names the remote worktree after the checkout. A reviewer placed from a checkout spec (the hub advertises `checkout_spec_v1`) has no directory here and its job record names no `checkout`: the node prepares its worktree, reap removes it there under the node's `<label>-checkout` name, and nothing here is created or removed. After each terminal pass outcome, the pass leaves the reviewer pane for panewire's node to close and removes its remote detached worktree through the hub's guarded removal once the pane is gone; a refusal is recorded as `pending-cleanup`. A pane still present is recorded as `pending-cleanup` and the checkout is kept. Each cleanup writes a `reviewer-cleanup` row to `reap.jsonl` and to `decisions.jsonl`. Every later reap pass revisits a checkout still recorded as kept or `pending-cleanup` while no pane uses it: it is removed once its reviewer job is `completed`, `escalated` or `lost`, or once its label's spawn time is more than 24 hours back, and kept while the job is still open. A reviewer job still `claimed` or `spawned` with no result past `panewire.stuck_after` is treated as ended once its pane is proven gone: mergewake closes the job `abandoned` (the close that lets panewire's sweep end the record) and removes its worktrees in the same pass; while its pane is still present the pane is left for panewire's node and the label is marked `stuck-reviewer` once, with the age. A job close the hub refuses as `job_unknown` (the hub does not know the job) is settled from the job's node dir: a terminal event there (`job.revoked`, `job.reaped`, `job.completed` or `job.lost`) counts as closed, and otherwise the close is backed off and retried, with no error row, and the label gets one `kept` row, `hub does not know the job; not closable via hub`. Through the hub the node's `job.result` answers with its `terminal_event` field when it sends one, and with only `job.completed` when it does not. A checkout older than 24 hours whose job record is missing is removed locally only; a remote checkout with no job record is not revisited. The record is `reap.jsonl`, so it survives a restart. A directory under this checkouts path that no reviewer-job record of any repo names (its `checkout` field, or its label's path here) and that no pane or process uses is an orphan: a whole-repo reap pass removes it from the owning repo's pass with `git worktree remove` (never forced) once it is older than `panewire.stuck_after` and is a detached worktree of that repo that is clean and holds no ignored file outside `reap.regenerable`; it writes one `reviewer-cleanup` row with reason `orphan-checkout` and a `reviewer-reap` audit record in `decisions.jsonl`. A directory that is not a git worktree is kept and reported once as `not-a-git-worktree`, by the alphabetically first configured repo's log only; an unreadable job record stops the orphan sweep for that pass. Process working directories come from `/proc`, or from `lsof -a -d cwd` plus `ps -A` filtered to the effective uid where `/proc` is absent; there `lsof` supplies the readable working directories and `ps` supplies the same-user pid list, so a pid with no cwd record is unknown rather than invisible. A same-user process that denies its working directory (on `/proc`, a non-dumpable one such as an `sshd` session or systemd's `sd-pam`; on the lsof path, a pid whose cwd lsof did not report) is treated as holding no checkout when its open files could be read and none lies under this path, or when its executable, or a kernel-reported name, never enters a work directory (the lsof path's `ps` name is the process's own argv0 and is not trusted for this); any other such process, and a host with no scan at all, makes the sweep remove nothing and fail no pass, and writes one `skipped` row with reason `process-scan-unavailable` naming the pid and the process name, once per UTC day and host, a new day reporting it again even when the process is unchanged, so a sweep that can never run stays visible. Shadow mode only plans (`would-remove`). Once per UTC day and host, a `reviewer-checkouts-daily` audit row in `decisions.jsonl` counts the checkouts removed and kept on the day before, with the kept reasons; it is written only when the reap acts (not in shadow or dry-run). |
| `panewire.lane` | `panewire.owner` | Retired: findings are no longer read from a lane. The key is still accepted, so existing configs keep loading, and its value is still kept out of traces. mergewake lists and acknowledges no findings inbox, so findings events a pre-`--result` pane announced there stay as they are. |
| `panewire.exclude_hosts` | Empty | Hosts the pane must not run on, passed as `--exclude-host a,b` (needs a panewire whose `spawn` has `--exclude-host`). Must not contain `panewire.host`. |
| `panewire.stuck_after` | `6h` | How old a reviewer job — or the worker seat a refused reviewer role retries as — still `claimed` or `spawned` with no result may get before the reviewer sweep treats it as ended for cleanup: once the job's pane is proven gone mergewake closes the job `abandoned` and removes its worktrees in the same pass; while the pane is still present the pane is left for panewire's node and the label is marked `stuck-reviewer` once, with the age. A job record that names no pane is proven gone only when its label is absent from a fresh scan of every host it could be on; a host whose scan failed, could not read its full pane list or could not be refreshed keeps the job. Other roles keep their hold. |
| `panewire.stale_after` | Twice `timeout` | How long a pass's recorded reviewer job may stay live before mergewake writes a `reviewer-stale` audit row and tells the desk once per PR, head and pass. A pass with a recorded job never spawns another reviewer: the next run reads the job's result when it completed, or replays the recorded spawn to re-attach the wait. A replay that panewire ends before any pane exists, with no hk job record for the label, is dead: mergewake writes a `reviewer-spawn-dead` audit row, drops the record and spawns a fresh reviewer under a new label, written as a `reviewer-spawn-replaced` row, in the same evaluation when time is left and otherwise on the next one. The quota wait (exit 86) and the unknown-quota answer (exit 89) are not dead: panewire parks their requests, which may still land. The quota escalation (exit 87) is dead: panewire ends the request and asks the parent lane to act, so no job will ever run it (panewire docs/config.md, "Exit codes"). A fresh spawn that ends on 87 leaves no record either, and it fails the pass, so review-retry backs off and escalates like any other spawn failure. An event-driven pass places its reviewer and returns instead of waiting, so a replay never sees that exit: with no hk job record for the label, it reads the request once with `panewire spawn status` and treats the answers that end it — 87, the pre-start refusals (74, 77), the placement refusals that leave no pane (78, 79, 80, 81, 88) and 84 for a request that ended before it landed — as dead. A dead request found this way fails the pass once and the replacement spawns on a later run: an asynchronous spawn only reports back once its own request ends, so a replacement spawned in the same evaluation would be replaced again on the next one, with no failure counted and no back-off. A parked, landed, unresolved or in-flight request, and a read that cannot answer or timed out mid-landing (70, 75, 85, 91, 124, or a panewire without the subcommand), keep the record replayed as before. |

#### Large PRs

GitHub answers HTTP 406 to the diff media type for very large PRs. Only on that
status does review build the same patch locally from the repo's `root`: it
fetches the PR base ref and `pull/<n>/head`, then diffs the merge base to the
head with rename detection (`git diff -M`), so moved code is listed as a rename
instead of repeated content. The local path applies the same 2 MiB reviewer
budget as the API path. When the patch is still larger, complete file blocks are
kept in their stable diff order while they fit, and each later block is replaced
by its header and a digest of the omitted text. Every summarized or purely
renamed file is listed in the review body, and the pass marker records
`diff_source: local` with `diff_summarized`, so no file disappears silently. Any
other HTTP error fails the review as before.

#### Panewire executor

With `executor: panewire` each missing pass is first offered to a reviewer pane:

1. The PR's `Mergewake-Task` trailer (PR body, then head commit) is read
   through the handoffkeep sink; the task's `refs.job_id` is the author's
   panewire job and its title marker (`(grade A, T2)`) gives `--t`. The
   author's hk job record (`GET /v1/jobs/<author job>`, read over HTTP with
   mergewake's own [hk HTTP credential](#hk-http-credential)) supplies
   `--builder-family`; a builder spawned before job records have no record,
   and the trailer/commit inference answers instead. A task with no bound job
   (the author did not run through panewire) is not skipped: the pass runs as a
   top-level reviewer job, `--builder-job` is omitted, and `--builder-family`
   still carries the author's family (trailer/commit inference) so the
   reviewer excludes it.
2. A fork PR (head repository other than the repo) is not offered: panewire can
   replicate only a spec origin carries. The pass claim is taken, then the PR
   head is fetched so that a real ref carries it:
   `git -C <root> fetch --no-tags origin +pull/<n>/head:refs/remotes/origin/pull/<n>/head <base>`,
   and, on the fallback below, the branch. When the hub advertises the
   `checkout_spec_v1` spawn feature, the spawn names that ref instead of a
   local checkout, and no worktree is added under `panewire.checkouts` and no
   directory is created there: panewire's node prepares the detached worktree
   at the exact head from
   `--checkout-origin <the checkout's origin remote> --checkout-commit <sha>
   --checkout-ref pull/<n>/head` (or `refs/heads/<ref>` for a taken branch
   fallback), and refuses a commit the fetched ref does not contain. Otherwise
   — an older hub, an unverifiable feature answer, a CLI that does not know
   the flags, or a checkout whose `origin` is not a remote — a worktree
   detached at the exact head SHA is added under `panewire.checkouts`
   (`git worktree add --detach <checkout> <sha>`), as before. mergewake creates
   no local branch, and the author's branch being checked out in another
   worktree is no obstacle. A local worktree is removed after the wait.
   GitHub can lose `refs/pull/<n>/head` for an open PR while its branch is
   intact. When that fetch fails (for any reason), a same-repo PR whose head
   ref is a plain branch name (no leading `-`, `..`, `:`, whitespace, `*`, `^`,
   `~`, `?`, `[` or `\`, not under `pull/`, at most 200 bytes: the names
   panewire's transport carries) fetches
   `+refs/heads/<ref>:refs/remotes/origin/<ref> <base>` instead, and the pass
   continues only when that branch's tip **equals** the head: the remote
   reviewer fetches the branch and requires it to be the head, so a branch
   that moved on (or was force-pushed) skips the pass, naming both errors, and
   the next pass reviews the new head. A taken fallback deletes a local
   `refs/remotes/origin/pull/<n>/head` left by an earlier fetch, which
   panewire would otherwise prefer; panewire then builds the remote spec from
   the branch. The 406 local-diff path uses the same fetch. Each attempt writes
   one `pr-head-fallback` row (repo, PR, head, ref, first error redacted and up
   to 512 bytes, `result`: `ok`, `fetch-failed`, `head-moved` or
   `pull-ref-kept`, and `detail`: the first fetch's redacted stderr, at most 512
   bytes, present only when the first failure was a git command error) to
   `decisions.jsonl`. A pass skipped after a failed fallback carries as its
   operator detail the first fetch's stderr, then the fallback's, joined by
   `; `. A fork PR or an invalid ref name skips the pass as before, with no row.
3. mergewake writes a brief naming the PR, head SHA, base, lens, the
   author family, the output contract and the same PR context (answered
   findings and operator decisions), then runs
   `panewire spawn (-c <checkout> | --checkout-origin <origin>
   --checkout-commit <sha> --checkout-ref <ref>) -m auto -p <brief> -l
   mwr-<pr>-<sha7>-p<pass>-<unix> --t <T> --task <task> [--builder-job <author job>] --builder-family <family from the author's job record, else inferred> --role
   reviewer --report <jobs>/<label>/report.md [--exclude-host <hosts>] [--exclude-pool <pools>] --wait
   --wait-timeout <rest of review.timeout>`. panewire picks host and model and,
   for the reviewer role, excludes the author job's family; on another host it
   clones the repo under `$HOME/work` and adds a worktree detached at the same
   head, fetched as the ref the spawn named. Exit 77 (no admitted seat outside that family) retries
   once with `--role worker`, which may pick the author's family; the receipt
   records that as `fallback`.
   A replacement of a lost or timed-out reviewer also passes `--exclude-pool`
   with the quota pools its lost reviewers ran on, read from the `pool` of their
   hk job records and written to the `review-replacement-pools` audit row. A
   lost reviewer whose record names no valid pool excludes nothing for that
   reviewer, and the audit row lists it as unknown. When the exclusion leaves
   panewire no candidate (exit 80 or 81 with a "no host admitted" or "no
   placement candidate accepted" text), the spawn is placed once more without
   `--exclude-pool`, audited as `review-replacement-pool-fallback`; a resumed
   replay is retried the same way, and its record is rewritten to the argv that
   was sent. The lost reviewers of a pass stay in `replaced-reviewers/<pr>-p<pass>.json`
   beyond the reviewer record, so a later spawn of that pass still excludes their
   pools.
   A reviewer's hk record that ended `lost` with an end_reason
   `quota_stop pool=<p> ... replaced_by=<job>` was carried by panewire onto
   another pool, not lost: the pass follows `<job>` (at most the two hops
   panewire writes, `<old>-q1` and `<old>-q1-q2`), writes
   `review-reviewer-replaced` with the stop's pool, counts no loss and spawns
   no reviewer of its own. The same end_reason with no `replaced_by=` is a
   give-up: no other pool could take the review. The pass writes
   `review-quota-given-up` with the pool, counts the review as lost
   (`review-result-lost`) and replaces it at once through the lost path,
   leaving the give-up's own pool out of the replacement's placement. When the
   pass's replacement is already spent, a give-up behaves as a spent lost
   result. When no placement candidate is left even without the exclusion, the
   pass does not drop the review: it writes `review-quota-stopped` and waits
   for that pool's reset, and the failed attempt's record goes so the run after
   the reset starts a fresh reviewer with its one replacement unspent. A live
   record is never a loss before `review.timeout`, which outlasts panewire's
   ten-minute carry window.
   The hub's `checkout_spec_v1` support is read by panewire itself: it asks the
   hub for the feature before it submits, and stops with exit 2 (`hub does not
   support --checkout-origin`) or 70 (`unable to verify hub support`) before a
   request exists, which an older CLI answers with its unknown-flag refusal.
   Such a refusal makes the pass add the local checkout, rewrite the recorded
   argv in the `-c` form, write a `reviewer-checkout-spec-refused` row to
   `decisions.jsonl` and spawn again. A refusal that says the hub or the CLI
   cannot carry the spec is remembered for 10 minutes, so an old hub costs one
   refused attempt per window rather than one worktree per attempt, and an
   upgraded hub is picked up without a restart. A local checkout that cannot be
   made after the refusal placed nothing: the pass releases its lease at once,
   drops the reviewer-job record and records a `panewire reviewer local
   checkout failed` skip, and no receipt names a directory that was never
   created. The reviewer-job record keeps
   the argv actually spawned, and carries no `checkout` field for a
   spec-placed reviewer: its worktree lives on the node, named
   `<label>-checkout` there, and reap removes it there through the hub's
   guarded removal once its pane is gone. A spec-placed spawn that ends without
   a pane result writes a `pending-cleanup` receipt naming that
   `<label>-checkout` worktree by label, since the pass holds no node path; the
   reviewer sweep resolves its host from the job record and its path from that
   host's `work_root` when no record names one, and keeps a terminal row only
   when no record and no work root can build a path.
4. The pane first checks `git rev-parse HEAD` against the head SHA; on a
   mismatch (a stale remote worktree) it writes
   `{"refused":"…"}` instead of reviewing, and the pass fails. Otherwise
   it writes `{"findings":[…]}` to `findings.json` in its job directory
   (`<jobs>/<label>/` here, panewire's jobs root on another host) and runs
   `panewire done <label> --result "$J/findings.json" --report "$J/report.md"`,
   which stores the findings on the pane's hk job record in the same write
   as `state: completed`.
5. After the wait, mergewake reads `GET /v1/jobs/<label>` (same endpoint and
   credential as above) and applies the W4 rule: a completed
   record's `result` is used; a completed record without a result, an
   `escalated` or `lost` record (the receipt records the state), a still
   pending or missing record, and an unreadable hk all fail the pass — the last
   three after polling every 5s for up to 30s (at most half the remaining
   deadline), an unreadable hk reporting reason `hk-unavailable`.
   Before a completed record without a result is lost, or a still pending
   record fails the pass, the host the record names is read: `job.result`
   through the hub, or `panewire node job-result` for this host, returns the
   `findings.json` the pane wrote there, and a result found there is used
   like a record's. An `escalated` or `lost` record is held as above, and a
   missing record or an unreadable hk names no host. A resumed reviewer is
   read the same way (`reviewer_job.go`): a finished or overdue reviewer's
   record is read again and then its host, and a reviewer hk cannot read is
   looked for on the host its spawn receipt names. Nothing else is read. Findings left only in
   mergewake's own `<jobs>/<label>/findings.json`, or announced on a
   findings lane by a pane from before `panewire done --result`, are
   ignored: no findings inbox is listed or acknowledged and no attachment is
   fetched. A host read that the review deadline cuts short keeps the
   reviewer for the next run, and so does any read of a resumed reviewer's
   hk record or host that a cancelled or expired run cuts short: such a run
   returns the context error and sends no lost notice, schedules no wait,
   nudges nothing and replaces nothing. A completed record whose `result`
   fails validation is a lost result, like one without a result; only a
   run whose context has already ended skips it as rejected findings. The
   findings go through validation (new-side
   diff lines, severities, at most 50) and are posted with the same receipt,
   claim and done marker. The receipt adds `"executor":"panewire"`, the
   reviewer is `panewire:<seat>`, and the family is the one on the reviewer's
   job record — a remote pane's spawn events live on its own host — with the
   local `job.spawned` event (or the profile whose gate profile is the seat)
   as the fallback only when the record has none; an unknown family counts as
   same-family. The checkout is removed after the wait.

The panewire executor is the only executor of the pass: nothing runs headless.
No task trailer or T marker on a PR outside the fleet, no repo `root`, a fork
PR, a failed checkout, a panewire refusal or wait failure, a refusal by the
pane, no findings or rejected findings all fail the pass. The run records a
`review_missing` event and a `review-skipped` audit record with the panewire
skip, posts nothing, writes no `-done` marker and fails with "review
unavailable". The daemon's review retry backs off, runs the review again and
so spawns a new reviewer pane, and escalates once after
`daemon.pr.review_retry.failures` failures. A pane cut off by `review.timeout`
records `review_deadline` instead, and a completed reviewer whose result was
lost records `review-result-lost` and a `notice-cause` row (hk 5361). Such a
loss is told to `notifier.tool_lane` only when one cause repeats past
`notifier.cause_threshold` in one repo within `notifier.cause_window`, and then
once per window; a single loss is recorded with `sent: false`. The cause is
`lost:overdue`, `lost:pane-finished`, `lost:never-placed` or `lost:other`.
Exhausted retries still notify the repository owner lane once per head and
cause, with a `notice-cause` row of cause `gave-up`. Both
are retried the same way, with one difference: a lost result is mergewake's collection
failure, not the review's, so its retries back off without counting toward
the escalation. A fresh pass whose completed record has no result, on the
record or its host, is lost at once; a resumed one is replaced once by a new
reviewer, and only a lost replacement fails the pass. A pass that fails
because its record answered without findings (no record, `hk-unavailable`,
a state that is not terminal) is a skip, counted like any other. A
resumed reviewer whose record is still open (not terminal) and whose pane
is idle or done with no result is not lost before it is nudged once to
write its two files and run `panewire done --result` (`reviewer-nudge`) and
has had two minutes to do so (`reviewer-nudge-outcome`), if the review
budget still holds them. Files it writes count only once its record or its
host returns them. The wait holds only an open record: a nudged reviewer
whose record becomes completed without a result, even inside the two
minutes, is read on its host and, without the result there, lost and
replaced once like any completed reviewer without a result, with no second
nudge. The receipt's
`executor` is always `panewire`, and its `fallback` carries only the
worker-seat note from step 3.

A reviewer that is replaced for any cause (a lost result, `review.timeout`, or a moved PR head) has its hk job closed `abandoned`. Its pane is never closed here: panewire's node closes it, and the owed pane is settled once the pane is gone. A job that already ended (`completed`, `escalated`, or `lost`) holds no hub claim, so it gets no job close. A job close that panewire refuses because the job already has a terminal record counts as closed; any other refusal stays owed. A failed close stays owed and is retried on each later review run of the PR until the job is closed or gone.

A reviewer replaced for an overdue loss (`review.timeout` passed while it still ran) keeps its label on the replacement's record. While the replacement runs, a later review run of the same PR, head, and pass collects that reviewer's result if it has since completed validly, and the pass completes with it. The replacement is then closed as collected. A late result counts only for the head it reviewed; a reviewer replaced for any other cause is never collected late. Audit row `review-result-late-collected` records the late label, the current label, and `seconds_past_budget`.

Under the panewire executor, review does not call the quota tool and does not
read `review.profiles` or `review.pairings` to choose reviewers: panewire
selects the reviewer. `review.profiles` is still read for the author's model
and family from a `Mergewake-Author-Profile` trailer, for the author model on
the outcomes record, and to name the family of a seat whose job record and
spawn events carry none. The keys `review.select`, `review.quota` and
`review.scopefuel` were removed; a config that sets one fails at load. Passes of
a blocking PR are separate panes; panewire
enforces only the author-family exclusion between them.

panewire fails closed when the remote host cannot stand at the head: an
existing worktree there that is not detached at that commit or is dirty is a
placement refusal for that pass, which fails it.

#### Blocked fleet reviews

With `executor: panewire`, a PR opened by `review.login` or a
`review.fleet_authors` account is not failed and retried as above when it
lacks what panewire needs. Its review is blocked until the author fixes the PR
or the task:

| Outcome | Cause | Fix |
| --- | --- | --- |
| `blocked: missing-task-trailer` | Neither the PR body nor the head commit has one valid `Mergewake-Task` trailer. | Add `Mergewake-Task: <id>` to the PR body. |
| `blocked: task-missing-T-marker` | The task's title has no grade and T marker. | Add `(grade X, Tn)` to the hk task title, for example `(grade B, T1)`. |

For a blocked head, mergewake:

- records the outcome on the PR's outcome row, under `review_blocked_by_head`;
- posts one PR comment per head that names the fix, deduplicated by a hidden
  head key in the comments of `review.login`;
- emits one `decision` event per head and outcome to the repo's owner lane
  (`repos[].lane`), or to `notifier.fallback_lane` (else `notifier.lane`) when
  the repo names none, except missing-trailer and unknown-family notices. Those
  go to the PR author route once per PR body, independent of head; one audit
  row records each distinct suppressed repeat. The owner lane hears only when
  the author route is gone.

mergewake takes no pass claim and writes no receipt or `-done` marker, so the
merge gate reports `review_missing`. A block is not a review failure: it adds no
retry backoff and no failure escalation, and it sends no review-stalled alert: the head
waits on its author, and the BLOCKED notice already went to the author route. The
next poll after the fix runs the review on the same head and clears the outcome.

A read error is not a block. If the commit or the task cannot be read, the pass
fails and is retried as above. Other authors and heads that already have their passes or inherit them are not blocked. A repo without `root` fails the pass with a skip reason.
`mergewake review` exits non-zero on a blocked head. A dry run only prints the
decision. Shadow mode also records the outcome, and it posts and emits
nothing.

Profile `family`, `args` model values, and `gate_profile` remain available for family and model lookup. The `command` and `gate_effort` fields are accepted but unused.

Each pass claims a GitHub ref `mergewake/reviews/<pr>/<head>[-pN]` before
invoking the model. When every required pass is posted, the
`mergewake/reviews/<pr>/<head>-done` marker is created and the daemon skips
that head. A head whose patch id equals a reviewed head's inherits that review
and gets the marker without a pass (see [Patch-id review verdicts](#patch-id-review-verdicts)). An interrupted pass retains a timestamped empty-commit lease. After
`review.timeout` plus one minute it is reclaimed automatically with a
fast-forward ref update; competing renewals cannot both win. Legacy claims use
their commit date for expiry. Unknown claim timestamps fail closed. Posted reviews embed a `mergewake-pass` JSON marker recording
the pass number, lens, reviewer profile, family, and `same_family` flag;
receipts and outcomes bind to the exact PR head, so a head change invalidates
them. Blocking PRs use a correctness/regressions lens for pass 1 and a
state/migration/security/deploy lens for pass 2.
Landing counts distinct pass numbers, whatever the reviewer family: a pass by
the author's family is a full pass, and `same_family` only records it. Routine
PRs use the default lens.
Before invoking a reviewer, the review adds PR context ahead of the diff so a
settled point is not re-flagged on every new head. It lists this PR's review
threads whose finding has a reply from someone other than its author (resolved
or not): path, line, the first ~300 characters of the finding and of the latest
reply, newest first, under "Already answered on this PR". The reviewer is told
not to repeat a matching finding unless it gives a new, concrete reason the
answer is wrong, and to quote the answer it disputes. A thread holding only its
author's own comments is not included. A PR-body section headed `Operator
decisions` (markdown heading of any level, up to the next heading of the same or
higher level) is included verbatim, capped at 4 KB, as settled scope rather than
defects. The added context is capped at 12 KB; the prompt says when answered
findings or the decisions section were cut. If the thread read fails the review
runs without the answered findings (the PR-body decisions still apply) and the
audit event carries `context_error`. Each pass
receipt and the `review` audit event record `answered_context: <n>`, the number
of answered findings the prompt carried (`context_truncated` marks a cut).

#### Review rounds, contract and budget

A review converges when each round reads less than the one before. The
**round** of a head is read from the reviews mergewake already posted on the PR
(each pass embeds `round` in its `mergewake-pass` marker), so every host agrees
on it.

- **Round one reviews the whole diff.** Every later round reviews only the
  *interdiff*: the hunks of the new PR diff whose added and removed lines the
  previous reviewed head's PR diff (`compare <base>...<last reviewed head>`) does
  not already carry, per file. Both are PR-only patches, so a clean base merge
  or a rebase that only moves line numbers or context adds nothing. A hunk that
  mixes old and new changes counts as changed and is kept whole. The prompt
  says it is a delta round, not a fresh full review: the reviewer checks the
  changed lines and their impact paths (callers, config and data they feed) and
  reports a defect outside the shown hunks only when a changed line causes it,
  anchored on that changed line (`fleet-review` §Rounds); a finding on any other
  line fails the same new-side-line check as before. The panewire brief carries the delta patch inline instead of `git diff`.
  Secret-like strings in removed lines are redacted, including in mixed hunks;
  added and context lines are verbatim.
  When the previous patch cannot be read, the round reviews the whole diff and
  the audit event says why (`round_note`). A round counts only when the
  interdiff contains a hunk the previous reviewed patch did not carry. A new
  head that adds no such hunk (including one that removes earlier PR-only
  hunks whole) reuses the previous round number, reviews the whole diff for
  reference only, and reports only a new critical or high finding. Hunks are
  compared by their changed lines, so reverting only part of a hunk leaves a
  hunk the previous patch did not carry, and that head counts as a new round.
- **Prior findings stay settled.** From round two the prompt lists every finding
  the review login posted in earlier rounds with its disposition (`resolved`,
  `answered: <latest reply>`, or `none yet (still open)`), and tells the reviewer
  not to reopen or repeat any of them unless a changed line gives new evidence,
  in which case it quotes the disposition it disputes. A reply by the review
  login itself counts as a disposition when it is not itself a finding:
  mergewake usually posts under the operator account the PR's author also
  replies from. These replace the review login's own entries in "Already
  answered on this PR", and `answered_context` counts the prior findings
  carried with a disposition. The 12 KB context cap still holds.
- **No duplicate is posted.** A finding with the same path as an existing review
  thread (any author) and substantially the same words (word-set overlap of at
  least a half, or a short finding almost contained in a longer one) is dropped
  before posting, as is a finding repeating one an earlier pass of the same run
  posted. The `review` audit event counts them as `duplicates_dropped`.
- **Closed contract.** A PR-body section headed `Acceptance` (`Acceptance
  criteria`, `Acceptance (…)`; any heading level, up to the next heading of the
  same or higher level), else the same section in the body document named by
  the task's `body_doc` field for the task named by the `Mergewake-Task`
  trailer when the sink can read task text, is capped at 4 KB and passed to the reviewer as
  the closed list of what the change must do. The reviewer marks a finding
  outside it with `"advisory": true`. mergewake then caps the finding at
  `medium` (unrated becomes `low`), posts it with an `<!-- mergewake-advisory -->`
  marker and an "Advisory" label, and the merge gate grades it as advisory: it
  needs no answer and never blocks. A `critical` finding (security, data loss, or
  a defect in the stated change) keeps its severity and is never advisory. Without
  an Acceptance section the flag is ignored and every finding is in scope; the
  `review` event records `contract: <source>`.
- **Budget.** After `review.budget_rounds` rounds (default 3; negative turns it
  off) later rounds are *quiet*: the prompt asks for critical and high findings
  only and mergewake drops medium and low ones (`budget_dropped`). A quiet pass
  receipt carries `quiet: true`, and every pass records `round` and, for a delta
  round, `delta` (the previous reviewed head). Reviews posted before round
  counting count one round per reviewed head.
- **Late-round findings notice** (`late-round-findings`). After all required
  passes for the first round past `review.budget_rounds` (round 4 by default)
  post, mergewake sends the PR's design lane one informational notice when that
  round has at least one new critical/high finding on a line added after the
  first head. It reports the round, total new critical/high findings, the
  subset on lines added after the first head, the subset disputing an earlier
  author reply, and the subset on documents (`*.md` or `docs/**`). It is
  deduplicated by PR and round across restarts and does not hold or delay review.
  A finding disputes an earlier reply when it repeats an earlier thread that
  contains a reply by the PR author or review login, or when its text quotes at
  least five consecutive words from such a reply, or shares at least two reply
  terms of four or more characters alongside an explicit rebuttal cue (`but`,
  `still`, `however`, a negation, `fails`, `instead`, or `contradicts`).
- **Review-round rule (consult, ladder, hold).** A round ends without a pass when its review
  leaves at least one critical or high finding still open (not fixed or dismissed
  with evidence) after the verdict; the hub consult below uses its own, lower bar (any open
  finding above low). The round thresholds come from configuration
  (`review.round_rule`, defaults 3 / 5 / 7). The consult and the ladder act on the first qualifying round at or
  after their number, once per PR, not at one exact round: a round that was not done before the author pushed, or
  that had nothing open, does not use the step up, and a round that reaches its number with nothing qualifying still
  does nothing (an all-low round gets no consult, decision 250):
  - **Round consult (`review.round_rule.consult`, default 3):** The findings delivery to the worker carries the directive once:
    `stop patching; ask your design lane for a consult one grade above your seat (another family preferred, not required): ESC --type decision to your owner lane with the packet as an hk doc key (every finding so far, ask for the root cause); do not run panewire consult request and do not call any model API yourself`
    No panewire call or lane event is emitted.
  - **Hub consult (`review.round_rule.hub_consult`, default false):** When on, the consult round asks the hub for the
    consult instead of telling the author to (operator decision 250; panewire docs/consult-on-request.md). With the
    switch on, the request is gated on findings above low rather than on the round rule's blocking bar: the round must
    be complete (the head's `-done` marker) and at least one finding must still be open above low — `medium`, `high`
    or `critical`, with the legacy aliases `blocking` (high) and `major` (CodeRabbit's rank above minor). Open
    severities come from the PR's review threads, as the gate reads them: the first comment's label, unresolved and not
    disposed (a medium finding by any human answer, a critical or high one only by a verified fix or a reasoned
    dismissal). An all-low round requests nothing (decision 250); with the switch off, the directive path below keeps
    its blocking condition. The consult is owed on the first done round at or after the consult round that has
    such a finding open; a consult that may have reached the hub is never asked again for the PR (decision 250), whatever
    round or head the PR is at later. mergewake writes a question document to hk (the PR, the reviewed head, every open
    finding with id, severity, path:line and text, and the task's contract pointer), then runs once per PR,
    on the daemon host bounded at 60 s:
    `panewire consult request --task <id> --question hk:doc/<key> [--author-job <job> | --author-family <family>]
    --pr <owner/repo#n> --head <sha> --requester mergewake/<owner/repo>#<n> --json` through the hub credential
    (`reap.hub`). A PR without a Mergewake-Task trailer skips the consult. The row lives in the round-rule state
    file, keyed by the head it was requested for. Later passes read a non-terminal row with
    `GET /v1/consults/{id}` (over `reap.hub`, whatever `reap.transport` is) and never re-request it (a re-POST of a
    terminal row would retry it), until `review.round_rule.hub_consult_wait` (default 2h, the hub's consult timeout)
    passes; the wait also bounds a failed task read or question write, and it is measured from the first attempt.
    The obligation is owed until its outcome is delivered, whatever the current review round or head says. A push
    before the answer does not drop the consult: the row is still followed on the hub (never POSTed again, its
    question document never rewritten) and its outcome goes out once with the next author delivery, naming the head
    it was asked for; the new head's findings are not held behind it. A request that never reached the hub (its
    question write or task read failed first) is the only one a push drops (`consult-superseded`), and the new head
    then owes the consult. While a consult for the current head is queued or running that round's findings wait — a
    restart resumes the poll from the state file. The row is journaled in the audit log
    before the state file, so an id lost to a failed state write is reconciled from it instead of re-requested.
    `answered` delivers the findings with the `answer_doc` key (passed through unread) and no ask-a-consult
    directive; `refused`, `failed`, `cancelled`, an rc 1/2 request failure or an expired wait delivers them as today
    plus one line saying the consult could not be had and why. A settled consult records one hk task comment (id,
    state, seat, answer_doc) that retries on its own until hk accepts it. Request, state change, a dropped request,
    outcome and delivery are audited as `consult-request` / `consult-state` / `consult-superseded` /
    `consult-outcome` / `consult-delivered`.
  - **Round ladder (`review.round_rule.ladder`, default 5):** on the first done round at or after the ladder round
    with a critical or high finding open, mergewake runs, once per (task, PR, round ladder), on the daemon host bounded at 60 s:
    `panewire escalate-author --task <id> --job <author job id> --pr <owner/repo#n> --round <n> --findings-file <path>`,
    where the file is a JSON array of `{ref, severity, path, line, title, body}` for every unresolved finding so far (resolved threads are left out).
    rc 0 marks the step escalated (`ladder-escalated`); rc 2 marks it escalated too, the author
    untouched (`ladder-intact`); rc 124 is pending — audited `ladder-pending`, not marked, and
    retried by a later evaluation with the same arguments until a 2-hour bound, after which it is
    treated as rc 1; rc 1 (or pending past the bound) marks it escalated (`ladder-ended-no-successor`)
    and sends the owner lane one `decision` event that the author ended without a successor; any
    other rc, timeout or missing command is logged (`escalate-author-error`), marked escalated, and
    review continues. The whole step runs under the round-rule state lock, so concurrent evaluations
    of one PR cannot call it twice.
  - **Round hold (`review.round_rule.hold`, default 7):** mergewake holds the PR (no further review rounds or merges for it; gate blocks with `round_hold`)
    and sends the design lane (the task's lane, else the desk) exactly one `decision` event naming the PR,
    round count, open findings, and the two choices: cut the scope or request one SS consult on the design.
  - **Releasing the hold:** The hold ends when a PR comment matching `Mergewake-Round-Hold: release` is posted after the hold began
    by a non-bot account — the review login counts, since it is also the operator's account. Comments mergewake generated
    (they carry a `<!-- mergewake` marker, including the hold decision text itself) never count. Once released, review and gate resume;
    the release is audited with the releasing login.

Round, `delta_base`, `quiet`, `prior_context`, `advisory` and the drop counts
are on the `review` audit event.



Output must be exactly one JSON object:

```json
{"findings":[{"path":"src/tool.go","line":12,"severity":"high","body":"Concrete bug and consequence"}]}
```

Severity is `critical`, `high`, `medium`, or `low`; legacy `blocking`, `major`,
`minor`, and missing severity remain readable. Missing severity is recorded as
`unspecified` and cannot support a clean pass. An empty findings array is valid.
Lines refer to the new side of the diff. At most 50 findings and 4000 bytes per
body are accepted; unknown fields, extra JSON and findings outside the diff fail.

### Outcomes and shared measurement

Observations are appended to `<state_dir>/outcomes.jsonl`, even in shadow mode.
The file is also a durable publication outbox; it is protected by an OS lock,
uses mode 0600, and is fsynced before shared publication. Dry-run writes no ledger
or reps. Daemon PR polling records drafts too, revisits tracked PRs after they
leave the open list, and observes desk merges and closures.

Author attribution comes from these PR **body trailers**:

```text
Mergewake-Author-Family: openai
Mergewake-Author-Profile: example-profile@high
Mergewake-Author-Model: concrete-model-id
Mergewake-Experiment: experiment-name/baseline
Mergewake-Grade: A
Mergewake-Tier: T1
```

`Mergewake-Author-Model` is optional when the configured author profile has a
concrete `--model` argument. Use `baseline` and `downshift` arm names for exploration compatibility.
Grade and tier come from the trailer, else from the `(grade X, Tn)` marker in the
title of the PR's `Mergewake-Task`, else they are unknown; no config default fills
them. A row with an unknown grade is never published; `outcomes report` counts it
in `grade_unknown`. Each posted pass records its reviewer in `reviewers_by_head`.
Missing attribution stays visible locally and
is retained for repair rather than fabricated for a shared rep.

The first CI observation uses attempt 1 of the earliest PR run of each required
workflow, across prior heads. A successful rerun cannot overwrite an initial red
run. Missing, expired or pending evidence remains unknown. Existing exact-head reviews and severity markers are recovered from GitHub as well
as from the local pass. Findings are counted
by severity and head without counting the same review twice. Merge latency is
measured from GitHub's `created_at` to `merged_at`. Recovery creation is recorded
separately from an actual merged watch revert within seven days.

The conservative rule is shared by ledger reports and structured rep notes:

- **clean_pass** requires first CI green, a completed review of the landed head without critical/high
  (or legacy blocking) findings, and a completed seven-day window with no revert.
- **failure** includes critical/high/blocking findings or a merged watch revert in
  that window.
- **with_fixes** covers an initially red CI run or unspecified finding severity
  after the other observations complete; initial red CI alone is not demotion evidence.
- **pending** covers incomplete observations, missing review, and an
  unfinished seven-day window. An open or unmerged PR cannot earn a clean pass.

Merge time is an efficiency metric and never changes evidence classification or
a grade. Mergewake does not change model grades automatically.

`outcomes.command` defaults to `review.panewire.command`, which is `panewire`. Its replaceable reps interface uses
`panewire reps add`, the handoffkeep bench reps store, rather than writing hk
HTTP/SQL directly. Each finalized merged/closed PR publishes profile, model,
effort, PR URL as task ref, role `impl`, task grade, tier, rounds, blocking
findings count, and completed `1` only for a landed PR. Rounds is the number of
distinct PR heads that received a mergewake review before the outcome was
recorded: `1` when the PR merged on its first reviewed head, `0` when no head
was reviewed. Structured notes contain
first CI, severity counts (unknown until the landed head is reviewed), seven-day
revert, merge time, experiment/arm, stable
outcome/event IDs, revision and rule classification. `high_by_head` lists the
critical plus high count of each reviewed head, oldest first; it is left out when
the review order of the heads is not recorded (an outcome kept before the order
was). Shadow suppresses GitHub
mutations while still publishing factual measurement reps.

A seven-day result or later revert is an append-only follow-up rep, since the
current reps CLI has no mutable update command. Consumers must collapse records
by `notes.outcome.task_ref` and take the latest revision; a follow-up is not a
second implementation sample. `mergewake outcomes report --source hk` does this.
Quota/grade consumers must use this same convention before counting outcome reps.
Reports expose an attribution_missing count; missing metadata does not stall other
PRs or publish invented samples. The local outbox records confirmed server receipts. It checks stable event IDs
before retrying to cover successful writes followed by a crash before local ack;
unconfirmed/pending CLI responses remain buffered. Inspect and retry errors with
`outcomes sync`; no test writes fabricated reps into the shared store.

Every outcomes CLI call (`reps list`, `reps list --profile`, `reps add`) runs
with the daemon's environment minus these variables, every occurrence, matched
on the whole name before `=`:

- `HANDOFFKEEP_URL`
- `HANDOFFKEEP_TOKEN`
- `HANDOFFKEEP_TOKEN_SVC_MERGEWAKE`
- `HANDOFFKEEP_CF_ACCESS_CLIENT_ID`
- `HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET`

They carry the daemon's own [hk HTTP credential](#hk-http-credential). The child
never receives `HANDOFFKEEP_URL`, `HANDOFFKEEP_TOKEN`, `HANDOFFKEEP_TOKEN_SVC_MERGEWAKE`
or the Cloudflare Access ids. panewire reads `HANDOFFKEEP_URL` with
`HANDOFFKEEP_TOKEN_SVC_PANEWIRE` or its own `hk-svc.env`, not hk's `config.env`.
Everything else is kept, including `PATH`, `HOME`, `XDG_*`,
`HANDOFFKEEP_CONFIG`, `PANEWIRE_*` and `GH_TOKEN`, so panewire reaches the bench
through its own configuration. The daemon's own environment is not
changed; its hk HTTP clients keep these variables.

Only a `srv:` row is a receipt. Before publishing, mergewake lists the
profile's reps and looks for a row whose notes carry the same outcome id. A
`srv:` row takes precedence over pending local rows:

| Earlier row | Result |
|---|---|
| `id=srv:N` | Confirmed and recorded as published; no add. |
| `id=origin:N` | Run `reps pending retry origin:N` within the same 45-second bound. A reply with a positive server `id` records `srv:<id>`; on failure, the outcome stays in the outbox and the next flush retries. |
| `id=local:N` | No retry or add. The outcome is not published and stays in the outbox: `rep delivered locally only (local:N), not shared; retained in outbox`. |
| None | `reps add`. A reply other than `recorded rep id=srv:N` stays in the outbox; the next flush finds its row and retries it if it has an `origin:N` id. |

A `local:N` row stops duplicate adds but is never counted as shared:
`outcomes report --source hk` reads `srv:` rows only.

Publishing runs in one background publisher in the daemon, while the `pr` role
is on. It runs at start, on a nudge after each land and each closed-row
reconcile that read a row, and on a timer of `poll_interval`; each publish is bounded to 45 seconds and a run
attempts every pending outcome of every repo (it has no shared deadline that
slow outcomes could use up), and the publisher stops with the daemon. Land, the PR sweep and the reconcile never publish, so none of them
waits for a publish and the repo's merge lock is never held across one. A one-shot
`mergewake land` leaves its outcome in the outbox for the daemon or
`mergewake outcomes sync`.
The ledger lock is held only to read the outbox and to append each receipt, so
other ledger writers are never blocked by a publish. A flush, whether the
publisher's or `mergewake outcomes sync`, takes a publish lock
(`<state_dir>/outcomes.publish.lock`) first and reads the outbox after it, so two
flushes never publish at once; the stable event ID check above covers a crash
between a publish and its receipt. The publisher's flush does not
fail when publication fails. It writes one `outcome-publish-error`
audit row to the repo's `decisions.jsonl` (`repo`, `count` of failed outcomes,
and `error` cut to 512 bytes of valid UTF-8), logs a warning, and keeps the
outcomes in the outbox for the next flush. A ledger read or write failure,
or an `outcome-publish-error` row that cannot be written, is logged by the
publisher and retried on its next run. `mergewake outcomes sync` stays strict: it exits
non-zero when any outcome fails to publish.

```sh
mergewake outcomes report --since 2026-10-01
mergewake outcomes report --since 168h --source hk
mergewake outcomes sync
```

`--since` filters by PR open time and accepts YYYY-MM-DD, RFC3339 or a positive Go
duration ago. Reports group by profile@effort and experiment/arm and show clean,
failed, with-fixes, pending, CI-known, revert-known and latency denominators separately.
`--source local` is the default. The hk source reads confirmed mergewake notes
through the configured reps CLI, not other manual bench records.

### `notifier`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `type` | `noop` (also empty) | `noop`, `stdout`, or `panewire`. |
| `command` | `panewire` for that adapter | Executable path. Ignored for noop/stdout. |
| `lane` | Required for panewire | Destination lane; e.g. `operator-desk`. Must match panewire's lane rule: 1–128 characters, starting with an ASCII letter or digit and continuing with ASCII letters, digits, `.`, `_`, `:`, or `-`. |
| `fallback_lane` | `lane` | Lane for PRs whose task trailer resolves to no lane and which carry no `Mergewake-Notify-Lane` trailer; also retried once when delivery to the resolved lane fails. Must match the same panewire lane rule as `lane`. |
| `tool_lane` | Desk lane | Lane that owns mergewake's own automation (lost review results, exhausted review retries). Empty falls back to the desk (`notifier.fallback_lane`, else `notifier.lane`), so nothing changes until it is set. Must match the same panewire lane rule as `lane`. |
| `cause_threshold` | `10` | Reviewer-problem events of one cause in one repo, within `cause_window`, at which one notice goes to `tool_lane` with the cause and the count; the notice then stays quiet for the rest of the window. Must be a positive integer when set. Every such event is recorded as a `notice-cause` audit row, sent or not. A fix-needed cause (`spawn:auth`, `spawn:workspace`, `spawn:node-upgrade`, `spawn:host-unconfigured`, `spawn:repo-root`, `spawn:brief-upload`) is told once per repo per hour at once, whatever its count. Spawn causes come from the reviewer skip text (`spawn:quota`, `spawn:admission-refused`, `spawn:timeout`, `spawn:expired`, `spawn:other`, and the fix-needed ones above). |
| `cause_window` | `1h` | The window `cause_threshold` counts in, and the quiet time after a threshold notice. Must be a positive duration when set. |

The panewire adapter executes literal arguments equivalent to:

```sh
panewire emit --kind lane.event --lane operator-desk --event-id EVENT_ID --text TEXT
```

An event may carry its own lane (the `daemon.pr.notify` routing rules above);
an explicit event lane overrides `notifier.lane` for that emit only.

No-op emits nothing; stdout prints event ID and text. Review and landing produce
local decision output; CI recovery and attempted installs also use the adapters.
Healthy repeated agent polls do not emit a fresh installation notification.

#### Notice routing

Notices mergewake's own automation raises go to the tool lane
(`notifier.tool_lane`), not the desk; notices that need a person go to the
PR's design lane. Event ids are stable, so the hub dedupe still works.

| Notice | Lane | Fallback |
| --- | --- | --- |
| Lost review result (`mergewake-review-result-lost-…`) | Tool lane | The desk, when no tool lane is set. One notice per incident; the 10-minute dedupe still applies. |
| Lost review, retries exhausted (`…-owner-pr-<pr>`) | PR's design lane (`repos[].lane`) | The desk, when the repo names no lane. One notice per PR, as before. |
| Review retries exhausted (`mergewake-review-failed-…`) | PR's design lane, plus the tool lane (same text, `…-tool` event id) | Without a tool lane the design-lane notice is the only copy, exactly as before. One notice per PR and head on each lane. |
| Round hold / ladder ended (`mergewake-round-hold-…`, `mergewake-ladder-ended-…`) | The `Mergewake-Notify-Lane` trailer, else the task's lane, else the repo's lane | The desk, only when no lane resolves (a repo with no design session). |

### `sink`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `type` | `noop` (also empty) | `noop` or `handoffkeep`. |
| `command` | `handoffkeep` for that adapter | The hk CLI: still run for deploy approvals (`deploy_task`), and named to reviewers as the command that reads a large review brief. The task, attachment and document calls below do not run it. |
| `reopen_state` | `reopened` | Only `reopened` is accepted; terminal tasks use the explicit reason-required operation. |
| `reconcile_every` | `1h` | Positive duration, or `off`. How often the daemon checks hk's unreconciled PR rows against GitHub and writes the merged and closed ones back; see [unreconciled-row sync](#unreconciled-row-sync). |

The handoffkeep adapter reads the task (`GET /v1/tasks/ID`), posts the reason
as a comment (`POST /v1/tasks/ID/comments`), then reopens a merged or dropped
task (`POST /v1/tasks/ID/reopen` with the reason). An already reopened task
only receives the comment. Empty reasons are refused. A server that lacks
reopen returns an explicit failure; the recovery comment is retained and the
watch notification reports the failure. Protected upstream authority fences
remain enforced. No fallback generic transition is attempted. Structured
reports are `note` documents (`PUT /v1/documents/KEY`). These calls go over
HTTP with the [hk HTTP credential](#hk-http-credential).

#### unreconciled-row sync

hk keeps one change row per PR, and the daemon writes it as its passes see
PRs. A PR merged outside mergewake's pass is never written as merged, so its
row stays open and the hk tasks waiting on it stay stuck between merged and
deployed. With the `handoffkeep` sink the daemon closes that gap: every
`sink.reconcile_every` (default `1h`, `off` disables it) it reads hk's
`GET /v1/changes/unreconciled` rows — rows hk holds open, and rows hk holds
merged with no merge commit, oldest first — and for each row of a configured
repository it reads the PR through the client (`Pull`, whose merged commit
comes from GraphQL) and writes the state through the change-state writer. A
merged row is written with `merge_sha` and `merged_at`, a closed-unmerged row
as `closed`, and a PR that is still open gets no write. Rows of repositories
this host does not configure are counted and skipped, never read.

The endpoint pages by `after_id`: a run reads pages forward from the cursor —
the last row the previous run reached — until it has checked 500 rows or the
list ends (`next_after_id` null). At the end the cursor goes back to 0, so the
rotation covers the whole list however long it grows, and rows past the first
page are reached. A run that uses up its time bound stops where it is: the
cursor stays at the last row it finished, so the rows it did not reach are the
next run's first. A row whose PR read fails, or that is merged with no merge
commit yet, does not wait for the rotation either: the next run reads from just
before the smallest such row and checks those rows first, within a bounded
retry pass (5 minutes, 100 rows, oldest out first); a retry id the list no
longer has leaves the set. An hk without `after_id` paging ignores the
parameter and answers one page with no `next_after_id`: the run then reads that
page only and rotates inside it, starting after the last row the previous run
checked and wrapping at the end. GitHub reads go through the client's memo and
ETag cache, and the GraphQL resolver runs only for a merged PR. Each run logs
one `hk change sync run` line with `rows`, `merged`, `closed`, `open`,
`other_repo` and `errors`; a failed read (an hk without the endpoint answers
404) is in that line, and the next run retries.

#### hk HTTP credential

mergewake's own HTTP calls to hk authenticate as the hk client
`svc-mergewake`, never with the hk CLI's credential. These calls are:

- job records (`GET /v1/jobs/<id>`);
- change priority and review records (`GET /v1/changes?pr=`);
- unreconciled change rows (`GET /v1/changes/unreconciled`);
- change state (`POST /v1/changes/batch`, `PUT /v1/changes/<section>`);
- task events (`POST /v1/task-events`);
- through the hk public client (`github.com/mgh3326/handoffkeep/client`), the
  sink's task read for the lane, `refs.job_id`, title, priority and state
  (`GET /v1/tasks/<id>`), the recovery comment and reopen
  (`POST /v1/tasks/<id>/comments`, `POST /v1/tasks/<id>/reopen`), structured
  reports (`PUT /v1/documents/<key>`), the large review brief upload
  (`PUT /v1/attachments`), the job archive's attachment uploads
  and task comment, and the deploy approval's task read and record-only
  decision request (`POST /v1/tasks/<id>/decision-request`, which opens no
  Linear issue).

hk records the comments, reopens, decision requests, documents and
attachments as written by `svc-mergewake`. They read the process environment only:

| Variable | Meaning |
| --- | --- |
| `HANDOFFKEEP_URL` | hk base URL. HTTPS is required off loopback and the tailnet. |
| `HANDOFFKEEP_TOKEN_SVC_MERGEWAKE` | Bearer token of `svc-mergewake`, kept in Infisical `/mergewake`. |
| `HANDOFFKEEP_CF_ACCESS_CLIENT_ID`, `HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET` | Optional Cloudflare Access pair. Set both or neither. |

mergewake does not read the hk CLI's `~/.config/handoffkeep/config.env` or
its machine token `HANDOFFKEEP_TOKEN`.
`sink.command` is still accepted, and mergewake runs no hk command with it: a
large review brief's prompt still tells the reviewer to run
`<sink.command> attach get` on its own host.

mergewake reads no attachment and sets no upload cap of its own: hk refuses
an attachment over `HK_ATTACH_MAX_BYTES` (default 50 MiB) with
`attachment_too_large`, a comment over 64 KiB and a document over 512 KiB.

The daemon gets the variables from the unit's `EnvironmentFile`. On oci the
unit does not load `~/.config/mergewake/environment` (the file does not exist
there). A unit drop-in adds `EnvironmentFile=%h/.config/mergewake/hk-svc.env`
(mode 0600) instead; see
[deploy/mergewake.service](../deploy/mergewake.service). mergewake reads the
environment only and knows no file path. A command run from a shell, such as
`mergewake review` or `mergewake watch`, needs the variables in that shell's
environment. A mergewake run by macOS launchd (for example a `mergewake
daemon` LaunchAgent) gets no hk HTTP unless its launcher sets the two keys.

If `HANDOFFKEEP_URL` or `HANDOFFKEEP_TOKEN_SVC_MERGEWAKE` is unset, every hk
HTTP call behaves as on a host with no hk endpoint. Job records cannot be
read, so a panewire review pass fails with `hk-unavailable`. Task events and
change state are not sent, and hk review history and change priority are
empty. Each sink call (task read, comment, reopen, report document,
attachment) fails with `handoffkeep endpoint unconfigured`; there is no
fallback to the hk CLI or its `config.env`. Callers report that failure as
they reported a failed CLI call before: the watch notification carries a
failed reopen, a watch report records the failed write, and a job
archive is an `error` row retried an hour later. With `sink.type: handoffkeep`, the daemon logs one error line at
start, `startup check failed`, that names the unset keys and no value. The
daemon keeps running.

**Rollout.** Do not roll out the mergewake build that reads only
`HANDOFFKEEP_TOKEN_SVC_MERGEWAKE` (hk 3545) before
`~/.config/mergewake/hk-svc.env` exists on the host. Do these in order:

1. The operator or handoffkeep-design writes `HANDOFFKEEP_URL` and
   `HANDOFFKEEP_TOKEN_SVC_MERGEWAKE` (from Infisical `/mergewake`) into
   `hk-svc.env`, mode 0600. If hk is behind Cloudflare Access, add the CF
   Access pair too.
2. Add the unit drop-in `EnvironmentFile=%h/.config/mergewake/hk-svc.env`.
   The deploying session only adds the drop-in and checks that both key
   names are in the file.
3. Restart the daemon.
4. Check that its log has no `startup check failed` line.

### `discord`

Posts the events of every watched repo to Discord from the daemon, so no repo
needs its own workflow or webhook secret. Only `mergewake daemon` posts; other
commands do not.

```yaml
discord:
  enabled: true
  webhooks_file: ~/.config/mergewake/discord-webhooks.env
  kinds:
    pr: DISCORD_WEBHOOK_PR_LOG
    ci: DISCORD_WEBHOOK_GITHUB_ACTION
    revert: DISCORD_WEBHOOK_DEPLOY_LOG
  opt_out: [example/keeps-its-own-workflows]
```

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Turns the feed on. |
| `webhooks_file` | Required when enabled | Absolute or `~/` path to a mode-0600 env file of `KEY=URL` lines. |
| `kinds` | Required when enabled | Maps an event kind to a key in that file. A kind left out posts nothing. Keys are `pr`, `ci` and `revert`. |
| `opt_out` | empty | `owner/name` repos that post nothing, through any path. |
| `max_failures` | `5` | Failed posts a message gets before it drops to a log line. |

A webhook URL is a credential. A URL inline in the YAML is refused at load, even
with the feed off, with the error `discord: inline webhook URL refused`. Errors
and logs name the kind and the key, never the URL. A URL must be `https`; plain
`http` is accepted only to a loopback address.

A group- or world-readable file, or a kind whose key is missing from the file,
fails config load. A file that does not exist yet does not: messages stay queued
until it appears. The file is reread while messages are queued, so a rotated URL
takes effect without a restart.

#### Point the three channels

1. In each Discord channel (`#pr-log`, `#github-action`, `#deploy-log`), open
   **Edit Channel > Integrations > Webhooks** and create a webhook. Copy its URL.
2. Write the URLs to the file, and restrict it:

   ```sh
   cat > ~/.config/mergewake/discord-webhooks.env <<'ENV'
   DISCORD_WEBHOOK_PR_LOG=<#pr-log webhook URL>
   DISCORD_WEBHOOK_GITHUB_ACTION=<#github-action webhook URL>
   DISCORD_WEBHOOK_DEPLOY_LOG=<#deploy-log webhook URL>
   ENV
   chmod 600 ~/.config/mergewake/discord-webhooks.env
   ```

3. Add the `discord` section above to the host config, with `opt_out` listing any
   repo that keeps its own workflows until they are retired.
4. Restart the daemon. The first pass for each repo records a baseline.

#### What posts

| Kind | Event | Posted by | Dedupe key |
| --- | --- | --- | --- |
| `pr` | PR opened; a draft counts once it is ready for review | `pr` role | PR |
| `pr` | PR merged or closed | `pr` role | PR, head |
| `ci` | Required check failed on an open PR's head | `pr` role | PR, head |
| `ci` | Required or `watch_checks` check failed on the default branch | `watch` role | head |
| `revert` | Revert PR opened, culprit reported, or watch hold | `watch` role | head, action |

Each event posts one message, and restarts stay quiet. Some consequences:

- A force push is not a new PR, so it does not post again. A new red head does.
- `ci` posts the checks that have failed when the head is first seen red. A
  sibling check that fails later on the same head does not post again.
- A PR older than the repo's baseline does not post, so turning the feed on does
  not replay history. A merged or closed PR also has to be under 72 hours old. A
  `ci` failure has no such limit: heads already red when the feed turns on post
  once.
- PRs left out by `daemon.pr.skip_bots` stay out of the feed.
- A reopened PR does not post.

A kind only posts while its role runs: enable `daemon.watch` for default-branch
`ci` and for `revert`. Shadow mode still posts PR and `ci` events, because
reporting is not acting. It posts no `revert`, because shadow mode opens no revert. `--dry-run` posts nothing and writes no feed
state.

#### Delivery

A role writes each message to a local outbox and moves on; one sender goroutine
posts them. A slow or down webhook therefore never holds up a poll, and a feed
problem never counts as a role failure: a GitHub read that fails is logged in
`decisions.jsonl` as `discord-feed-error` and retried on the next pass.

| Response | What the sender does |
| --- | --- |
| 2xx | Done. If the rate-limit bucket is spent, the webhook waits for it to reset. |
| 429 | Waits for `Retry-After` (or the body's `retry_after`) and retries. It is not a failure. |
| Network error, 408 or 5xx | Backs off from 5s to at most 5m. After `max_failures`, drops to a log line. |
| Any other 4xx | Cannot succeed on retry, so drops to a log line at once. |

A dropped message is logged to stderr as `discord message dropped` with the repo,
the key and the reason, and does not post again. Messages for other webhooks keep
flowing. Past 1000 queued messages, a new one drops the same way. A crash between
a successful post and recording it can post that message once more.

## Per-repo policy

| Key | Required / default | Meaning |
| --- | --- | --- |
| `version` | Required: `1` | Policy schema version. |
| `enabled` | `false` | Explicit opt-in for acting `land` (auto-merge on a READY verdict) and `watch`. A caller can force shadow with `--shadow`; there is no CLI bypass of `false`. |
| `required_checks` | Required, nonempty | Exact, unique GitHub check-run names or commit-status contexts. Every required result must be successful. |
| `watch_checks` | `required_checks` | Exact, unique check names that judge a main-branch commit (watch and bisect). |
| `deploy_checks` | Empty | Exact, unique check names of the deploy stage, for example `[deploy-ok]`. When set, the watch deploy stream judges by them; empty means the repository has no deploy stage and the main checks judge the commit. |
| `notify_checks` | Empty | Exact, unique check names that only notify: a red streak on the default branch sends the design lane one notice per streak and never reverts, latches or bisects. See [Notify-only checks](#notify-only-checks). |
| `blocking` | Empty list | Rules identifying changes whose harm may happen before a revert. Routine is the default for other changes. A change to this policy file itself always counts as blocking. |
| `review_gate` | Absent | Review-gate settings. With `land: graded` the land gate grades review findings instead of counting unresolved threads; see [Graded review findings](#graded-review-findings). Without `land: graded` the section does not change the land gate (raw count). |
| `fast_lane` | Absent | `{max_changed_lines: N}`: a PR that touches no blocking path and changes at most `N` lines (additions plus deletions) is held only by block-class findings. See [Graded review findings](#graded-review-findings). |

Policy files decode strictly: a key not listed here, or in the sections below,
is an error that names it. These keys were removed, and a policy that still sets
one is refused:

- `require_up_to_date` and `auto_update_branch`. A PR whose base moved on merges
  on its own head; the land and notify records carry `base_behind: true`, and
  the post-merge CI checks the composed result. mergewake no longer holds a head
  as `base_stale` and no longer updates a PR branch. Records and state files
  from earlier releases stay in place and are not read.
- `main_checks`. `watch_checks` is the one name for the checks that judge a
  main-branch commit.
- `bot_updates`. Every bot PR stays dropped by `daemon.pr.skip_bots`, or goes
  through review like any other PR.
- `any_ref_timeout`. The any-ref wait is fixed at 45 minutes.


Each blocking rule has:

| Key | Required / default | Meaning |
| --- | --- | --- |
| `paths` | Required, nonempty | Path globs; any match qualifies. Both sides of renames are checked. |
| `kinds` | Empty / all kinds | Allowed GitHub change statuses: `added`, `modified`, `removed`, `renamed`, `copied`, `changed`, `unchanged`. |
| `reason` | Required, nonempty | Concrete reason harm could precede a revert; included in the decision record. |

A rule matches `(any path) AND (any kind)`. `**` must be a whole slash-separated
segment and spans zero or more components. Ordinary `*` never crosses `/`.
Leading `/`, backslashes, empty segments and `..` are refused. Blocking rules
combine with OR; there are no per-rule merge overrides, finding severity bypasses,
or advisory rounds for blocking findings.

### Graded review findings

Without `review_gate.land: graded` or `fast_lane`, the land gate holds on the raw
count of unresolved review threads (`threads_open:<n>`); a resolved thread is
resolved at any severity. A `review_gate` section alone changes
nothing, so a policy that already carries one for other reasons keeps its
behaviour. With `review_gate: {land: graded}` (all other keys optional) the gate
grades each thread by the author and severity of its first comment instead:

| Grade | Holds the merge until | Reason |
| --- | --- | --- |
| block | the thread is resolved, or a human reply (not the finding's author, not a bot; for a `review.login` finding, that login's own reply that is not itself a finding) carries a block disposition: it opens with `Fix`/`Fixed`/`Fixes` and names a commit that GitHub knows, that comes after the head the finding was posted on, and that is in the PR head's history; or it opens with `Dismiss`/`Dismissed`/`Dismissal` followed by its reason. The latest answer decides. An acknowledgement, a defer, a bare "Dismissed.", or a fix naming an unknown commit or one outside the PR does not clear it. A commit lookup GitHub cannot answer holds the verdict (an error) rather than passing or blocking it. Neither needs a new head. | `blocking_findings:<n>` |
| triage | it has a disposition: someone other than the finding's author (and not a bot) replies on the thread with a fix SHA, a dismissal with its reason, or `Disposition: defer <task id>` naming an existing hk task other than the PR's own task (the id as `4346`, `hk 4346`, `hk:4346` or `#4346`, case-insensitive); or the thread is resolved. An own-task or missing-task defer is not accepted, and neither is an id hk cannot hold (`0`, `007`, or one too large for int64): that reply is not looked up. Neither needs a new head. | `triage_unanswered:<n>` |
| advisory | never holds and is not counted as unanswered. Threads from authors no rule names are advisory. | none |

A thread answered with a disposition counts as answered whether or not it is
resolved. A dismissed critical/high no longer holds the gate; the land record
counts it as `dismissed_blocking`, and the READY decision row's summary says
`<n> critical/high dismissed: check the evidence` so the desk checks the
dismissal before merging (`fleet-review` §Triage rubric).

Each reviewer whose latest review still requests changes adds one to the
blocking count (at least one when GitHub's overall decision is changes
requested), on top of any unresolved block-class threads. An unresolved
thread with no comment to grade counts as a blocking finding. Severities come from the `<!-- mergewake-severity:... -->` marker on
mergewake's own findings and from the prose labels CodeRabbit (`critical`,
`major`, `minor`, `nitpick`) and codex (`P1`–`P3`) use. Evidence the rules cannot
read still needs an answer: a finding by the `review.login` identity with a
missing or unknown severity, and on a blocking-class PR an unknown severity from
a graded author.

The defaults grade CodeRabbit `critical` and `major` and codex `P1` as block
(operator 2026-11-01: a CodeRabbit Major ranks with a high, fixed or dismissed
with a reason, never deferred), codex `P2` as triage, and CodeRabbit
`minor`/`nitpick` and codex `P3` as advisory. For the configured `review.login`,
`critical`/`high` block (the reviewer prompt's `blocking` grades like `high`),
while `medium` and `major` findings require triage on blocking-class PRs and remain advisory
on routine PRs. Its `low` findings remain advisory. Set
`review_gate.own_medium_triage: false` to disable the default medium and major rule;
when omitted or true, it is enabled.

Rules are `{author, severity: [...], classes: [...]}` lists under `block` and
`triage`; `classes` selects the PR change class. An explicit list replaces the
default for those two keys. Naming `review.login` in `block` replaces its default
block rule; naming it in `triage` replaces its default triage rule.
Thread comments are read in full (paged past 50), since a late reply answers a
finding. `ignore_authors` skips threads from the named bots. `ignore_outdated` (default
true) clears an outdated finding by anyone but `review.login`: a block finding
(a CodeRabbit `major` included) once its reviewer has reviewed the current
head. The other `review_gate` keys stay parseable but do not affect the land
gate.

### Patch-id review verdicts

A review verdict covers a patch, not a SHA. mergewake computes the patch id of
the PR diff (`merge-base..head`, as GitHub serves it) the way
`git patch-id --stable` does: a hash of each file's diff with whitespace and
hunk line numbers left out, summed over the files so their order does not
matter. Merging the base into the head, or rebasing it, changes the head SHA and
the diff's `index` and hunk-offset lines but not the patch id.

The id mergewake records and compares is that value, a colon, and twelve hex
characters of a second digest over the exact text of the changed lines and file
headers (context lines excluded). `git patch-id` ignores whitespace, so on its
own a whitespace-only edit, which can change behaviour (indentation, a Makefile
recipe), would inherit a review; with the digest it is a different patch and is
reviewed. The one other difference from git: a file ends at the next `diff`
line, so a hunkless rename or mode change is not folded into the following file
and file order never changes the id. Binary diffs hash their blob ids.

- Each posted review pass records the patch id it reviewed in its
  `mergewake-pass` marker (`patch`). The marker is posted under `review.login`,
  so a pushed ref cannot stand in for it.
- A head whose patch id equals a reviewed head's inherits that head's passes:
  `mergewake review` schedules nothing, prints `action: inherited-review` with
  `inherited_from`, and creates the new head's `-done` marker (not under
  `--dry-run`); the gate accepts the inherited passes without a new review. The
  inherited head must have all the passes the PR's class needs, each recorded
  for this patch id: a pass with another patch id, or none, does not count.
- A changed patch id schedules a review as before. A diff that cannot be read
  gives no patch id, and an unknown id equals nothing: that head is reviewed on
  its own. Passes recorded before patch ids existed inherit nothing.
- A moved base never asks for a review. A stale head keeps the review it has;
  once the base is merged in, the new head gets a fresh CI run and, with an
  unchanged patch, no new review, so landing waits on CI alone.
- Finding dispositions live on the PR's review threads, not on a head, so they
  carry across heads. A resolved thread is resolved at any severity; no new head
  is needed to release a resolved high.

`mergewake land --dry-run` (and every `land` audit record) carries a `review`
object: `patch_id` of the head, `rounds` (the number of distinct patch ids
reviewed; a round is one patch), `reviewed_patch_ids`, `inherited_from` (the head
whose passes the gate used, when it is not the head itself) and `dispositions`:
`open_blocking`, `open_triage`, `answered`, `resolved` and `advisory` thread
counts.

`fast_lane` takes only the PR's class and size: when the PR classifies as
routine and `additions + deletions` is at most `max_changed_lines`, triage
findings stop holding the merge and only block-class findings (and requested
changes) do; a `review.login` finding with an unreadable severity still needs
an answer. Routine PRs already need a single review pass. The fast lane alone
grades only the PRs it admits, with the rules of the `review_gate` section (or the
defaults); larger PRs keep the raw count unless `land: graded` is set.

```yaml
version: 1
enabled: true
required_checks: [test]
review_gate:
  land: graded             # block/triage/advisory grading with default rules
fast_lane:
  max_changed_lines: 50    # small routine PRs: only block-class findings hold
```

Pass receipts trust the configured `review.login`, exact commit SHA, and the
mergewake marker. They are not cryptographically signed or independently
attested. If PR authors or other sessions can post as `review.login`, they can
forge a matching pass. This shared-identity risk remains; use a dedicated review
identity and protect its credentials where independent provenance is required.

The repository's policy should cover its own verification controls where changing
them can admit irreversible actions. A PR cannot use its own new policy to grant
itself permission: `land` uses the current base-branch policy.

## PR metadata and automation

Add these optional standalone lines to a PR body:

```text
Mergewake-Author-Family: openai
Mergewake-Task: 42
Mergewake-Notify-Lane: worker-lane
```

`--author-family` overrides the first line. Without it or the trailer, the
family is inferred from the head commit's `Co-Authored-By` trailers, then its
author and committer identities (Claude → anthropic, Codex/GPT → openai,
Devin → cognition, and the other listed aliases); `review.default_family`
covers everything still unknown and the review record marks the family as
inferred. A task trailer overrides `repos[].task`.
Treat the family label as trusted automation metadata, not model provenance
attestation. A handoffkeep-backed watcher refuses recovery with no valid task ID.
`Mergewake-Notify-Lane` names the lane for `daemon.pr.notify` events when the
task trailer cannot be resolved to a lane; then the repo's `repos[].lane`; the configured fallback lane
(`notifier.fallback_lane`, defaulting to the operator lane) is the last
resort, and a delivery the notifier refuses is retried there once before the
verdict is parked with one logged decision. The task and notify-lane trailers
are also read from the head commit message when absent from the body. The
conflict ESC reads `Mergewake-Notify-Lane` from the PR body only and ranks it
first (see "Conflict ESC" under `daemon.pr.notify`).

Use `daemon` for complete polling, or invoke individual commands from optional
webhook/scheduler adapters. The binary never installs or enables service units;
examples in `deploy/` are for the operator's process supervisor.

## Credentials and trust

The built-in client uses `https://api.github.com` and GitHub's upload host, with
API version `2026-03-10`. Supply an environment credential with permissions for
the operations you enable. Read-only shadow use still needs access to the repo's
contents, checks/statuses, PRs and reviews. Acting landing needs PR/content write;
review claims and recovery need content write and PR write; release publication
needs content write. Review resolution uses a read-only GraphQL query. Token or
GitHub App installation policy remains the repository operator's responsibility.

No token fallback shells out to `gh`. An app's review identity must match
`review.login`. API errors omit response bodies to avoid leaking private content.
The source of trust is GitHub and operator-owned host config; manifests are not
cryptographically signed. Protect local config/state permissions and do not share
an install root between inventories or competing writers.

### GitHub App reads

One user token shares a single 5,000 requests per hour budget across every
worker, session and daemon pass that uses it. With `github_app` set, mergewake
sends **reads** with an installation token of a GitHub App, which has its own
budget, and keeps every **write** on the `token_env` credential. Review identity,
`review.login` and the merge gate are unchanged.

```yaml
github_app:
  app_id: 123456                          # positive integer
  private_key_file: /path/to/app-key.pem  # absolute path; or use private_key_env
  # private_key_env: MERGEWAKE_APP_KEY    # name of a variable holding the PEM
```

| Key | Meaning |
| --- | --- |
| `app_id` | Required. The App's numeric ID. |
| `private_key_file` | Absolute path to the App's PEM-encoded RSA private key (PKCS#1 or PKCS#8). `~` expands. |
| `private_key_env` | Name of an environment variable holding that PEM. Set exactly one of the two key sources. |

Loading the config fails, naming the key (never its content), when `app_id` is
not positive, both or neither key source is set, or the key cannot be read or is
not an RSA private key.

How requests are routed:

- `GET` requests and GraphQL **queries** use the installation token of the
  repository they are about. Every other REST call and every GraphQL document
  that mentions `mutation` or `subscription` uses the user token. A request that
  names no repository (for example `/user`) uses the user token.
- Mergewake signs an App JWT (RS256, issued 60 seconds in the past, valid 9
  minutes), finds the installation with `GET /repos/{owner}/{repo}/installation`
  and exchanges it at `POST /app/installations/{id}/access_tokens`. These two
  calls never carry the user token. The installation token is cached and renewed
  5 minutes before it expires; concurrent callers share one renewal.
- The per-pass memo and the object cache work as before. A write on the user
  token still invalidates what it invalidated.
- Rate limits are tracked per credential. A pending wait or an exhausted budget
  on one credential never delays requests that use the other. The daemon
  schedules passes by the credentials that serve reads: the App installations,
  plus the user token only while a repository reads on it as a fallback. A user
  token drained by workers and sessions therefore does not hold passes back;
  the writes in them still wait for that budget.
- Each credential admits four requests at a time (a constant, not a config key):
  the user token and every App installation have their own gate, so a write on
  one never queues behind reads on another. A request waits out its credential's
  throttle before taking a slot and checks again after taking it.
- The ETag and poll cache are kept per credential kind, so a response recorded
  under one credential is never replayed for, or revalidated by, the other.
- The `github requests` log line adds `by_credential` (requests the App and the
  user token each sent, including the two App token calls), `rate_remaining`
  (the last reported `X-RateLimit-Remaining` of each credential seen) and
  `gate_wait_ms` (the longest wait one request spent queued for its gate).

#### App permissions

Grant the App these **read** permissions on the repositories it serves:

| Permission | Level | Reads that need it |
| --- | --- | --- |
| Metadata | Read | Every repository request (granted by default). |
| Contents | Read | Refs, commits, trees, files, compare, release assets. |
| Pull requests | Read | PRs, files, diffs, reviews, review threads (GraphQL). |
| Checks | Read | Check runs. |
| Commit statuses | Read | Commit statuses. |
| Actions | Read | Workflow runs. |

A repository where the App has no installation (`404` on the installation
lookup), or where an App read answers `401` or `403` (a missing permission, a
revoked token, an exhausted App budget), reads with the user token instead.
Mergewake retries a refused read once with the user token, uses the user token
for that repository for 10 minutes (1 minute after a lookup that failed for a
transient reason), and then tries the App again. It logs
`GitHub App not used for <owner/name>: <reason>` once per repository per process,
not per request. A missing permission therefore costs the user budget again, not
a failed pass: read that log line, add the permission and the repository returns
to the App at the next retry.

## Fleet events and crash reports

Every reviewer that is skipped or fails in a review pass is recorded with its
cause. The skip entries in the `review` and `review-skipped` decisions
(`<state_dir>/<repo>/decisions.jsonl`) carry:

- `reviewer` and `reason`;
- `host`, the `review.panewire.host` value for panewire panes;
- `profile`, the launch profile the reviewer ran as: the profile on the pane's
  hk job record for a spawned panewire reviewer. It is omitted when no profile was chosen yet (a
  skip before any spawn, or a spawn that failed before its record was read);
- for a failed process, `exit_code` and `stderr_tail`, the last 512 bytes of
  stderr after redaction;
- otherwise `cause`, the bounded error text.

The same facts are appended as fleet-event v1 lines to
`<state_dir>/events/fleet-events.jsonl`. The file rotates to `.1` at 4 MiB, and
every field is redacted and bounded. Grafana Alloy ships these lines to Loki.

| `kind` | `outcome` | `reason` | `detail` |
| --- | --- | --- | --- |
| `reviewer` | `failed` (exit and stderr) or `skipped` | the skip reason as a code (`panewire_spawn_exit`, ...) | `pass= reviewer= profile= host= exit= reason="..."` then `stderr:` or `cause:` (`profile=` is empty when no profile was chosen yet) |
| `review` | `missing` | `review_missing` (no reviewer produced the pass) or `review_deadline` | `pass= skipped=` and the skip summary |
| `role_error` | `failed` | `role_failed` (one code for every failed daemon pass) | `role= failures= error="..."`: the role, its consecutive failure count after this pass, and the quoted error text |

A daemon role pass that returns an error appends one `role_error` line, so every
failure stays visible after the status file's `last_error` is overwritten or
cleared. A successful pass writes no line, and a failed event write never stops
the loop or the status save.

The shipping config, dashboard and alerts are in panewire `deploy/alloy` and
`deploy/grafana`.

When `SENTRY_DSN_MERGEWAKE` is set in the daemon's environment, a fatal panic
in any daemon goroutine is written to `<state_dir>/crash/`. The next daemon
start sends it to Sentry as one fatal event and then deletes the file. The
event carries the panic line and a redacted traceback in which the home
directory is replaced with `~`. A host sends at most three crash reports in
any 24 hours, counted in `<state_dir>/crash/sent` across restarts (only reports Sentry
accepted count, so an outage keeps the files for the next start), so a
restart loop cannot exhaust the Sentry free tier. Reports over that budget are
dropped. Mergewake sends no other errors, traces, breadcrumbs
or user data to Sentry. When the variable is unset, nothing is written or sent.

## Local state and operator recovery

For `example/project`, state resides beneath
`<state_dir>/example/project/`:

| Path | Meaning |
| --- | --- |
| `decisions.jsonl` | Append-only observations/intent and completed recovery records. A planned action is not proof it completed. |
| `reap.jsonl` | Append-only reap receipts: worktree removals, branch deletions, reviewer-reap and reviewer-cleanup rows, skips and `would-*` plans, each with repo, PR, host and worktree; `archive-job` rows record job-dir archives; `check-pr` rows stamp refetch attempts and `files` names refusing ignored paths. Old `close-pane` and `job-done` rows from before panewire's node took over pane closes still load but no new ones are written. |
| `../archive-jobs.jsonl` | `<state_dir>/archive-jobs.jsonl`, host-wide: one `archive-job` row per job dir `archive-jobs` looked at — archived (with the Drive directory, hk ids and excluded names), planned or skipped with its reason. |
| `../archive-jobs-next-host` | `<state_dir>/archive-jobs-next-host`, host-wide: the host name a full `archive-jobs` run starts at after a run stopped on its budget. A `--dry-run` run reads it to order its preview and never writes it. Absent means config order. |
| `last-green` | Last observed green main SHA; validated again during diagnosis. |
| `recovery.json` | The red-main incident: when main first turned red, the culprit and green baseline, the 2/2 any-ref evidence, the fix window and the revert PR. Persisted, so a restart keeps the same window; deleted once main is green. |
| `recovery-urgent.json` | PRs watch named as the fix for a red main (`Fixes-Red-Main`). The review queue reads it through `ReviewPriority`, so the fix is reviewed first. Deleted with the incident. |
| `watch-reported` | Most recent 128 hold/report keys already notified; repeat polls stay quiet. |
| `notify-checks.json` | Per `notify_checks` name: the last green head, the head its red streak began on and whether the design lane was told. Deleted names are simply forgotten; the file is only written while a streak is tracked. |
| `pr-notify-sent` | Most recent 256 `verdict PR head [reasons]` keys already lane-notified by the pr role, plus `decided` keys for verdicts parked after a failed delivery; a new head re-arms notification and restarts stay quiet. |
| `pr-notify-merged-pending.json` | MERGED notices awaiting successful delivery or suppression audit, keyed by PR and gated head; retained across restarts and retried on repository sweeps for up to 24 hours after the PR merged. |
| `conflict-esc/<pr>.json` | Per PR: the heads whose conflict ESC a lane accepted (event id, lane, rule, time) and the heads still pending (attempts, next attempt, the lane pinned before it was last offered the ESC, last error). Never pruned; deleting it can send a head's ESC again (panewire drops it as a duplicate on the same lane). |
| `pr-review-wait.json` | When this host first saw each `(PR, head)` waiting on its review — in the merge gate on `review_missing`, or on the review path while the review is held, quota-stopped, pending or its poll was skipped; the clock of the 60-minute review-stalled alert, kept a week. |
| `pr-<PR>-stall-state.json` | Per open PR: the idle and per-check red clocks behind the `STALLED` notices — last activity, head and sent flag per cause. A new head or the cause clearing re-arms its notice. |
| `pr-ready-renotify.json` | Per READY key: the worker lane it would have gone to, when it was recorded; kept a week. `merge_overdue` counts `ready_too_long` from it. |
| `findings-deliveries.json` | Findings deliveries of the last 30 days: event id, PR, head, thread ids, lane, owner, sent/re-sent/escalated/acked times. Deleting it re-delivers every open unanswered finding once. |
| `discord-sent` | Most recent 2048 Discord feed keys posted or dropped for this repo. Deleting it can post recent events again. |
| `discord-baseline` | When the feed first ran for this repo. Older PRs do not post. |
| `../discord/outbox/` | `<state_dir>/discord/outbox/`, host-wide: messages waiting to post, one JSON file each. They hold no webhook URL. |

For a failed independent review, inspect
`mergewake/reviews/<PR>/<full-head-SHA>` (plus `-p2` for a second pass) and
GitHub reviews first. Only delete a claim if no reviewer remains active and no
complete review was posted; then a rerun can attempt that head. The
`mergewake/reviews/<PR>/<full-head-SHA>-done` marker is created only when every required pass is
posted; removing it makes the daemon re-check the head, while retained pass
claims still prevent duplicate model invocation. Completed claims can remain as
evidence.

For CI recovery, inspect the revert PR, the fixed `mergewake-recovery-latch` tag,
and any `mergewake/recovery` branch. The tag survives automatic PR branch
deletion and stays while recovery is in flight. Watch clears the latch itself,
and audits `recovery-latch-cleared` once, when the revert PR it opened is
merged, the newest main head contains that merge commit, and the newest main is
green on the watched checks; it then deletes the tag and any remaining branch
and sends one info notice. A green main also closes a revert PR the latch belongs
to that never merged, with a `main is green again at <sha>; revert not needed`
comment, and clears the latch (`recovery-fix-won`). The latch stays for a person
when main is red or still pending after the merge, and when no revert PR that
watch opened can be tied to the latch (a tag a person created, or a
blocking-path culprit that was only reported). A person deletes the tag and any
remaining branch in those cases. After an API timeout or a reporting failure,
inspect GitHub/task records and local audit rather than blindly rerunning a
mutation. The initial release has no automatic report outbox/reconciliation.

## Daemon

```yaml
daemon:
  act: false
  pr: {enabled: true, interval: 60s, notify: {enabled: true}, findings: {enabled: true, ack_after: 15m}}
  watch: {enabled: true, interval: 60s}
```

The `pr` role's closed-row outcome reconcile runs in a loop of its own beside
it, not in its sweep: every `daemon.pr.interval`, while `pr` is enabled, with no
config key of its own, and one repository per goroutine. It reads the open PRs
from the `pr` engine's own copy of its queue's open set, so it makes no GitHub request or ledger read for them and waits (retrying
every five seconds) for each repository's first sweep. Each due closed row is
read at most hourly. A run is bounded to ten minutes and logs one
`outcome reconcile run` line per repository. The loop stops with the daemon.

The hk unreconciled-row sync also runs in a loop of its own, with the
`handoffkeep` sink and a change-state writer, every `sink.reconcile_every`
(default `1h`, `off` disables it). It runs one bounded sync per interval and
logs one `hk change sync run` line; see
[unreconciled-row sync](#unreconciled-row-sync). It stops with the daemon.

All roles default disabled; omitted intervals inherit poll_interval. `daemon`
runs only configured roles, one goroutine each. Shadow is the default for all
roles; acting needs both `daemon.act: true` and `mergewake daemon --apply`.
Land/watch additionally require enabled repository policy. Standalone review needs `--apply` to mutate. `--shadow` forces the daemon
back to observation, and `--dry-run` suppresses pipeline state too. The daemon
still maintains its supervision locks and status in shadow/dry-run modes.

### `daemon.webhook`

```yaml
daemon:
  webhook:
    enabled: true
    listen: 127.0.0.1:9477
    path: /github
    secret_env: MERGEWAKE_WEBHOOK_SECRET
```

The optional in-process receiver accepts GitHub webhook deliveries and wakes
the matching roles early. Check and review evidence expires after ten minutes
and refreshes on the next read. Relevant signed deliveries invalidate cached
evidence for the repository immediately. A landing decision reads the checks
and review threads for its pinned head from GitHub. Without a delivery, other
observations can use evidence up to ten minutes old.

Each daemon pass logs `github requests` with its total, its `not_modified` count
(304 responses, which a conditional request gets for an unchanged resource and
which GitHub does not charge to the rate limit) and its busiest routes. The
role's status file also records counts by method, route and response status.
An HTTP status of `0` counts a request that received no response.

#### Wake signals

Each event also puts the PR it names on the [PR event queue](pr-queue.md), which
evaluates that PR at once; the pass the signals below start is the sweep that
queues every open PR. A `pull_request*` delivery wakes the `pr` and `watch`
roles. A `check_run`, `check_suite` or `status` delivery never wakes the `pr`
sweep: it queues the PRs it names (a check only once it completed), and it wakes
`watch` at once only for the head of the branch that repository's `watch` role
reads (`repos[].branch`, else the repository's default branch), so the reaction
to main CI stays event-driven. The `pr` role starts a pass for one of three
reasons. The pass log line (`loop completed`) names it in its `wake` field:
`webhook`, `reviewer` or `interval` (the first pass after a start counts as
`interval`).

| Signal | What it is | Earliest start after the last pass ended |
| --- | --- | --- |
| `webhook` | A signed GitHub delivery that names the role (see `min_gap`). | `min_gap` |
| `reviewer` | A reviewer job of a recorded review finished. | 30 s (a constant) |
| `interval` | The role's `interval`; it reconciles whatever the other two missed. | its schedule |

The `reviewer` signal is a watch that runs while the `pr` role runs, whether or
not the webhook receiver is enabled. Every 30 s it reads the hk job record of
each live reviewer job recorded under the state directory's `reviewer-jobs/`
(at most 50 per tick, each read bounded at 5 s). A job whose record does not
exist yet is read again only every 5 minutes. It makes no GitHub request.
When a job is `completed`, `escalated` or `lost`, or its result is available, it
marks that PR as changed (so the skip rule below does not leave it alone) and
wakes the role, once per job; each such job logs one `reviewer watch: job
finished` line with the repo, PR, label and state. A wake that arrives while a
pass runs becomes one pass right after it. A reviewer wake never waits for
`min_gap`, and a pending webhook wake that is still inside `min_gap` is pulled
forward by it. If hk cannot be read, the watch logs one line per outage, wakes
nothing and leaves the interval and webhook behaviour as it was.

GitHub request volume is held down by skipping and caching, not by delaying the
reaction to a finished review.

#### Skipping unchanged PRs

With the receiver enabled, the `pr` role leaves a PR alone when nothing has
changed since it last evaluated the PR in full. The PR then keeps its last
verdict: no gate run, no GraphQL read, no review, notice or merge action. Its
whole cost is the conditional requests (304s) that prove it unchanged. A PR is
unchanged only when **all** of these hold; if any fails, it is evaluated in
full, as without this rule:

- the ETags of the PR, its review list, and its head's check runs and
  commit statuses equal those of the last evaluation (each read answers 304);
- its head commit and the head of the branch it targets are the same;
- no `pull_request*`, `pull_request_review*`, `check_run`, `check_suite` or
  `status` delivery has named it since that evaluation started. Pull-request
  events name a PR by number. Check events name it by the PRs the payload lists
  and by head commit, so a check delivery that lists no PR (a fork, a re-run)
  still marks the PR whose head it ran on. A delivery marks only the PRs it
  names, never the rest of the repository;
- that evaluation is under 60 minutes old.

The PR's `updated_at` does not decide this: it does not move when a check run
completes or a review thread is resolved. A resolved thread is not visible in
REST at all, so its delivery (`pull_request_review_thread`) is the signal, and
the 60-minute bound is the safety net for a delivery that never arrives. The
bound caps skipping; it is not a cache lifetime. It also bounds how long a
time-based hold can lag: a cancelled check's settling wait, a stall notice and a
review retry backoff are re-read at most 60 minutes late for a skipped PR.

Only a settled PR is remembered: a draft, or a PR whose head has its review done
marker, evaluated without an error. A PR still being reviewed, one whose review poll the GitHub client skipped, one whose merge is
held (its content landed while GitHub left it open), or one whose last evaluation failed, is
evaluated every pass. Memory is in the daemon process: a restart
evaluates every PR once. Without `daemon.webhook`, nothing is skipped.

Each repository's pass logs `pr pass` with `prs_skipped`, `prs_evaluated`,
`evaluated_reasons` (a count per reason) and `evaluated` (`<pr>:<reasons>`).
Reasons: `first` (no clean evaluation remembered), `webhook`, `head`, `checks`,
`reviews`, `pr`, `base`, `age`, `unvalidated` (a read failed or came without an
ETag).

The HTTP cache behind the 304s keeps up to 8,192 responses (128 MiB; no body
over 4 MiB) and evicts the least recently used one, so a pass that reads
thousands of keys still finds the validators of the last. A delivery
invalidates evidence but never drops a validator: revalidating is free and
always correct.

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Serve the receiver next to the daemon loops; it shuts down with the daemon. |
| `listen` | Required when enabled | Loopback `host:port` only: `localhost`, any `127.0.0.0/8` address or `::1`. Other hostnames and wildcard binds are refused, and `localhost` is rewritten to a `127.0.0.1` literal so resolution can never steer the bind off loopback. Remote delivery needs an operator-run tunnel (for example a reverse SSH forward or a cloudflared/smee-style relay) terminating on loopback. |
| `path` | `/github` | Exact request path; other paths return 404. |
| `secret_env` | Required when enabled | Name of the environment variable holding the GitHub webhook shared secret. The daemon refuses to start while it is unset or empty (other commands load the config without it); the value never appears in config or logs. Deliveries may use either content type (`application/json` or the form `payload` field); `application/json` is recommended. |
| `min_gap` | `60s` | Shortest time from the end of a role's pass to a pass a delivery wakes. Every pass reads all repos and a CI run sends dozens of check deliveries, so without a gap the roles run back to back and spend the GitHub rate limit. A delivery also never cuts short a rate-limit wait. |

Requests must `POST` the exact path with a body of at most 5 MiB and a valid
`X-Hub-Signature-256` (HMAC-SHA256 over the raw body). Bad or missing
signatures get 401 and trigger nothing; verified deliveries get 202 whether
or not anything was triggered. One log line per delivery records the
`X-GitHub-Event`, the `X-GitHub-Delivery` id, `repository.full_name` and the
roles woken — never the body, the signature or other headers. A repeated
delivery id is logged once more as a repeat and does nothing; `hook_id` tells
two hooks apart when they send the same delivery id.

| `X-GitHub-Event` | Roles woken |
| --- | --- |
| `pull_request`, `pull_request_review`, `pull_request_review_thread`, `pull_request_review_comment`, `check_suite`, `check_run`, `status` | `pr`, `watch` |
| `ping`, any other event | none |

Only enabled roles wake, and only for repositories in `repos`.

`daemon.pr.notify` defaults to enabled and accepts `enabled`,
`conflict_owner_after` (`30m` by default), `idle_after` (`1h`) and
`check_red_after` (`30m`). `renotify_after` and `renotify_max`
were removed with the READY reminder; a config that sets either fails at load.
`daemon.pr.author_fix_wait` defaults to `30m`; set it to `0` to disable the
bounded merge wait. When the PR's author job is live and a current-head review
finding has no reply from the PR author, the gate reports `author_fixing` until
the author replies, the head changes, the author job ends, or the newest such
finding reaches this age. After the bound, findings keep their existing
severity behavior.
While the `pr` role runs, each poll runs the merge gate for every open,
non-draft PR at its current head and sends one lane event for a BLOCKED verdict
and records a READY one — the notify path observes rather than merges, so it
also runs in shadow mode. `READY`
requires every required check green on the head, zero unresolved review
threads, the `mergewake/reviews/<pr>/<head>-done` marker and the passes from
`review.login` it attests (on the head, or on a head with the same patch id), and a mergeable PR; a base that
moved on never blocks and is recorded as `base_behind: true`; `BLOCKED` names the hold from a
closed vocabulary: `check_failed:<name>` per failing required check,
`check_absent:<name>` per required check with no run or status on the head at all (see below),
`threads_open:<n>` (or, under a graded policy, `blocking_findings:<n>` and
`triage_unanswered:<n>`), `finding_outside_diff:<n>` (critical or high findings a pass listed in its review body because their path or line is not in the diff; each is tracked across heads by the `od-…` ref the BLOCKED notice names. A comment answers it when it names the ref (including ref-first `od-… Dismiss: <reason>`), its `path:line`, or its title and uses `Fixed in <sha>` naming a commit in the PR's history, `Dismiss: <reason>`, or `Disposition: defer <task>` (the task as `4253`, `hk 4253`, `hk:4253` or `#4253`); an answer line naming findings answers only those findings. The answer may come from the `review.login`: the trust check is content-based. A non-bot comment counts unless its body contains a `<!-- mergewake` marker; mergewake's own review bodies and notices therefore never answer findings. Decision 372 approved this trust bar. A later report of the same defect (same path, within three lines, and matching finding text) joins the existing ref, even if its wording changes. An answer or re-review stays closed on later heads. A later completed pass that does not report an open finding re-reviews it away only if its prompt listed it, which the pass receipt records. Answered findings are shown to reviewers with their answer; they must not repeat one unless they give a new, concrete reason the answer is wrong and quote the answer they dispute. A distinct new reason near an answered finding creates one new finding and routes its hold to the design lane. Reviews posted before the body listed findings keep the old rule: the opaque count holds only the current head's patch-matching passes and a person's `Dismiss:` comment naming the head's SHA prefix clears it), `review_missing`, `conflict`. Queued or running
check evidence emits nothing, and neither does a required check that has not reported yet
inside the grace described under `check_absent` below. Under acting mode,
`enabled` repository policy merges the gated head. MERGED is sent once per
(PR, head) to the PR's design lane; failed sends and audit rows remain pending
and retry on every repository sweep. Each `(PR, head,
verdict)` is handled once per the `pr-notify-sent` state file; a changed
BLOCKED reason set is a new verdict that emits again for the same head, a new
head re-arms notification, and a restart does not resend. BLOCKED is
declared with `panewire emit --type blocked`, so it stays unread on the
receiving lane until it runs `panewire inbox ack` (panewire
`docs/quiet-delivery.md` §Must-ack types; a panewire too old for a type gets the
event untyped). READY is recorded, never sent, because mergewake merges on its own
gate. MERGED writes one `pr-notify` row and its sent key after the row is durable;
no design lane is recorded as `"no design lane"`. Recent PRs merged outside
mergewake also receive one notice. Pending notices more than 24 hours after the PR's merge (else
after mergewake first saw it) are recorded as gave up, unless already delivered: a delivered notice
is recorded as sent. The head's
summary, and the author worker lane (job bound to the task, else the author
pane) it would have gone to in `lane` and `via`, if any. A READY also starts its
clock in `<state_dir>/<repo>/pr-ready-renotify.json`, which `merge_overdue`
counts `ready_too_long` from. No READY reminder is sent. The BLOCKED event
lane follows the PR author route used for review findings: the bound worker
job, a live takeover pane, an explicit author lane, or the desk when the author
is unknown. The owner lane gets one follow-up per PR only when the author route
is gone or the same conflicting head remains for `conflict_owner_after`; a
changed head restarts that timer. Each head's author notice and each suppressed
owner follow-up have audit records. The conflict notice uses this same route.
Route lookup errors are retried; they do not count as an absent author. The
owner timer still runs, so an owner notice is sent if the same conflict remains
unreadable through `conflict_owner_after`.
Set `notify: {enabled: false}` to silence the events; the key is rejected on
other roles.

##### Who hears each BLOCKED reason

A BLOCKED verdict goes only to the side that can act on it. Every reason the
gate can emit falls in one of three classes:

| Reason | Class | What the notifier does |
|---|---|---|
| `check_failed:<name>`, `check_cancelled:<name>`, `check_absent:<name>` | author | Live job lane first (the findings route); with no live job, the `Mergewake-Notify-Lane` trailer, then the task lane unless it is the desk, then the desk as a `decision` (unknown author). |
| `blocking_findings:<n>`, `triage_unanswered:<n>` | author | Same route. |
| `conflict` | author | Not sent as BLOCKED: the conflict ESC below tells it, once per head. |
| `review_missing (<block detail>)`, for example `missing-task-trailer` | author | Same route: the review waits on something only the author can add. |
| `review_missing` with no block detail | mergewake-internal wait | Nothing is sent. Only the stall alert below. |
| `threads_open:<n>` | needs a human decision | Unchanged: the task lane, trailer or notifier lane, in the `notifyLane` order, type `blocked`. |

`check_absent:<name>` means the required check has no check run and no commit
status on the head, which is different from a queued or running check. It
usually means the PR branch's workflow predates the job, and updating the branch
fixes it. The gate waits quietly (pending) until the head commit's committer date
is 20 minutes old, or until every other required check has completed (green, red
or skipped) with at least one of them present, when it is the only required check
absent; then it blocks with the reason. With two or more absent required checks,
none is named before the 20 minutes. A
PR with a `conflict` never reports its checks absent, because GitHub runs no
workflow for it. The committer date is the clock because it needs no stored
state, so a commit made long before it was pushed reaches the grace sooner than
the push time alone would.

A mixed reason set is sent when any reason is author or human, and its text
lists only those reasons. A set made only of internal waits sends nothing. A
`conflict` is left out of the BLOCKED text but not out of the verdict: the sent
key and event id still name it, so a head that starts or stops conflicting is a
new verdict and its other reasons are told again, once. A verdict whose only
reason is the conflict (with or without an internal wait) sends no BLOCKED and
logs its `pr-notify` row with `"suppressed":"conflict ESC"`, and a mixed verdict
keeps the author route the conflict gave it.

When `blocking_findings` is a reason, the BLOCKED text also lists the threads
holding the head: up to ten, then `and N more`, within a byte budget that keeps
the event under panewire's 2048-byte limit. Each entry names the thread (its URL,
else its id), the finding's author, its severity (`unrated` when none is
readable), and `path:line`. A thread with a reply names the latest one and says
`reply not accepted`, since a block-class thread clears when it is resolved,
or with a reply of `Fixed in <sha>` or `Dismiss: <evidence>`. When the head's
review pass is complete and no holding finding was posted on the head, the
text says the latest review pass on the head posted no holding finding and
that the threads above hold from earlier passes. The list can be shorter than the `blocking_findings:<n>`
count, because a reviewer still requesting changes counts toward the count but
is not a thread.

`READY` is recorded, not sent. `MERGED` is sent once per (PR, head) to the PR's
design lane and retried until sent, so the design session hears merge notices
and the BLOCKED reasons that need its decision.

A head that keeps waiting for its review for more than 60 minutes gets one
`review stalled` alert (type `blocked`) on the `notifyLane` lane. It names the PR,
head and the wait so far, then how far the head's review got. On the gate path
(the `review_missing` wait) that is the passes posted against those needed and
the reviewer job recorded for the head, if any; on the review path, where the
review itself did not run, only the reviewer job recorded for the head is
known, so that path names the reviewer job and not a pass count. When the
review itself did not run, the alert also names that wait, with the reason its
audit row recorded where the pass knew one: `held (round-hold)`,
`quota-stopped (pool <pool>)`, `pending`, or `skipped` for a poll
the GitHub client dropped. The wait starts when this host first sees
the head waiting for its review — in the merge gate on `review_missing`, or on
the review path when the review is held, quota-stopped, pending or skipped — and
it is kept in `pr-review-wait.json`. The alert is sent once per `(PR, head)`,
deduped across restarts by a `stalled <PR> <head>` key in `pr-notify-sent`, by
whichever path first sees the wait past the threshold, and a delivery the
notifier refuses is retried on the next pass. The review path sends the alert
and nothing else: findings delivery and Land wait for a review that posted
something. On the gate path it is sent only while `review_missing` is the whole
reason set. A head that is later reviewed and blocks
again follows the rules above. This is the only review stall alert: the older
elapsed-time `STALLED review` alarm, which the daemon stopped calling when
reviews moved to retry accounting, is removed with its `review-wait.json` and
`review-stall-sent` state. The `daemon.pr.stall_after` key was removed; a config that sets it fails at load.

Two stall notices cover waits no verdict names, each sent once per
`(PR, head, cause)` as a `STALLED` event (type `blocked`) on the
`notifyLane` lane. An open, non-draft PR with no new commit and no review
activity for `idle_after` gets `cause=idle`; a required check red on the head
for `check_red_after` gets `cause=check_red:<name>` per check. Activity is the
later of the PR's creation and last update, its review submissions and its
review-thread comments; a new head or the cause clearing re-arms the notice.
The clocks live in `pr-<PR>-stall-state.json`.


##### Conflict ESC

A PR that conflicts with its base (GitHub reports `mergeable: false` or
`mergeable_state: dirty`) gets one ESC per head, before and independent of its
review. The `pr` role checks every evaluated open PR on the configured base,
drafts included, before the unchanged-PR skip and before review, so a head whose
review is held, quota-stopped, still running or not started is told too;
`NotifyPR` checks the same way before its draft and gate returns. A listed
dependency bot's PR is checked too; it still gets no reviewer and lands as
before. Mergeability GitHub has not computed yet (`null` or `unknown`) neither sends nor clears anything. The
ESC never changes review placement, the gate or the merge: the gate and
`mergewake status` still say `blocked`/`conflict`, and a new head or a resolved
conflict gets its missing review passes through the normal queue whether or not
the ESC was delivered.

Once per head: the identity is repository, PR, `conflict` and the full head
SHA, and the event id is derived from that alone (no summary, lane or time). A
head whose ESC a lane accepted is never sent again — not after a restart, a
force-push from A to B and back to A, a conflict that flaps through unknown or
clean, or a change to the routing trailers. A new head that conflicts gets its
own ESC. The receipts live in `<state_dir>/<repo>/conflict-esc/<pr>.json`,
read, decided, sent and written under a file lock, separate from the bounded
`pr-notify-sent` ledger.

Routing follows the same author route as review findings: a task-bound worker
job, a live takeover pane, an explicit author lane, or the desk when the author
is unknown. Each head's author event is recorded in the per-PR conflict state
under a file lock and has a matching `conflict-esc` audit row. A repeat head is
audit-only. If the author route is gone, the owner lane receives the one
per-PR owner event immediately. Otherwise `conflict_owner_after` (default
30m) starts when a conflicting head is first observed; a new head restarts the
clock, and the owner hears once only if that same head still conflicts at the
deadline. Later owner notices are audit-only. The event is a `decision` whose
text names the PR, short head, conflict and rebase action. The event id stays
stable across retries so an accepted delivery can be replayed without losing
its destination. Delivery failures leave pending state and a
`conflict-esc-failed` audit row. An unreadable author route is retried, not
treated as absent; the owner still hears once if that head remains conflicted
through `conflict_owner_after`.

It honours `notify: {enabled: false}`; a dry run sends and records nothing;
shadow mode sends.

#### Overdue notice (`merge_overdue`)

The `pr` role measures how long each head waits and sends one `merge_overdue`
notice per overdue condition. It is detection and notice only: nothing merges
because of it. The thresholds are constants in `internal/pipeline/overdue.go`
(they are not config keys), taken from the merge-sla-lead-escalation policy.

Four clocks run per `(PR, head)`, kept in `pr-overdue-clocks.json` so a restart
does not reset them: `head_seen` (the first pass that saw the head, after the PR
left draft), `checks_done` (every required check completed: green, red or
skipped), `review_done` (the head's done marker and all needed passes, or an
earlier head's review of the same patch) and `ready` (the READY verdict
mergewake recorded for the head). Each is the raw time its event was recorded,
and no pass moves it. Draft time does not count: while the PR is a draft no
condition fires, and every draft the head went through is kept as a hold span
(`from`, `to`), the hold in progress included. A hold that ends records the
pass that saw the PR open, so the next hold starts where it ended. An age is
computed when it is read: the time since the clock's time, less the union of the
hold spans it overlaps, so a clock that starts inside a hold counts from the
hold's end and spans that overlap or touch are taken off once. Nothing depends on
the order the clocks were filled in or on which pass could reach a read: a hold
that closed before a clock was filled still comes off it. A file an earlier
release wrote has no spans, and its clocks already carry the holds they went
through, so it keeps its ages. A new head starts its own clocks. mergewake does
not update PR branches (see `require_up_to_date` above), so no head inherits
clocks from an earlier head.

| Condition | Overdue when |
| --- | --- |
| `review_not_started` | 20 minutes after `checks_done`, no reviewer job is recorded for the head, no pass is posted and the review is not done. CI still running owes no review. |
| `not_landed` | 10 minutes after `review_done`, the PR is not merged. |
| `ready_too_long` | 45 minutes after `ready`, the PR is not merged. No READY was sent for the head, so a head that never became READY owes nothing, however long it waits. |

A head is not overdue, and is looked at again on every pass, while the gate holds
it for any reason but mergewake's own `review_missing` queue state: an open
critical/high finding (`blocking_findings`, `finding_outside_diff`), a failed or
expired-cancelled check, `check_absent`, `conflict`, and the other
author or settling waits. A pending verdict that waits only on GitHub's
mergeability computation is held for that evaluation; one that waits on CI is
not, so a head READY 45 minutes ago whose checks are rerunning still owes
`ready_too_long`. A T3 change (the `Mergewake-Tier` trailer, else the
hk task title's marker) and a PR that touches a `deploy_approval` path of the repo
policy are never announced. The hk hold exclusion is skipped: no hk read exposes
a hold to mergewake.

The notice is a `decision`: `[mergewake] MERGE_OVERDUE <repo>#<n> head=<sha7> |
<condition> after <age> | gate: <state> <reasons> | checks: <name>=<state>, ... |
review: passes x/y, reviewer <label or none>`. The notice goes to the lane `lead` first.
When the notifier refuses that lane (it is not registered on the host), the notice
goes to the PR's `notifyLane` lane (task lane, `Mergewake-Notify-Lane` trailer,
repo lane, then the configured fallback), and the desk gets one short `info` FYI
line unless that lane is the desk. A notice no lane takes is parked under a
`decided` key and not retried. Each delivered notice is deduped across restarts by
a `merge_overdue <PR> <head> <condition>` key in `pr-notify-sent`.

To read a day of counts: every delivered notice writes a `merge_overdue` row to
`decisions.jsonl` (`repo`, `pr`, `head`, `condition`, `age`, `route`, `lane`), and
the `pr pass` log line carries `merge_overdue`, the notices sent since the last
line.


#### Findings delivery

`daemon.pr.findings` (default enabled; `{enabled: false}` turns it off) sends
review findings straight to the PR's author instead of through the desk. Each
`pr` poll collects the open PR's unresolved, unanswered findings that no
delivery named yet: mergewake's own (once the head's `-done` marker exists, so
a review round goes out whole) and CodeRabbit `critical`/`major` threads.
CodeRabbit `minor`/`nitpick` threads are not delivered. They go out as **one
lane event per delivery**, declared `--type findings`, naming each finding's
severity, `path:line` and comment link, what is required (fix every
critical/high/Major with `Fixed in <sha>` or `Dismiss: <evidence>`; a
disposition for every other) and how to acknowledge.

The author lane uses the same binding as reap: `Mergewake-Task`, the task's
`refs.job_id`, then the job's `hub_lane`, `lane`, or `label`. Worker and builder
jobs keep their recorded author lane after completion, but only while the job
is live. A terminal job whose pane is gone is a finished author: once every
configured host has been read and none shows a pane for the job, its label, or
its recorded pane id, the event does not go to that lane. It goes to the
`Mergewake-Notify-Lane` trailer's lane, else the task's lane (not the desk) as a
`findings` event, else the desk as a `decision`. An unacknowledged delivery
already sent to the finished lane is resent once to the new lane under the event
id `<id>-reroute`, audited as `findings-route-changed`; an acknowledged one is
never rerouted. A terminal job whose hosts could not all be read keeps its
lane and is audited as `findings-route-partial` with `"liveness":"unknown"`.
A takeover is not taken while any configured host cannot be read: the bound
author lane stays selected, and `findings-route-partial` records the candidate
panes observed on the hosts that were read.
A task or job read error retries without rerouting.

`BLOCKED` notices for `blocking_findings`, `triage_unanswered`, or `check_failed`
use this author route too. They do not use the task's operator lane. An author
delivery failure does not create an immediate desk copy.

`ack_after` defaults to `15m`. An unacknowledged delivery is resent once after
that interval. An inbox acknowledgment stops reminders. A verified disposition
stops both reminders and aging. `review.fleet_authors` lists additional GitHub
logins whose replies count, including bot logins. Login matching is case
insensitive and ignores the `[bot]` suffix. Fix replies still require a commit
in the PR after the finding. Allowlisting an account does not let it dispose
its own findings. The existing same-login exception for mergewake's own review
identity remains.

The desk gets one `decision` copy when an unresolved finding reaches
`daemon.pr.findings.escalate_after` (default `2h`) or `escalate_rounds` (default
`3`). A round is a distinct observed PR head, not a poll. Zero rounds disables
the round threshold. Acknowledged but unresolved findings keep aging. Named
failed checks use the same thresholds across heads and reset when that check
stops failing. Advisory findings go to the author once and do not escalate.

```yaml
review:
  fleet_authors: [example-fleet-author]
daemon:
  pr:
    findings:
      ack_after: 15m
      escalate_after: 2h
      escalate_rounds: 3
    review_retry:
      initial: 1m
      maximum: 30m
      failures: 3
```

Deliveries persist in `<state_dir>/<repo>/findings-deliveries.json`. BLOCKED
declares `blocked`; READY is recorded, not sent, while MERGED is sent once per
(PR, head) to the design lane and retried until sent. Watch holds declare `watch-hold` once per red main episode and hold
reason; culprit recovery declares `culprit-hold`. Deployment decisions
continue through the handoffkeep approval adapter. Panewire's reviewed desk
policy keeps routine events in its inbox and decision events as interrupts.

#### Review recovery

Every acting PR poll checks review receipts, including heads with a `-done`
marker. A marker without trusted pass receipts cannot skip the review. Expired
leases are reclaimed. A live review lease waits for completion or expiry.
The existing HTTP 406 fallback reads the local Git diff when GitHub cannot
supply the diff.

`daemon.pr.review_retry` sets the first delay, maximum delay, and failure
threshold. Failed attempts back off exponentially and persist per PR head in
`<state_dir>/<repo>/review-retry-<pr>.json`. The defaults are `1m`, `30m`, and
`3` failures. A new head resets the counter, and so does a reviewer that really
lands: a synchronous spawn whose wait returned or timed out on a placed pane, a
spawn receipt naming a pane and its host, or an hk job record for its label.
An asynchronous spawn's `ACCEPTED` receipt alone is no landing, so a request the
hub accepts and then refuses keeps counting toward the threshold. At the
threshold, one `automation-failed` event goes to the PR's design lane and one
(same text, `…-tool` event id) to the tool lane; later
attempts keep the bounded backoff. While a failed head conflicts or its
required checks are red, retries and the failure escalation wait: the first
wait per head and reason writes a `review-retry-waiting` row, and the count,
backoff and last error stay as they were until the head can merge cleanly
again. These counters are per host. GitHub review leases still
prevent simultaneous claims of a pass across hosts.

A review poll the GitHub client skips inside its poll interval is a wait, not a failure: it counts as no review, does not back off or count toward escalation, and the head is not settled.

The daemon uses retry failures instead of the elapsed-time-only STALLED alarm.
`stall_after` was removed; a config that sets it fails at load. Shadow and
dry-run modes do not retry reviews or write retry state.
`daemon.pr.skip_bots: true` excludes bot-authored PRs from review, notification,
and landing.
They still count as open for outcome reconciliation.

`daemon.pr.review_parallel` caps how many PRs of one repository the `pr` role
evaluates (reviews) at once; see the [PR event queue](pr-queue.md). Unset or `0`
means no cap: every PR is evaluated as soon as an event queues it. A positive value is an explicit cap
(`1` reviews one PR at a time) with no maximum; a negative value is a config
error. The operator decided that reviewers are not capped by default, and that
a limit is set only when watching the pass log shows one is needed. The limits
that remain are panewire's spawn quota and the host's capacity. The pr pass log
line of each repository carries `reviews_concurrent_max`, the most reviews that
ran at once since the previous line, for Grafana and Sentry; the line is
written when the webhook receiver is enabled. Each PR's review is followed by its findings
delivery, notification and land; those settle steps run one PR at a time within
a repository. The PR role runs all configured repositories at once, so the cap
applies to each repository, not to the daemon; no global reviewer cap exists.
The other roles visit repositories one after another.

With the `handoffkeep` sink, when more PRs wait than the cap lets run they start
in hk priority order (1 is highest; no priority sorts last; ties keep arrival
order). A PR's
priority is hk's change priority for its PR URL, else the priority of its
`Mergewake-Task` task, else none. A priority-1 review does not wait for a free
`review_parallel` slot: under an explicit cap it starts at once, with at most
two such reviews past the cap per repository, and takes the next free slot
beyond that. With no cap this changes nothing, since no PR waits. A failed or slow hk read gives that PR no priority and never fails the pass. Only the
review start order changes; settling, land, the merge gate, retries and
notifications are unchanged. Each review start writes a `review-start` row to
`decisions.jsonl` with the priority and where it came from.

Use `mergewake status --daemon --config /path/to/config.yaml` for JSON status.
Each role writes `<state_dir>/daemon/<role>.json`: last_run, last_finish,
last_error, next_run, running and failures. Values are the last saved observation;
a hard crash can leave a stale running/next_run value. Role locks use the OS user
cache under `mergewake/daemon-locks/`, independent of state_dir. Use one service
account per host. Locks release automatically at exit.

Delays have positive jitter up to 20%, exponential failure backoff up to 30m (or
the base interval if longer), and rate-limit pacing. Conditional GET caches are
bounded to 32 MiB and revalidated on every request; mutations invalidate them.
SIGINT/SIGTERM cancel work; command process groups stop together and any rollback
finishes with an independent timeout. Configure the supervisor's stop timeout
above the total possible rollback duration for your inventory.

## CI watch and recovery

`watch` diagnoses red main from the merge-history graph, not timestamps. It
walks the first-parent ancestry of the configured branch: a merge commit's
second parent is the merged branch, and the merge commit itself is the
integration unit. A candidate is a merged PR whose recorded landing SHA
(`merge_commit_sha`) sits inside the red bracket, covering squash merges, merge
commits and rebased chains alike.

Only a successful check run is green evidence. Missing, pending, skipped or
cancelled runs are unknown, never green and never a red boundary. The search
brackets the red head with the nearest confirmed-green ancestor for the same
checks; if exactly one candidate PR remains it is the culprit, otherwise the
watch holds and reports (`action: hold`) without reverting, listing candidate
PRs in the inspected bracket. Merge-commit culprits are held and reported because
the current revert adapter supports only single-parent commits. A culprit that
touched a blocking path is reported (`action: report-culprit`), never
auto-reverted. Holds and reports notify once per head via the `watch-reported`
state file, which retains the most recent 128 report keys.

A red window alone never reverts. Before watch opens a revert PR it confirms the
failure through the repo's any-ref CI: the green baseline (the bracket's
first-parent green) must pass 2/2 and the culprit must fail 2/2. The recorded
result counts where one exists; the rest are re-runs dispatched at the same sha
and read as commit statuses newer than the dispatch. Mixed results are flaky:
watch holds, blames nobody and tells the repo's design-session lane once. So do
a timed-out re-run, a reached dispatch bound, and a repo with no
`any_ref_workflow`; in those cases a person bisects.

The 30-minute fix window counts from the moment watch records the incident
(`red_since` in `recovery.json`), not from confirmation, so the time spent
confirming is part of it. Once confirmed, the opened revert PR waits out what is
left of the window. If main turns green inside it, watch closes the revert PR and clears the latch. An open
PR whose body or head commit message carries `Fixes-Red-Main: <culprit-or-head-sha>`
is the fix for this red: the revert waits for it up to twice the window and the
PR is reviewed with priority 1. After the window, watch merges the revert with
the same squash merge it uses for normal PRs only when main is still red, the
head descends from the culprit, the revert PR has no conflicts and green CI on
the repo's watch checks, and the revert's diff is the exact reverse of the
culprit's. That last check compares the touched paths' pre- and post-images in
the culprit parent, the culprit, the red head and the revert head: a later edit
or a different tree refuses the merge and holds. One design-lane line names the
red since, the culprit PR, the revert PR, the window and the reopened task.

Immediately before opening a revert PR the watch rechecks the current main head
and its CI, and the recovery latch tag still serializes recovery attempts.
The latch clears itself after the revert PR merges and main is green, or when a
fix wins (see the recovery notes above).

### Main CI and cancel-in-progress

Fleet rule (hk `policy/fleet-ci-main-cancel`): push-to-main CI runs with
`cancel-in-progress: true` per ref, so a new push cancels the run on the older
head. Watch reads that as follows:

- The newest `main` head green is success (`action: healthy`, checkpoint at that
  head), even when older heads were cancelled and never ran to the end.
- The newest head cancelled or pending is `waiting-for-ci`: a newer push will
  run, and nobody is blamed.
- A cancelled head is unknown evidence, never red. When the newest head is red,
  watch brackets it with the nearest green ancestor and bisects.
- Without `policy.any_ref_workflow`, a cancelled head in that bracket holds with
  `incomplete CI evidence in recovery interval`, and a person bisects.
- With it, watch dispatches the any-ref CI for exactly the commits bisect needs
  (see `policy.any_ref_workflow` below) and blames the first red.

The workflow named by `any_ref_workflow` is the repository's own file: it needs a
`workflow_dispatch` input `ref` and must post each watched check as a commit
status on that sha. mergewake's own `.mergewake/policy.yaml` sets
`any_ref_workflow: ci-any-ref.yml`.

`policy.watch_checks` names the checks that judge a main-branch commit when their
names differ from the PR merge checks. When it is not set the main set is
`required_checks`. Watch judges the newest head and bisect candidates by this
set, never by deploy checks, so a missing `deploy-ok` is not a red main head. For
example a project with `Compile and RunPureChecks (PR)` can watch
`Compile and RunPureChecks (main)` without weakening the PR gate. First-PR CI
measurement always uses required_checks.

`policy.deploy_checks` names the checks of the deploy stage (for example
`deploy-ok`, posted after the main stage is green on the same commit). When it is
set, watch judges the deploy stream by these checks, so `ci-ok` alone never clears
the deploy stage. An empty or absent `deploy_checks` means the repository has no
deploy stage, and the main checks judge a commit.

### Notify-only checks

`policy.notify_checks` lists main-branch checks that only notify, for a stage
whose failures an operator wants to hear about but that must never trigger
recovery — for example a deploy verification that a repository fixes forward
rather than reverts. The names are exact and unique like `watch_checks`, a name
in the watch set is refused (a check cannot both judge main for recovery and
only notify), and a name outside the list is not read at all.

When a named check is red on the default-branch head, the repo's design lane —
the lane MERGED notices go to — gets one `info` notice per red streak. The
notice names the check and the first red SHA, and lists the merged PRs
(numbers and titles) between the last green SHA and that first red SHA. A
repeated red head in the same streak stays quiet; the streak ends when the
check is green again, and the next red streak sends one new notice. Pending,
cancelled, skipped or absent evidence changes nothing.

A notify-only check never reverts, takes the recovery latch, bisects or
dispatches any-ref CI: it is not part of the watch set, so a red
`notify_checks` entry cannot make a main head red for recovery. State lives in
`notify-checks.json` in the repo state dir, so a restart neither repeats a sent
notice nor forgets an open streak. A shadow pass tracks the streak but sends
nothing; `--dry-run` reads and writes nothing. A repo without the key behaves
exactly as before.

`policy.any_ref_workflow` optionally names a workflow file in `.github/workflows`
that runs the checks bisect judges a commit by for any commit: it has a
`workflow_dispatch` trigger with input `ref` and posts those checks (the watch
checks, or `any_ref_checks` below) as commit statuses on that sha (for example
`any_ref_workflow: ci-any-ref.yml`). When a red main head's bisect needs
a commit that has no green or red result, an acting watch dispatches the workflow
once for that commit on the watched branch, records the dispatch in
`any-ref-dispatch.json` in the repo state dir, and holds with
`waiting for any-ref CI on <sha7>`. A later pass reads the result. The wait never
happens inside a pass, and waiting holds do not notify. A restart never
dispatches the same commit twice for the same red head.

Each red head is one incident; a new red head starts a new one, and a green head
ends it. Per incident, watch dispatches at most `ceil(log2(n)) + 2` times, where
`n` is the number of commits after the green boundary. It waits at most
45 minutes for each commit after its dispatch (there is no policy key for it). Past either bound, or when GitHub refuses the dispatch (HTTP 403, 404
or 422), watch holds with a reason that names the cause and notifies once per
incident. A refusal is not retried until the next incident. Shadow and
`--dry-run` passes dispatch nothing and name the commit they would dispatch in
`any_ref_dispatch`. Without the key, unknown evidence holds as before. The token
needs permission to dispatch workflows.

`policy.any_ref_checks` optionally lists the checks bisect judges a candidate
commit by, for a repo whose any-ref workflow posts fewer checks than the main run
(for example `watch_checks: [ci-ok, image]` with `any_ref_checks: [ci-ok]`, when
the image builds only on a push to main). Unset keeps the watch set, which is
`watch_checks`, else `required_checks`. Each name must be in the watch set and
the list must not be empty or repeat a name; otherwise the policy is invalid and
the error names `any_ref_checks`. The newest main head is still judged by the
whole watch set, so it decides healthy or red as before. `any_ref_checks` applies
to the commits bisect reads, including the green ancestor search and the commits
an any-ref run reports on.

`policy.any_ref_deploy_checks` optionally lists the checks the any-ref workflow
posts when it runs the `deploy` suite, for a repo whose deploy run reports under
names the real deploy stage never posts (for example `deploy_checks: [ci-ok,
deploy-ok]` with `any_ref_deploy_checks: [ci-ok-deploy]`). It applies to the
deploy stream only: a commit bisect needs is judged by its own deploy run
(`deploy_checks`) when that is green or red, and otherwise by these checks, so a
dispatched candidate whose run posted `ci-ok-deploy` is green or red instead of
holding until the 45-minute wait ends. The deploy head is still judged by
`deploy_checks` alone, the head-versus-candidate guard above does not apply to
it, and the confirmation re-runs read these checks. The revert PR, the recovery
latch and `last-deploy-revert` work as before. The key needs
`any_ref_workflow` and a non-empty `deploy_checks`, must not be an empty list or
repeat a name, and the names need not be in `deploy_checks`; otherwise the
policy is invalid and the error names `any_ref_deploy_checks`. Unset, the deploy
stream judges candidates by `deploy_checks` as before. Adding the key while a
deploy incident is recorded holds its confirmation with `the confirmation check
set changed; rediagnose`.

Main watch notifications are deduplicated by red episode, from the first held
red main head until the next green main head. Later held heads remain in the
audit log with no repeated lane event; each distinct held head still writes
its `revert.hold` feed event. A green main clears the episode key.

When the newest head is red only on checks outside `any_ref_checks`, no commit
can be shown green or red on them, so bisect would blame on partial evidence.
Watch holds instead, queries no ancestor and notifies once per head, with the
reason `red only on checks any-ref CI does not report: <names>`. The hold ends
when the head turns green, or when a check inside `any_ref_checks` is red too.

`mergewake ci-policy` lists the checks bisect judges by and warns when the
any-ref workflow obviously cannot post one: no job is named for it, no job name
is an expression, and the file never spells it.

A check run and a commit status with the same name combine as follows: a pending,
cancelled or skipped value yields to a green or red one. Two decisive values
that disagree are an error.

Panewire's node closes an ended job's pane itself and reads its composer with
`pane.read` to do it: only `empty` permits closing. `has_text` maps to
`composer-text`; unknown, missing, changed/gone panes and hub errors keep the
pane. Codex placeholder text remains nonempty unless the node can prove it is
an accepted placeholder. The node-side `pane.close` rechecks the visible
composer before and after its settle interval; text, an unreadable composer or
an unknown composer keeps the pane even if its name, status and cwd did not
change. Mergewake never calls these: it only waits for the pane to be gone
(`pane-in-use`) before cleaning the worktree, and for the gone state to age
past `min_age` before archiving the job dir.


Launcher cutover: set `review.executor: panewire`. Remove the former
`review.runtug.command` (the runtug executable) and `review.runtug.panewire`
(the separate inbox executable) before moving the remaining settings to
`review.panewire`; remove the old `review.runtug` block. Set
`review.panewire.command` to the panewire executable, or omit it for the
`panewire` PATH default. Do not copy the old executable settings: strict decoding
rejects the retired nested `panewire` field, and the old `command` would still
launch runtug. The old executor key is refused.

For example, after retaining the desired workspace/owner/host settings:

```yaml
review:
  executor: panewire
  panewire:
    command: /usr/local/bin/panewire
    workspace: reviews
    owner: review-desk
    host: auto
```

The single binary supplies both spawn and inbox calls. Spawn needs the fleet
hosts config (`FLEET_HOSTS_CONFIG`) with its hub operator credential. The default
CLI returns ACCEPTED after a durable reservation; mergewake keeps `--wait`,
which observes spawn completion and then the existing job completion contract.
A reconciling attempt is never retried on another host.

In a PR’s `Operator decisions` section, cite `hk:doc decision/...` on each decision line to have it verified against hk and treated as settled product scope. A trailing `.` or `/` after the cited key is not part of the key: it is sentence punctuation, so a citation at the end of a sentence still verifies, while an inner `.` stays in the key. Unverified claims do not suppress findings. A verified decision also shows the start of its hk document, up to 600 bytes, so the reviewer can compare it with the PR’s lines. When the context budget is full, the excerpt is shortened or omitted, and the review says so.

## Server store connection

The server store reads its MySQL DSN from exactly one of
`MERGEWAKE_MYSQL_DSN` and `MERGEWAKE_MYSQL_DSN_FILE`. The DSN must select a
database and set `tls=true` to use system roots. To trust a private CA, set
`MERGEWAKE_MYSQL_CA_FILE` to a regular PEM file containing CA certificates
(mode 0600 or 0644, at most 64 KiB), and set
`MERGEWAKE_MYSQL_SERVER_NAME` to the expected exact DNS SAN or, only when there
is no SAN extension, the certificate CN. The DSN must then use
`tls=mergewake-ca`. The private CA pool is pinned: system roots are not used.
Missing names, unsupported TLS modes, invalid chains and certificate name or
validity failures refuse the connection; plaintext fallback is never allowed.

For the MySQL HeatWave `mergewake` endpoint with a private CA named
`MySQL_Endpoint_CA` and a CN-only server certificate named
`MySQL_Endpoint_Server`, set `MERGEWAKE_MYSQL_SERVER_NAME=MySQL_Endpoint_Server`
and use `tls=mergewake-ca` in the DSN, even when the connection address is an IP.
Keep DSNs and certificate contents out of YAML, source control, logs and command
arguments.
