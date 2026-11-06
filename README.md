# mergewake

**The after-merge pipeline for your agent fleet.**

mergewake is one Go binary for policy-based landing, one independent review per
PR head, main CI recovery, release manifests, verified file and command rollouts, a polling daemon, and
experimental resident monitors. It stays on Git and GitHub. Hosts and model
pairings are configuration, so the same tool runs across macOS, Linux, and
Linux arm64 hosts.

The shared policy rule is simple: if harm can occur before a revert lands, the
change is **blocking**. Blocking changes need green CI and every finding resolved.
Other changes can land once CI is green. `land` and `watch` default to shadow mode;
a repository must explicitly set `enabled: true` in its policy before they act.

## Quick start

Requires Go 1.27 or newer to build, GitHub API credentials to act, and a supported
reviewer CLI on a reviewer host. The binary itself requires no Go installation.

```sh
go build -o mergewake ./cmd/mergewake
./mergewake --help
./mergewake superfan --name ci
```

Create an operator-owned host config from [examples/host.yaml](examples/host.yaml).
Replace `example/project`, the install inventory, smoke command, reviewer login,
and placeholder model IDs with your values. Put a policy based on
[examples/policy.yaml](examples/policy.yaml) in the target repository. Required
check names must match the repository's actual GitHub checks. Supply a GitHub
credential through the environment variable named by `token_env`; never put its
value in either YAML file.

```sh
./mergewake land --config /path/to/host.yaml --repo example/project --pr 7 --dry-run
./mergewake land --config /path/to/host.yaml --repo example/project --pr 7 --shadow
./mergewake review --config /path/to/host.yaml --repo example/project --pr 7 \
  --author-family openai --dry-run
./mergewake watch --config /path/to/host.yaml --repo example/project --once --shadow
```

Review, release, rollout and agent commands stay in shadow mode unless passed
`--apply`. `land` and `watch` also require repository policy opt-in; `--shadow`
forces observation. `--dry-run` makes no remote writes, runs no reviewer or smoke
command, and writes no pipeline state. Daemon status and role locks are local
supervision state; the daemon keeps them even while observing.

Install a published binary without Go (the command below is for an operator):

```sh
curl -fsSLo /tmp/mergewake-install.sh https://raw.githubusercontent.com/mgh3326/mergewake/v0.2.0/install.sh
sh /tmp/mergewake-install.sh --tag v0.2.0
# Use --tag latest for the latest release; default destination: ~/.local/bin/mergewake
```

## Components

| Command | Behavior |
| --- | --- |
| `land --repo … --pr …` | Run the merge gate for one PR — `--dry-run` prints the verdict — and squash-merge the exact gated head through GitHub. |
| `review --repo … --pr …` | Claim one PR head, select a different model family from the pool with the most quota left to burn before its reset, review only the diff, post inline review comments. |
| `watch --repo …` | Poll main CI, find a green-to-red boundary among squash commits using existing checks, open a revert PR, reopen the task when its API permits, and notify. `--once` inspects once. |
| `release --repo …` | Verify green main ancestry and publish an immutable manifest as a GitHub Release asset. `--commit` accepts a full SHA. |
| `rollout --repo …` | Pull the newest mergewake manifest, verify bytes, install permitted files or container digests, smoke/health-test, roll back on failure, and report. |
| `daemon` | Run enabled PR, CI watch, green-main release and host rollout loops concurrently. Shadow by default; acting needs `daemon.act: true` and `--apply`. |
| `status --daemon` | Show each role’s last run, last error, failure count and next run. |
| `agent` | Poll all configured repositories for verified rollouts. `--once` runs a single poll; `--repo` selects one. |
| `status` | Read local receipts and check installed digests and modes. Returns failure on drift; supports `--repo`. |
| `superfan --name …` | Print a relay, CI, quota, or drift monitor spec. `--launch --runner /path/to/runner` sends it to a trusted session runner on stdin. |

All acting commands support `--dry-run`. There is no merge lock, mandatory
rebase, multi-round tester loop, or committed manifest/hash-pin file. A fixed
recovery tag (`mergewake-recovery-latch`) is a durable stop after **one** revert
PR; the watcher never merges the revert itself. An operator must inspect recovery
and remove that tag and any remaining `mergewake/recovery` branch before another
recovery can begin. The tag survives automatic deletion of merged PR branches.

Release inventories describe regular files at a verified Git commit, their
SHA256, mode, and relative install targets. Hosts separately authorize the file
inventory, install root, deny patterns, and smoke command. Successful agent polls
verify current bytes and avoid repeated writes and reports. Smoke failures restore
previous bytes and permissions and suppress retries of that release until an
operator clears its rejection receipt.

## Integrations and limits

Adapters use only APIs or CLI argument vectors:

- GitHub REST plus a read-only GraphQL query for review-thread resolution.
- `panewire emit` for notifications, with stdout and no-op alternatives.
- `handoffkeep tasks show`, `comment`, `reopen`, and `doc put` for task recovery
  and rollout records; a no-op sink is available.
- hk `POST /v1/task-events` for PR, review, merge and revert events on tasks
  named by a `Mergewake-Task` trailer (`hk_task_events`, on by default).
- Independent reviewers (Claude, Codex, Devin, Grok, Kimi, pi, Command Code,
  agy) chosen by pace to reset: free models first, then pools projected to end
  their window well under 100%, then any under 100%; tight or unmeasured pools
  are never picked. Subscription/exclusion filtering and a gate still run
  before invocation. See [review selection](docs/config.md#review).
- One-shot reviewer processes run in their own process group, which is
  terminated (SIGTERM, grace, SIGKILL) and reaped on exit, timeout,
  cancellation or mergewake's own SIGINT/SIGTERM/SIGHUP.

The handoffkeep sink uses explicit `tasks reopen ID --reason TEXT`, entering the
separate `reopened` state. A recovery comment survives if an older CLI/server
lacks the operation, and the failure is returned and included in the notification.
The server update must be deployed separately; protected task authority fences
remain enforced.

Polling needs no public ingress. Conditional GitHub requests, rate headers,
jitter, bounded backoff, role locks and local status support supervision; examples
are in [deploy/](deploy/). An optional in-process receiver (`daemon.webhook`)
serves HMAC-signed GitHub deliveries on loopback and wakes matching roles
early; polling remains complete.

Command installs use host-owned recipes with `{sha}` arguments, exact-output
verification, smoke checks, prior-SHA journals and rollback receipts. `release`
installs download the same checksummed binaries as `install.sh`. Serial/canary
ordering uses success receipts supplied through an operator-owned shared or
replicated directory. See [docs/config.md](docs/config.md) for templates and
remaining operating limits, and [DESIGN.md](DESIGN.md) for failure boundaries.

Container installs deploy `repo@sha256:...` digests through `docker` or `podman`,
stop-and-swap the previous container, poll an HTTP health check, restore the
preserved container on failure, and garbage-collect old images. `rollout --adopt`
records a hand-started container; `status` reports its digest and health. See
[examples/container.yaml](examples/container.yaml).

## PR lane notifications

While the `pr` role is enabled, each poll recomputes the land decision for every
open PR at its current head and tells the owning lane once that head becomes
mergeable or blocked — workers do not need to watch CI. Notification is
observation, not merging, so it also runs while the daemon is in shadow mode;
`--dry-run` only prints the decision.

```text
[mergewake] READY example/project#7 head=a1b2c3d | Add retry to rollout smoke | lane=worker-lane via task 2252
[mergewake] BLOCKED example/project#7 head=a1b2c3d | check_failed:vet; check_failed:test | lane=worker-lane via task 2252
[mergewake] BLOCKED example/project#7 head=a1b2c3d | threads_open:1; base_stale | lane=operator-desk via config
[mergewake] MERGED example/project#7 head=a1b2c3d | Add retry to rollout smoke | lane=worker-lane via task 2252
```

READY means the merge gate passed: every `required_checks` entry green on the
head, zero unresolved review threads, the `mergewake/reviews/<pr>/<head>-done`
marker plus the head-bound passes from the trusted `review.login` (the marker
alone is forgeable), the base branch an ancestor of the head, and no merge
conflict — the same inputs `land` merges on. BLOCKED names the reason from a
closed vocabulary: `check_failed:<name>` per failing required check,
`threads_open:<n>` for unresolved review threads (`blocking_findings:<n>` and
`triage_unanswered:<n>` instead when the repo policy grades findings), `review_missing` when the
marker or passes are absent, `base_stale` when the base moved ahead of the
head and the repo sets `require_up_to_date: true`, and `conflict` when GitHub reports the PR unmergeable. Pending or missing
check evidence emits nothing. When acting and `base_stale` is the only blocker (it needs `require_up_to_date: true`), `auto_update_branch` defaults to true and updates the PR branch once per base SHA; an ambiguous attempt may be retried after a fresh gate check. Shadow mode reports BLOCKED; set it to false in `.mergewake/policy.yaml` to notify instead. A failed update notifies with `update_failed:<status>` (or `unknown` for an ambiguous failure). Each `(PR, head, verdict)` emits once — a
changed BLOCKED reason set counts as a new verdict and emits again for the
same head — while a new head re-arms the notification and sent keys persist
across daemon restarts under `<state_dir>/<repo>/pr-notify-sent`. READY, BLOCKED, and MERGED are declared `--type ready|blocked|merged`, so they stay unread on the lane
until `panewire inbox ack`, and a READY PR nobody merged is notified again every
`daemon.pr.notify.renotify_after` (default 30m), at most `renotify_max` times
(default 3).

A `review_missing` BLOCKED with no block detail is mergewake's own queue state and
sends nothing; a head that keeps it for more than 60 minutes gets one
`review stalled` alert on the notify lane. The reason table and routing are in
[`docs/config.md`](docs/config.md).

Review findings and author-actionable BLOCKED notices use the task's bound
worker or lead job lane. A completed job keeps that route. Missing author
ownership goes to the desk as a decision. Unresolved findings reach the desk
only after `daemon.pr.findings.escalate_after` (default 2h) or
`escalate_rounds` (default 3 distinct heads). Inbox ACKs stop reminders;
verified dispositions stop aging too. `review.fleet_authors` accepts configured
fleet-account replies under the same fix and dismissal checks.

The daemon retries missing or stalled reviews with persistent exponential
backoff. It rechecks trusted review receipts even when a done marker exists,
uses the local-diff fallback, and interrupts the desk only after the configured
failure threshold. See [review recovery](docs/config.md#review-recovery).

Auto-merge is opt-in per repository with `enabled: true` in
`.mergewake/policy.yaml` (default off). While the daemon acts
(`daemon.act` + `--apply`), a READY verdict merges exactly like the repo
session — squash, pinned to the gated head — and emits MERGED. A head that
moves between gate and merge is re-gated, never merged blind.

The lane is resolved, never guessed, in this order — the choosing rule is
recorded in the event text after `via`:

1. `Mergewake-Task: <id>` trailer — PR body first, then the head commit
   message — read through the configured handoffkeep sink (`tasks show`); a
   read error retries instead of rerouting.
2. `Mergewake-Notify-Lane: <lane>` trailer, in the same order.
3. The configured `notifier.lane`.

Events go through the existing notifier (`panewire emit --kind lane.event
--lane <lane> --event-id mergewake-<state>-<repo>-<n>-<sha7>`); per-event
lanes override the configured one. Disable the feature with
`daemon.pr.notify: {enabled: false}` — it defaults to on while the role runs.

## Discord feed

With a `discord` section in the host config, the daemon posts every watched
repo's events to Discord, so no repo needs its own workflow or webhook secret:
PRs opened, merged and closed, required checks that failed, releases and
rollouts, and watch reverts and holds. Other deployers report through
`mergewake notify deploy --repo R --sha S --status ok|failed --text T`.

Webhook URLs stay in a mode-0600 file; a URL inline in the config is refused. A
slow or down webhook never holds up a poll. See
[the `discord` reference](docs/config.md#discord) for the config, the delivery
rules, and how to point the three channels.

## Reaping finished work

With `reap.enabled`, the `reap` role closes the panes of merged work and
cleans its worktrees. A merged PR carrying `Mergewake-Task` is reaped through
its job — `Mergewake-Task` → hk task → bound job → the pane panewire recorded
(host and label) — so a lead is found wherever it was launched, even in
another repo's worktree. The lead closes only when the whole job is finished:
no open PR in any configured repo carries the task, the pane is `idle` or
`done`, its job dir exists, and its composer is empty. Its last screen is
saved to `<job dir>/pane-screen.txt` (or a fresh `report.md` is kept), the
pane closes, and mergewake runs `panewire done <job>`, so nobody has to tell the
lead to finish. Protected labels, panes the operator has focused or that the
job record does not name, panes without a job dir and panes with unsent
composer text are never closed. A PR without the trailer keeps the worktree
match. Every decision lands in `<state_dir>/<repo>/reap.jsonl`.

Rollout is the desk's: deploy, run `mergewake reap --repo <repo> --dry-run`
once per repo and compare the plan with the panes you expect to close, then
set `reap.act: true`. Rollback is `reap.act: false`. See
[docs/config.md](docs/config.md#reap).

## Development

```sh
go vet ./...
go test -race ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/mergewake-linux-amd64 ./cmd/mergewake
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o dist/mergewake-linux-arm64 ./cmd/mergewake
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o dist/mergewake-darwin-amd64 ./cmd/mergewake
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o dist/mergewake-darwin-arm64 ./cmd/mergewake
```

CI runs vet, the race-enabled test suite, and all four CGO-disabled cross-builds.
Tests use fake GitHub, task sinks, notifiers, and local runners; they do not invoke
models or fleet services. No Go dependency is imported from an integration project.
The only direct external dependency is `gopkg.in/yaml.v3`. Linux outputs are
statically linked. CGO-disabled Darwin outputs use macOS system libraries;
fully static macOS executables are not provided.

Inspired by Anthropic's [Redesigning source control](https://www.anthropic.com/research/redesigning-source-control):
post-merge verification, derived data outside the source tree, review of intent,
and component-focused monitors. mergewake applies those ideas without replacing
Git or running a source-control server.

Model measurement uses PR outcomes rather than tester-round counts. Shadow polling
buffers an append-only local ledger and publishes finalized evidence to handoffkeep
bench reps via the configured quota/reps CLI. `mergewake outcomes report --since 168h` groups clean passes, failures and pending evidence by author profile, effort
and experiment arm. See [configuration](docs/config.md#outcomes-and-shared-measurement)
for the conservative seven-day rule, outbox recovery and shared follow-up semantics.

Workers can read each PR's last merge-gate verdict without spending GitHub budget:
the daemon serves `GET /v1/pr-status` on its webhook listener and `mergewake pr-status`
reads the same snapshot from local state. See [PR status](docs/pr-status.md).

Licensed under [Apache-2.0](LICENSE).
