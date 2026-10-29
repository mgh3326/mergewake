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
| `land --repo … --pr …` | Classify paths and change kinds using policy from the current base branch; squash-merge the exact reviewed head through GitHub. |
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
are in [deploy/](deploy/). An optional in-process trigger seam can wake roles from
trusted webhook adapters without changing the pipeline.

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
[mergewake] BLOCKED example/project#7 head=a1b2c3d | failing checks: vet,test | lane=worker-lane via task 2252
[mergewake] BLOCKED example/project#7 head=a1b2c3d | needs 2nd review; open blocking findings: 1 | lane=operator-desk via config
```

READY means green required checks, the required independent review passes for
the change class, a clean review gate, no merge conflicts, and green main —
the same inputs `land` merges on. BLOCKED names the reason: `failing checks:
<names>` when a required check fails, `open blocking findings: N` or
`unanswered findings: N` for held review threads, `needs review` /
`needs 2nd review` / `needs N reviews` for missing passes, `missing required
reviews: <bots>` for opted-in bots that have not reviewed this head,
`conflicts` when GitHub reports the PR unmergeable, and `main CI red` while
the configured branch is failing. Pending or missing check evidence emits
nothing. Each `(PR, head, state)` emits at most once; a new head re-arms the
notification and sent keys persist across daemon restarts under
`<state_dir>/<repo>/pr-notify-sent`.

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

Licensed under [Apache-2.0](LICENSE).
