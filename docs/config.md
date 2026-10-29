# Configuration reference

Each host has one operator-owned YAML file, normally
`~/.config/mergewake/config.yaml`. Each repository has a policy at
`.mergewake/policy.yaml`. Unknown keys, duplicate YAML keys, and multiple YAML
documents are errors. There is no environment interpolation except expansion of
a leading `~/` in local config, state, install-root and container `env_file`
paths. Credentials belong in the environment or the external CLI's own credential
store.

Use [examples/host.yaml](../examples/host.yaml) and
[examples/policy.yaml](../examples/policy.yaml) as starting points. Their repo,
host, model IDs, commands, and task ID are placeholders. None is a discovery rule.
Different hosts may use the same inventory and different deny lists.

## Host file

| Key | Required / default | Meaning |
| --- | --- | --- |
| `version` | Required: `1` | Host-config schema version. |
| `host` | Required | Operator-supplied host label, used in receipts and notifications. Use a public-safe placeholder in shared configs. |
| `state_dir` | `~/.local/state/mergewake` | Private local audit, checkpoints, receipts, locks, and journals. Keep it outside Git and install roots. |
| `token_env` | `GITHUB_TOKEN` | Name of the environment variable containing the GitHub credential. Its value is never a config key. |
| `poll_interval` | `60s` | Positive Go duration between completed `watch` or `agent` iterations. |
| `watch_limit` | `100` | Maximum commit interval inspected during recovery, from 2 through 1000. |
| `repos` | Empty list | Repositories this host may operate on. Names must be unique; `agent` requires at least one. |
| `daemon` | All roles disabled | Role config and explicit acting opt-in; see below. |
| `review` | Empty object | Independent-review configuration described below. |
| `notifier` | No-op | Notification adapter described below. |
| `sink` | No-op | Task and report adapter described below. |

### `repos[]`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `name` | Required | GitHub `owner/name`; commands cannot select an unconfigured repo. |
| `branch` | `main` | Base branch for landing, CI watching, release ancestry, and recovery PRs. |
| `policy` | `.mergewake/policy.yaml` | Clean relative policy path inside the repository. Loaded through GitHub at a trusted base commit. |
| `root` | Required for file and command/release rollout | Explicit local installation directory; leading `~/` expands. Use a dedicated root per inventory. Filesystem root is refused. Container inventories do not use it. |
| `deny` | Empty list | Per-host relative target globs. These override all manifest/install permissions. Example: `bin/relay-hub`, `bin/operator-daemon`, `protected/**`. |
| `install` | Required for release and rollout | Authorized source/target inventory; see below. This is intent/configuration, not generated hashes. |
| `smoke` | Required for file and command/release rollout | Operator-owned smoke command; see below. Never supplied by a manifest or PR. Container targets use `install[].health` instead. |
| `smoke_timeout` | `30s` | Positive Go duration for the smoke command. |
| `task` | Empty | Optional fallback numeric handoffkeep task ID when the originating PR has no `Mergewake-Task` trailer. |

Each `install[]` entry has:

| Key | Required / default | Meaning |
| --- | --- | --- |
| `kind` | `file` | `file`, `command`, `release`, or `container`. A repo inventory cannot mix these families. |
| `source` | Required for file | Regular Git file at the release commit, relative to repo root. Symlinks, trees, submodules and files over 32 MiB are refused. |
| `target` | Required | Clean relative path beneath `root`, or the logical label for command/release/container targets. Absolute paths, traversal, duplicate targets, and file/directory overlaps are refused. |
| `mode` | Quoted `"0644"` | Octal string: only `"0644"` and `"0755"`. Must agree with the manifest. |

The published inventory can contain denied targets; hosts skip them. Other
manifest targets must match a host inventory entry's source, target, and mode.
A host refuses a manifest with no permitted targets. Removing a source or entry
does **not** remove an existing installed file.

`smoke.command` is the executable and `smoke.args` is a YAML array of literal
arguments, defaulting to empty. It runs with `root` as its working directory.
For example:

```yaml
smoke:
  command: ./bin/tool
  args: [--version]
```

Commands are invoked directly, with no shell expansion. Use a separately
reviewed script for more complex checks. Smoke should be read-only: rollback
restores installed files, not external actions taken by a smoke command.

### `review`

| Key | Default | Meaning |
| --- | --- | --- |
| `login` | Required | GitHub identity posting the independent exact-head review. |
| `timeout` | `10m` | Bounds recommendation, quota gates, model invocation and submission. |
| `quota` | `scopefuel` | Executable implementing `--recommend GRADE --json` and `gate -m PROFILE`. Set a compatible pacelever path when installed. |
| `scopefuel` | Empty | Legacy alias used when `quota` is omitted. |
| `default_grade` | `B` | Task grade when the PR has no `Mergewake-Grade: A` body trailer. |
| `pairings` | Empty map | Optional author-family → ordered profile override, intersected with eligible recommendations. Never bypasses subscription, exclusion, or quota gates. |
| `profiles` | Empty map | Operator-owned profile → family, CLI and model selection data. Families describe the underlying model, independently of CLI or quota pool. |

Profiles contain `family`, `command`, `args`, optional `gate_profile` (defaults
to the profile name), and optional `gate_effort`. Commands may be installed paths
ending in `claude`, `codex`, `devin`, `grok`, `kimi`, `pi`, or `cmd`. Args are
option/value pairs: `--model` for all adapters, `--effort` for Claude, Codex,
Grok and Command Code, or `--provider`/`--thinking` for pi. Other options are
refused. Codex effort becomes a TOML override; Kimi effort is set in its isolated
model config from `gate_effort`. Use concrete model IDs from the installed CLI.

Selection preserves the quota tool's ranked `rows` order, admits only
`kind: candidate` rows, filters out the author's family, and tries quota gates
in order. Unknown profiles are skipped. `gate_effort` must match an explicit
recommended effort; it is also passed to the gate. Missing executables and failed
gates fall through. A failed recommendation fails closed. After a head claim and
model invocation, failures or invalid output retain the claim; there is no second
model pass or output repair. Dry-run and shadow may read recommendations but
never invoke a reviewer, gate, claim or post a review.

Subscription and exclusion apply to **every** CLI. Rows marked unsubscribed,
`subscribed=false`, excluded pool classes, and matching recommendation exclusions
are ineligible, including through `pairings`. The quota tool remains the policy
authority; mergewake never enables a pool. Inspect it with `scopefuel policy list`.
For example, keep the Kimi profile configured and use:

```sh
scopefuel policy set kimi --subscribed on
scopefuel policy set kimi --subscribed off
```

A pool also classified `exclude` stays excluded until that policy is cleared or
changed (inspect `scopefuel policy list`; `scopefuel policy clear kimi` clears its
pool override). Once the quota tool returns an eligible Kimi recommendation, no
mergewake code or config change is needed. Do not switch providers merely to
circumvent an underlying pool's subscription state.

Every adapter receives the diff on stdin in an empty temporary working directory.
Only adapter-owned control files may be present (Command Code's deny hook).
GitHub credentials are removed from the review process environment. Claude uses
safe/restricted print mode, no built-in tools, no MCP and no settings discovery;
pi disables tools, MCP and context discovery; Command Code uses safe mode, deny
permissions and a PreToolUse deny hook. Devin print mode receives a private deny
configuration disabling file, shell, MCP, imports and subagents as far as its CLI
supports. Grok uses an empty tool list, MCP deny, no web and no subagents. Codex
exec ignores user config/rules, disables shell, code mode, search, plugins, MCP,
apps and delegation, and uses a read-only sandbox. These are CLI restrictions,
not a uniform OS sandbox for the CLIs themselves; credentials remain owned by
the provider CLI.

Kimi uses ACP instead of `-p`, which only accepts an argv prompt. ACP receives
one stdin prompt, no filesystem/terminal client capabilities or MCP servers, and
any attempted tool call ends the pass. Its temporary private home keeps only
provider/model configuration and transient credential copies; global tool access
is disabled. Temporary files are removed on normal completion/cancellation.
The Kimi configuration semantics are documented in its
[configuration reference](https://moonshotai.github.io/kimi-code/en/configuration/config-files)
and [ACP reference](https://moonshotai.github.io/kimi-code/en/reference/kimi-acp.html).
Codex controls follow the official [configuration schema](https://github.com/openai/codex/blob/main/codex-rs/core/config.schema.json).

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
concrete `--model` argument. Use `baseline` and `downshift` arm names for pacelever exploration compatibility.
Grade defaults to `review.default_grade`; tier defaults
to `outcomes.default_tier` (`T1`). Missing attribution stays visible locally and
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

`outcomes.command` defaults to `review.quota`. Its replaceable reps interface uses
`scopefuel reps add`, the handoffkeep bench reps store, rather than writing hk
HTTP/SQL directly. Each finalized merged/closed PR publishes profile, model,
effort, PR URL as task ref, role `impl`, task grade, tier, rounds `1`, blocking
findings count, and completed `1` only for a landed PR. Structured notes contain
first CI, severity counts (unknown until the landed head is reviewed), seven-day
revert, merge time, experiment/arm, stable
outcome/event IDs, revision and rule classification. Shadow suppresses GitHub
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
| `lane` | Required for panewire | Destination lane; e.g. `operator-desk`. |

The panewire adapter executes literal arguments equivalent to:

```sh
panewire emit --kind lane.event --lane operator-desk --event-id EVENT_ID --text TEXT
```

No-op emits nothing; stdout prints event ID and text. Review and landing produce
local decision output; CI recovery and attempted installs also use the adapters.
Healthy repeated agent polls do not emit a fresh installation notification.

### `sink`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `type` | `noop` (also empty) | `noop` or `handoffkeep`. |
| `command` | `handoffkeep` for that adapter | Executable path or a compatible CLI wrapper. |
| `reopen_state` | `reopened` | Only `reopened` is accepted; terminal tasks use the explicit reason-required operation. |

The handoffkeep adapter calls `tasks show ID`, `tasks comment ID --body TEXT`,
then `tasks reopen ID --reason TEXT` for a merged or dropped task. An already
reopened task only receives the comment. Empty reasons are refused. An older
CLI/server that lacks reopen returns an explicit failure; the recovery comment
is retained and the watch notification reports the failure. Protected upstream
authority fences remain enforced. No fallback generic transition is attempted.
Structured reports use `doc put --key KEY --kind note --file FILE`.

## Per-repo policy

| Key | Required / default | Meaning |
| --- | --- | --- |
| `version` | Required: `1` | Policy schema version. |
| `enabled` | `false` | Explicit opt-in for acting `land` and `watch`. A caller can force shadow with `--shadow`; there is no CLI bypass of `false`. |
| `required_checks` | Required, nonempty | Exact, unique GitHub check-run names or commit-status contexts. Every required result must be successful. |
| `blocking` | Empty list | Rules identifying changes whose harm may happen before a revert. Routine is the default for other changes. |

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

The repository's policy should cover its own verification controls where changing
them can admit irreversible actions. A PR cannot use its own new policy to grant
itself permission: `land` uses the current base-branch policy.

## PR metadata and automation

Add these optional standalone lines to a PR body:

```text
Mergewake-Author-Family: openai
Mergewake-Task: 42
```

`--author-family` overrides the first line. A task trailer overrides `repos[].task`.
Treat the family label as trusted automation metadata, not model provenance
attestation. A handoffkeep-backed watcher refuses recovery with no valid task ID.

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

## Local state and operator recovery

For `example/project`, state resides beneath
`<state_dir>/example/project/`:

| Path | Meaning |
| --- | --- |
| `decisions.jsonl` | Append-only observations/intent and completed recovery/rollout records. A planned action is not proof it completed. |
| `last-green` | Last observed green main SHA; validated again during diagnosis. |
| `watch-reported` | Most recent 128 hold/report keys already notified; repeat polls stay quiet. |
| `rollout/lock/` | Local install exclusion; never stolen automatically. |
| `rollout/transaction/journal.json` | Previous file existence/modes and previous receipt for crash recovery. |
| `rollout/transaction/*.bak` | Private previous bytes. Retained if rollback cannot finish. |
| `rollout/installed.json` | Last successful manifest and install result. Used with actual hashes, never trusted alone. |
| `rollout/rejected.json` | Release that failed after mutation. Prevents repeated retries of that commit. |
| `rollout/container-installed.json` | Current and previous digests per container target, with the last managed release commit. |
| `rollout/container-journal.json` | In-flight container swap; replay restores the previous container before another attempt. |
| `rollout/container-rollback-*.json` | Rollback audit for an interrupted or failed container deploy. |
| `rollout/container-rejected.json` | Container digest that failed after mutation. Prevents repeated retries of that digest. |

After an interrupted install, confirm no process still owns `rollout/lock`, inspect
the journal, and remove only that stale lock. The next acting rollout restores the
journal before proceeding. If rollback fails, preserve its journal/backups and
repair the cause before another attempt. After investigating a smoke failure,
removing `rejected.json` authorizes a retry; stopping the polling agent while doing
this avoids an immediate unintended retry. No dry run clears locks or recovers a
journal.

For a failed independent review, inspect
`mergewake/reviews/<PR>/<full-head-SHA>` and GitHub reviews first. Only delete the
claim if no reviewer remains active and no complete review was posted; then a
rerun can attempt that head. Completed claims can remain as evidence.

For CI recovery, inspect the revert PR, the fixed `mergewake-recovery-latch` tag,
and any `mergewake/recovery` branch. After recovery is complete, an operator may
delete the tag and any remaining branch to authorize another recovery. Keep the
tag while recovery is in flight; it survives automatic PR branch deletion. After an API timeout or a reporting failure,
inspect GitHub/task records and local audit rather than blindly rerunning a
mutation. The initial release has no automatic report outbox/reconciliation.

## Superfan session contract

`superfan` reads embedded specs and requires no host config. Omitting `--name`
prints all four as JSON lines. Each spec includes `name`, `component`, `prompt`,
`allowed_read_only_sources`, and `allowed_actions`. Source names describe inputs
the operator supplies; they are not shell commands executed by mergewake.

`--launch --runner /path/to/session-runner` executes that trusted runner with no
arguments and sends one spec JSON object on stdin. The runner owns the low-cost
model choice, host placement, session lifetime, source collection, and enforcement
of allowed actions. It should return after launching a resident session. With
`--dry-run`, mergewake prints the spec and does not execute the runner. Existing
fleet spawners can be wrapped around this protocol without importing their code.

## Daemon

```yaml
daemon:
  act: false
  pr: {enabled: true, interval: 60s}
  watch: {enabled: true, interval: 60s}
  release: {enabled: true, interval: 2m}
  rollout: {enabled: true, interval: 2m}
```

All roles default disabled; omitted intervals inherit poll_interval. `daemon`
runs only configured roles, one goroutine each. Shadow is the default for all
roles; acting needs both `daemon.act: true` and `mergewake daemon --apply`.
Land/watch additionally require enabled repository policy. Standalone review,
release, rollout and agent need `--apply` to mutate. `--shadow` forces the daemon
back to observation, and `--dry-run` suppresses pipeline state too. The daemon
still maintains its supervision locks and status in shadow/dry-run modes.

Use `mergewake status --daemon --config /path/to/config.yaml` for JSON status.
Each role writes `<state_dir>/daemon/<role>.json`: last_run, last_finish,
last_error, next_run, running and failures. Values are the last saved observation;
a hard crash can leave a stale running/next_run value. Role locks use the OS user
cache under `mergewake/daemon-locks/`, independent of state_dir. Use one service
account per host. Locks release automatically at exit. Install-transaction locks
still require inspection after crashes.

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

While main is red or its watch evidence is unknown, ordinary `land` calls pause
(`action: pause-red-main`, or `would-pause-red-main` in shadow). Unknown main
evidence uses `pause-main-unknown` / `would-pause-main-unknown`. Recovery PRs
from `mergewake/recovery` in the base repository remain landable; a fork using
that branch name does not qualify.
Immediately before opening a revert PR the watch rechecks the current main head
and its CI, and the recovery latch tag still serializes recovery attempts.

## Container installs

`kind: container` covers the small subset of Kubernetes this fleet needs: one
immutable image per logical target, host-published ports, an operator-owned env
file, optional volumes, a restart policy, and an HTTP health check. The runtime is
the host's `docker` by default, or `podman` with the same CLI subset; mergewake
never calls a registry API or a cluster.

| Key | Required / default | Meaning |
| --- | --- | --- |
| `target` | Required | Logical label used in the manifest, deny list and gate receipts. |
| `image` | Required | Registry/repository without a tag or digest. |
| `name` | Required | Runtime container name; the previous container is kept as `<name>.mergewake-prev`. |
| `ports` | Empty | `host_ip:host_port:container_port`, or `host_port:container_port`. The host address defaults to `127.0.0.1`; a supplied address must be an IP literal. |
| `env_file` | Empty | Absolute operator-owned path passed as `--env-file`. Its values are never read or printed. |
| `volumes` | Empty | `source:destination[:ro\|rw]` entries passed as `--volume`. |
| `restart` | `unless-stopped` | `no`, `always`, `unless-stopped`, or `on-failure[:N]`. |
| `health` | Required | `url`, expected `status` (default 200), per-request `timeout` (default `5s`), and `retries` (default 20, one second apart). |
| `keep` | `2` | Previous digests kept as history and spared by garbage collection. `0..100`. |
| `runtime` | `docker` | `docker` or `podman`. |
| `user` | Empty | Optional `--user` value. |
| `digest` | Empty | `sha256:<64 hex>` pinned into the release manifest. Without it, `rollout --digest` must supply the deploy digest. |

### Digest source

A container deploy always names an immutable `repo@sha256:...`. mergewake takes
the digest from the release manifest (`install[].digest`, published by `release`)
or from `rollout --digest repo@sha256:...`. The explicit flag must name the same
repository as `image`, and is accepted only when the inventory has exactly one
container target. A tag, a missing digest, or a digest for another repository is
refused before any pull. The manifest commit remains the release identity used for
ancestry, CI and receipts.

Image names are compared literally with the runtime's repository digests, so use
the fully qualified form (`registry.example/api`, or `docker.io/library/nginx`
rather than `nginx`); the daemon normalizes Docker Hub short names.

The rollout still fetches the newest published manifest, so `--digest` supplies
the payload while the manifest supplies the release identity (commit, ancestry and
CI). Publish one with `mergewake release`; the host inventory may omit `digest`
when the desk intends to pass it explicitly.

### Deploy sequence and swap strategy

The old and new container bind the same host ports, so they cannot overlap.
mergewake therefore performs a short, explicit stop-and-swap rather than starting
a temporary second container behind a proxy:

1. Verify the release commit, CI, deny list, gates and receipt ancestry.
2. Inspect the running container and refuse on drift: a missing or stopped
   container, a digest mismatch, an unmanaged container, or a changed image
   repository.
3. Pull every new digest before touching a running container.
4. Write `container-journal.json`, then stop and rename the old container to
   `<name>.mergewake-prev`. Its writable layer is preserved for rollback.
5. Start `<name>` from the new digest and poll `health` until it passes or its
   retries are exhausted.
6. Persist the receipt (the new digest becomes current; the old digest is
   prepended to `previous`, capped at `keep`), remove the preserved container,
   remove the journal, then garbage-collect images.

On any failure between steps 4 and 6, the new container is removed, the preserved
container is renamed back and started, its health is checked again, and a
`container-rollback-*.json` audit plus `container-rejected.json` is written. The
previous receipt is restored, so `status` matches the running container again. A
digest that rolled back is refused until an operator inspects it and removes
`container-rejected.json`. A crash is recovered from the journal on the next
acting rollout; a journal whose receipt already records the new digest is treated
as a completed deploy, so only the preserved container and journal are cleaned up.

### Image garbage collection

After a successful deploy mergewake lists images of that repository and removes
every digest that is neither the current digest nor one of the newest `keep`
previous digests, except that it never removes an image referenced by any
container, stopped ones included. Removed digests are reported under
`removed_images`; a failed removal becomes a warning and never rolls back a
healthy deploy. Images pulled by a failed attempt are collected by a later
successful deploy.

### Adopt and status

```sh
mergewake rollout --config /path/to/host.yaml --repo example/project --adopt api
```

`--adopt <target>` records an already-running, hand-started container as the
current receipt, including its resolved repository digest, so the first managed
deploy has a known previous digest. The container must be running; a start from a
tag is resolved through the image's `RepoDigests`, and adopt refuses when no
repository digest can be resolved. A rollout refuses to replace a container that
has no receipt and tells the operator to adopt it first. With `--dry-run` (or
without `--apply`) adopt plans and prints the result without writing state.

`mergewake status` prints one JSON line per container target with `running`,
`digest`, `health`, `previous`, `images_kept`, and `status`. Drift or an unhealthy
target is reported and returns a failing exit status.

Container inventories use no `root`, `smoke`, `source`, or `mode`. They cannot be
mixed with file or command/release entries in one repo inventory. `serial` and
`canary` ordering, `receipts_dir` gate receipts, the deny list, `--dry-run`, and
the installed receipt all apply unchanged. See
[examples/container.yaml](../examples/container.yaml) for a template.

## Command installs and receipt ordering

Every install kind accepts `serial`, `canary` and `receipts_dir` for receipt
ordering. Command/release entries additionally accept the recipe fields below:

| Key | Default | Meaning |
| --- | --- | --- |
| `command.command`, `command.args` | Required for command | Trusted installer executable and literal arguments. Args must contain `{sha}`. |
| `verify.command`, `verify.args` | Required | Read-only verification command, run after install and rollback. |
| `verify_output` | `{sha}` | Exact trimmed stdout expected from verify; must include `{sha}`. Use a wrapper to validate a version plus revision. |
| `initial_sha` | Empty | Full previous installed SHA, required until a receipt exists. Verified before mutation. Retain it when editing config so the recipe fingerprint stays stable. |
| `timeout` | `5m` | Timeout for each install + verify; rollback gets a fresh bounded context. |
| `serial` | Empty | Ordered list of host labels. A host waits for all preceding hosts' successful receipts for the same commit. |
| `canary` | Empty | Host label whose successful smoke receipt every other host must see first. |
| `receipts_dir` | Required with serial/canary | Trusted shared or desk-replicated receipt directory. No network transport is supplied. |

Placeholders `{sha}`, `{repo}`, `{root}` and `{target}` expand in argument values
only; commands never pass through a shell. Target is a clean relative logical
install label and remains subject to the per-host deny list. For `release`, it is
the binary path beneath root. Commands and their other paths are operator-owned;
manifest targets cannot constrain the side effects of an arbitrary script.

Use separate inventories for files and command installs. The following are
independent templates, with public-safe placeholders:

```yaml
# (a) An existing clone; a reviewed wrapper checks out the exact SHA, then runs
# that repository's own ./install.sh. No git pull or branch name is substituted.
install:
  - kind: command
    target: tools/example
    command:
      command: /path/to/install-checkout
      args: [/path/to/configured-clone, '{sha}']
    verify:
      command: /path/to/check-installed-sha
    initial_sha: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    canary: host-a
    serial: [host-a, host-b]
    receipts_dir: /path/to/shared-receipts
```

A reviewed `install-checkout` wrapper can use the following sequence. It must
refuse a dirty clone and ensure the configured repository identity is correct;
the clone must already contain the requested commit:

```sh
#!/bin/sh
set -eu
cd "$1"
[ -z "$(git status --porcelain)" ]
git checkout --detach "$2"
[ "$(git rev-parse HEAD)" = "$2" ]
./install.sh
```

```yaml
# (b) uv installs directly from the pinned commit. The verify wrapper must
# independently inspect the installed package's recorded source revision.
install:
  - kind: command
    target: tools/example
    command:
      command: uv
      args: [tool, install, --force, 'git+https://github.com/<owner>/<repo>@{sha}']
    verify: {command: /path/to/check-installed-source-sha}
    initial_sha: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

```yaml
# (c) mergewake release binaries, identical assets to install.sh. Pure release
# inventories poll published v* releases without needing a manifest release.
root: /path/to/dedicated-root
install:
  - kind: release
    target: bin/mergewake
    verify: {command: ./bin/mergewake, args: [commit]}
    initial_sha: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
smoke: {command: ./bin/mergewake, args: [--help]}
```

The initial SHA must correspond to the installed binary, and a published binary
release must exist for it so rollback can reinstall it. Bootstrap with install.sh
as a separate desk action. Binary installs resolve tags to full SHAs, check CI
and ancestry, verify size and SHA256SUMS, and atomically replace the binary.

Command state uses `command-installed.json` (per-target sha/previous_sha and recipe
fingerprint), `command-journal.json`, `command-rejected.json`, and timestamped
`command-rollback-*.json` under the repo's rollout state directory. Recovery
refuses changed recipes or denied targets. Shared gate receipts live at
`<receipts_dir>/<sha>/<host>/<target>.json` and are written only after all verify
and smoke checks pass. Protect that directory: an operator-supplied receipt is a
trust assertion. Missing, failed, wrong-host, wrong-repo or wrong-SHA receipts
never authorize a follower. No empty prior SHA is treated as a rollback plan.

`status` checks file digests or runs command verify as appropriate; denied targets
are reported without execution. There are no implicit service restarts, database
migration reversals or notifications outbox. Smoke and verification should be
read-only, and install scripts must be safe to repeat at the same commit.


`policy.watch_checks` optionally names main-branch checks when their names differ
from PR merge checks. Omission reuses `required_checks`. For example a project
with `Compile and RunPureChecks (PR)` can watch `Compile and RunPureChecks (main)`
without weakening the PR gate. First-PR CI measurement always uses required_checks.
