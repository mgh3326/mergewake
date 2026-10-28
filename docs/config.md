# Configuration reference

Each host has one operator-owned YAML file, normally
`~/.config/mergewake/config.yaml`. Each repository has a policy at
`.mergewake/policy.yaml`. Unknown keys, duplicate YAML keys, and multiple YAML
documents are errors. There is no environment interpolation except expansion of
a leading `~/` in local config, state, and install-root paths. Credentials belong
in the environment or the external CLI's own credential store.

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
| `root` | Required for rollout and agent | Explicit local installation directory; leading `~/` expands. Use a dedicated root per inventory. Filesystem root is refused. |
| `deny` | Empty list | Per-host relative target globs. These override all manifest/install permissions. Example: `bin/relay-hub`, `bin/operator-daemon`, `protected/**`. |
| `install` | Required for release and rollout | Authorized source/target inventory; see below. This is intent/configuration, not generated hashes. |
| `smoke` | Required for rollout and agent | Operator-owned smoke command; see below. Never supplied by a manifest or PR. |
| `smoke_timeout` | `30s` | Positive Go duration for the smoke command. |
| `task` | Empty | Optional fallback numeric handoffkeep task ID when the originating PR has no `Mergewake-Task` trailer. |

Each `install[]` entry has:

| Key | Required / default | Meaning |
| --- | --- | --- |
| `kind` | `file` | `file`, `command`, or `release`. A repo inventory cannot mix files with commands/releases. |
| `source` | Required for file | Regular Git file at the release commit, relative to repo root. Symlinks, trees, submodules and files over 32 MiB are refused. |
| `target` | Required | Clean relative path beneath `root`. Absolute paths, traversal, duplicate targets, and file/directory overlaps are refused. |
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

| Key | Required / default | Meaning |
| --- | --- | --- |
| `login` | Required for review and blocking land | GitHub login that posts the independent reviews. Only that identity's exact-head marker counts. For an app installation, use its review bot login. |
| `timeout` | `10m` | Positive Go duration bounding optional quota gates, model invocation, and review submission. |
| `scopefuel` | Empty / disabled | Optional `scopefuel` executable path. Candidates without `gate_profile` are skipped when this is enabled. |
| `pairings` | Empty map | Author-family label → ordered list of allowed reviewer profile names. All named profiles must exist and differ from the author family. |
| `profiles` | Empty map | Named reviewer definitions. The name is an arbitrary config identifier. |

Each profile has:

| Key | Required / default | Meaning |
| --- | --- | --- |
| `family` | Required | Underlying model family label, such as `anthropic` or `deepseek`. Compared with the author's label, regardless of CLI brand. |
| `command` | Required | Installed `pi` or `cmd` executable, optionally an absolute path ending in that name. |
| `args` | Empty list | Option/value pairs for `--model`, `--provider`, `--thinking`, or `--effort`. Other flags are refused to preserve diff-only semantics. Model identifiers must be valid for the chosen CLI/provider. |
| `gate_profile` | Empty | Profile passed to optional `scopefuel gate -m`. Gate failures try the next configured independent candidate. |

Select actual model IDs with the installed CLIs' model-list commands. The
example Claude profile uses pi with the Anthropic provider; the DeepSeek profile
uses Command Code. Provider credentials and billing belong to those CLIs.
mergewake does not embed API keys, choose a model by CLI name, or infer a family
from a provider's changing model catalog.

The review command adds its own context/tool restrictions. Output must be exactly:

```json
{"findings":[{"path":"src/tool.go","line":12,"body":"Concrete bug and consequence"}]}
```

An empty `findings` array is valid. Lines refer to the right/new side of the diff.
At most 50 findings and 4000 bytes per finding body are accepted. Invalid output
stops the pass and retains its review claim. No output-repair round runs.

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
| `rollout/lock/` | Local install exclusion; never stolen automatically. |
| `rollout/transaction/journal.json` | Previous file existence/modes and previous receipt for crash recovery. |
| `rollout/transaction/*.bak` | Private previous bytes. Retained if rollback cannot finish. |
| `rollout/installed.json` | Last successful manifest and install result. Used with actual hashes, never trusted alone. |
| `rollout/rejected.json` | Release that failed after mutation. Prevents repeated retries of that commit. |

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
