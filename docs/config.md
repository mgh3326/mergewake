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
| `source` | Required | Regular Git file at the release commit, relative to repo root. Symlinks, trees, submodules and files over 32 MiB are refused. |
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
| `reopen_state` | `in_progress` | State requested after a CI regression. The upstream queue must permit that transition. |

The handoffkeep adapter calls `tasks show ID`, then `tasks comment ID --body TEXT`,
then `tasks transition ID --to STATE --note TEXT`. For `in_progress`/`claimed`, it
adds an explicit `--no-job` reason: recovery does not claim an agent session.
If the task is already in the requested state, only the recovery comment is
added. Task IDs must be positive integers. Structured recovery/rollout reports
use `doc put --key KEY --kind note --file FILE` with a temporary JSON file.

The currently referenced handoffkeep API has no legal transition out of terminal
`merged` or `dropped`. The adapter still records the comment and attempts the
requested transition, but returns a clear error if it is refused; the watcher
includes that failure in its notification. Another Sink implementation or a
compatible future API can supply terminal reopen. No database or integration
source code is modified.

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

Schedule `review --repo OWNER/REPO --pr NUMBER` on PR opened/synchronize events,
then invoke `land` after CI/review updates. Schedule `release` after green main
CI; it only publishes a manifest and never commits one. Run `watch` and `agent`
as foreground processes under your existing process supervisor, or use `--once`.
Signal interruption exits resident loops; no launchd/systemd configuration or
service installation is performed by the binary.

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
