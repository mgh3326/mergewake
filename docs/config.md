# Configuration reference

Each host has one operator-owned YAML file, normally
`~/.config/mergewake/config.yaml`. Each repository has a policy at
`.mergewake/policy.yaml`. Unknown keys, duplicate YAML keys, and multiple YAML
documents are errors. There is no environment interpolation except expansion of
a leading `~/` in local config, state, install-root and container `env_file`
paths. Credentials belong in the environment, the external CLI's own credential
store, or a mode-0600 file the config names (`reap.hub.token_file`,
`discord.webhooks_file`).

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
| `hosts` | Empty list | Session hosts scanned by `reap`; see below. |
| `reap` | Disabled | Merged-PR session/worktree reaping; see below. |
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
| `lane` | Empty | Lane for this repo's PR events when neither the task trailer nor a `Mergewake-Notify-Lane` trailer names one; checked before `notifier.fallback_lane`. |
| `hk_task_events` | `true` | With the `handoffkeep` sink, post a task event to hk's `POST /v1/task-events` for each PR with a `Mergewake-Task` trailer: `mergewake.pr` when an open, ready PR is first observed, `mergewake.review` when a review run claims a pass, `mergewake.merge` after mergewake's own merge, and `mergewake.revert` when a watch revert PR is merged (a revert merged more than seven days before mergewake first sees it is not posted). Dry runs and shadow mode post nothing. A failed post never fails the action; each post is an `hk-task-event` row in `decisions.jsonl`. Set `false` for a repo whose hk predates the endpoint (handoffkeep#136). |

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

### `hosts[]`

Session hosts where agent panes may run, used only by `reap`.

| Key | Required / default | Meaning |
| --- | --- | --- |
| `name` | Required | Unique label for receipts; same alphabet as `host`. |
| `ssh` | Empty / `local` | Empty or `local` (with no `machine`) runs commands on this machine. Any other value is a single `ssh(1)` destination such as `ops@node.example`, used by the ssh transport; options, whitespace and leading `-` are refused. |
| `machine` | `name` | The panewire machine id a remote host is reached as under the panewire transport. Setting it makes the host remote; under the ssh transport such a host also needs `ssh`. A job-bound lead is looked for only on the host whose `name` or `machine` equals its hk job record's `host` (panewire's `HUB_MACHINE_ID`), so name a local host after its machine id; otherwise its leads are skipped as `job-host-unconfigured`. |
| `jobs` | Empty | Base directory holding one job directory per named pane (`<jobs>/<pane name>`; `<jobs>/<job id>` for a job-bound lead). Needed before any pane on a local or ssh host can be closed: a pane is skipped with `jobs-dir-unset` without it. Leading `~/` expands for local hosts only; ssh hosts must give an absolute path because `~` does not expand through the quoting used. Unused through the hub, where the node uses its own jobs inbox. |

### `reap`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Gates the `reap` command and daemon role. |
| `act` | `false` | Reap's own acting opt-in, separate from `daemon.act`. Without it every mode — including `--apply` — stays observational. Requires `enabled`. |
| `worktree_after` | `168h` | Earliest age of a merge before its worktree can be removed. |
| `scan_after` | `720h` | Merged-PR listing window: only PRs merged within this span are discovered each pass. Older work still arrives via prior `reap.jsonl` rows. |
| `protect` | `*-integrator`, `director-*`, `fleet-tooling-*`, `consult-*`, `cmd` | Pane-name globs that are never closed. A non-empty configured list replaces the defaults. Unnamed panes and panes outside a linked worktree are always protected. |
| `transport` | `panewire` when `hub.url` is set, else `ssh` | How remote hosts are reached: `panewire` sends every remote operation through the hub's node commands; `ssh` runs `herdr`/`git` over `ssh(1)`. Local hosts always run locally. |
| `hub.url` | Empty | Panewire hub origin, `https://` (plain `http://` only on loopback), with no path, query or userinfo. Required by `transport: panewire`. |
| `hub.token_file` | Required by `panewire` | Absolute path (leading `~/` expands) to the mode-0600 env file of a scoped hub service credential: `HUB_MACHINE_ID=svc-mergewake` and its `HUB_TOKEN` (see "Hub credential" below). A file whose `HUB_MACHINE_ID` is `operator`, or that has none, is refused when the config loads and again when each pass reads it; a file other users can read is refused. |
| `hub.cf_file` | Empty | Optional mode-0600 env file with `CF_ACCESS_CLIENT_ID` and `CF_ACCESS_CLIENT_SECRET` for a hub behind Cloudflare Access. |
| `archive` | Disabled | Job-dir archiving to a synced Google Drive folder plus hk; see `reap.archive` below. |
| `regenerable` | `.venv`, `node_modules`, `__pycache__`, `.pytest_cache`, `dist`, `build`, `target`, `obj`, `bin`, `Library`, `Temp`, `Logs`, `UserSettings` | Gitignored path globs that are expendable (build/cache output), so a worktree holding only them may still be removed. A pattern without `/` matches only the ignored entry's last segment — an ignored file inside a tracked `bin/` or `Logs/` dir is not regenerable just because an ancestor shares the name; a pattern with `/` matches the whole path. Any other ignored path — `.env`, notes, unknown output — refuses removal. A non-empty configured list replaces the defaults. |

`mergewake reap --repo owner/name [--pr N] [--dry-run|--apply]` and the `reap`
daemon role close merged-PR sessions, then clean their worktrees once they
age out. For each merged PR on a configured repo's base branch, every host is
scanned with `herdr agent list` — remote hosts through the panewire hub (one
`GET /v1/nodes/agents` per pass, see "Panewire transport" below) or, under
the ssh transport, over `ssh <ssh> herdr agent list`. A pane matches only when its `cwd` is a linked git worktree, its
current branch equals the PR head branch, its checked-out commit equals the
PR head SHA, and the worktree's origin remote names the configured
repository. Titles and names are never used for matching; a reused branch
whose tip moved on does not match.

One exception covers mergewake's own branch update. When it merges the base
into a stale PR branch, the PR head becomes a merge commit the worker never
checked out. Such a worktree still matches when the PR head has exactly two
parents, the first is the worktree's checkout, and the second is the base SHA
of an `ok` base-update receipt for that PR in the repo's state dir
(`<state_dir>/<repo>/base-updates/`, written by the branch update, only read
here). With no receipt, a receipt that is not `ok`, a different first parent
(the worktree holds commits the PR head lacks) or a single parent, nothing
matches. The exception applies to every use of the match: closing the pane,
the worktree recorded on a job-bound row, and worktree and branch removal.
The head commit is read once per PR per daemon process, shared with the task
lookup below; a worktree that matches the head exactly costs no extra call.

Before a matching pane is closed it must be reported as `idle` or `done` —
an allowlist; `working`, `blocked`, `waiting`, empty and unknown states all
keep the pane — and `<jobs>/<name>` must already exist, which is what proves
the fleet spawned the pane: a named operator pane merely cd'd into a merged
worktree is skipped as `not-a-job`. The pane's output must then be captured:
an existing `<jobs>/<name>/report.md` wins only when it is newer than the
merge — a stale file could belong to a reused job name — otherwise the pane's
last screen is saved to `<jobs>/<name>/pane-screen.txt` first. The saved path
is recorded in the receipt. Immediately before `herdr pane close <pane_id>` the agent list
is re-read and the pane must still be present with the same id, status, cwd
and name; a pane that changed or went back to work is skipped as
`pane-changed` or `pane-gone`. Closing happens only in acting mode.
Two more guards apply to every close: a pane herdr reports `focused` is the
operator's (`operator-owned`), and a pane whose input line holds text nobody
sent is kept. The composer is read with `herdr pane read <id> --source
visible` — claude's input line between its last two dividers, codex's `›`
line — and anything else reads as `composer-unknown`, which also keeps the
pane; a dim placeholder reads as text and keeps it too.

#### Job-bound leads

A merged PR whose body or head commit carries `Mergewake-Task: <id>` is
reaped through its job instead of its worktree: the same binding the findings
router uses — hk task → bound job (`refs.job_id`) → the job record panewire
wrote at spawn (`host`, `label`, `pane_id`). The pane is the one named after
the label on the configured host whose `name` or `machine` equals the
record's `host`. herdr pane ids are host-local, so no other host is searched:
a record whose host is missing or not configured is `job-host-unconfigured`,
and more than one match is `job-pane-ambiguous`. Its cwd does not matter, so a lead that launched in
one repo's worktree and owns PRs across repos is still found. The binding is
read once per merged PR per daemon process (a task with no job yet is asked
again each pass). A PR without the trailer, or whose task has no bound job,
keeps the worktree match above — the latter still behind the open-PR rule
below; an hk read error closes nothing that pass.

The lead closes only when the whole job is finished, checked in this order:

1. idle or done, not protected, `<jobs>/<job>` resolvable (`jobs-dir-unset`,
   `job-name-invalid`), a record that names its pane (`pane-id-unrecorded`),
   not focused and that very pane (`operator-owned`);
2. no open PR — against any base, in any configured repo, trailer in the body
   or head commit — carries the task; otherwise `job_has_open_pr`, with the
   open PRs (`owner/name#N`) in the row's `open`. A repo mergewake does not
   configure is not seen;
3. an empty composer (`composer-text`, `composer-unknown`);
4. the job dir exists (`not-a-job`) and the output is saved first, by the
   same capture rule as above, into the job's directory `<jobs>/<job id>`;
5. the same re-read before `herdr pane close`.

After the close, mergewake marks the job done the way the lead would:
`panewire done <job> --report <saved>` on the job's host, a `job-done` row with
`done` (`would-done` in shadow). A job hk already holds terminal is skipped as
`already-<state>`. If the done fails, the next pass retries it once the pane
is gone, until a `done` row or a terminal hk state ends it. When the job's host
was read and no pane wears the job's label, and no earlier pass closed that
job's pane, reap writes one `close-pane` row with `skipped` and the reason
`job-pane-missing` (with `task`, `job` and `host`), so a task bound to a lost
job leaves a trace; the same outcome on a later pass writes no new row. A host
whose panes could not be read writes none. Each job is decided once per pass,
on the first of its merged PRs. Job-bound rows carry `task` and `job`; they
carry `cwd` and `worktree` only when the lead sits in that PR's own
worktree, which worktree cleanup then picks up as before.

Rollout (the desk): deploy, run `mergewake reap --repo <repo> --dry-run`
once per configured repo and compare the planned `would-close` and
`job_has_open_pr` rows with the panes you expect, then set `reap.act: true`.
Rollback: `reap.act: false`.

After `worktree_after` with no revert, a merged PR's worktree is removed —
but only for PRs that name a candidate worktree on some configured host (a
matching pane cwd or a prior `reap.jsonl` row); a merged PR with nothing to
reap produces no rows and no follow-up GitHub calls. The pull list endpoint
omits `merge_commit_sha`, so when candidates do exist it is resolved through
the single-PR endpoint before the revert check. The
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
stays. Worktrees are found from the panes and from prior `reap.jsonl` rows, so
a worktree whose pane already closed still gets cleaned.

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
- capture uses `pane.capture` with the merge time and, only in acting mode,
  `write: true`: the node applies the same job-directory, fresh-`report.md`
  and `pane-screen.txt` rules against its own jobs inbox, and the receipt's
  `saved` is the node's path;
- the close uses `pane.close` with the discovered id, name, status and cwd;
  the node re-reads herdr and closes only an exact match, answering
  `pane-changed` or `pane-gone` otherwise, which become the same receipt
  reasons as the ssh path;
- composer checks use identity-checked `pane.read`; only recognized `empty`
  permits closing, and hub errors or unknown composers keep the pane;
- after closing a job pane, `job.done` runs the guarded host-local completion
  command using the report saved by capture.

The hub host's aged-out worktree is removed through node command
`worktree.remove`, with the inspected PR head, base branch and configured
`reap.regenerable` list. Its refusal outcomes become the same receipt reasons
as the ssh path (`dirty`, `pane-in-use`, `unmerged-or-unpushed`, etc.);
ignored paths retain the `ignored-files:<count-hash>` reason and `files` list.
An unrecognized node outcome is skipped as `cleanup-unknown:<outcome>`.
Shadow mode records `would-remove` without calling the removal command. The
hub path never falls back to ssh. Hosts with
empty `ssh` and no `machine` keep running every step locally.

##### Hub credential

Reap authenticates to the hub as the scoped service client `svc-mergewake`,
never with the operator token (panewire `docs/hub-scopes.md`). It sends
`X-Panewire-Machine-ID: <HUB_MACHINE_ID>` with `Authorization: Bearer
<HUB_TOKEN>`. The hub's auth file grants it exactly the reap scopes, on every
machine (operator decision D1):

```sh
HUB_SCOPES_svc-mergewake=nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:worktree.remove@*,node.command:pane.capture@*,node.command:pane.read@*,node.command:job.done@*,node.command:pane.close@*
```

| Scope | Reap uses it for |
| --- | --- |
| `nodes.agents.read` | discovery, `GET /v1/nodes/agents` |
| `node.command:agents.list@*` | not sent by reap today (discovery uses the fleet-wide list); granted with D1 for a per-machine re-read |
| `node.command:worktree.inspect@*` | worktree matching and the aged-out worktree check |
| `node.command:worktree.remove@*` | guarded cleanup of merged PR worktrees |
| `node.command:pane.read@*` | identity-checked visible composer classification before a close |
| `node.command:job.done@*` | host-local guarded `panewire done` after closing a job pane |
| `node.command:pane.capture@*` | saving the report or screen before a close |
| `node.command:pane.close@*` | the exact-match close |

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
discovery or `worktree.inspect` it fails the host for the pass; on a capture
or close it is an `error` receipt for that pane. Either way the error reads
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
| `text_max_bytes` | `1048576` | Size cap for job text files (`*.md`, `*.json` — brief, report, events) stored as hk attachments; at most 3 MiB. |
| `min_age` | `24h` | How long after reap closed a job's pane its job dir is archived; also the `archive-jobs --older-than` default. Accepts Go durations and whole days (`14d`). |

A panewire job dir (`<jobs>/<job>` on a `hosts[]` entry) is archived as one
unit, never its worktree — that is in git and is cleaned by the worktree
rules above. Archiving runs these steps in order, and any failure keeps the
local job dir and reports the step:

1. **Plan** (read-only): the jobs dir is resolved on its own host — a
   leading `~/` is expanded with that host's `$HOME`, since quoted paths
   never tilde-expand, and a relative one is refused as
   `jobs-dir-not-absolute`. A job dir that is itself a symlink is refused as
   `job-dir-symlink` (checked again right before the delete); otherwise
   list every non-directory entry. Credential-like
   paths — any path segment matching `.env*`, `*token*`, `*.pem` or `*key*`,
   case-insensitively — are excluded from Drive and hk and named in the
   record. Symlinks and special files are not copied (following a link could
   archive a secret from outside the job dir). The hk task id comes from the
   job's panewire events (`events/*-job.task_link.json`, falling back to
   `*-job.claim.json`); a job without one is skipped as `task-unknown`. The
   destination is `<drive_dir>/<host>/<YYYY-MM>/<job>/`, the month of the
   job's newest entry. A destination that already holds a `manifest.json` is
   a finished archive: when that manifest names the same job, host and task
   with the same files and sizes, the plan is a delete-only `resume` (the
   files must still hash to the manifest); otherwise it is skipped as
   `drive-dir-exists`. A job with its own top-level `manifest.json` is
   skipped as `manifest-collision`.
2. **Copy**: the remaining regular files are streamed as an uncompressed tar
   from the job host and unpacked straight into the destination on the
   archive host, relayed through this machine (`ssh` on either side as
   configured); the Drive copy is a plain directory, not an archive file. A
   job with nothing but excluded or non-regular entries copies nothing.
3. **Verify**: every file's sha256 is computed on both sides and must match;
   a missing, duplicate or unexpected hash record fails the archive.
4. **hk attachments**: each text file under `text_max_bytes` goes through
   `handoffkeep attach put`; its returned sha256 must match. `attach put`
   has no task reference, so the ids are tied to the task by the comment.
5. **Record**: `manifest.json` (job, host, task, source, every file's path,
   size and sha256, excluded and skipped paths, attachment ids) is attached
   to hk, then `handoffkeep tasks comment <task>` posts the Drive directory,
   file count, total bytes, the manifest's attachment id, every attachment
   id, the excluded and skipped names, and the per-file sha256 list (the
   first 200; the rest are in `manifest.json`).
6. **Manifest**: only then is `manifest.json` written beside the files, so a
   failed upload or comment leaves no manifest and the job is retried in
   full.
7. **Delete**: the job dir is listed again and must be unchanged — a file
   added or modified since the plan was never archived, so the job is kept
   as `job-changed` — then removed with `rm -rf`. Excluded credential files
   go with it; their names stay in the record.

The hk command is `sink.command` when the sink is `handoffkeep`, otherwise
`handoffkeep`. The job host needs `find`, `stat`, `tar`, `xargs` and
`sha256sum` or `shasum`; the archive host `tar` and the same hashing tools;
this machine `bash`. A job dir whose listing exceeds the 4 MiB command
output limit fails and is kept.

In the reap role the stage runs after the PR loop, from `reap.jsonl`: each
job whose pane reap `closed` (the row's `job`, or the directory of its saved
report for older rows) is archived once that close is older than `min_age`.
It needs no GitHub call. Acting needs `reap.act` and `--apply` like every
reap mutation; otherwise the plan — what moves to Drive, what goes to hk,
what is excluded and what would be deleted — is printed as an
`archive-plan` line with a `would-archive` row (shadow `would-close` rows are
planned too, once per job in the daemon). A job whose name is again a live
pane is skipped as `pane-live`. Each outcome is an `archive-job` row in
`reap.jsonl` with the Drive directory, hk ids and excluded names; `archived`
and every refusal other than `pane-live`/`pane-scan-failed` are final, and a
failed attempt is retried at most once an hour.

`mergewake archive-jobs [--older-than 14d] [--host NAME] [--dry-run|--apply]`
runs the same plan and apply over a host's existing backlog: every job dir in
`hosts[].jobs` whose newest entry is older than `--older-than` (default
`min_age`), skipping names that are a live pane or hold a pane's cwd — a host
whose pane list cannot be read is skipped whole. Without `--apply` it only
plans; rows go to `<state_dir>/archive-jobs.jsonl` unless `--dry-run`. It
needs no GitHub token.

### `review`

| Key | Default | Meaning |
| --- | --- | --- |
| `login` | Required | GitHub identity posting the independent exact-head review. |
| `fleet_authors` | Empty | Additional fleet accounts. Their replies count under the same fix and dismissal checks as `login`. A PR that `login` or one of these accounts opened must carry a task under `executor: panewire` (see [Blocked fleet reviews](#blocked-fleet-reviews)). |
| `timeout` | `10m` | Bounds recommendation, quota gates, model invocation and submission. |
| `quota` | `scopefuel` | Executable implementing `--recommend GRADE --json`, `--json` (usage snapshot) and `gate -m PROFILE`. Set a compatible pacelever path when installed. |
| `scopefuel` | Empty | Legacy alias used when `quota` is omitted. |
| `default_grade` | `B` | Task grade for review selection when the PR has no `Mergewake-Grade: A` body trailer. Outcomes never use it. |
| `default_family` | Empty | Reviewer family used when the PR has no author-family trailer and the head commit's Co-Authored-By/author identities hold no known model alias. Must name a configured profile family; reviewed heads record the inference. |
| `budget_rounds` | `3` | Review rounds a PR gets before later rounds report only critical and high findings; negative turns the budget off. See "Review rounds, contract and budget". |
| `select` | `pace` | `pace` orders admitted reviewers by projected use at each pool's reset; `rank` keeps the quota tool's recommendation order and never reads usage. |
| `pairings` | Empty map | Optional author-family → ordered profile override, intersected with eligible recommendations. Never bypasses subscription, exclusion, or quota gates. |
| `profiles` | Empty map | Operator-owned profile → family, CLI and model selection data. Families describe the underlying model, independently of CLI or quota pool. |
| `executor` | `headless` | `headless` runs each pass with a profile's CLI on the diff on this host; `panewire` spawns a visible reviewer pane through panewire and keeps headless as the fallback. |
| `panewire.command` | `panewire` | Single executable for spawn and inbox calls. |
| `panewire.workspace` | `reviews` | herdr workspace (`-w`) for reviewer panes. |
| `panewire.owner` | Empty | Owner lane (`--owner`); empty inherits the author job's owner. |
| `panewire.host` | `auto` | `--host`; anything but `auto` is passed with a recorded override reason. |
| `panewire.jobs` | `~/work/herdr-inbox/jobs` | panewire's jobs root on this host; the brief lives in `<jobs>/<label>/`, and so do the findings and report of a pane that runs here. |
| `panewire.checkouts` | `<state_dir>/review-checkouts` | Parent of the detached PR-head worktrees created from the repo's `root`; panewire names the remote worktree after the checkout. After each terminal pass outcome, the reviewer pane is closed (only when its name, cwd and composer still match) and its remote detached worktree is removed through the hub's guarded removal; a refusal is recorded as `pending-cleanup`. Each cleanup writes a `reviewer-cleanup` row to `reap.jsonl` and to `decisions.jsonl`. A pane whose wait timed out is never closed here. |
| `panewire.lane` | `panewire.owner` | panewire lane a pre-`--result` pane announces its findings attachment on (`--deliver inbox`), read with `<panewire> inbox list --lane <lane> --json`. Kept for one release as the fallback when the reviewer job record has no result; each use is counted. Prefer a lane dedicated to mergewake: hub policy may still deliver the event to an idle pane on that lane. Empty (and no owner): only a pane on this host can return findings through the fallback. |
| `panewire.exclude_hosts` | Empty | Hosts the pane must not run on, passed as `--exclude-host a,b` (needs a panewire whose `spawn` has `--exclude-host`). Must not contain `panewire.host`. |

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
   author's hk job record (`GET /v1/jobs/<author job>`, read through the same
   handoffkeep endpoint and credentials the sink's CLI uses) supplies
   `--builder-family`; a builder spawned before job records have no record,
   and the trailer/commit inference answers instead. A task with no bound job
   (the author did not run through panewire) is not skipped: the pass runs as a
   top-level reviewer job, `--builder-job` is omitted, and `--builder-family`
   still carries the author's family (trailer/commit inference) so the
   reviewer excludes it.
2. A fork PR (head repository other than the repo) is not offered: panewire can
   replicate only a spec origin carries. The pass claim is taken, then
   `git -C <root> fetch --no-tags origin +pull/<n>/head:refs/remotes/origin/pull/<n>/head <base>`
   (panewire builds the detached spec for another host from that ref, not from
   `FETCH_HEAD`) and a worktree
   detached at the exact head SHA is added under `panewire.checkouts`
   (`git worktree add --detach <checkout> <sha>`). mergewake creates no local
   branch, and the author's branch being checked out in another worktree is no
   obstacle. The worktree is removed after the wait.
3. mergewake writes a brief naming the PR, head SHA, base, lens, the
   author family, the output contract and the same PR context (answered
   findings, operator decisions) the headless prompt carries, then runs
   `panewire spawn -c <checkout> -m auto -p <brief> -l
   mwr-<pr>-<sha7>-p<pass>-<unix> --t <T> --task <task> [--builder-job <author job>] --builder-family <family from the author's job record, else inferred> --role
   reviewer --report <jobs>/<label>/report.md [--exclude-host <hosts>] --wait
   --wait-timeout <rest of review.timeout>`. panewire picks host and model and,
   for the reviewer role, excludes the author job's family; on another host it
   clones the repo under `$HOME/work` and adds a worktree detached at the same
   head, fetched as `pull/<n>/head`. Exit 77 (no admitted seat outside that family) retries
   once with `--role worker`, which may pick the author's family; the receipt
   records that as `fallback`.
4. The pane first checks `git rev-parse HEAD` against the head SHA; on a
   mismatch (a stale remote worktree) it writes
   `{"refused":"…"}` instead of reviewing, and the pass falls back. Otherwise
   it writes `{"findings":[…]}` to `findings.json` in its job directory
   (`<jobs>/<label>/` here, panewire's jobs root on another host) and runs
   `panewire done <label> --result "$J/findings.json" --report "$J/report.md"`,
   which stores the findings on the pane's hk job record in the same write
   as `state: completed`.
5. After the wait, mergewake reads `GET /v1/jobs/<label>` (same endpoint and
   credential convention as above) and applies the W4 rule: a completed
   record's `result` is used; a completed record without a result, an
   `escalated` or `lost` record (the receipt records the state), a still
   pending or missing record, and an unreadable hk all fall back — the last
   three after polling every 5s for up to 30s (at most half the remaining
   deadline), an unreadable hk reporting reason `hk-unavailable`. A historical pane from before `runtug done --result` (runtug was replaced by panewire) has no result on its record, so the lane announcement + `handoffkeep attach get` (or, on this
   host, `<jobs>/<label>/findings.json`) is still tried and each use is
   counted: the receipt's `fallback` notes it and a
   `review-inbox-fallback` audit record carries the running count. The
   findings go through the same validation as headless output (new-side
   diff lines, severities, at most 50) and are posted with the same receipt,
   claim and done marker. The receipt adds `"executor":"panewire"`, the
   reviewer is `panewire:<seat>`, and the family is the one on the reviewer's
   job record — a remote pane's spawn events live on its own host — with the
   local `job.spawned` event (or the profile whose gate profile is the seat)
   as the fallback only when the record has none; an unknown family counts as
   same-family. The checkout is removed after the wait.

No task trailer or T marker on a PR outside the fleet, no repo `root`, a fork
PR, a failed checkout, a panewire refusal or wait failure, a refusal by the
pane, no findings or rejected findings all fall back to the
headless executor for that pass under the same claim; its receipt records
`"fallback":"panewire: <reason>"`. All executors share `review.timeout`, so a
pane that used the whole deadline leaves nothing for the fallback. In panewire
mode a failing quota tool is not fatal: it only leaves the headless fallback
without candidates. Passes of a blocking PR are separate panes; panewire
enforces only the author-family exclusion between them.

panewire fails closed when the remote host cannot stand at the head: an
existing worktree there that is not detached at that commit or is dirty is a
placement refusal for that pass, which falls back to headless.

#### Blocked fleet reviews

With `executor: panewire`, a PR opened by `review.login` or a
`review.fleet_authors` account does not take the fallback above when it lacks
what panewire needs. Its review is blocked until the author fixes the PR or the
task:

| Outcome | Cause | Fix |
| --- | --- | --- |
| `blocked: missing-task-trailer` | Neither the PR body nor the head commit has one valid `Mergewake-Task` trailer. | Add `Mergewake-Task: <id>` to the PR body. |
| `blocked: task-missing-T-marker` | The task's title has no grade and T marker. | Add `(grade X, Tn)` to the hk task title, for example `(grade B, T1)`. |

For a blocked head, mergewake:

- records the outcome on the PR's outcome row, under `review_blocked_by_head`;
- posts one PR comment per head that names the fix, deduplicated by a hidden
  head key in the comments of `review.login`;
- emits one `decision` event per head and outcome to `notifier.fallback_lane`,
  or to `notifier.lane` when no fallback is set.

mergewake takes no pass claim and writes no receipt or `-done` marker, so the
merge gate reports `review_missing`. A block is not a review failure: it adds no
retry backoff and no failure escalation, and the stall alarm skips the head. The
next poll after the fix runs the review on the same head and clears the outcome.

A read error is not a block. If the commit or the task cannot be read, the pass
takes the fallback above. Other authors, the headless executor, a repo without
`root` and a head that already has its passes or inherits them are unaffected.
`mergewake review` exits non-zero on a blocked head. A dry run only prints the
decision. Shadow mode also records the outcome, and it posts and emits
nothing.

Profiles contain `family`, `command`, `args`, optional `gate_profile` (defaults
to the profile name), and optional `gate_effort`. Commands may be installed paths
ending in `agy`, `claude`, `codex`, `devin`, `grok`, `kimi`, `pi`, or `cmd`. Args are
option/value pairs: `--model` for all adapters, `--effort` for agy, Claude,
Codex, Grok and Command Code, or `--provider`/`--thinking` for pi. Other options are
refused. Codex effort becomes a TOML override; Kimi effort is set in its isolated
model config from `gate_effort`. Use concrete model IDs from the installed CLI.

Only `kind: candidate` rows are admitted. With `select: pace` (the default) the
quota tool's usage snapshot (`<quota> --json`) is read once per review and each
admitted profile is placed by the pool its recommendation row names (`pool`):

- **Projection.** For every window that applies, projected use at reset is
  `used_pct ÷ elapsed fraction of the window`, where the elapsed fraction is
  `1 − (resets_at − now) ÷ window` (floored at 5% so a window that just reset
  does not turn a few percent into a large multiple). The worst window wins.
  Account- and group-scoped buckets apply to every profile of the pool;
  model-scoped buckets only to a profile whose `--model` contains the scope
  name (ignoring case and punctuation). An unused window projects 0%. A
  `credit` window has no reset: its `used_pct` is the projection, and it never
  counts as waste.
- **Order.** Free models first (recommendation `billing: free`, or a matching
  model bucket noted *Free*), then pools projected under 70% (they would end
  the window with quota unused) by lowest projection, then the remaining pools
  under 100% by lowest projection. Ties keep the recommendation order.
- **Skip.** A pool projected at or above 100%, an excluded or stale/degraded
  pool, a missing pool, a bucket with unknown usage, an unknown window, a
  missing reset (with usage above 0) or a reset already passed are never
  picked. When every admitted profile is skipped the review fails with each
  profile and the reason (`… projected 104% at 7d reset`, `… pool not
  measured`), and nothing is gated, claimed or invoked.

The chosen reviewer's pool, tier and projection are recorded as `pace` in the
review audit event (stdout and `decisions.jsonl`); dry-run prints the pace of
every candidate and lists skipped profiles with their reason (`pace_skipped`). `pairings`, when set, fixes the order of other-family
reviewers ahead of pace but never admits a skipped pool. `select: rank` preserves the quota tool's ranked `rows` order
instead, for quota tools without pools or a usage snapshot. Routine changes need one diff-only pass on the exact PR
head; blocking paths need two. For each pass the reviewer is chosen in this
order: a different model family than the author, preferring `pairings` order and
families not already used on this head; any different family; a different
family again on a profile an earlier pass used; the author's
family with a different model (from `Mergewake-Author-Model` or the configured
author profile's `--model`); the author's family at all; and finally any
admitted profile in a fresh diff-only call. Review never stops merely because
no different-family reviewer has quota; only an empty eligible set fails.
Unknown profiles are skipped. `gate_effort` must match an explicit recommended
effort; it is also passed to the gate. Missing executables and failed gates
fall through. A failed recommendation fails closed. After a head claim, a
reviewer that exits non-zero or returns invalid output falls through to the
next candidate of the same pass, once each and never beyond the candidate
list; nothing is posted from a failed run and there is no output repair. A
failure whose stderr shows a rate limit or exhausted quota (`rate limit`,
`429`, `Too Many Requests`, `quota`, `usage limit`, `reached … limit`) also rules
out sibling profiles on the same free-billed pool for the rest of the review.
For a free-billed Devin SWE-2 profile that reports when its limit lifts
(`reset in 1 hour 2 minutes`, `(at 11:01 UTC)` or an RFC 3339 time), the reset
is recorded through the quota tool, `<quota> observe devin-free-limit --reset
<RFC3339> --observed <RFC3339> --rung swe-2 --source mergewake:<repo>#<pr>`, so
later reviews stop ranking the rung; other pools have no observe path and are
only skipped. Each skipped reviewer and its reason (`quota exhausted` with the
`reset`, `review failed`, `quota gate refused`, `reviewer CLI not installed`)
appears as `skipped` in the review audit event. When every candidate of a pass
fails the review ends with `review unavailable: no reviewer produced pass N
(<reviewer>: <reason>; …)` and an audit `review-skipped` record; a failed pass
retains its lease until the configured review timeout plus one minute, after
which it can be reclaimed for a fresh call. All candidates share the review
timeout; once it expires no further reviewer is tried. Dry-run and shadow may
read recommendations but never invoke a reviewer, gate, claim or post a review.

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
state/migration/security/deploy lens for pass 2, including after quota fallbacks.
Landing counts distinct pass numbers and requires distinct lenses when the
reviewer family is the same. Routine PRs use the default lens.
Every pass prefers a reviewer family other than the PR author's. When quota
admits only one other-family profile, pass 2 runs it again (a fresh call with
the pass 2 lens) before falling back to the author's family; the author's
family is used only when no usable other-family candidate remains (quota
refusal, an unavailable CLI or a failed invocation all count).

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
  When the previous patch cannot be read, the round reviews the whole diff and
  the audit event says why (`round_note`). When nothing changed at all, the round
  does not count, reviews the whole diff for reference only, and reports only a
  new critical or high finding.
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
  same or higher level), else the same section in the body of the task named by
  the `Mergewake-Task` trailer when the sink can read task text (`tasks show`
  `body`, else `description`), is capped at 4 KB and passed to the reviewer as
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

Round, `delta_base`, `quiet`, `prior_context`, `advisory` and the drop counts
are on the `review` audit event.

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
Reviewer processes run in their own process group. On timeout, cancellation or
mergewake's own SIGINT/SIGTERM/SIGHUP the whole group receives SIGTERM, then
SIGKILL from a timer after a 5s grace, independent of any read still in progress; after every exit, including success, the group is
killed and reaped, so a child the CLI forked cannot outlive the call or hold the
review open through an inherited stdout. On Linux the leader also gets
`PDEATHSIG=SIGKILL` for a parent killed by SIGKILL; descendants that leave the
process group (`setsid`) are outside this guarantee. A timeout is reported as
`<cli> reviewer: env timed out; process group terminated`. The temporary
directory (including private homes such as Kimi's) is removed on return, and
each review removes this user's `mergewake-review-*` directories older than
twice `review.timeout`, which only a SIGKILLed review leaves behind.
Only adapter-owned control files may be present (Command Code's deny hook).
GitHub credentials are removed from the review process environment. Claude uses
safe/restricted print mode, no built-in tools, no MCP and no settings discovery;
pi disables tools, MCP and context discovery; Command Code uses safe mode, deny
permissions and a PreToolUse deny hook. Devin print mode receives a private deny
configuration disabling file, shell, MCP, imports and subagents as far as its CLI
supports. Grok uses an empty tool list, MCP deny, no web and no subagents. agy print mode
takes its prompt only as an argv value, so the diff is sent as one stream-json
user message on stdin (`--print= --input-format stream-json --output-format
stream-json`); it runs with `--sandbox`, slash commands disabled, and
session-only settings denying shell, file, URL and MCP tools and non-workspace
access, on top of headless agy denying any tool permission it would prompt for.
Any tool step in its event stream, a result other than one successful turn, or
an empty response fails the pass. agy keeps its own conversation history; no
flag disables it. Codex
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
| `fallback_lane` | `lane` | Lane for PRs whose task trailer resolves to no lane and which carry no `Mergewake-Notify-Lane` trailer; also retried once when delivery to the resolved lane fails. |

The panewire adapter executes literal arguments equivalent to:

```sh
panewire emit --kind lane.event --lane operator-desk --event-id EVENT_ID --text TEXT
```

An event may carry its own lane (the `daemon.pr.notify` routing rules above);
an explicit event lane overrides `notifier.lane` for that emit only.

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

### `discord`

Posts the events of every watched repo to Discord from the daemon, so no repo
needs its own workflow or webhook secret. Only `mergewake daemon` and
`mergewake notify deploy` post; other commands do not.

```yaml
discord:
  enabled: true
  webhooks_file: ~/.config/mergewake/discord-webhooks.env
  kinds:
    pr: DISCORD_WEBHOOK_PR_LOG
    ci: DISCORD_WEBHOOK_GITHUB_ACTION
    deploy: DISCORD_WEBHOOK_DEPLOY_LOG
    revert: DISCORD_WEBHOOK_DEPLOY_LOG
  opt_out: [example/keeps-its-own-workflows]
```

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Turns the feed on. |
| `webhooks_file` | Required when enabled | Absolute or `~/` path to a mode-0600 env file of `KEY=URL` lines. |
| `kinds` | Required when enabled | Maps an event kind to a key in that file. A kind left out posts nothing. Keys are `pr`, `ci`, `deploy` and `revert`. |
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
| `deploy` | Release published, or publishing failed | `release` role | commit, result |
| `deploy` | Rollout finished or failed | `rollout` role | commit, result |
| `deploy` | Another deployer's report | `mergewake notify deploy` | commit, result |
| `revert` | Revert PR opened, culprit reported, or watch hold | `watch` role | head, action |

Each event posts one message, and restarts stay quiet. Some consequences:

- A force push is not a new PR, so it does not post again. A new red head does.
- `ci` posts the checks that have failed when the head is first seen red. A
  sibling check that fails later on the same head does not post again.
- A failed deploy that later succeeds on the same commit posts both. A failure
  repeated every pass posts once.
- A PR older than the repo's baseline does not post, so turning the feed on does
  not replay history. A merged or closed PR also has to be under 72 hours old. A
  `ci` failure has no such limit: heads already red when the feed turns on post
  once. So does the head the `release` role publishes next.
- PRs left out by `daemon.pr.skip_bots` stay out of the feed.
- A reopened PR does not post.

A kind only posts while its role runs: enable `daemon.watch` for default-branch
`ci` and for `revert`. Shadow mode still posts PR and `ci` events, because
reporting is not acting. It posts no `deploy` or `revert`, because shadow mode
deploys nothing and opens no revert. `--dry-run` posts nothing and writes no feed
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

#### `mergewake notify deploy`

Deployers the daemon does not run report through this command. It needs no GitHub
token, and the repo does not have to be in `repos`.

```sh
mergewake notify deploy --repo owner/name --sha "$SHA" --status ok|failed \
  [--text "pull deploy finished"] [--config PATH]
```

| Flag | Meaning |
| --- | --- |
| `--repo` | `owner/name` that was deployed. |
| `--sha` | Deployed commit, 7 to 64 lowercase hex characters. |
| `--status` | `ok` or `failed`. |
| `--text` | Optional message text. Credential-shaped substrings are redacted. |

It posts before it returns and prints one JSON line. `"posted": false` with exit
status 0 means the repo is opted out, `deploy` has no channel, or this repo,
commit and status was already posted from this host, so a retrying deployer posts
once. It exits non-zero if `discord.enabled` is off or the post fails, and a later
call can then retry. One call takes at most 2 minutes, so a deployer should not
fail a deploy on it:

```sh
mergewake notify deploy --repo owner/name --sha "$SHA" --status ok || true
```

## Per-repo policy

| Key | Required / default | Meaning |
| --- | --- | --- |
| `version` | Required: `1` | Policy schema version. |
| `enabled` | `false` | Explicit opt-in for acting `land` (auto-merge on a READY verdict) and `watch`. A caller can force shadow with `--shadow`; there is no CLI bypass of `false`. |
| `auto_update_branch` | `true` | While acting, update a reviewed PR branch when `base_stale` is its only blocker; set `false` to notify instead. `base_stale` applies only when the base moved ahead of the head and GitHub reports the PR `behind` (branch protection requires an up-to-date branch). Otherwise a conflict-free PR with a moved base is not blocked, its branch is not updated, and the land and notify records carry `base_behind: true`. Each PR/base SHA is attempted once, except an ambiguous outcome may be retried after a fresh gate check. |
| `required_checks` | Required, nonempty | Exact, unique GitHub check-run names or commit-status contexts. Every required result must be successful. |
| `blocking` | Empty list | Rules identifying changes whose harm may happen before a revert. Routine is the default for other changes. A change to this policy file itself always counts as blocking. |
| `review_gate` | Absent | Review-gate settings. With `land: graded` the land gate grades review findings instead of counting unresolved threads; see [Graded review findings](#graded-review-findings). Without `land: graded` the section does not change the land gate (raw count). |
| `fast_lane` | Absent | `{max_changed_lines: N}`: a PR that touches no blocking path and changes at most `N` lines (additions plus deletions) is held only by block-class findings. See [Graded review findings](#graded-review-findings). |

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
| triage | it has a disposition: someone other than the finding's author (and not a bot) replies on the thread with a fix SHA, a dismissal with its reason or a follow-up task reference, or the thread is resolved. Neither needs a new head. | `triage_unanswered:<n>` |
| advisory | never holds and is not counted as unanswered. Threads from authors no rule names are advisory. | none |

A thread answered with a disposition counts as answered whether or not it is
resolved. A dismissed critical/high no longer holds the gate; the land record
counts it as `dismissed_blocking`, and the READY event says
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
with a reason, never deferred), codex `P2` as triage, CodeRabbit
`minor`/`nitpick` and codex `P3` as advisory, and add one rule for the configured `review.login`:
`critical`/`high` block (the reviewer prompt's `blocking` grades like `high`).
Its `medium`/`low` findings (and `major`/`minor`, graded like them) are
advisory: the author still gives each a disposition (fix, dismiss with a
reason, or defer to a task), but they never hold the gate and are not counted
in `triage_unanswered`. Only critical and high findings need a fix. Rules are
`{author, severity: [...], classes: [...]}` lists under `block` and `triage`;
an explicit list replaces the default for those two keys. Naming the
`review.login` in `block` replaces its default block rule; naming it in
`triage` makes those of its severities triage again.
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
- `base_stale` never asks for a review. A stale head keeps the review it has;
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
are also read from the head commit message when absent from the body.

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

## Fleet events and crash reports

Every reviewer that is skipped or fails in a review pass is recorded with its
cause. The skip entries in the `review` and `review-skipped` decisions
(`<state_dir>/<repo>/decisions.jsonl`) carry:

- `reviewer` and `reason`;
- `host`, which is the configured host for headless reviewers, or the
  `review.panewire.host` value for panewire panes;
- `profile`, the launch profile the reviewer ran as: the profile on the pane's
  hk job record for a spawned panewire reviewer, or the configured reviewer
  name for a headless one. It is omitted when no profile was chosen yet (a
  skip before any spawn, or a spawn that failed before its record was read);
- for a failed process, `exit_code` and `stderr_tail`, the last 512 bytes of
  stderr after redaction;
- otherwise `cause`, the bounded error text.

The same facts are appended as fleet-event v1 lines to
`<state_dir>/events/fleet-events.jsonl`. The file rotates to `.1` at 4 MiB, and
every field is redacted and bounded. Grafana Alloy ships these lines to Loki.

| `kind` | `outcome` | `reason` | `detail` |
| --- | --- | --- | --- |
| `reviewer` | `failed` (exit, stderr, `review failed`, `quota exhausted`) or `skipped` | the skip reason as a code (`review_failed`, `quota_gate_refused`, `panewire_spawn_exit`, ...) | `pass= reviewer= profile= host= exit= reason="..."` then `stderr:` or `cause:` (`profile=` is empty when no profile was chosen yet) |
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
| `decisions.jsonl` | Append-only observations/intent and completed recovery/rollout records. A planned action is not proof it completed. |
| `reap.jsonl` | Append-only reap receipts: pane closes, worktree removals, branch deletions, skips and `would-*` plans, each with repo, PR, host and the saved report/screen path and job name; job-bound rows add `task` (and `open` for `job_has_open_pr`), and `job-done` rows record `panewire done`; `archive-job` rows record job-dir archives; `check-pr` rows stamp refetch attempts and `files` names refusing ignored paths. |
| `../archive-jobs.jsonl` | `<state_dir>/archive-jobs.jsonl`, host-wide: one `archive-job` row per job dir `archive-jobs` looked at — archived (with the Drive directory, hk ids and excluded names), planned or skipped with its reason. |
| `last-green` | Last observed green main SHA; validated again during diagnosis. |
| `watch-reported` | Most recent 128 hold/report keys already notified; repeat polls stay quiet. |
| `review-wait.json` | When this host first saw each ready head waiting for a review (the stall alarm's clock). |
| `review-stall-sent` | Most recent 256 `PR head` keys already alarmed as stalled. |
| `pr-notify-sent` | Most recent 256 `verdict PR head [reasons]` keys already lane-notified by the pr role, plus `decided` keys for verdicts parked after a failed delivery; a new head re-arms notification and restarts stay quiet. |
| `pr-ready-renotify.json` | Per READY key: lane, first and last send, reminders sent; kept a week. Deleting it restarts the reminder clocks. |
| `findings-deliveries.json` | Findings deliveries of the last 30 days: event id, PR, head, thread ids, lane, owner, sent/re-sent/escalated/acked times. Deleting it re-delivers every open unanswered finding once. |
| `discord-sent` | Most recent 2048 Discord feed keys posted or dropped for this repo. Deleting it can post recent events again. |
| `discord-baseline` | When the feed first ran for this repo. Older PRs do not post. |
| `../discord/outbox/` | `<state_dir>/discord/outbox/`, host-wide: messages waiting to post, one JSON file each. They hold no webhook URL. |
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
`mergewake/reviews/<PR>/<full-head-SHA>` (plus `-p2` for a second pass) and
GitHub reviews first. Only delete a claim if no reviewer remains active and no
complete review was posted; then a rerun can attempt that head. The
`mergewake/reviews/<PR>/<full-head-SHA>-done` marker is created only when every required pass is
posted; removing it makes the daemon re-check the head, while retained pass
claims still prevent duplicate model invocation. Completed claims can remain as
evidence.

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
  pr: {enabled: true, interval: 60s, notify: {enabled: true, renotify_after: 30m, renotify_max: 3}, findings: {enabled: true, ack_after: 15m}, stall_after: 30m}
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

Each daemon pass logs `github requests` with its total and busiest routes. The
role's status file also records counts by method, route and response status.
An HTTP status of `0` counts a request that received no response.

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
roles woken — never the body, the signature or other headers.

| `X-GitHub-Event` | Roles woken |
| --- | --- |
| `pull_request`, `pull_request_review`, `pull_request_review_thread`, `pull_request_review_comment`, `check_suite`, `check_run`, `status` | `pr`, `watch` |
| `release`, `workflow_run`, `push` | `release`, `rollout` |
| `ping`, any other event | none |

Only enabled roles wake, and only for repositories in `repos`.

`daemon.pr.notify` defaults to enabled and accepts `enabled`, `renotify_after`
and `renotify_max`. While the
`pr` role runs, each poll runs the merge gate for every open, non-draft PR at
its current head and emits one lane event for that head's verdict — the notify
path observes rather than merges, so it also runs in shadow mode. `READY`
requires every required check green on the head, zero unresolved review
threads, the `mergewake/reviews/<pr>/<head>-done` marker and the passes from
`review.login` it attests (on the head, or on a head with the same patch id), the base branch an ancestor of the
head, and a mergeable PR; `BLOCKED` names the hold from a
closed vocabulary: `check_failed:<name>` per failing required check,
`threads_open:<n>` (or, under a graded policy, `blocking_findings:<n>` and
`triage_unanswered:<n>`), `review_missing`, `base_stale` (only when GitHub requires an
up-to-date branch), `conflict`. Pending or
missing check evidence emits nothing. A READY verdict under acting mode with
`enabled` repository policy merges the gated head and emits `MERGED`. Each `(PR, head,
verdict)` emits once per the `pr-notify-sent` state file; a changed
BLOCKED reason set is a new verdict that emits again for the same head, a new
head re-arms notification, and a restart does not resend. READY, BLOCKED, and MERGED are
declared with `panewire emit --type ready|blocked|merged`, so they stay unread on the
receiving lane until it runs `panewire inbox ack` (panewire
`docs/quiet-delivery.md` §Must-ack types; a panewire too old for a type gets the
event untyped). A READY PR that stays open and READY on the same head is not
deduped forever: it is notified again `renotify_after` (default `30m`, `off`
disables) after the last send, at most `renotify_max` times (default 3), with
`reminder <k>/<max>, unmerged for <d>` after `via` and event id
`…-reminder-<k>`. The clock is `<state_dir>/<repo>/pr-ready-renotify.json`; a
READY sent before it existed starts its clock on the next poll. A merged or
closed PR gets no reminder. The event
lane is resolved in order — the choosing rule is recorded in the event text
after `via`: a `Mergewake-Task` trailer read through the handoffkeep sink
(`tasks show` task lane), a `Mergewake-Notify-Lane` trailer, then the
configured `notifier.lane`. Both trailers are read from the PR body first and
then the head commit message; a task-read error retries rather than rerouting.
Set `notify: {enabled: false}` to silence the events; the key is rejected on
other roles.

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
jobs keep their recorded author lane after completion. An explicit
`Mergewake-Notify-Lane` trailer also identifies the author. Without either
binding, the event goes to `notifier.fallback_lane`, or `notifier.lane`, as a
`decision`. A task or job read error retries without rerouting.

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

Deliveries persist in `<state_dir>/<repo>/findings-deliveries.json`. READY,
BLOCKED, and MERGED declare `ready`, `blocked`, and `merged`. Watch holds declare
`watch-hold`; culprit recovery declares `culprit-hold`. Deployment decisions
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
`3` failures. A new head resets the counter. At the threshold, one
`automation-failed` event interrupts `notifier.lane`; later attempts keep the
bounded backoff. These counters are per host. GitHub review leases still
prevent simultaneous claims of a pass across hosts.

The daemon uses retry failures instead of the elapsed-time-only STALLED alarm.
`stall_after` remains readable for compatibility and no longer controls daemon
escalation. Shadow and dry-run modes do not retry reviews or write retry state.
`daemon.pr.skip_bots: true` excludes bot-authored PRs from review, notification,
and landing. They still count as open for outcome reconciliation.

`daemon.pr.review_parallel` bounds how many PRs of one repository a PR pass
reviews at once; the default is `4`, and `1` reviews one PR at a time. Each PR's
review is followed by its findings delivery, notification and land; those
settle steps run one PR at a time. Repositories still run one after another.

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

`policy.any_ref_workflow` optionally names a workflow file in `.github/workflows`
that runs the watch checks for any commit: it has a `workflow_dispatch` trigger
with input `ref` and posts the watch checks as commit statuses on that sha (for
example `any_ref_workflow: ci-any-ref.yml`). When a red main head's bisect needs
a commit that has no green or red result, an acting watch dispatches the workflow
once for that commit on the watched branch, records the dispatch in
`any-ref-dispatch.json` in the repo state dir, and holds with
`waiting for any-ref CI on <sha7>`. A later pass reads the result. The wait never
happens inside a pass, and waiting holds do not notify. A restart never
dispatches the same commit twice for the same red head.

Each red head is one incident; a new red head starts a new one, and a green head
ends it. Per incident, watch dispatches at most `ceil(log2(n)) + 2` times, where
`n` is the number of commits after the green boundary. It waits at most
`policy.any_ref_timeout` (a Go duration; default `45m`) for each commit after its
dispatch. Past either bound, or when GitHub refuses the dispatch (HTTP 403, 404
or 422), watch holds with a reason that names the cause and notifies once per
incident. A refusal is not retried until the next incident. Shadow and
`--dry-run` passes dispatch nothing and name the commit they would dispatch in
`any_ref_dispatch`. Without the key, unknown evidence holds as before. The token
needs permission to dispatch workflows.

A check run and a commit status with the same name combine as follows: a pending,
cancelled or skipped value yields to a green or red one. Two decisive values
that disagree are an error.

The hub reap transport reads composers with `pane.read`: only `empty` permits
closing. `has_text` maps to `composer-text`; unknown, missing, changed/gone panes
and hub errors keep the pane. Codex placeholder text remains nonempty unless the
node can prove it is an accepted placeholder. After closure, `job.done` runs on
the job host with the saved report path; only `done` and `already-done` succeed.
The node-side `pane.close` rechecks the visible composer before and after its
settle interval; text, an unreadable composer or an unknown composer keeps the
pane even if its name, status and cwd did not change after capture.
Grant both scopes before enabling this caller; older hubs fail safely and keep
panes. The operator credential cannot substitute for these service-only ops.


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
