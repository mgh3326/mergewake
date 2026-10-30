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
| `ssh` | Empty / `local` | Empty or `local` runs commands on this machine. Any other value is a single `ssh(1)` destination such as `ops@node.example`; options, whitespace and leading `-` are refused. |
| `jobs` | Empty | Base directory holding one job directory per named pane (`<jobs>/<pane name>`). Needed before any pane on this host can be closed: a pane is skipped with `jobs-dir-unset` without it. Leading `~/` expands for local hosts only; ssh hosts must give an absolute path because `~` does not expand through the quoting used. |

### `reap`

| Key | Required / default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Gates the `reap` command and daemon role. |
| `act` | `false` | Reap's own acting opt-in, separate from `daemon.act`. Without it every mode — including `--apply` — stays observational. Requires `enabled`. |
| `worktree_after` | `168h` | Earliest age of a merge before its worktree can be removed. |
| `scan_after` | `720h` | Merged-PR listing window: only PRs merged within this span are discovered each pass. Older work still arrives via prior `reap.jsonl` rows. |
| `protect` | `*-integrator`, `director-*`, `fleet-tooling-*`, `consult-*`, `cmd` | Pane-name globs that are never closed. A non-empty configured list replaces the defaults. Unnamed panes and panes outside a linked worktree are always protected. |
| `archive` | Disabled | Job-dir archiving to a synced Google Drive folder plus hk; see `reap.archive` below. |
| `regenerable` | `.venv`, `node_modules`, `__pycache__`, `.pytest_cache`, `dist`, `build`, `target`, `obj`, `bin`, `Library`, `Temp`, `Logs`, `UserSettings` | Gitignored path globs that are expendable (build/cache output), so a worktree holding only them may still be removed. A pattern without `/` matches only the ignored entry's last segment — an ignored file inside a tracked `bin/` or `Logs/` dir is not regenerable just because an ancestor shares the name; a pattern with `/` matches the whole path. Any other ignored path — `.env`, notes, unknown output — refuses removal. A non-empty configured list replaces the defaults. |

`mergewake reap --repo owner/name [--pr N] [--dry-run|--apply]` and the `reap`
daemon role close merged-PR sessions, then clean their worktrees once they
age out. For each merged PR on a configured repo's base branch, every host is
scanned with `herdr agent list` (remote hosts over `ssh <ssh> herdr agent
list`). A pane matches only when its `cwd` is a linked git worktree, its
current branch equals the PR head branch, its checked-out commit equals the
PR head SHA, and the worktree's origin remote names the configured
repository. Titles and names are never used for matching; a reused branch
whose tip moved on does not match.

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

A runtug job dir (`<jobs>/<job>` on a `hosts[]` entry) is archived as one
unit, never its worktree — that is in git and is cleaned by the worktree
rules above. Archiving runs these steps in order, and any failure keeps the
local job dir and reports the step:

1. **Plan** (read-only): a job dir that is itself a symlink is refused as
   `job-dir-symlink` (checked again right before the delete); otherwise
   list every non-directory entry. Credential-like
   paths — any path segment matching `.env*`, `*token*`, `*.pem` or `*key*`,
   case-insensitively — are excluded from Drive and hk and named in the
   record. Symlinks and special files are not copied (following a link could
   archive a secret from outside the job dir). The hk task id comes from the
   job's runtug events (`events/*-job.task_link.json`, falling back to
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
| `timeout` | `10m` | Bounds recommendation, quota gates, model invocation and submission. |
| `quota` | `scopefuel` | Executable implementing `--recommend GRADE --json`, `--json` (usage snapshot) and `gate -m PROFILE`. Set a compatible pacelever path when installed. |
| `scopefuel` | Empty | Legacy alias used when `quota` is omitted. |
| `default_grade` | `B` | Task grade when the PR has no `Mergewake-Grade: A` body trailer. |
| `default_family` | Empty | Reviewer family used when the PR has no author-family trailer and the head commit's Co-Authored-By/author identities hold no known model alias. Must name a configured profile family; reviewed heads record the inference. |
| `select` | `pace` | `pace` orders admitted reviewers by projected use at each pool's reset; `rank` keeps the quota tool's recommendation order and never reads usage. |
| `pairings` | Empty map | Optional author-family → ordered profile override, intersected with eligible recommendations. Never bypasses subscription, exclusion, or quota gates. |
| `profiles` | Empty map | Operator-owned profile → family, CLI and model selection data. Families describe the underlying model, independently of CLI or quota pool. |
| `executor` | `headless` | `headless` runs each pass with a profile's CLI on the diff on this host; `runtug` spawns a visible reviewer pane through runtug and keeps headless as the fallback. |
| `runtug.command` | `runtug` | runtug executable. |
| `runtug.workspace` | `reviews` | herdr workspace (`-w`) for reviewer panes. |
| `runtug.owner` | Empty | Owner lane (`--owner`); empty inherits the author job's owner. |
| `runtug.host` | `auto` | `--host`; anything but `auto` is passed with a recorded override reason. |
| `runtug.jobs` | `~/work/herdr-inbox/jobs` | runtug's jobs root; the pane's brief, findings and report live in `<jobs>/<label>/`. |
| `runtug.checkouts` | `<state_dir>/review-checkouts` | Parent of the detached PR-head worktrees created from the repo's `root`. |

#### Runtug executor

With `executor: runtug` each missing pass is first offered to a reviewer pane:

1. The PR's `Mergewake-Task` trailer (PR body, then head commit) is read
   through the handoffkeep sink; the task's `refs.job_id` is the author's
   runtug job and its title marker (`(grade A, T2)`) gives `--t`.
2. The pass claim is taken, then `git -C <root> fetch origin pull/<n>/head
   <base>` and `git worktree add --detach` create a checkout of the exact head
   under `runtug.checkouts`.
3. mergewake writes a brief naming the PR, head SHA, base, lens, the author
   family, the output contract and the same PR context (answered findings,
   operator decisions) the headless prompt carries, then runs `runtug spawn -c <checkout> -m auto
   -p <brief> -l mwr-<pr>-<sha7>-p<pass>-<unix> --t <T> --parent-job <author job>
   --role reviewer --report <jobs>/<label>/report.md --wait --wait-timeout <rest
   of review.timeout>`. runtug picks host and model and, for the reviewer
   role, excludes the author job's family. Exit 77 (no admitted seat outside
   that family) retries once with `--role worker`, which may pick the author's
   family; the receipt records that as `fallback`.
4. The pane writes `{"findings":[…]}` to `<jobs>/<label>/findings.json` and
   runs `runtug done`. The file goes through the same validation as headless
   output (new-side diff lines, severities, at most 50) and is posted with the
   same receipt, claim and done marker. The receipt adds `"executor":"runtug"`,
   the reviewer is `runtug:<seat>`, and the family is the one runtug recorded
   on the job's `job.spawned` event (or the profile whose gate profile is the
   seat); an unknown family counts as same-family. The checkout is removed
   after the wait.

No task trailer, no bound job, no repo `root`, a failed checkout, a runtug
refusal or wait failure, a missing findings file or rejected findings all fall
back to the headless executor for that pass under the same claim; its receipt
records `"fallback":"runtug: <reason>"`. All executors share
`review.timeout`, so a pane that used the whole deadline leaves nothing for
the fallback. In runtug mode a failing quota tool is not fatal: it only
leaves the headless fallback without candidates. Passes of a blocking PR are
separate panes; runtug enforces only the author-family exclusion between them.
The findings file is read from `runtug.jobs` on the host running mergewake,
so a pane placed on another host must share that jobs directory (or pin
`runtug.host`).

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
that head. An interrupted pass retains a timestamped empty-commit lease. After
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

## Per-repo policy

| Key | Required / default | Meaning |
| --- | --- | --- |
| `version` | Required: `1` | Policy schema version. |
| `enabled` | `false` | Explicit opt-in for acting `land` (auto-merge on a READY verdict) and `watch`. A caller can force shadow with `--shadow`; there is no CLI bypass of `false`. |
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
count of unresolved review threads (`threads_open:<n>`), plus any review.login
finding resolved without a new head. A `review_gate` section alone changes
nothing, so a policy that already carries one for other reasons keeps its
behaviour. With `review_gate: {land: graded}` (all other keys optional) the gate
grades each thread by the author and severity of its first comment instead:

| Grade | Holds the merge until | Reason |
| --- | --- | --- |
| block | the finding is fixed: its thread is resolved by a new head. A reply never clears it, and a finding posted on the current head stays held when resolved without a new head, whoever wrote it. | `blocking_findings:<n>` |
| triage | someone other than the finding's author (and not a bot) replies on the thread. An answered triage finding needs no new head, and a resolved one no longer holds. | `triage_unanswered:<n>` |
| advisory | never holds. Threads from authors no rule names are advisory. | none |

Each reviewer whose latest review still requests changes adds one to the
blocking count (at least one when GitHub's overall decision is changes
requested), on top of any unresolved block-class threads. An unresolved
thread with no comment to grade counts as a blocking finding. Severities come from the `<!-- mergewake-severity:... -->` marker on
mergewake's own findings and from the prose labels CodeRabbit (`critical`,
`major`, `minor`, `nitpick`) and codex (`P1`–`P3`) use. Evidence the rules cannot
read still needs an answer: a finding by the `review.login` identity with a
missing or unknown severity, and on a blocking-class PR an unknown severity from
a graded author.

The defaults grade CodeRabbit `critical` and codex `P1` as block, CodeRabbit
`major` and codex `P2` as triage, and add rules for the configured
`review.login`: `critical`/`high` block, `medium` triage, `low` advisory (the
reviewer prompt's `blocking`, `major` and `minor` grade like `high`, `medium`
and `low`). Rules are `{author, severity: [...], classes: [...]}` lists under
`block` and `triage`; an explicit list replaces the default for those two keys,
though the `review.login` defaults are kept or replaced per list: naming the login
in `block` replaces its default block rule, naming it in `triage` replaces its
default triage rule, and the other list keeps its default.
Thread comments are read in full (paged past 50), since a late reply answers a
finding. `ignore_authors` skips threads from the named bots. `ignore_outdated` (default
true) clears an outdated finding by anyone but `review.login`: a block finding
once its reviewer has reviewed the current head, a triage finding when it is a
`major`. The other `review_gate` keys stay parseable but do not affect the land
gate.

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
task trailer cannot be resolved to a lane; the configured fallback lane
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

## Local state and operator recovery

For `example/project`, state resides beneath
`<state_dir>/example/project/`:

| Path | Meaning |
| --- | --- |
| `decisions.jsonl` | Append-only observations/intent and completed recovery/rollout records. A planned action is not proof it completed. |
| `reap.jsonl` | Append-only reap receipts: pane closes, worktree removals, branch deletions, skips and `would-*` plans, each with repo, PR, host and the saved report/screen path and job name; `archive-job` rows record job-dir archives; `check-pr` rows stamp refetch attempts and `files` names refusing ignored paths. |
| `../archive-jobs.jsonl` | `<state_dir>/archive-jobs.jsonl`, host-wide: one `archive-job` row per job dir `archive-jobs` looked at — archived (with the Drive directory, hk ids and excluded names), planned or skipped with its reason. |
| `last-green` | Last observed green main SHA; validated again during diagnosis. |
| `watch-reported` | Most recent 128 hold/report keys already notified; repeat polls stay quiet. |
| `pr-notify-sent` | Most recent 256 `verdict PR head [reasons]` keys already lane-notified by the pr role, plus `decided` keys for verdicts parked after a failed delivery; a new head re-arms notification and restarts stay quiet. |
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
  pr: {enabled: true, interval: 60s, notify: {enabled: true}}
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

`daemon.pr.notify` defaults to enabled and accepts only `enabled`. While the
`pr` role runs, each poll runs the merge gate for every open, non-draft PR at
its current head and emits one lane event for that head's verdict — the notify
path observes rather than merges, so it also runs in shadow mode. `READY`
requires every required check green on the head, zero unresolved review
threads, the `mergewake/reviews/<pr>/<head>-done` marker and the head-bound
passes from `review.login` it attests, the base branch an ancestor of the
head, and a mergeable PR; `BLOCKED` names the hold from a
closed vocabulary: `check_failed:<name>` per failing required check,
`threads_open:<n>` (or, under a graded policy, `blocking_findings:<n>` and
`triage_unanswered:<n>`), `review_missing`, `base_stale`, `conflict`. Pending or
missing check evidence emits nothing. A READY verdict under acting mode with
`enabled` repository policy merges the gated head and emits `MERGED`. Each `(PR, head,
verdict)` emits at most once per the `pr-notify-sent` state file; a changed
BLOCKED reason set is a new verdict that emits again for the same head, a new
head re-arms notification, and a restart does not resend. The event
lane is resolved in order — the choosing rule is recorded in the event text
after `via`: a `Mergewake-Task` trailer read through the handoffkeep sink
(`tasks show` task lane), a `Mergewake-Notify-Lane` trailer, then the
configured `notifier.lane`. Both trailers are read from the PR body first and
then the head commit message; a task-read error retries rather than rerouting.
Set `notify: {enabled: false}` to silence the events; the key is rejected on
other roles.

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
