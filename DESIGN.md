# Design

mergewake moves routine coordination into a small deterministic pipeline. A
repository owns the policy describing harm that can happen before a revert; a
host owns what it may install and which local model CLIs it may invoke. Neither
PR text nor a release manifest authorizes a host command.

## From PR open to a healthy fleet

```mermaid
flowchart TD
    A[PR opens or head updates] --> B[Independent diff-only review]
    A --> C[Classify paths and change kinds]
    B --> D[Post one review for the exact head]
    C --> E{Blocking?}
    E -->|yes| F[Green CI and resolved findings]
    D --> F
    E -->|no| G[Green CI]
    F --> H[Shadow decision or GitHub squash merge]
    G --> H
    H --> I[Main CI]
    I -->|red| J[Locate regression and open one revert PR]
    J --> K[Task recovery and notification]
    I -->|green| L[Publish derived release manifest]
    L --> M[Host verifies and installs allowed files]
    M --> N{Smoke and digest checks pass?}
    N -->|yes| O[Receipt and report]
    N -->|no| P[Restore snapshots and reject release]
    O --> Q[Resident component monitors]
```

`daemon` runs one goroutine per enabled role: PR polling feeds review and land,
main CI feeds watch, green-main detection feeds release, and host polling feeds
rollout. Each role polls every configured repo and reports errors independently.
Polling alone completes delivery; no public ingress is required. The daemon
package exposes `Trigger(role)` for optional trusted webhook adapters. Timers
remain active if no adapter is present.

Every role uses positive jitter and exponential backoff (capped at 30 minutes,
or its configured interval when longer). The shared GitHub transport serializes
requests, revalidates cached GET bodies with ETags, observes X-Poll-Interval,
X-RateLimit-Remaining/Reset and Retry-After, and delays subsequent requests.
Mutations are never implicitly retried and invalidate cached representations.

OS advisory locks in the user's cache directory exclude duplicate host roles
even across different state/config directories, and release automatically at
process exit. Operators running multiple users must use a single service account.
Per-role status files are atomically written under state_dir/daemon. Cancellation
stops new polls and aborts API/model/install work. Local command process groups
are killed together; rollback uses a fresh bounded context before shutdown
returns. File and command journals preserve recovery after a hard kill.

Shadow is the daemon default. Acting requires host `daemon.act` and `--apply`;
landing and recovery also obey repository policy. Standalone review, release and
rollout likewise require `--apply`.

## Package boundaries

| Package | Owns |
| --- | --- |
| `config` | Strict YAML, model-option validation, explicit host and repo selection. |
| `policy` | Slash-aware path globs, previous names on renames, change-kind matching. |
| `github` | Bounded HTTP requests and pagination, CI evidence, PR operations, inverse trees, release assets. |
| `daemon` | Independent role scheduling, advisory locks, status, backoff and optional triggers. |
| `pipeline` | Landing, one-pass review, CI diagnosis, release and rollout orchestration. |
| `manifest` | Deterministic inventories, SHA256 and size validation, safe relative paths. |
| `rollout` | Local exclusion, snapshots, durable journals, atomic per-file replacement, container digest swaps, rollback. |
| `integrations` | CLI runner, notifier and task-sink interfaces and adapters. |
| `superfan` | Embedded prompts, allowed read-only sources and allowed actions. |
| `reap` | herdr pane and git worktree inspection, ssh execution, remote-URL parsing, protect globs. |

The command layer handles flags, signals, and polling. Tests replace each external
boundary with a fake or a local HTTP test server. Mutating operations have no
implicit HTTP retries: a timeout can leave a remote action's outcome uncertain.

## Policy and landing

Policy is fetched at the current base-branch commit, never from the PR head or
an untrusted checkout. Globs use `*`, `?`, character classes, and `**` as a complete
path segment; `**` spans zero or more segments. A rule matches any listed path
and any listed kind; omitting kinds means all kinds. Blocking wins over routine.
Renames test both the old and new names. Unknown kinds and empty change lists
classify as blocking. A truncated file list stops landing.

Every required named check must have an explicit successful result at the PR
head. Missing, queued, skipped, neutral, and ambiguous check/status evidence
cannot be green. A routine PR can merge before the advisory review finishes.
A blocking PR needs the trusted independent-review marker for that head, zero
unresolved review threads (including outdated ones), and no outstanding changes
requests. Reviewing a subsequent head is another single pass, not a review round.

Before acting, landing rechecks base policy revision, CI, PR readiness, and
blocking findings. GitHub's merge call includes the expected head SHA and squash
method. GitHub handles concurrent main changes and conflicts. There is no global
merge lock or forced rebase. Review resolution and the merge call cannot be one
GitHub transaction; a new finding can still race the final check. Native branch
rules remain available to a repository that requires stronger enforcement.

## Independent review

The author family is supplied by a trusted caller or the PR's
`Mergewake-Author-Family` trailer. It describes the underlying model, not the CLI
brand. Pairings map that family to ordered profiles from different families;
optional scopefuel gates can deny a candidate. The caller remains responsible
for truthful author metadata and profile-family declarations.

A server-side ref named `mergewake/reviews/<PR>/<head>` claims the pass before
model invocation. It prevents two hosts from spending on the same head. The
reviewer runs in an empty temporary directory, receives only the API diff on
stdin, and cannot request a checkout. pi runs without tools, MCP, extensions,
skills, context files or session history. Command Code uses safe mode, disabled
skills, a one-turn cap, deny-all permissions, and a deny-all tool hook. Profile
arguments are limited to model/provider/thinking/effort option-value pairs.
A configured wrapper can supply provider credentials, but it must preserve the
CLI name and these semantics.

The response must be a bounded JSON findings array. Paths and new-side line
numbers must exist in the supplied diff. mergewake checks that the PR head is
still the same and submits the marker and all inline comments in one review.
There is no second tester, fix round, or automatic retry on invalid model output.
A failed or interrupted review leaves its claim for operator inspection. The
adapter trusts installed CLI behavior; these switches are not an OS sandbox.
Diffs larger than 2 MiB and unsupported output need manual review.

## Main CI recovery

The watcher stores the last observed green head locally. On red main it walks
first-parent history, bounded by `watch_limit`, and checks existing CI evidence.
On first use it discovers a green ancestor. Later green evidence inside an older
checkpoint interval supersedes that checkpoint, avoiding attribution to an older,
recovered failure. The remaining green-to-red interval is bisected using cached
check results. Incomplete evidence stops attribution. The boundary must map to
one merged PR whose squash SHA and base branch agree with GitHub.

Reverting uses GitHub tree and commit APIs. For every changed path, its current
blob and mode must still equal the culprit's postimage. The new tree restores
that path's preimage while preserving unrelated main changes. Later edits on a
changed path, non-regular files, ambiguous PR mappings, or non-linear history
require an operator. This is deliberately more conservative than applying fuzzy
reverse patches. The watcher opens a revert PR; it never auto-merges one.

The fixed `mergewake-recovery-latch` Git tag is created by an exclusive Git-ref
operation before creating the `mergewake/recovery` branch. It is a repository-wide
latch across watcher hosts and survives GitHub's automatic deletion of merged PR
branches. Clearing the tag and any remaining branch is the operator's explicit
authorization for another recovery. A leftover branch also stops recovery even
when its tag is absent. If branch or PR creation fails after the tag exists, the
latch remains. No distributed service or lease system is needed.

After opening a revert, the sink records recovery and attempts to reopen the
origin task, and the notifier posts an event with a deterministic ID. A
`Mergewake-Task` PR trailer selects the task, with a configured repo task as a
fallback. The handoffkeep adapter uses show, comment, then the explicit reopen
operation with a required reason, entering `reopened`. Unsupported older servers
and protected authority refusals return a clear failure after the comment; the
notification includes it and the recovery latch remains set.

## Merged-PR reaping

Agent panes and git worktrees accumulate on session hosts after their PRs
merge. The `reap` role closes the pane that worked on a merged PR, and after
`reap.worktree_after` (default seven days) with no revert it removes the
worktree and local branch.

Panes are matched by evidence, never by title or name alone: `herdr agent
list` runs on each configured `hosts[]` entry (`ssh <ssh>` for remote hosts),
and a pane's `cwd` must inspect as a linked git worktree whose current branch
equals the PR head branch and whose origin remote names the configured
repository. URL and scp-style `git@host:owner/name` remotes both parse; a
canonical clone, subdirectory checkout, detached head or foreign remote never
matches.

A matching pane is closed only when it is not `working`, is named, misses the
`reap.protect` globs, and its output was captured first: an existing
`<jobs>/<name>/report.md` counts, otherwise `herdr pane read` saves the last
screen to `<jobs>/<name>/pane-screen.txt`. The saved path lands in the
receipt. A host without `jobs` configured cannot lose panes.

Worktree removal is additive on top of the age gate: the merge must be older
than `worktree_after`, the watch role's recovery convention must show neither
an open recovery nor one merged within seven days, the worktree must still be
registered, unlocked, clean and unattached, no live pane (from `herdr pane
list`, which also sees plain shells) may use it, and the branch must carry no
unpushed commits — merged into the base or fully pushed. Removal is `git
worktree remove` and `git branch -d`; the code has no force flags to reach
for. Candidates come from live panes and from prior `reap.jsonl` rows so a
worktree survives its pane's earlier close. Anything unknown or ambiguous is
skipped with a reason rather than deleted.

## Manifests and verified installs

A release commit must have green required checks and be an ancestor of the
configured main branch. Sources come from regular Git blobs at that full SHA,
not the local working tree. The manifest is deterministic and contains schema
version, repo, commit, source path, target path, byte length, SHA256, and mode.
Only `0644` and `0755` are supported. Overlapping file targets are rejected.
The tag is `mergewake-<full-SHA>` and the asset is `mergewake-manifest.json`.

Publication reserves the commit tag, creates a draft release, uploads the
manifest, then publishes it. Interrupted drafts can be resumed with identical
content. Existing manifests and tags are never replaced. Manifests are derived
assets; nothing writes hash pins or generated inventories back into Git.

Hosts fetch the newest published mergewake release and confirm the asset's repo,
SHA, tag, current main ancestry, and green checks. A receipt prevents automatic
downgrades or divergent releases. The host's inventory must independently match
each permitted source, target, and mode. Deny patterns override the inventory;
denied files are never fetched, changed, or smoke-installed. No permitted targets
means refusal. Source deletion does not imply target deletion.

Installation has these steps:

1. Canonicalize the configured root and refuse symlinks beneath it.
2. Acquire a host-local per-repo directory lock and recover an unfinished journal.
3. Verify current receipts and real bytes; return quietly when already healthy.
4. Download and verify all permitted bytes before changing any target.
5. Snapshot previous bytes, permissions, existence, and the previous receipt.
6. Persist a rollback journal, then replace each file with a same-directory rename.
7. Run the configured smoke command with a timeout, then verify installed bytes and modes.
8. Persist the receipt, remove the journal, and report the result.

On write, smoke, or post-install verification failure, rollback restores prior
bytes and permissions and removes newly created files. It also restores the old
receipt, including after a crash between receipt creation and journal removal.
A rejection receipt prevents repeatedly trying a release that rolled back. If
rollback fails, evidence remains on disk and installation fails visibly. Dry
runs validate the manifest and permitted plan but do not download all payloads,
check live smoke behavior, lock, or write state.

File replacement is atomic per file, not across the entire inventory. Processes
can observe a mixed version during replacement. No service restarts, migration
rollbacks, or reversal of smoke-command side effects are attempted. Keep smoke
commands read-only and use dedicated roots without competing writers. The local
lock does not serialize two differently configured repos targeting the same root;
operators must give each inventory its own root.

## Failure boundaries and omissions

- Missing policy, incomplete API pagination, malformed data, and uncertain CI
  produce errors rather than permission to act. Resident loops log errors and
  poll again; once commands return a failing exit status.
- A crash leaves a local lock. Operators must inspect the process and journal
  before removing the stale lock. The next acting rollout restores the journal
  before starting another attempt; dry runs never perform recovery.
- Audit records, GitHub operations, task updates, and notifications cannot commit
  together. Reporting errors are returned, but there is no durable outbox or
  cross-host replay. Inspect the retained GitHub latch and local audit after
  partial failures. Unknown HTTP outcomes require inspection, not blind retries.
- The release asset is authenticated through GitHub and checked against Git
  contents and a tag. There is no signature, transparency log, or offline trust
  root; repository writers and host config owners are trusted.
- File manifests use schema 1; command/release manifests use schema 2. Mixed
  file and command inventories in one transaction are refused. Host recipes are
  trusted local code and can have side effects beyond the target label. Only the
  same pinned installer, verify and smoke can establish rollback success; this
  cannot reverse database changes or arbitrary external actions.
- Serial/canary gates trust operator-owned success receipts. The desk must share
  or replicate that directory across hosts; mergewake supplies no fleet transport.
- The v* workflow builds release binaries; no service activation or restart occurs.
  Service units are examples for the desk to install separately.
- Container deploys stop the previous container first, so each target is briefly
  unavailable. There is no blue-green swap, replica management, registry API call,
  or rollback of volume-contents or database changes.
- CI recovery uses recorded checks; any-ref workflow dispatch and flaky-test
  reruns are not implemented. Missing evidence deliberately requires an operator.
- Superfan contracts guide a trusted session runner; the binary neither enforces
  an external agent's capability boundary nor manages its session lifetime.
  There is no autonomous privileged monitor by default.
- All four builds disable CGO. Linux outputs are statically linked; Darwin outputs
  retain macOS system-library dependencies. Fully static Darwin executables are
  not provided.
- No git replacement, source-control server, global merge queue, lease framework,
  multi-round review manager, or integration-source imports are included.

These constraints keep the first release reviewable while providing real API
operations, durable local rollback, and an explicit path from shadow observation
to operator-approved activation.

## Command and release installs

A schema-2 manifest contains commit, repo and logical target/kind entries, plus a
digest for container targets. It never carries executable commands. The host
config independently supplies recipes, verify expectations and deny patterns. For
pure `release` inventories, rollout selects the newest compatible published v*
release and resolves its tag to a commit, then checks main ancestry and CI as
usual. The binary downloader resolves the requested SHA to a release tag and
checks SHA256SUMS and asset size before atomic replacement; it rechecks the tag
after download.

Before mutation every selected target needs an installed receipt or an explicit
initial_sha whose verify command proves the prior installation. The journal
records each previous SHA and recipe fingerprint before each command begins.
On install/verify/smoke failure, cancellation or interrupted-process recovery,
the exact same recipe reinstalls the previous SHA, verifies it and runs smoke.
Failed rollback retains the journal; every attempt writes a rollback receipt and
rejects the failed release. An operator must inspect before retry. Changed recipes
or newly denied rollback targets cause refusal rather than unauthorized execution.

Only after every target's verification and smoke pass are the installed receipt
and per-target gate receipts published. A serial host waits for earlier hosts;
a canary follower waits for the canary's success for this exact repo/commit/target.
Repeated healthy polls reverify commands and can repair missing shared receipts.

## Container installs

The container kind deliberately implements a small subset of cluster scheduling
for single-host services: an immutable image digest, host ports, one env file,
optional volumes, a restart policy and an HTTP health check. It does not model
replicas, services, ingress or storage classes. A schema-2 manifest entry names
the logical target, `kind: container`, and normally the digest; `rollout --digest`
supplies or overrides it for a single target. Tags are never resolved or deployed.

Both containers need the same host ports, so a rolling start is impossible without
a proxy or a second port set. The chosen swap is stop, rename to
`<name>.mergewake-prev`, start the new digest, then health-check. The rename keeps
the previous writable layer, so rollback does not need a pull or a writable-layer
copy. The cost is a short outage per target, reported as normal rollout state.
If the new container is unhealthy, it is removed, the preserved container is
renamed back and started, and its health is checked again before the prior receipt
is restored. Both the journal and the receipt are written before the operations
they authorize, and a crash is replayed from the journal.

After success, images of that repository are collected except the current digest,
the newest `keep` previous digests, and any image referenced by a container. The
receipt's digest history is the retention list, so collection does not depend on
image timestamps. Removal failures are warnings; they do not reverse a healthy
deploy. Adopt records an operator-started container as the first receipt, and
status compares the receipt with the runtime and the health URL. Drift always
refuses a deploy rather than silently replacing an unrecorded container.
