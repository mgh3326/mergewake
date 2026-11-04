# Promotion and admission

Deploy is the strict gate. A consumer deploys only the newest *certified* main snapshot: the full post-merge suite is green on that exact commit, and that commit's exact image is verified. mergewake owns this with two commands:

- `mergewake promote` certifies one main commit and its image digest, writes an immutable record, and advances a forward-only pointer. One identity runs it.
- `mergewake admit` resolves the pointer to the recorded digest, or refuses. With `--deploy`, it hands that digest to shiftgate.

This slice covers one consumer, brewdial-api on the OCI host (`docs/deploy-inventory.md`, §"Pilot"). Promotion is manual. Automatic advancement, revocation, reconciliation and a status view come later.

## Objects

| Object | Where | Written by | Rule |
|---|---|---|---|
| Suite definition | `internal/promotion/suites.json`, keyed by `owner/name` | mergewake maintainers | Lives in mergewake, not the candidate repo, so a candidate cannot weaken its own certification. Its version and sha256 go into each record. |
| Pointer | `refs/heads/deployable` in the service repo, at a main commit | the promotion App | Forward-only: the new commit is on main's first-parent history and strictly newer than the current one. Updated with `force: false`. Never rewound. |
| Record | Annotated tag `promotion/<seq>` in the service repo. Its target is the certified commit, and its message is the record as canonical JSON. | the promotion App | Create-only. A record without a pointer move authorizes nothing. |

The record holds: schema version, repo, seq, certified SHA, the previous promotion's seq and SHA, the artifact (image, platform, the tag that was resolved once, digest), the suite version and digest, the evidence (per workflow: path, workflow id, run id, run attempt, event, branch, head SHA, and each required job's conclusion), the policy version, the controller identity and generation, and the creation time. It holds no secrets.

## Promote

```
GITHUB_TOKEN=<promotion App token> mergewake promote --repo mgh3326/brewdial --sha <full sha> --controller <app name> [--seed] [--dry-run]
```

1. Take the per-repo lock (`flock` in `--lock-dir`, non-blocking).
2. Read the pointer. `--seed` requires that it does not exist yet; without `--seed` it must exist.
3. Walk main's first parents from its head. The candidate must appear strictly before the current pointer. A late green for an older commit, the current commit, or a commit off first-parent history is refused.
4. Certify: each required workflow, matched by file path, has exactly one push run on main at that SHA. The run and every required job of that attempt are `completed/success`. A missing, pending, skipped, cancelled, neutral, duplicate or partial result refuses.
5. Resolve the image tag that the commit's own build wrote, once. Verify the manifest and config bytes against their digests, the platform, and the revision label against the full SHA.
6. Create `promotion/<max+1>`, then move the pointer, then read both back through `Admit`.

"Newest" is decided by ancestry, never by webhook arrival or CI completion time. Intermediate snapshots that never certified are skipped, not certified retroactively. In the G → A → B → R sequence from the design input, the pointer goes G → R, and A and B are never admitted.

Serialization: GitHub's ref update has no expected-old-SHA parameter, so this is not a compare-and-swap. There is one writer, by construction. The rulesets admit only the promotion App, and the App's credential lives on one executor, which holds the per-repo lock for the whole read-certify-write-verify sequence. A crash after the record and before the pointer move leaves an unused record. The next promotion takes a new seq and chains from the admitted pointer.

## Admit

```
mergewake admit --repo mgh3326/brewdial            # prints the digest, or exits 1 and prints nothing
mergewake admit --repo mgh3326/brewdial --json     # prints the record
mergewake admit --repo mgh3326/brewdial --deploy --service brewdial-api
```

Admission reads over the public git protocol (`git ls-remote` and a tag-object-only fetch into `--cache`). It needs no token and spends no API rate limit. Admission:

1. Reads `refs/heads/deployable`. If the pointer is absent or unreadable, admission refuses.
2. Takes the newest annotated `promotion/<n>` whose peeled target is that commit. If no such record exists, the tag is lightweight, the tag object targets another commit, the JSON is not canonical, or the record names another SHA, repo, seq or image, admission refuses.
3. Prints the recorded digest. Admission never resolves an image tag.

`--deploy` implements steps 1–4 of the inventory's §"Exact consumer change for slice 2":

| shiftgate state for the service | Action | Exit |
|---|---|---|
| status unreadable, not JSON, or the service is missing | refuse | 1 |
| `hold.digest` == admitted digest | do not deploy (`deploy` would clear the hold) | 0 |
| `active.digest` == admitted digest | nothing to do | 0 |
| an operation is pending | wait | 75 |
| otherwise | `sudo -n /usr/local/bin/shiftgate deploy <svc> --digest <d>`; its exit 75 stays 75 | 0, 75, or shiftgate's code |

Manual `shiftgate rollback` stays outside the gate and remains the recovery path. A rollback holds the digest it left, so the next `admit --deploy` reports `held` and does not redeploy it. Promoting a newer commit is the way forward.

Fail-closed: if GitHub, the registry, the evidence, the policy or shiftgate is unavailable, admission holds and no new install happens. A running service keeps running.

## Invariants

| # | Invariant | Input | Transition | Enforcement location | Dependency | Independently observable result |
|---|---|---|---|---|---|---|
| I1 | No uncertified SHA is ever admitted | Candidate SHA; push runs and jobs on main at that SHA; the suite definition | `promote` certify | `Promoter.certify` (`internal/promotion/promote.go`): one push run per required workflow path, each required job exactly once, `completed/success`, bound to the SHA and attempt | GitHub Actions REST API; checked-in `internal/promotion/suites.json` | The record's `evidence` lists run id, attempt and each job's conclusion. `gh api repos/<repo>/actions/runs/<run_id>/attempts/<n>/jobs` shows the same. `TestScenario*` and `TestPromoteRefusesSkippedCancelledNeutralJobs` |
| I2 | The artifact is the commit's own image | Image tag written by the commit's build; manifest and config bytes | `promote` artifact | `Promoter.artifact` plus `OCIRegistry.Inspect`: resolve once, verify the manifest and config bytes against their digests, require the platform and `org.opencontainers.image.revision` == full SHA | Public registry API | The record's `artifact.digest`. `docker buildx imagetools inspect <image>@<digest>` shows the label. `TestPromoteDeny_RevisionLabelMismatch`, `TestOCIRegistryVerifiesBytesAndPicksPlatform` |
| I3 | The pointer never moves backward | Current pointer; main's first-parent history | `promote` forward check, then pointer write | `Promoter.forward`: one walk from main's head must meet the candidate strictly before the pointer. GitHub: `force: false`. Ruleset 2: `non_fast_forward` and `deletion`, no bypass | GitHub refs API; ruleset 2 | `git ls-remote <repo> refs/heads/deployable` only ever advances. Every record's `previous.sha` is an ancestor of its `sha`. `TestScenarioLateGreenOlderSHA_NoBackwardMove` |
| I4 | The pointer moves only to a SHA with a record created first | Record tag; pointer | Create the record tag, then move the pointer, then read both back | `Promoter.Promote` write order; read-back through `Admit` | GitHub git refs and tags API | `promotion/<n>` exists for the pointer's commit. `TestPointerNeverMovesWithoutRecordFirst`, `TestRecordWithoutPointerMoveAuthorizesNothing` |
| I5 | Pointer and record agree | Pointer SHA; record tag target; record JSON | `admit` | `Admit` (`internal/promotion/admit.go`): the tag is annotated, the listed peel and the tag object both target the pointer, the record is canonical JSON, and it names the same SHA, repo, seq and image | git protocol (`ls-remote`, a tag-object fetch) | `mergewake admit --repo <repo> --json` prints the record. `TestAdmitDeny_RecordTagTargetIsNotPointer`, `_RecordForAnotherSHA`, `_TagObjectTargetDiffersFromListedTarget`, `TestGitReaderAdmitsThroughRealGit` |
| I6 | Missing or unreadable anything denies | Any read failing or returning nothing | `admit`, `promote`, `admit --deploy` | Every error path returns a refusal and prints nothing to stdout; no default or fallback value exists in the code | GitHub, registry and shiftgate availability | Exit status non-zero, stdout empty. `TestAdmitDeny_*`, `TestDeny_GitHubUnreachable`, `TestDeploy_DeniesUnreadableStatusOrUnknownService` |
| I7 | The consumer never deploys a held digest | `shiftgate status --json`: `state.hold.digest` | `admit --deploy` | `Deploy` (`internal/promotion/deploy.go`) checks the hold before it calls `shiftgate deploy`, because `deploy` clears a hold | shiftgate status JSON | The pass prints `held` and runs no `deploy`. `TestDeploy_RefusesHeldDigest` |
| I8 | The consumer never uses a tag | The record's digest | `admit`, `admit --deploy` | `Reader` has no tag lookup. `Deploy` calls only `shiftgate status --json` and `shiftgate deploy <svc> --digest <d>` | none | The Dagu step and shiftgate's swap log show `--digest`. `TestAdmitNeverFollowsMutableTags`, `TestDeploy_DeploysAdmittedDigestByDigestOnly` |
| I9 | Only one writer | Promotion App token; per-repo lock | `promote` | Rulesets 1 and 3a: only the App creates or updates `deployable` and creates `promotion/*`. 3b: no one updates or deletes a record. `FileLock` (`flock`, non-blocking) serializes promotions of one repo on the executor | GitHub rulesets; one executor host | A non-App push to `deployable` or `promotion/*` is rejected (runbook step 3) |

## Mapping to the planned MySQL store

The record maps to one row of a future `promotions` table keyed by `(repository, seq)`. That table has a UNIQUE `(repository, sha)` index, and its columns are `sha`, `previous_seq`, `artifact_digest`, `suite_version`, `suite_digest`, `policy_version`, `controller_identity`, `controller_generation` and `created_at`. The evidence goes into a child `promotion_evidence` table, one row per (workflow path, run id, attempt, job, conclusion). The canonical JSON is kept verbatim in a `record_json` column, with its sha256 alongside, so the tag and the row can be compared byte for byte. Seq allocation becomes the store's per-repository `next_sequence` under the coordinator lease, and the controller generation is the lease's owner epoch (`docs/server.md`, §"MySQL 8 schema, claims and migrations"). The store-before-do rule maps directly: insert the row, then create the tag, then move the pointer. While the store is unreachable, "outage stops new authority" applies, and no promotion happens. The git tag stays the deploy-time authority until a later slice makes the store reachable from the consumer. The row is then a mirror, and reconciliation compares the two.

## Rulesets

`deploy/rulesets/` holds the GitHub ruleset specs. The desk applies them; mergewake does not. Bypass applies to a whole ruleset, so each guarantee that must hold with no bypass is in its own ruleset:

| File | Target | Rules | Bypass |
|---|---|---|---|
| `1-deployable-writer.json` | `refs/heads/deployable` | creation, update | the promotion App only |
| `2-deployable-integrity.json` | `refs/heads/deployable` | deletion, non_fast_forward | none |
| `3a-promotion-records-create.json` | `refs/tags/promotion/*` | creation | the promotion App only |
| `3b-promotion-records-immutable.json` | `refs/tags/promotion/*` | update, deletion, non_fast_forward | none |

`actor_id: 0` is a placeholder for the App's id. Substitute it when you apply the spec: `jq --argjson id <app id> '.bypass_actors[].actor_id = $id'`. Worker credentials, the operator's normal token and workflow tokens are not bypass actors.
