# Approval by deployment contents

Every repository, including trading applications, follows the same gate. A
release with no matching paths deploys automatically after its release CI passes.
Each repository configures `deploy_approval` in `.mergewake/policy.yaml`:

```yaml
deploy_approval:
  - paths: [alembic/versions/**, deploy/schedules/**]
    reason: Production schema or scheduled execution changes
```

An explicit `deploy_approval: []` allows routine changes. Missing policy holds.
The gate reads policy at the previously deployed commit; changing the policy
itself always holds. Rename source and destination paths are checked. A missing
receipt, divergent history, unavailable comparison, or comparison reaching
GitHub's 300-file limit also holds. Environment and secret paths always hold.
Host environment and credentials remain operator-managed and are never installed
by automatic rollout.

Configure a dedicated hk task per repository and host with `deploy_task: '123'`.
Use the handoffkeep sink. The rollout asks through `handoffkeep decisions ask`
with `--human-only`, which provides the Linear/Discord mirror when enabled in hk.
Do not share these tasks: a different open question is held without superseding
it. Polling the same question is idempotent. No response, rejection, expiry,
withdrawal, or an hk error keeps the current version running. Only option A on
an answered human-only request for the exact question permits installation.
The question contains host, repository, previous commit, release commit,
matched paths, and reasons. A newer release requires a different answer.

## Operator rollout

1. Merge and release this mergewake version through the normal desk process.
2. Add `deploy_approval` to each managed repository. Let the policy commit reach
   a deployed version through an approved bootstrap. Until then rollout holds.
3. Create a dedicated hk task for each repository/host, set `deploy_task` in
   the private host inventory, and configure `sink.type: handoffkeep`.
4. Ensure hk supports `decisions ask` and its Linear and Discord mirrors.
5. Run `mergewake rollout --help` for inventory selection flags, then a dry run.
   A fresh installation must print `hold` with an unknown previous receipt.
6. Enable rollout polling only after reviewing the proposed inventory, rollback
   SHA/digest, smoke command, and the first bootstrap decision. Answer through hk.
7. Verify a success receipt and hk deploy record. A healthy repeat poll should
   perform no installation and create no new decision.

Rollback: disable the rollout role first. Restore the previous mergewake binary
and private inventory, preserving receipts and decision history. A deployment
rollback uses the installer's recorded previous SHA/digest and smoke checks;
never delete receipts merely to bypass a hold.

The gate also fingerprints host, repository, policy path, deny list, install
recipes (including env-file paths and mounts), smoke command, and selected
manifest/digest. Missing or changed inventory approval holds; a changed payload
at the same commit also holds. It writes the fingerprint only after successful
installation. A `current` result cannot attest changed or legacy unrecorded
configuration: operator bootstrap is required. It never reads env-file contents.
For long path lists, the complete evidence is written to a content-addressed hk
document referenced by the question, preserving hk's notification size limit.

For repositories with different PR and main check names, release/rollout use
`watch_checks` when present, otherwise `required_checks`. This preserves full
main/exact-release CI requirements for applications whose PR gate is selective.
Approval never overrides a failing release CI check.
