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
Use the handoffkeep sink. The rollout reads the task and asks over hk's HTTP API
as `svc-mergewake` (`GET /v1/tasks/{id}`, `POST /v1/tasks/{id}/decision-request/ask`)
with a human-only request; it runs no hk CLI. The ask route also opens the
request's Linear issue and Discord post, so hk must have Linear decisions
enabled: hk answers `linear_decisions_disabled` otherwise, and the rollout holds.
Without the `svc-mergewake` credential the rollout holds too.
Do not share these tasks: a different open question is held without superseding
it. Polling the same question is idempotent. No response, rejection, expiry,
withdrawal, or an hk error keeps the current version running. Only option A on
an answered human-only request for the exact question permits installation.
The question contains host, repository, previous commit, release commit,
matched paths, and reasons. A newer release requires a different answer.

### Lookup by request id

The rollout looks the answer up by hk request id, not by question text. After
it asks, it writes the request id, the task and the question to
`<state_dir>/<repo>/rollout/deploy-decision.json` (mode 0600, replaced atomically),
beside the rollout receipts. A later poll, including one after a restart,
counts the task's current request only when all of these hold:

- its id is the recorded id, or it supersedes the recorded id (the record then
  moves to the new id);
- the record is for the same task and question;
- its question text still equals the question, as an extra check;
- it is human-only.

A request with the same question text but another id is never approved. If it
is open and not expired, the rollout holds with "another deployment decision is
open"; otherwise the rollout asks its own request and records that id.

Compatibility with requests asked before this lookup (one time): while
`deploy-decision.json` does not exist, the old rule applies. A human-only
request whose question text equals the question is adopted, and its id is
recorded. From then on it is matched by id. Once the file exists, adoption by
question text is over for that repository, for every later question. Do not
delete the file to approve a request: deleting it reopens the one-time adoption.

## Operator rollout

1. Merge and release this mergewake version through the normal desk process.
2. Add `deploy_approval` to each managed repository. Let the policy commit reach
   a deployed version through an approved bootstrap. Until then rollout holds.
3. Create a dedicated hk task for each repository/host, set `deploy_task` in
   the private host inventory, and configure `sink.type: handoffkeep`.
4. Ensure hk serves `POST /v1/tasks/{id}/decision-request/ask` with Linear
   decisions enabled and its Discord mirror, and that the host has the
   `svc-mergewake` hk credential.
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
configuration: operator bootstrap is required. Runtime hashes env files locally without exposing their values; changed or unreadable env identity holds for approval. It never writes those files.
For long path lists, the complete evidence is written to a content-addressed hk
document referenced by the question, preserving hk's notification size limit.

For repositories with a deploy stage, release and rollout read the policy's
`deploy_checks` (for example `deploy-ok`, posted after the main stage is green on
the same commit); a repository without one uses its main set, `main_checks`
(`watch_checks` is its legacy alias), else `required_checks`. `ci-ok` alone does
not release a deploy-stage repository, and the manifest records the deploy
observation it was published on. Approval never overrides a failing release CI
check.

Rollout also skips a release on a commit listed in `state/<repo>/rollout-deny`,
and every commit after a listed culprit while the branch head is not green, so an
intermediate commit between a culprit and a red head is never installed.
