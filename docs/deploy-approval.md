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
Use the handoffkeep sink. The rollout reads the task and records a human-only
request over hk's HTTP API as `svc-mergewake` (`GET /v1/tasks/{id}`,
`POST /v1/tasks/{id}/decision-request`); it runs no hk CLI. This is the
record-only route of `handoffkeep decisions ask` without `--linear`: it opens
no Linear issue and needs no Linear decisions setting in hk. The rollout never
calls `/decision-request/ask`. Without the `svc-mergewake` credential the
rollout holds.
Do not share these tasks: a different open question is held without superseding
it. Polling the same question is idempotent. No response, rejection, expiry,
withdrawal, or an hk error keeps the current version running. Only option A on
an answered human-only request for the exact question permits installation.
The question contains host, repository, previous commit, release commit,
matched paths, and reasons. A newer release requires a different answer.

### Lookup by request id

The rollout looks the answer up by hk request id, not by question text. After
it asks, it records the request id and the question under the deploy task's id
in `<state_dir>/<repo>/rollout/deploy-decision.json` (mode 0600, replaced
atomically), beside the rollout receipts:

```json
{"by_task": {"123": {"question": "Approve deployment ...", "request_id": "dr-123-4", "root": "dr-123-4"}}}
```

`root` is the root hk gives the recorded request: the first revision of its
chain of superseding requests. An entry written before roots were recorded has
none.

The file holds one entry per deploy task, so the tasks of one repository (one
per target host) neither replace nor block each other. A later poll, including
one after a restart, counts the task's current request only when all of these
hold:

- it belongs to the recorded request: its id is the recorded id, it supersedes
  the recorded id, or its hk root is the recorded id or the recorded root, so a
  request superseded any number of times is still followed (the entry then
  moves to the new id);
- the entry's question is the question;
- its question text still equals the question, as an extra check;
- it is human-only.

A request with the same question text but another id is never approved. If it
is open and not expired, the rollout holds with "another deployment decision is
open"; otherwise the rollout asks its own request and records that id.

hk answers an ask that repeats the task's current request (same question and
body) with that request marked duplicate, whatever its status or requester.
Once the task has an entry, a duplicate that does not belong to the recorded
request is a request this rollout did not ask: it is not recorded and not
approved, and the rollout holds with "hk returned an existing request this
rollout did not ask (duplicate dr-...)". A duplicate that belongs to the
recorded request is kept. Before the task has an entry, a duplicate is adopted
only under the one-time rule below.

The file must be absent or valid. A present file must be the map shape above,
with `by_task` an object and a non-empty `question` and `request_id` in every
entry, or the complete single-record shape below. Any other content, such as
`null`, `{}`, `{"by_task": null}`, an empty entry, a wrong type or an unknown
field, is corrupt: the rollout holds with "deploy decision record ... is
corrupt" before it reads hk, asks nothing and leaves the file as it is. A
corrupt file never reopens adoption by question text. Repair it from the
task's request id in hk; do not empty it.

If the ask succeeds but writing the file fails, the poll returns the error and
the new request is not recorded. If the task already has an entry, that request
is then not counted: the rollout holds ("another deployment decision is open"
while it is open, or a duplicate refusal once it is answered) until the request
is superseded or the task gets a different request. Mergewake's own asks carry
no due time, so they do not expire by themselves. This hold is safe; it never
installs.

The rollout's repository lock (`decision-lock` in the rollout state) is held
for the whole poll, so two rollouts never rewrite the file at once.

Compatibility with requests asked before this lookup (one time per task):
while the file is absent, or is a valid map with no entry for a task, the old
rule applies to that task. A human-only request whose question text equals the
question is adopted, and its id and root are recorded. From then on it is
matched by id or root. Once the task has an entry, adoption by question text is
over for that task, for every later question. Do not delete the file or an
entry to approve a request: that reopens the one-time adoption. A file in the
single-record shape `{"task", "question", "request_id"}` (written only by an
unreleased build) with all three fields set is read as a one-entry map and
rewritten in the map shape on the next write.

## Operator rollout

1. Merge and release this mergewake version through the normal desk process.
2. Add `deploy_approval` to each managed repository. Let the policy commit reach
   a deployed version through an approved bootstrap. Until then rollout holds.
3. Create a dedicated hk task for each repository/host, set `deploy_task` in
   the private host inventory, and configure `sink.type: handoffkeep`.
4. Ensure hk serves `POST /v1/tasks/{id}/decision-request` and that the host
   has the `svc-mergewake` hk credential.
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
the same commit); a repository without one uses its main set, `watch_checks`,
else `required_checks`. `ci-ok` alone does
not release a deploy-stage repository, and the manifest records the deploy
observation it was published on. Approval never overrides a failing release CI
check.

Rollout also skips a release on a commit listed in `state/<repo>/rollout-deny`,
and every commit after a listed culprit while the branch head is not green, so an
intermediate commit between a culprit and a red head is never installed.
