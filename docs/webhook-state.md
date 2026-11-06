# Webhook state

With `daemon.webhook.enabled`, the receiver keeps what each verified delivery
carries and the GitHub client reads it before the API. This is step 1 of using
webhook data instead of polling for the same facts. Nothing here adds a config
key, and a repository without the receiver never uses it.

## What is kept

Under `<state_dir>/webhook-state/<owner>/<name>.json`, written atomically every
two seconds when it changed and when the daemon stops. A missing or unreadable
file starts empty: everything in it can be read from the API again.

| Event | Kept |
| --- | --- |
| `pull_request` (any action) | Per open PR: number, state, draft, merged, title, body, URL, author, head and base SHA, ref and repo, labels, merge commit, changed files, additions, deletions, timestamps. No mergeability. |
| `check_run` | Per head SHA, per (check suite, name): the latest run's id, status, conclusion, details URL, app, started and completed time. |
| `check_suite` | Per head SHA: the suite's id, status, conclusion, app, `updated_at`. Kept for the change record that follows; the gate does not read it. |
| `status` | Per head SHA, per context: the latest status's id, state, target URL, `updated_at`. |
| `pull_request_review` | Per PR: review id, author, state, commit id, `submitted_at`, body. A body with a `<!-- mergewake-` marker is kept whole; any other is cut to 1 KiB. |

Every other event, and every other field, is ignored.

**Ordering.** A value is replaced only by a newer one, by the event's own time:
`updated_at` for a PR, a status and a suite; for a check run a higher run id (a
rerun), then the later `started_at` / `completed_at`, then the further status
when a payload has no times; for a status a higher id, then `updated_at`. An older
or equal delivery never overwrites. A review is added once; an `edited` or
`dismissed` delivery replaces it unless it is older. A delivery id that was
applied before is a no-op, and the ids persist.

**Bounds.** 2,000 PRs, 1,024 heads (the one untouched longest goes first), 2,000
runs per head, 500 reviews per PR, 1,024 delivery ids. A PR that closes is
removed with its reviews and its head's checks, and its number is remembered with
the closing time so a straggler cannot bring it back; a newer delivery (a reopen)
can.

**Failures.** A payload that does not parse is logged once per event type
(`webhook state ignored a delivery`) and changes nothing: the response and the
wake are what they were.

## Reads

`Client.Pull`, the per-head check read the gate uses (`CheckStates`,
`CheckSnapshot`) and `ReviewHistory` consult the stored state. Each API read also
seeds it, which is how a head's checks and a PR's reviews become complete: a
delivery alone cannot say it has seen them all.

**Phase 1 (shipped, `github.ServeWebhookState = false`).** The read still goes to
the API and its result is what the gate gets. The stored value is compared with
it, and each differing field logs one line:

```json
{"level":"WARN","msg":"webhook_state","result":"mismatch","repo":"o/r","pr":7,"head":"…","field":"head_sha","stored":"…","api":"…"}
```

`field` is a PR field (`state`, `draft`, `merged`, `head_sha`, `head_ref`,
`base_sha`, `base_ref`, `labels`, `body`, `updated_at`), `check <name>` or
`review <id> <field>`. A stored PR newer than the API's came in during the read
and is not reported. The daemon logs `webhook_state_hits`,
`webhook_state_misses` and `webhook_state_mismatches` at most once a minute.

**Phase 2 (code present, off).** Change `ServeWebhookState` to `true`
in `internal/github/client.go`. A read that is not
fresh is then answered from stored state when the entry is recent and goes to
the API when it is missing. Recent means confirmed by an API read (or, for a PR,
first seen by a delivery) less than 10 minutes ago, which is how stale the
evidence cache already allowed a read to be; a lost delivery can hide a change
for no longer than that. A write this process makes to the repository (a merge,
a review, a comment) withdraws every confirmation until the next API read,
because its own delivery may not have arrived.

Enable it after the mismatch log is clean. Two consequences to read first:

- A PR served from stored state has no `Mergeable` or `MergeableState`. The gate
  reads them (`pr.Mergeable == nil` waits), so under phase 2 a non-fresh read
  holds the PR pending, never merges it. Decide how the gate gets mergeability
  before turning phase 2 on.
- `ReviewedHeads` and the review threads (GraphQL) are not served from it.

## Never from stored state

Reads under `github.FreshEvidence` always go to the API. That is the land-time
head check (`Land`: the PR, its head and the SHA-bound recheck before the merge),
the aggregate `Check`, mergeable state, review threads, and every repository
without the webhook receiver.

## hk change records

Mergewake sends observations to hk's change record for each PR using the same
endpoint and bearer as task events. Applied webhook deliveries supply PR state
and check observations; GitHub reads supply state, checks, and the complete
review-thread set. Posted review passes supply findings, and gate decisions
supply their reasons and the Git blob SHA of the policy read at the base.
Thread excerpts are limited to 2,000 characters and finding bodies to 8 KiB.

Writes are queued by PR and coalesced by section. The writer tries the batch
route and falls back to section routes when batch is unavailable. A rejected
batch is retried as one batch per section. A rejected section logs hk's bounded, redacted reason; a stale
response is counted and does not affect the pipeline. If neither route exists,
the writer logs once and pauses for ten minutes. Network failures and hk
responses do not change a gate verdict, review pass, or webhook response. The
`github requests` log includes `hk_state_writes`, `hk_state_stale`,
`hk_state_rejected`, and `hk_state_unavailable` counters.
