# PR status

Workers used to poll `gh pr view` and `gh pr checks` for PR state, and every
poll spent the shared GitHub budget. The daemon already decides each PR's merge
gate every pass, so it keeps that decision where others can read it, with no
GitHub call.

## Snapshot

Each time the `pr` role's gate decides a PR (the same pass that sends the
READY or BLOCKED notice, so `daemon.pr.notify` must be on), it writes
`<state_dir>/<owner>/<name>/pr-status/<n>.json` atomically. A dry run writes
nothing. A PR that left the open list gets a final snapshot: its last gate
fields under `state` `merged` or `closed`.

| Field | Meaning |
|---|---|
| `repo`, `pr` | The PR. |
| `head` | Newest head known. Equals `gate_head` until a fresh read sees a newer one. |
| `gate_head` | The head the gate decided on. |
| `state` | `ready`, `blocked`, `pending`, then `merged` or `closed`. |
| `reasons` | The gate's closed vocabulary (`review_missing`, `check_failed:<name>`, `check_absent:<name>`, `threads_open:<n>`, ...). |
| `checks` | Each required check and its state: `green`, `red`, `pending`, `cancelled`, `skipped`. |
| `review` | `passes` (pass, lens, family) bound to the head, `done` marker, and `inherited_from` when the passes came from an earlier head with the same patch. |
| `threads` | `blocking` (holds the gate), `other` (triage and advisory), `unresolved` (all unresolved threads; absent when the gate stopped before reading them). |
| `findings` | Each unresolved thread: `id`, `location` (`path:line`), `severity`, `blocking`, `summary` (first line, at most 200 characters), `url`, `author_replied`. At most 50 are listed; `threads.unresolved` is the total. |
| `base_fresh`, `base_behind` | Whether the base is an ancestor of the head (absent while the gate stopped earlier), and whether it is not while GitHub still merges the PR. |
| `observed_at` | When the gate decided. |
| `fresh`, `fresh_at` | The last fresh read and its time; both `null` until one happened. |
| `source` | Always `last_pass` for the gate fields. |

The next gate decision replaces the snapshot, `fresh` and `fresh_at` included.

## HTTP

The status route is served on the webhook listener (`daemon.webhook.listen`,
loopback), so it needs `daemon.webhook.enabled`. It takes no signature and adds
no listener, address or auth. The webhook path itself behaves as before.

```
GET /v1/pr-status?repo=owner/name&pr=N
GET /v1/pr-status?repo=owner/name
GET /v1/pr-status?repo=owner/name&pr=N&fresh=1
```

- With `pr`: the snapshot as JSON. `404` when there is none, `400` for a
  malformed `repo`, `pr` or `fresh`, `405` for methods other than GET and HEAD.
- Without `pr`: `{"repo": ..., "prs": [...]}`, the open PRs' snapshots, newest
  PR first. Merged and closed PRs are left out.
- Responses stay under 64 KiB (the list drops the oldest PRs and sets
  `truncated`) and carry no tokens or secrets.
- Reading never calls GitHub.

### `fresh=1`

One GraphQL query reads the head, the status check rollup, the unresolved
review thread count (first 100) and the mergeable state. The result is written
through to that PR's snapshot:

- `head`, `fresh` and `fresh_at` take the new values; `state`, `reasons`,
  `checks`, `findings` and `observed_at` stay. The gate is never recomputed.
- If the head, the checks (as success, failure or pending) or the unresolved
  thread count differ from the snapshot, the PR is marked for re-evaluation. A
  marker file `<n>.dirty` is written; the next `pr` pass lists marked PRs first
  (so they settle first and, among reviews of equal hk priority, start first;
  a higher hk priority still starts earlier), evaluates them even when the
  unchanged-PR skip would leave them alone, and clears the marker when it
  decides the PR. Through the daemon listener the
  request also drops the repo's cached GitHub evidence and wakes the `pr` role
  (the trigger webhooks use). A later fresh read compares against the stored
  fresh read, so the same change wakes once.
- A failed read still returns the snapshot, with `fresh_error`.

`fresh=1` needs `pr`. It spends one GraphQL request from the daemon's budget.

## CLI

```
mergewake pr-status --repo owner/name --pr N [--fresh] [--json] [--config PATH]
```

Reads the same snapshot from local state: no daemon, no GitHub call, no token.
`--json` prints exactly what the HTTP endpoint returns; without it, a short
summary. `--fresh` makes the same one query (it needs a token) and writes the
same snapshot fields and marker, but there is no daemon to wake, so the marker
alone takes effect on the next pass.
