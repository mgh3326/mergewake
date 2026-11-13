# PR status

Workers used to poll `gh pr view` and `gh pr checks` for PR state, and every
poll spent the shared GitHub budget. The daemon already decides each PR's merge
gate every pass, so it keeps that decision where others can read it, with no
GitHub call.

## Snapshot

Each time the `pr` role's gate decides a PR (the same pass that records READY or
sends the BLOCKED notice, so `daemon.pr.notify` must be on), it writes
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

## CLI

```
mergewake pr-status --repo owner/name --pr N [--fresh] [--json] [--config PATH]
```

Reads the PR's snapshot from local state: no daemon, no GitHub call, no token.
`--json` prints the snapshot as JSON, with `fresh_error` when a fresh read failed;
without it, a short summary. A PR with no snapshot is an error, and `--fresh`
checks that before it reads GitHub.

### `--fresh`

`--fresh` makes one GraphQL request and needs a token. The request bypasses the
caches and reads:

- the PR's head;
- the status check rollup of that head (`SUCCESS`, `FAILURE`, `PENDING`, `ERROR`, or none);
- the unresolved review threads, the first 100 (`unresolved_threads_capped` is set when there are more);
- GitHub's `mergeable` and `merge_state`.

It writes the result through to that PR's snapshot:

- `head`, `fresh` and `fresh_at` take the new values. `state`, `reasons`,
  `checks`, `threads`, `findings` and `observed_at` stay: the read never
  recomputes the gate, and it never feeds the merge gate.
- If the head, the checks class (success, failure or pending; no checks is its
  own class) or the unresolved thread count differs, and the PR is still open,
  the command writes the marker `<n>.dirty` in the PR's `pr-status` directory.
  The file holds a timestamp. The baseline is the previous fresh read when there
  is one, else the gate's snapshot. The thread count is compared only when
  neither read hit the 100-thread cap.
- If a gate decision replaced the snapshot while the read was in flight, the
  read is dropped and nothing is written or marked.

The next `pr` pass of that repo handles a marked PR first:

- Marked PRs are queued ahead of the others. The queue starts the lowest hk
  priority rank first and keeps arrival order within a rank, so a marked PR
  starts first among PRs of equal rank, and a higher rank still starts earlier.
- The PR is evaluated even when the unchanged-PR skip would leave it alone.
- The marker is removed when the pass that decides the PR started after the
  marker was written. A marker written during that pass stays for the next one.

There is no daemon to wake, so a marker written by the command takes effect on
the next `pr` pass.

If the GitHub read fails, the snapshot is still printed, with `fresh_error` set
to `GitHub read failed` (the summary shows `fresh: GitHub read failed`). Nothing
is written and no marker is set.

Each `--fresh` spends one GraphQL request from the token's budget. A plain read
spends none.
