# Durable server store (unit 1)

Design acceptance: handoffkeep document
`decision/2026-11-03/mergewake-server-design-accept`, task 2721.

This unit provides the MySQL 8 store and deterministic PR reducer. It does not
start the server, ingest webhooks, invoke workers, or change the current daemon's
landing behavior. Later units connect those boundaries. The accepted design is
in [server.md](server.md).

## Operator commands

After the desk provisions a database and a migration account, supply credentials
through exactly one of `MERGEWAKE_MYSQL_DSN` and `MERGEWAKE_MYSQL_DSN_FILE`.
The DSN file must be a regular mode-0600 file, at most 64 KiB. The DSN must
select a database and use `tls=true`, which verifies the server with system roots,
or use `tls=mergewake-ca` with the private CA configuration below. The store
refuses plaintext fallback and every other TLS mode. No credentials belong in
YAML, command arguments, source control or logs.

### Private CA and HeatWave

Set `MERGEWAKE_MYSQL_CA_FILE` to a regular PEM file containing one or more CA
certificates. The file must be mode 0600 or 0644 and at most 64 KiB. Set
`MERGEWAKE_MYSQL_SERVER_NAME` to the exact DNS SAN or, only when the certificate
has no SAN extension, the exact subject CN. With this CA file configured, the
DSN must use the registered TLS name `tls=mergewake-ca`; `tls=true` is refused.
The store builds the server chain against only the certificates in this CA file,
checks certificate validity and server authentication usage, and does not consult
system roots. A missing server name, invalid CA file, expired certificate, chain
failure or name mismatch refuses the connection.

This supports the MySQL HeatWave `mergewake` endpoint when its certificate is
signed by the private `MySQL_Endpoint_CA`, has CN `MySQL_Endpoint_Server` and no
subjectAltName, and the connection address is an IP. Configure the expected name
as `MySQL_Endpoint_Server`; do not use the IP address as the certificate name.
The CA and server certificates are deployment inputs and must never be copied
into the repository or logs.

The DSN may come from `MERGEWAKE_MYSQL_DSN` or the existing mode-0600 file
selected by `MERGEWAKE_MYSQL_DSN_FILE`. For example, its TLS parameter is:

```text
user:password@tcp(203.0.113.10:3306)/mergewake?tls=mergewake-ca
```

Supply the DSN as a protected environment value or through the DSN file; do not
put a real credential in a command line or documentation.

```sh
mergewake server migrate
mergewake server check
```

These commands are available now; `server serve` is not. `migrate` requires
DDL authority and a dedicated migration window. `check` needs read access to
the schema and `information_schema`. A runtime account for future controllers
needs DML on these tables, without DDL authority. Both commands fail on missing
configuration, connection failure, unknown schema or incompatible definitions.

The Go MySQL driver is pure Go; no database server is installed or started by
the binary. See the [driver's configuration reference](https://github.com/go-sql-driver/mysql#dsn-data-source-name)
and [MySQL locking-read documentation](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)
for the transport and transaction behavior used here.

## Durable operations

`Acquire` grants or renews a coordinator lease using database time. Use a unique
process identity for its owner, retain its epoch and renew before expiry.
Another owner cannot acquire an unexpired lease. Each mutating store operation
checks the fence in its transaction; a replacement owner fences its predecessor.

`Apply` checks an expected PR version and deduplicates a source/delivery identity
and digest. It commits the reducer's next snapshot, audit record and effect jobs
together. A duplicate returns current state without creating work; a reused
identity with different content is refused. Observations supersede obsolete
jobs, while preserving active remote attempts for reconciliation.

`Claim` commits an attempt epoch and UUID before returning work. Review claim
also advances `review_queued` to `reviewing` atomically. Jobs are ordered within
each PR; independent PRs can run concurrently. Escalation bypasses blocked work.
An authorized resume starts a new generation and retires prior blocked effects,
including failed escalation deliveries. Cancellation and escalation are durable
control jobs: they bypass blocked work and do not prevent current-generation
refreshes from progressing.

`Reopen` requires a verified current GitHub open fact for a closed PR. It advances
generation and the receipt `Lifecycle`, clears prior review qualification and
preserves fix-round/operational holds. Future receipt adapters must include that
lifecycle in reuse checks; equal patch-id cannot reuse a closed lifecycle's
receipt. `Merged` remains final.

`MarkDraft` and `ReadyForReview` change eligibility at the same SHA and advance
generation. They retain blocked-author/system holds independently of the draft
flag. Superseded attempts remain reserved and recoverable. A cancellation claim
includes `CancelThrough`, the persisted inclusive cutoff for older remote review/
fix jobs belonging to that PR; a
delayed cancellation must not target generations above that cutoff. Completing
the cancellation request alone never confirms the original remote job terminal.

`CIChanged` classifies pending, success, failure, cancelled and timed_out. Waiting
CI failures go to the author, reruns wait, and success refreshes eligibility.
Expected CI outcomes do not increment system failures or fix rounds. Independent
queued/running code review may finish, but observes current CI eligibility before
requesting a gate. Pending or failed CI cannot become READY.

Ingress captures `ExpectedVersion` before reading GitHub, then commits the fresh
normalized fact with that version. Late reads lose the CAS; old generations
cannot apply a close, draft or result after reopening/readiness changes. Raw
webhook payloads are not reducer commands. These are contracts for unit 2;
no event adapters are wired in this unit.

Initial durable budgets are six reviews (at most five automatic) and four fix
workers. The operator class is an internal capability granted by a future
authenticated ingress adapter. It must never be copied from PR/webhook text.
This unit enforces global admission; performance/load placement and pool quota
admission arrive in unit 3. There are no fixed per-host caps.

`Uncertain` retains an attempt's capacity. `Outstanding` pages running and
uncertain identities, including superseded jobs, so restart does not lose the
handles needed for remote lookup. Lease expiry never requeues remote work.
Only authoritative absence or terminal confirmation permits `ConfirmAbsent`
or `Complete`. Transport retries of the same spawn must retain its UUID; a new
attempt receives a fresh UUID only after the old one is known absent.

`Complete` commits terminal status and any state transition/follow-up jobs in
one transaction. Old-generation completion releases its confirmed capacity but
cannot advance the new head. Duplicate completion is harmless. Future result
adapters must validate reviewer family, patch scope, receipt trust and evidence
before providing reducer facts. The reducer never infers READY from a review;
only a separate validated gate fact can do so.

`ConfirmAbsent` persists exponential backoff with positive jitter. Three
consecutive failures block the operation and queue one escalation. Confirmed
success of eligible PR work resets the PR's consecutive failure counter;
cancellation acknowledgments never clear failures accumulated by that work.
Cancellation completions cannot carry PR transition facts, and their persisted
job kind determines this rule. Capacity and CI waits
do not call the failure operation. An authorized resume resets the cycle;
ordinary new heads cannot reset exhausted fix rounds.

## Schema recovery

Each checksummed migration creates one table. A connection-scoped advisory lock
serializes migrations. Before DDL, the migration is recorded as started; runtime
schema checks refuse an incomplete set. MySQL DDL can commit independently of
the marker transaction. Rerunning the migration command validates existing table
columns, nullability, ASCII identity collation, indexes, foreign keys and engine
against the embedded definition before marking an interrupted step complete.
Unknown migration names/checksums or incompatible definitions fail closed.

Do not edit applied migration files or drop evidence to make checks pass. An
incompatible partial table requires explicit operator inspection and repair or
restore before rerunning. There is no destructive automatic down-migration.
Future units add receipt, routing, cache and placement fields through new
migrations; they do not rewrite the applied core migrations.

DB failure returns no claim, committed transition or merge authority. There is
no JSON fallback. This unit performs no external effects, so no partial MySQL
transaction can itself cause a spawn or merge. A future adapter must recheck
authority immediately before dispatch and reconcile an external request whose
response or database commit is ambiguous.

## Verification

Local commands (no local database server):

```sh
go test -p 2 ./internal/server/...
go test -p 2 ./cmd/mergewake -run TestServerStoreBoundary
```

The dedicated `mysql8-store` CI job runs the same server packages with the race
detector and a disposable MySQL 8 service. Its integration fixture proves atomic
delivery deduplication, version conflicts, migration restart/drift refusal,
coordinator fencing, stale completion, failure escalation/resume and unavailable
DB refusal. `TestMySQLSixReviewsReserveAndRestart` queues automatic and operator
work, holds five automatic attempts, admits the operator as the sixth, restores
all six after coordinator replacement and completes six fake workers across a
shared barrier. No real model jobs are launched.

That fixture proves store admission and concurrent fake execution. It does not
prove panewire dispatch or load placement; unit 3 stays incomplete until hub
task 2724 lands. Recorded GitHub event-mix calls/hour and the strictly-under-2500
REST/h target are measured in unit 2, before connecting webhook/reconcile work.
This store-only unit issues zero GitHub calls and makes no savings claim against
the current sweep.
