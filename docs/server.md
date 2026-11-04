# PR follow-up server on OCI

Status: accepted with revisions in handoffkeep document
`decision/2026-11-03/mergewake-server-design-accept` (task 2721).
The operator's November 3 update supersedes the initial
SQLite and fixed host-cap proposal: use managed OCI MySQL 8, performance/load
placement without fixed per-host caps, and herdr 0.9.5 reap guards.

## Problem and existing boundaries

`cmd/mergewake/daemon.go:pollPRs` runs observation, review, findings, notification
and landing synchronously across PRs and repositories. One long review delays
unrelated PRs. `internal/daemon/webhook.go` authenticates deliveries but wakes
whole roles without durable acceptance. More goroutines around that sweep would
retain its GitHub amplification and introduce competing JSON writers.

The server replaces the PR scheduler and its JSON state ownership. Existing
GitHub evidence, exact-head landing, policy opt-in, shadow/apply behavior,
disposition verification and expiring review leases remain safety boundaries.
`internal/pipeline/review_round.go` already supports patch identity and interdiffs;
its quiet-after-budget policy changes to the stop rule below. Same-family
fallback currently counts toward landing; that changes explicitly.

The server coordinates remote workers through panewire and reads handoffkeep
through APIs. It never opens either service's database. Host rollout and local
installation remain outside the server. The host-binary decision in
[daemon-packaging-decision.md](daemon-packaging-decision.md) continues to apply
to those duties; a separate remote-worker server image needs no runtime socket,
host checkout mounts or local model CLIs.

## Usage and module ownership

Proposed operator surface (not implemented commands):

```text
mergewake server --config /run/mergewake/config.yaml
mergewake server inspect --repo example/project --pr 7
mergewake server request-review --repo example/project --pr 7
mergewake server migrate --config /run/mergewake/config.yaml
```

Inspect and request-review call the authenticated server API. Only an
authenticated operator request receives reserved-slot priority; webhook bodies,
PR text and authors cannot grant it. Boundary adapters validate signed deliveries
and remote results before calling the engine:

```go
engine.Accept(ctx, verifiedEvent)       // commits input and PR refresh intent
engine.RequestReview(ctx, operatorReq) // commits an authorized priority request
engine.Complete(ctx, verifiedResult)   // checks attempt/head/scope fencing
engine.Run(ctx)                        // runs short transitions and dispatches
```

Conceptual domain sketch, not exported transport or SQL representations:

```go
type PRKey struct { Repository RepositoryID; Number PullNumber }
type Revision struct { Head CommitID; Base CommitID; Generation uint64 }
type ReviewScope struct { Kind ScopeKind; Patch PatchID; Prior ReceiptID }
type Job struct { ID JobID; PR PRKey; Revision Revision; Sequence uint64; Kind JobKind }
type Attempt struct { Job JobID; Epoch uint64; Request RequestID; Remote RemoteJobID }
type Result struct { Attempt Attempt; Reviewed Revision; Scope ReviewScope; Receipt Receipt }
```

`internal/server` owns typed transitions, review planning, ordering, admission,
MySQL transactions and the notification outbox. A pure reducer maps current
state plus validated observations to the next state and effect intents. SQL and
wire payloads stay private to their adapters. `internal/github` owns GitHub
transport and fresh evidence; `internal/integrations` owns panewire/handoffkeep
protocols. `internal/reap` owns destructive session guards. The CLI starts these
components, without duplicating policy. Pipeline review/gate capabilities are
extracted where necessary rather than invoking a shared mutable Service from
concurrent workers. The server path has one owner of retries, findings aging and
notification deduplication; it does not also write legacy JSON ledgers.

## PR states and transitions

All transitions commit through the engine transaction, using a version and
generation check. GitHub supplies facts, workers supply evidence, and neither
can assign READY directly.

| State | Trigger and next state | Decision owner |
| --- | --- | --- |
| observing | Fresh head/base/policy/files/CI establishes draft, waiting_ci, review_queued or gate_pending | Engine planner |
| draft | Verified ready-for-review fact returns to observing in a fresh generation; draft conversion also invalidates reviewing/ready work at the same SHA | GitHub observation, engine |
| waiting_ci | Required code-ready checks start/succeed as policy requires; review_queued; failures go to awaiting_author | Engine, configured readiness policy |
| review_queued | Capacity reservation and committed dispatch intent enter reviewing | Scheduler |
| reviewing | Valid result enters awaiting_author or gate_pending; transport uncertainty stays reviewing | Result validator |
| awaiting_author | Verified dispositions or a changed head return to observing; exhausted fix rounds enter blocked_author | Author action, engine |
| blocked_author | A changed head is recorded but remains blocked; only explicit authorized resume starts a new bounded cycle; aging may notify operator | Author/operator, engine |
| gate_pending | Fresh exact-head gate passes to ready; pending CI waits; failures route to author | Existing policy/gate |
| ready | Opted-in acting mode requests pinned-head merge; confirmed result enters merged; moved head returns to observing | Merge adapter |
| merged | Final; cancel obsolete queued work and retain evidence/ownership | GitHub observation |
| closed | A verified current GitHub open fact reopens into a fresh generation and receipt lifecycle; old attempts/receipts cannot authorize it | GitHub observation, engine |
| system_blocked | K consecutive unexpected failures suspend that operation; authorized retry or verified recovery resumes | Engine, operator on escalation |

Operational wait reasons (quota, capacity, rate deadline, missing telemetry,
remote outcome unknown) are separate from review verdicts. An unresolved merge
request enters an operation-level uncertain status until GitHub confirms its
outcome. Pending checks never masquerade as failures or permission to merge.

Per-PR sequence orders transitions and head-scoped effects, not webhook arrival
timestamps as GitHub truth. Observation can advance a generation while an old
remote review is running. New heads supersede queued old-head jobs; cancellation
of running work is requested, not assumed. Old results are audit-only and cannot
publish evidence for the current head. A superseded attempt retains capacity
until remote termination is known. No PR lock or SQL transaction lasts for the
duration of a review.

Both webhook-driven refresh and reconciliation use this same transition rule:
while `blocked_author`, record the latest head/base and advance the generation
to fence obsolete work, but preserve the blocked state and exhausted fix-round
count. A new commit does not enqueue another review or restart aging. Only an
authorized resume records a new bounded cycle.

Generation fences eligibility as well as head/base changes. Converting an open
PR to draft advances it even at the same SHA, supersedes queued review/gate work
and persists cancellation intents for older remote work. Draft eligibility is
stored separately from author/system holds so draft/ready toggles cannot erase
an exhausted fix budget. Ready-for-review advances generation again and refreshes
eligibility; late draft facts/results from the prior generation are refused.
Cancellation carries a persisted inclusive cutoff generation, never the current
head alone; a delayed cancellation cannot target new eligible work. Requesting
cancellation does not release reservations before remote terminal confirmation.

Closed is distinct from merged. Reopening requires a verified current open fact,
increments generation and the receipt lifecycle namespace, and invalidates prior
review qualification without resetting the fix budget or operational holds.
Equal patch-id alone cannot reuse evidence from a closed lifecycle. Merged PRs
remain final. A delayed old close cannot close a reopened generation.

Required-check observations distinguish pending, success, failure, cancelled
and timed_out. Waiting-CI failure/cancellation/timeout enters awaiting_author;
a pending rerun returns to waiting_ci and success refreshes eligibility before
a fresh gate. These are domain facts, not consecutive system failures. CI-only
changes may preserve an independent review already queued/running, but its
completion observes the current CI state and cannot make pending/failed CI READY.

Every refresh captures the expected PR version before its GitHub read and commits
through that CAS. Normalize webhook signals through current GitHub facts; do not
stamp an old delivery/read with the latest version after it finishes. Duplicate
delivery keys alone cannot prevent a late read from regressing eligibility.
Fixtures cover reopen, same-SHA draft/ready, CI rerun, stale reads/results,
cancel cutoff persistence and unconfirmed reservation retention.

## MySQL 8 schema, claims and migrations

Use managed OCI MySQL 8 with InnoDB, UTC `DATETIME(6)`, explicit primary/foreign
keys and case-sensitive ASCII identity columns (`ascii_bin`); human text uses
`utf8mb4`. One active coordinator is initially deployed, with bounded concurrent
short transactions. The managed DB is an operator decision, not a claim that a
second writer is required. A durable coordinator lease/epoch fences database
mutations by duplicate server instances. External dispatch and merge authority
also require the downstream contracts below; the database lease alone cannot
fence a network request from a paused former owner.

| Table | Keys and important fields |
| --- | --- |
| schema_migrations | version PK, checksum, applied_at, status |
| coordinators | service PK, owner_epoch, lease_until |
| deliveries | (source, delivery_id) PK, payload_digest, received_at, normalized payload |
| prs | (repository, number) PK, head, base, generation, version, state, draft eligibility, receipt lifecycle, CI status, next_sequence, fix_rounds, author_route, route_provenance, first_wait_at |
| jobs | id PK, PR FK, sequence, generation, kind, scope_digest, priority, status, available_at, owner_epoch, attempt_epoch, lease_until, failure_count, last_error_code |
| attempts | (job_id, epoch) PK, owner_epoch, request_uuid UNIQUE, remote_job_id, chosen_host, canonical_pool, state, dispatch/result timestamps |
| reservations | attempt FK UNIQUE, class, host, pool, predicted_cpu/memory/work, terminal_confirmed |
| cancellations | job FK/PK, through_generation; old-generation control work does not block current PR progress |
| capacity | resource PK, revision; locks for global/class/pool accounting, no fixed host ceilings |
| receipts | id PK, PR/lifecycle/head/patch/policy/context/family/scope identities, prior_receipt FK, trusted result digest |
| outbox | event_id PK, owner_epoch, job/PR identity, effect_kind, route, redacted payload, status, available_at, failures |
| reconcile_cursors | repository PK, last_complete_at, next_page, generation |
| request_cache | resource key digest PK, validator, bounded body, observed_at, freshness/deadline |
| audit | id PK, PR/job/attempt identity, transition, reason_code, timestamp |

Use a UNIQUE logical work key `(repository, number, generation, kind,
scope_digest)`; a separate per-PR sequence unique key prevents duplicate ordering.
Index jobs on `(status, available_at, priority, id)` and
`(repository, number, sequence, status)`. Delivery uniqueness rejects duplicates
atomically; a repeated ID with a different digest is quarantined. Outbox keys
encode PR/generation/verdict/reason-set and reminder ordinal. Successful duplicate
deliveries return the original acceptance.

Under READ COMMITTED, claim eligible jobs with `FOR UPDATE SKIP LOCKED`, lock
the PR row, verify no preceding non-superseded conflicting effect, then reserve
global/class/pool capacity and insert the attempt/dispatch intent in the same
transaction. Admission locks capacity rows in a consistent order and reads
current reservations for projected host load. Competing schedulers cannot both
spend the last review slot. Version/epoch compare-and-swap fences late results.
Network calls never occur in SQL transactions. Deadlocks are bounded transaction
retries with jitter, not partial commits. Use DB time for leases.

Migrations are ordered, checksummed SQL files applied by an explicit migration
command with one advisory migration lock. MySQL DDL implicitly commits: each
version records started/completed and validates `information_schema` before
resuming a partial migration. Never assume a transaction rolls back DDL. Runtime
startup refuses dirty/unknown versions. Prefer additive changes, backfill in
bounded batches, validate, then remove old columns in a later unit; no automatic
down-migration that drops evidence. CI runs real MySQL 8 migration/claim/restart
tests; local development uses focused pure reducer and adapter fixtures, with no
local database server.

The host supplies `MERGEWAKE_MYSQL_DSN` or a mode-0600 secret file selected by
`MERGEWAKE_MYSQL_DSN_FILE` (exactly one), with verified TLS and a least-privilege
runtime account; migration authority is separate. DSNs, certificates and secret
contents never enter source, logs, metrics or PR bodies. Configure bounded
connect/query timeouts, connection count and lifetime.

If MySQL is unreachable, startup/readiness fails, webhook acceptance returns
503, and no new review, fix, notification side effect or merge is dispatched.
No JSON/in-memory fallback confers authority. Running remote workers may finish;
their results are retrieved after recovery or their callbacks receive retryable
503. Reconnect revalidates the coordinator epoch and reconciles pending effects
before admission resumes. Merge dispatch requires a fresh durable permit and
successful DB check immediately before the GitHub call; failure prevents it.
A fresh check is necessary but is not an external lease fence: the process can
pause after it and resume after another owner acquires the lease. Persist
`owner_epoch` with every external effect intent and dispatch attempt, alongside
an immutable operation ID and payload digest. An old epoch cannot gain a new
operation ID or change the payload of an existing operation during recovery.

For panewire dispatch, task 2724's downstream contract must reject stale owner
epochs before creating a new job and durably deduplicate retries of an already
accepted operation ID/digest to the original outcome. Recording the newer epoch
and accepting a dispatch must be serialized at that downstream boundary. The
adapter cannot claim safe automated failover until this contract is tested.

GitHub's [merge API](https://docs.github.com/en/rest/pulls/pulls#merge-a-pull-request)
does not accept this coordinator epoch. A pinned-head merge
protects the target head, but does not fence owner epochs or policy changes;
checking posted receipts before retry also cannot prevent concurrent posts.
Therefore direct GitHub mutation authority has no lease-only automatic takeover:
the supervisor/operator must prove the previous mutation executor is terminated,
its egress path is fenced, and outstanding requests are terminal/reconciled
before enabling a replacement. If that proof is unavailable, keep mutations,
including merges, disabled. Persist uncertain operations across takeover and
reconcile each immutable target before an operation-specific retry. This is a
deployment/adapter acceptance prerequisite, not a property supplied by MySQL.

Fault fixtures must pause an owner between DB check and dispatch, advance the
epoch, and verify stale dispatch rejection and stable-ID replay. For direct
GitHub adapters, the same fixture must refuse replacement mutation authority
until prior-executor shutdown/egress fencing and pending-operation reconciliation
are proven. No external adapter is complete on the strength of a lease test alone.
A GitHub request already sent cannot be recalled by a subsequent DB outage:
record it as uncertain and reconcile before any retry. Never claim atomic commit
across MySQL and GitHub.

## Store before do, retries and remote uncertainty

Persist effect intent, coordinator owner epoch, deterministic request UUID,
attempt epoch, scope, selected
placement and reservation before sending a spawn. Keep the same identity across
transport retries. Results authenticate their source and match job, epoch, head,
scope and actual reviewer family. Validate findings and exact reviewed patch
before posting a trusted receipt. Duplicate results are harmless; stale ones
cannot advance state.

A timeout/504 is not evidence that spawning failed. Mark dispatch_unknown,
retain reservations and query the same request/job. Do not blindly expire its
permit or spawn another worker. Retry only after authoritative absence or
confirmed termination. Posting GitHub reviews also uses a durable receipt key:
after an ambiguous response read trusted reviews before posting again. Merge
uncertainty reads current PR/head/merge result rather than reissuing blindly.

Unexpected failures use persisted exponential backoff (30 seconds initially,
15-minute cap, positive jitter), K=3 consecutive failures before one escalation.
Verified success resets the consecutive counter. CI pending, author findings,
quota exhaustion and known capacity waits do not increment it. Permission or
invalid configuration failures become structured decisions without retry storms.
Outbox delivery failures use the same durable policy. Aging is separately
tracked from first unhandled work and cannot reset on every reconcile.

Verified panewire integration limits: the installed CLI has no `spawn` command;
the documented hub `POST /v1/spawn` accepts request UUID, machine, cwd_key,
inline brief and restricted argv, with `GET /v1/spawn/{request_id}` status kept
only one hour in memory. This is insufficient for safe retry after hub restart.
The adapter may use that API until a sufficient CLI exists, but unattended
dispatch requires an upstream durable request-identity/outcome contract. Until
then uncertainty holds capacity and escalates; it never enables duplicate work.
The host agent prepares the exact-head worktree; the server does not depend on
a host-local checkout or read remote job directories.

## Worker admission and placement

Configure N total review slots, N >= 2 (initial N=6). Automatic reviews can consume at most
N-1; an authenticated operator request may use any free slot, including the
reserved last slot. Automatic requests never borrow that slot. Fix workers have
a separate bounded global budget (initially 4), and consume the same measured host resources
and applicable pool quota. Operator review reserve also preserves configured
predicted memory/CPU and quota headroom for one review; this is dynamic resource
reservation, not a fixed per-host cap. Physical capacity or quota can still make
an operator request wait; report that reason explicitly.

No fixed per-host worker counts or host-name exclusions. Place all review and
fix jobs using current load and measured performance. Primary source is
Prometheus host metrics for CPU utilization, Load5, core count, available memory
and swap use/activity, plus panewire heartbeat/agent projection for active panes
and freshness. Current node load projection exposes Load5/core count but not
all memory/swap fields, so it alone cannot supply the full score. Require metrics
with sample age <=60s and heartbeat age <=60s; unknown hosts wait rather than
being treated as idle. Obtain canonical pool/quota from pacelever and the hub
quota-aware placement API; never infer pool from a model name.

For each eligible host/profile, estimate job demand from the rolling median of
the last 20 successful comparable jobs (scope/size/model class), with a
conservative configured cold-start estimate and p90 memory demand. Configure a
finite, strictly positive bootstrap throughput `R_boot` for each host/profile
and comparable-job class, in the same work-units/second used for measured
throughput. Reject missing, zero, negative or non-finite bootstrap values before
admission. Use `R = R_boot` until at least five successful comparable jobs within
the last seven days exist, then use their median work/elapsed-time throughput
over at most the latest 20 samples. Ignore invalid/nonpositive samples; fall
back to `R_boot` if fewer than five valid recent samples remain. This makes a
new host or class rankable before its first job. Use these dimensionless terms:

```text
L = projected Load5 / core_count
C = projected CPU fraction
M = predicted job memory / available_memory
S = min(2, swap_used / physical_memory + normalized_recent_swap_activity)
A = (active_panes + starting_reservations) / core_count
Q = quota fraction required / safely available pool quota
score = estimated_work / R * (1 + 0.35*L + 0.25*C + 0.20*M + 0.10*S + 0.10*A + Q)
```

Choose the lowest score, break ties by oldest last assignment then opaque host
identity. Projections add resource demand from starting/unobserved reservations
once; remote job IDs link reservations to heartbeat jobs to avoid double-counting.
Recompute after every admitted reservation. Reject unhealthy/stale hosts, denied
or unknown pool quota, and insufficient predicted memory after operator reserve.
Honor hub resource-safety denials; quota boosts cannot bypass safety. Historical
performance is decayed; failures do not become fake fast samples. This allows
a faster, busier host to win when its expected completion time is better, while
pressure raises its score. There is no permanent penalty for a host's past load.

Pool concurrency/quota ceilings and global budgets remain explicit and
transactionally reserved. Starting, running and uncertain attempts count. Pool
identity, load samples, score components and selection reason are auditable in
private logs, using bounded aliases in metrics. Coefficients and demand estimates
are initial policy, to be calibrated with recorded workload replay; no speedup
or optimal placement is claimed before measurement.

## Review depth and family rule

Code-ready means non-draft with a complete diff, known author family and policy
readiness checks satisfied. Final green CI is still required at landing.

1. Docs-only: no model review only for <=50 changed lines when every change is
   an allowlisted prose file and no executable/config/build/generator/symlink/
   submodule change exists. Larger docs-only diffs get one cross-family
   delta-scale pass.
   Classifier uncertainty requires full review. The gate records the exemption
   against exact head and policy; CI and disposition checks still apply.
2. First code-ready patch: one full cross-family pass. Explicit stricter
   blocking-path policy can require more passes; the server does not silently
   weaken a repository's required evidence.
3. Equal nonempty stable patch-id: reuse only trusted cross-family evidence
   with matching repository/PR, policy/context/classification and review scope.
   Preserve chain provenance and revalidate current CI, base and findings.
   Empty/unknown patch IDs never compare equal. A delta receipt alone cannot
   stand for an initial full pass; its full-pass chain must validate.
4. Small fix: one cross-family delta pass with prior findings and acceptance
   context. Initial conservative bound: <=200 added/deleted lines, <=5 files,
   no policy/dependency/interface/security-sensitive path change. Unreadable or
   broader changes require full review, while retaining the cycle budget.
5. Initial pass plus at most two changed-patch fix rounds per cycle. Rebase-only
   equivalent patches do not consume a fix round. If findings remain after
   round two, enter blocked_author, stop automatic reviews/fixes, and notify the
   author. New commits do not silently reset that budget. Explicit authorized
   resume records why a new cycle is allowed; operator sees only aging unless
   the author requests a decision.

Same-family output is reference-only. It cannot satisfy full or delta review,
create a gate-satisfying completion, or be reused as qualifying patch evidence.
Unknown actual family is also nonqualifying. Cross-family capacity exhaustion
waits rather than falling back. Update the shared gate and its current
same-family landing tests, not just worker selection. Existing historical
same-family receipts remain audit evidence but require a new qualifying pass.

Required companion edits in rstack: make `wake` state the reference-only rule
and waiting behavior; make `fleet-review` use the identical full/delta/reuse
contract; align their two-fix-round and docs-only guidance; ensure spawn/result
templates include actual family, head, scope and receipt provenance. Those are
separate authorized repository changes, not silently included in this PR.

## Event ingestion and GitHub budget

HMAC validation, request size bounds, repository allowlist and JSON/form parsing
stay at ingress. Commit deduplicated delivery and targeted refresh intent before
2xx; do not perform GitHub reads in the webhook request. PR/review/thread events
target that PR. Check/status events map head SHA to known PRs, with a bounded
lookup when absent. Base pushes invalidate affected open PR gates; head pushes
refresh only matching branches. Relevant issue comments refresh dispositions.
Worker completion and operator requests are authenticated event sources too.

A 15-minute jittered, paginated reconcile discovers missing PRs and refreshes
open PRs only when their evidence is dirty/stale; persist cursors and complete
coverage watermark so paging cannot starve later repositories. Reconcile is a
backstop, never a second serial review runner. Stream reconnect/cursor gaps
schedule reconciliation. Do not rely on webhook delivery order or completeness.

Persist bounded ETag/body cache for conditional REST reads. Batch read-only
GraphQL PR/check/thread summaries with bounded query size, pagination and
partial-error handling. Observe REST remaining/reset, GraphQL cost/remaining,
Retry-After and secondary limits. A shared request budget returns a durable
retry deadline; a throttled endpoint never holds a PR execution slot asleep.
Fresh final gate reads bypass stale cache but still honor GitHub throttles.
Missing or partial evidence cannot land. Cache invalidation remains targeted.

Record a redacted, timestamped event mix and initial evidence snapshot, then
replay both the existing sweep and new controller against the same fake GitHub
clock/transport. Count REST requests (including 304), GraphQL requests and
points, mutations, waits and completed reviews separately, normalized per hour
over the recorded window. Unit 2 must keep REST usage strictly below 2500 calls/h
(50% of the shared 5000/h budget), including retries and reconciliation, with
headroom for other producers. Prefer a GitHub App installation token when
available so server calls do not exhaust the user's shared budget; report token
class and actual rate-limit headers without credentials. Include quiet and burst periods and dropped/duplicate
deliveries. No request saving is claimed from the desk's observed sleep count;
the supplied runtime measurements are motivation, not this comparison's result.

## Author routing and operator interruptions

Trailers win: task trailer in PR body then head message resolves its bound job
and worker lane (including completed jobs); explicit notify-lane trailer is next
with the same body/head precedence. Read failures retry and do not reroute.
Without trailers, resolve canonical repository + head repository + head branch
and, when available, worktree identity through hub job ownership records, then
job -> lane. Branch alone across forks is insufficient. Multiple matches require
an ownership decision; never guess from a title or GitHub login. Persist route
provenance and revalidate changed bindings. Completed ownership remains usable.

The documented hub active-job listing currently exposes neither branch/worktree
identity nor completed history. A retained, authenticated ownership projection
is an explicit upstream API dependency, not an endpoint claimed to exist.
Handoffkeep's existing job-by-ID projection is usable after resolution. Keep
timeouts, credential handling and response redaction behind the adapter.

READY, MERGED, findings and author-actionable BLOCKED go to the author lane;
follow-up workers attach to that job rather than the coordinator. Durable inbox
ACK and verified dispositions stop reminders as appropriate. Operator
interruptions have a closed reason vocabulary:

| Reason | Examples |
| --- | --- |
| decision | Ambiguous ownership, missing authority, requested policy exception |
| deploy_approval | Existing path-based deploy approval requirement |
| consecutive_failure | K=3 unexpected operation failures, including unresolved dispatch |
| aging | Unhandled author BLOCKED or capacity/review wait beyond configured threshold |

Default author aging is two hours, review-queue aging 30 minutes. Deduplicate
per cause/cycle, with bounded reminders and ACK tracking. Routine status and
idle wake events are metrics/logs or author inbox notices, not operator
interruptions. Unknown ownership is one decision, not every finding separately.

## Reap and herdr 0.9.5

Use `herdr --machine <saved-profile>` for remote herdr operations in
`internal/reap`, replacing its raw SSH wrapper. Saved profile names and
credentials are host configuration. Existing hub transport remains an API
boundary; neither path invents SSH destinations or arbitrary shell strings.
Remote filesystem/job operations must use a supported structured host operation;
if unavailable, skip with an explicit unsupported reason instead of falling
back to raw SSH. No profile or host configuration is changed by this work.

Carry `state_change_seq` and `completion_seq` from discovery through the close
plan, capture and fresh close-exact check, in addition to pane/session identity,
status, cwd and name. Compare exact sequences, not just monotonic inequalities:
idle -> working -> idle must be refused even if status/cwd/name look identical.
Absent or reset sequence evidence is unknown and refuses close. Keep the
composer guard; re-read it near close, with nonempty/unknown input refusing
closure. Preserve merged-job ownership, protected/focused-pane checks, all-task
PR completion checks and fresh capture requirements. If the selected herdr
transport cannot perform conditional close atomically, document the remaining
read-to-close race and fail closed for unattended reap until a guarded host
operation exists; do not call separate read/close atomic.

## Observability and verification

Expose Prometheus `/metrics` on an authenticated/internal listener, scraped into
Grafana. Labels are bounded repo/pool/host aliases, job kind, class, stage and
reason; never PR numbers, SHAs, job IDs, lane names, DSNs or arbitrary error text.

| Series | Type |
| --- | --- |
| mergewake_queue_depth{kind,class,state} | gauge |
| mergewake_review_wait_seconds{class} | histogram |
| mergewake_review_run_seconds{pool,scope} | histogram |
| mergewake_pool_inflight{pool,class} | gauge, includes uncertainty |
| mergewake_host_inflight{host,kind} | gauge, observation rather than host cap |
| mergewake_github_budget_remaining{resource} | gauge, REST and GraphQL separate |
| mergewake_github_requests_total{resource,operation,status} | counter |
| mergewake_github_graphql_cost_total | counter |
| mergewake_stage_duration_seconds{stage} | histogram |
| mergewake_operator_interrupts_total{reason} | counter |
| mergewake_db_available | gauge |
| mergewake_oldest_wait_seconds{kind} | gauge |

Start one Sentry trace per review job with child spans for dispatch, remote wait,
validation and posting. Propagate trace context with the job brief and bind
errors to job/attempt/generation privately. Do not enable default PII or attach
diffs/prompts/secrets. Persist structured JSON transition/audit logs on a mounted
volume with bounded rotation/retention; stdout alone and ephemeral journald are
insufficient. DB audit rows link effects to their transitions. Liveness reflects
process health; readiness includes DB and coordinator lease.

Focused fixtures per unit cover duplicate/out-of-order deliveries, concurrent
claims, crash before/after each external effect, stale completion, changed head,
ambiguous spawn, MySQL disconnect during admission/merge and failed migrations.
The concurrency fixture holds N-1 automatic fake reviews open, proves another
automatic request stays queued, then admits an authenticated request to reach N.
Replay changing load/performance/quota to prove a fast busy host can win, stale
telemetry defers work, quota ceilings hold, and uncertainty keeps reservations.
Review fixtures cover docs exemption, full/delta/reuse chains, same-family
refusal and two-round stop. Reap fixtures include identical visible fields but
changed sequence, missing sequence, composer text and profile argv dispatch.

Run only changed-package tests locally (`go test -p 2 ./<pkg>/...`), never a
local DB server. CI owns MySQL integration, migration/reconnect, race tests and
cross-builds. Every implementation PR names its focused tests, includes a replay
or fixture demonstrating N concurrent reviews and the reserved slot, reports
GitHub calls/hour for the recorded event mix against the old sweep, and must
have green CI. Build the common replay harness early enough to accompany unit 1;
until remote dispatch exists it uses the fake worker boundary and explicitly
labels that limit. Later units retain and extend that evidence.
The observability unit sends a synthetic review-job Sentry test event to the
configured test project and records the event/trace evidence privately; a fake
SDK test alone does not satisfy that acceptance criterion.

For the desk's first 24 hours after deployment, provide these Grafana queries
and compare matching windows using preserved baseline measurements:

```promql
sum(increase(mergewake_operator_interrupts_total[1h]))
histogram_quantile(0.95, sum by (le) (rate(mergewake_review_wait_seconds_bucket[1h])))
sum(rate(mergewake_review_wait_seconds_sum[1h])) / sum(rate(mergewake_review_wait_seconds_count[1h]))
sum by (pool) (mergewake_pool_inflight)
sum(increase(mergewake_github_requests_total[1h]))
```

No-sample means no data, not zero wait. Server interruption counts cover this
producer; the desk additionally measures its aggregate inbound stream to catch
other producers. The desk performs deployment and the 24-hour measurement.

## Synthesis, alternatives and delivery units

Two sketches were compared: materialized PR rows plus durable jobs, and a
per-PR event journal with replayed actor snapshots. Choose materialized state
and jobs for simpler MySQL inspection/migrations and one transition owner.
Graft the actor sketch's pure reducer and atomic effect-intent boundary without
requiring journal replay to reconstruct current authority. Retain audit history.
Both candidates identified remote uncertainty and ownership lookup gaps.
Independent model-family judging was unavailable; an additional read-only
architecture pass checks the synthesis. Candidate SQLite and host-cap details
are superseded by the operator update above.

Reject in-memory actors: mailbox/restart semantics leak into every caller.
Reject full event sourcing: versioned event replay adds complexity without a
current rebuild requirement. Accept managed DB availability as an execution
dependency in exchange for durable transactional coordination; DB outage stops
authority. Accept remote API prerequisites instead of pretending transient hub
presence proves job absence. A second active coordinator is outside initial scope.

After recorded design acceptance, use one PR per unit, each commit carrying
`Mergewake-Task: 2721`, with READY reported after each:

1. MySQL schema/migrations, store, typed state reducer, leases and failure rules.
2. Durable webhook ingestion and targeted reconcile with shared GitHub budgeting.
3. Panewire worker dispatch and performance/load placement, priority reserve,
   fix-worker budget, uncertainty recovery and concurrency replay.
4. Review depth/family enforcement in both receipts and landing gate.
5. Unified author routing through retained hub ownership and task APIs.
6. Metrics, Sentry job traces/test event, persistent logs and Grafana queries.
7. herdr saved-profile reap transport and sequence-aware close guards.
8. Server container image and deploy/migration/cutover/rollback runbook.

Panewire hub owns upstream durable spawn and retained branch/worktree ownership
contracts under task 2724. Units 3 and 5 stay incomplete until that dependency
lands and its contracts are verified. Delegate bounded units through the
available fleet spawning tool; a failed spawn falls back to the lead. No merge,
deployment or host edits are part of this lane.

The last PR body must give the desk concrete commands for: managed MySQL backup
and migrations; stopped-old-daemon JSON import with source checksums/idempotent
mapping of retries, sent notices, aging and outcomes; quarantine of unknown
formats; reconciliation of in-flight GitHub leases; observer validation before
acting; disabling the old PR role so there is one scheduler; secret injection,
mounted logs, health checks and supervision on OCI. Keep host rollout/reap duties
separately supervised as needed. Rollback stops server admission, drains or
records uncertain remote work, reconciles already posted/merged effects, restores
the prior compatible binary and an explicitly reconciled legacy state export,
then enables exactly one PR owner. Do not restore stale JSON over external
effects or assume a MySQL backup can undo a merge.

Implementation reconciliation: design accepted by the operator in handoffkeep
document `decision/2026-11-03/mergewake-server-design-accept`. Its revisions are
incorporated above: <=50-line docs exemption, pacelever quota source, panewire
hub task 2724 dependencies, REST budget target, initial 6 review slots (1
reserved) and fix-worker budget 4. Unit 1 may now begin. No acceptance is inferred
from elapsed time or successful event delivery.

Unit 1's independent-review follow-up separates closed/reopen from merged
finality, models same-SHA draft eligibility and CI reruns, and requires a fresh
receipt namespace on reopen. These lifecycle corrections supersede the original
combined merged/closed terminal row; external adapters remain unimplemented and
the unit 2 REST budget remains a measurement target.
