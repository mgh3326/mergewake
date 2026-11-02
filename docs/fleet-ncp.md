# Release-driven service rollout

Choose mergewake's digest-pinned container installer for the hub and task service
in this rollout. It has durable recovery, explicit adoption, existing hk records
and deploy-feed integration. It stops the previous container before starting
the replacement, so this choice has a bounded service interruption. Clients must
reconnect; this is not a zero-downtime claim.

shiftgate is the stronger choice where uninterrupted HTTP routing is required:
its state machine starts green, health-checks it, then flips HAProxy and drains
blue (`shiftgate/docs/design.md`, Deploy state machine). It needs two ports per
service and shared HAProxy admin access. It does not by itself enforce this
rollout's repository path decisions or deliver the existing mergewake receipts.
Both choices need an explicit migration compatibility decision. Direct container
rollout avoids adding a second live deployment authority and HAProxy migration
in the initial service adoption. The desk may choose a later shiftgate cutover;
never have both installers own one service.

## Inputs and reconciliation

A dedicated mergewake daemon on the service host polls GitHub releases with
`follow_release: true`. It resolves the release tag to a full commit, then reads
`ghcr.io/<owner>/<repo>:<commit>` through `docker buildx imagetools inspect`.
Only a valid immutable sha256 digest reaches the installer. Release CI and main
ancestry still gate it. A missing image or malformed registry response refuses;
publication order is handled by the next poll. GitHub webhooks can wake it early,
but polling is the complete reconciler; no remote notification chain is needed.
The main image workflows already publish full-commit tags. Registry write access
is trusted to bind those tags to builds of the named commit; never deploy a
mutable `main` or `latest` reference.

Use the approval gate from the companion PR before enabling: previous receipt
through new release commit, previous-policy paths, exact human-only hk answer,
unknown range hold, and inventory identity binding. The rule applies to every
project. For a trading application, use its dedicated path policy and existing
CI gate; this inventory does not bypass either.

## Backup, health, rollback and records

Configure a container `pre_deploy` command to start a blocking, oneshot database
backup unit. It runs after pulls and before journal/stop/rename/run. Nonzero exit
or timeout leaves the old container running. The unit must create and verify a
fresh restorable backup, not merely enqueue one. It must run with the same DB
scope as the service. Configure that existing unit privately; do not put database
credentials in the inventory.

`handoffkeep` runs migrations during startup (`internal/store/migration.go`);
its production migration paths must require approval. The old binary is safe to
restore only when the new schema remains compatible. Never automatically restore
or downgrade the database. An incompatible migration needs an operator maintenance
plan and verified backup restore procedure. A failed health check restores the
preserved old container and prior receipt; rollback failure holds for inspection.

Both services expose `/healthz`. Configure a loopback HTTP readiness probe with a
finite timeout/retry count. Confirm application identity after rollout using the
service's version/health output, beyond the liveness status used by the installer.
Existing Service.Rollout records host/repo/commit/digest/status in hk documents and
emits the Discord deploy feed after install. Configure the handoffkeep sink and
Discord deploy webhook privately. Reports are best effort after the durable
receipt, not an outbox: inspect local decisions.jsonl if hk/Discord delivery fails.

## Desk enablement

1. Merge the approval gate and this PR, release mergewake, and install that version
   on the service host. This repository change performs no host actions.
2. Review private container names, ports, env-file paths, volumes, user and restart
   options against the currently running services. Copy examples/fleet-services.yaml
   to the private mergewake config and fill the required review profile settings.
3. Set a dedicated `deploy_task` per repo/host, handoffkeep sink and Discord deploy
   feed. Add per-repo `deploy_approval` policy; task-service migrations always hold.
4. Set the task-service pre_deploy command to `systemctl start <backup-unit>`.
   Verify the existing backup unit runs synchronously, reports failure, and produces
   a restorable backup. Do not register a new database backup scheduler here.
5. With rollout disabled, run `mergewake rollout --config <private-config> --repo
   mgh3326/handoffkeep --adopt handoffkeep --dry-run`, inspect identity, then have
   the desk repeat with `--apply`. Do the same for panewire. Adoption does not
   invent a previous source commit; the first managed release holds for approval.
6. Run each `rollout --dry-run`; inspect the release SHA/digest and hold. Confirm
   the current image is immutable and the rollback image remains available.
7. Enable `daemon.rollout: {enabled: true, interval: 2m}` with `daemon.act: true`
   and a daemon process launched with `--apply`. The binary-only release role is
   optional for these follow-release inventories; do not publish a competing
   mutable manifest. Signed webhook configuration is optional and operator-owned.
8. Answer the first hk request only after backup/rollback review. Verify backup,
   success receipt, /healthz, version, hk document and Discord deploy log. A healthy
   repeat poll should do no work. Check a real execution trace within 24h.

Rollback: disable rollout, preserve journal/rejection/receipt files, inspect
whether automatic rollback completed, and use the recorded previous immutable
image to restore the service through the desk procedure. Never clear a rejection
or journal merely to force a retry. If schema is incompatible, stop and use the
operator's approved database recovery plan.

## Shared control-plane window

The task service and panewire hub must never restart in the same window. Both
repository identities always resolve to the reserved `control-plane` rollout
group; the example explicitly records it. Use one controller and one shared
state_dir for both services on a host. Do not run a second updater against either.
A shared lock spans reconciliation, install, health and reporting. Before mutation
an active.json intent identifies the owning repository/commit. Only success or a
verified healthy current service clears it. A returned failure retains the intent,
blocking the other service until the first is verified healthy. A crash can also
leave a stale directory lock; inspect the process/journal before clearing it.
A recovered current container receives an explicit health check before releasing
the window. Never delete active.json merely to unblock the other service; first
recover the named service and verify its readiness. If rollback fails, keep the
other service running and escalate to the desk.

An explicit `--adopt --apply` also records the operator-reviewed inventory
identity after comparing the configured recipe with the running service. It does
not invent source ancestry: the first managed range still requires a human hk
answer. This lets an already-current image establish a healthy receipt without
pretending an automatic no-op attested a changed recipe. Dry-run adoption writes
neither receipt nor identity. The environment-content follow-up (#66) must land
before host enablement.
