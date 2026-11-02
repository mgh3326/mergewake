# Binary fleet inventory and enablement

Use examples/fleet-binaries.yaml for mergewake and pacelever. The release adapter
now derives `<repository>-<os>-<arch>` asset names, so pacelever can use the same
checksummed v-tag flow as mergewake. The pacelever verify wrapper reads only
`--version` and emits its full build commit. Both version paths exit before
loading application configuration.

The dry runs against published releases resolved:

| Repository | Published release | Full commit | Plan |
|---|---|---|---|
| mgh3326/mergewake | v0.5.30 | 0de64bbbaa1102f2afd07c542a97a49173dbd108 | bin/mergewake |
| mgh3326/pacelever | v0.3.18 | 47e28f900ae5f637ba58391b90f9d97a6c49f206 | bin/pacelever |

These are real GitHub reads and actual `rollout --dry-run` output, not install
proof. The companion approval gate correctly holds a fresh state directory
instead of approving bootstrap. Initial SHA placeholders are not evidence of an
installed version and must be replaced privately.

## Desk steps

1. Merge/release the approval gate and this PR in that order. Keep rollout disabled.
2. Install the reviewed mergewake binary and pacelever binary under `/opt/fleet/bin`
   using the desk's checksummed release procedure. Preserve the prior binaries.
   Install scripts/verify-pacelever-commit.sh at `/opt/fleet/libexec/` executable.
3. Read `/opt/fleet/bin/mergewake commit` and
   `/opt/fleet/libexec/verify-pacelever-commit.sh /opt/fleet/bin/pacelever`.
   Replace each `initial_sha` with that exact full SHA; verify a published release
   exists for it, since rollback fetches that commit's release assets.
4. Copy examples/fleet-binaries.yaml to the private daemon config, preserving the
   host's review profiles, host identity, state directory, notification routes and
   signed webhook settings. Set a dedicated `deploy_task` per repository/host and
   configure `sink.type: handoffkeep`. Never replace existing state with examples.
5. Add per-repository `deploy_approval` policies. The first bootstrap is unknown
   and holds for an hk human-only answer, even with an `initial_sha`. Review the
   installed version and rollback plan before answering. For an already current
   legacy receipt without inventory identity, use an operator bootstrap; a no-op
   cannot attest a changed inventory.
6. Run:
   ```sh
   mergewake rollout --config <private-config> --repo mgh3326/mergewake --dry-run
   mergewake rollout --config <private-config> --repo mgh3326/pacelever --dry-run
   ```
   Inspect full release commits, selected targets and any hold. These dry runs
   never execute smoke, restart a daemon, or alter receipts.
7. Apply this private config diff only after the desk's checks:
   ```yaml
   daemon:
     act: true
     release: {enabled: true, interval: 2m}
     rollout: {enabled: true, interval: 2m}
   ```
   The service must launch `mergewake daemon --config <private-config> --apply`.
   Keep the existing signed webhook listener: release, workflow_run and push wake
   roles early. Polling remains the complete reconciler if deliveries are missed.
   Pure binary inventories follow v-tag assets directly; release manifests are
   supplemental and do not replace checksummed binary publication.
8. After an approved rollout, inspect command-installed.json, verify full SHAs
   with the commands above, inspect hk/Discord records, then repeat polling to
   confirm no writes or reports on a healthy current install.

The running daemon retains its old executable after atomic replacement. Restart
it from a separate supervisor only after its durable success receipt is written
and command-journal.json is absent. Never put `systemctl restart mergewake` inside
its own smoke command: that can kill the transaction before commit. Until the
supervisor receipt check is configured, the desk restarts the daemon after
verifying the receipt; binary replacement alone does not update the process.

Rollback: disable release/rollout first. Keep receipts, journal and rejection
files for inspection. Restore the previous checksummed binary atomically and
restart the owning daemon through the desk. Verify its commit and help output.
For an automatic smoke failure, the installer re-fetches the previous full SHA,
verifies it and writes a rollback receipt. Do not clear rejected state to retry
without resolving the cause.
