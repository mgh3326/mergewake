# Keep the deployment daemon as a host binary

The deployment daemon stays a host binary. Containerizing it is possible, but
it would require broader host authority and filesystem coupling than the binary
already needs. This decision changes no host configuration.

Evidence from this repository:

- `internal/integrations/Exec.Run` starts review commands with a host working
  directory. Reviewer profiles and `internal/pipeline/review.go` rely on the
  installed fleet CLIs, checkout paths, worktrees and process/session lifecycle.
- The daemon's release/rollout/reap roles coordinate local host state, and
  container rollout invokes a host Docker/Podman runtime. A daemon container
  would need host filesystem mounts, runtime socket access and matching path
  conventions, plus credentials and reviewer executables managed outside it.
- `internal/rollout/command.go` commits command-installed.json only after install,
  verify and smoke, then removes the recovery journal. An inline self-container
  restart during that transaction can interrupt the only writer before commit.
- `internal/fsutil.Atomic` and the release installer already atomically replace a
  host binary; an existing process can finish using its old executable. A
  separate supervisor can restart after the durable receipt.

The deployment artifact is the checksummed GitHub release binary. Store host
configuration, credentials and state outside source control. Keep service
containers (hub, task service and applications) separate from the daemon's own
packaging decision.

For self-update, the supervisor must check that command-installed.json names the
new full commit, the target binary's `commit` output matches it, no recovery
journal exists, and the running process still uses the prior executable. Only
then restart the owning service. A failed/unknown receipt leaves the existing
process running. Do not restart from within smoke, and do not run two updaters
against the same binary. Until that supervisor check is installed, a desk-owned
restart after receipt verification is required; full automatic process restart
is not claimed by this decision record.

A future container proposal must demonstrate real reviewer sessions inside the
container, worktree/path parity, bounded host socket authority, durable state
across replacement, self-update interruption recovery and supervisor-driven
restart. Merely building a daemon image does not prove those properties.
