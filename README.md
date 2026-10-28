# mergewake

**The after-merge pipeline for your agent fleet.**

mergewake is one Go binary for policy-based landing, one independent review per
PR head, main CI recovery, release manifests, verified file rollouts, and
experimental resident monitors. It stays on Git and GitHub. Hosts and model
pairings are configuration, so the same binary works across macOS, Linux, and
Linux arm64 hosts.

The shared policy rule is simple: if harm can occur before a revert lands, the
change is **blocking**. Blocking changes need green CI and every finding resolved.
Other changes can land once CI is green. `land` and `watch` default to shadow mode;
a repository must explicitly set `enabled: true` in its policy before they act.

## Quick start

Requires Go 1.27 or newer to build, GitHub API credentials to act, and `pi` or
`cmd` on a reviewer host. The binary itself requires no Go installation.

```sh
go build -o mergewake ./cmd/mergewake
./mergewake --help
./mergewake superfan --name ci
```

Create an operator-owned host config from [examples/host.yaml](examples/host.yaml).
Replace `example/project`, the install inventory, smoke command, reviewer login,
and placeholder model IDs with your values. Put a policy based on
[examples/policy.yaml](examples/policy.yaml) in the target repository. Required
check names must match the repository's actual GitHub checks. Supply a GitHub
credential through the environment variable named by `token_env`; never put its
value in either YAML file.

```sh
./mergewake land --config /path/to/host.yaml --repo example/project --pr 7 --dry-run
./mergewake land --config /path/to/host.yaml --repo example/project --pr 7 --shadow
./mergewake review --config /path/to/host.yaml --repo example/project --pr 7 \
  --author-family openai --dry-run
./mergewake watch --config /path/to/host.yaml --repo example/project --once --shadow
```

Without `--dry-run`, `review` invokes a model and posts its findings. Shadow mode
applies to landing and CI recovery only. Dry runs make no remote writes, run no
reviewer or smoke command, and write no local state. Shadow observations are
printed and appended to the host's `decisions.jsonl`.

## Components

| Command | Behavior |
| --- | --- |
| `land --repo … --pr …` | Classify paths and change kinds using policy from the current base branch; squash-merge the exact reviewed head through GitHub. |
| `review --repo … --pr …` | Claim one PR head, select a different model family, review only the diff, post inline review comments. |
| `watch --repo …` | Poll main CI, find a green-to-red boundary among squash commits using existing checks, open a revert PR, reopen the task when its API permits, and notify. `--once` inspects once. |
| `release --repo …` | Verify green main ancestry and publish an immutable manifest as a GitHub Release asset. `--commit` accepts a full SHA. |
| `rollout --repo …` | Pull the newest mergewake manifest, verify bytes, install permitted files, smoke-test, roll back on failure, and report. |
| `agent` | Poll all configured repositories for verified rollouts. `--once` runs a single poll; `--repo` selects one. |
| `status` | Read local receipts and check installed digests and modes. Returns failure on drift; supports `--repo`. |
| `superfan --name …` | Print a relay, CI, quota, or drift monitor spec. `--launch --runner /path/to/runner` sends it to a trusted session runner on stdin. |

All acting commands support `--dry-run`. There is no merge lock, mandatory
rebase, multi-round tester loop, or committed manifest/hash-pin file. A fixed
recovery branch is a durable stop after **one** revert PR; the watcher never
merges the revert itself. An operator must inspect recovery and remove that
branch before another recovery can begin. Disable GitHub's automatic deletion of
`mergewake/recovery`, or use a ruleset that preserves that branch.

Release inventories describe regular files at a verified Git commit, their
SHA256, mode, and relative install targets. Hosts separately authorize the file
inventory, install root, deny patterns, and smoke command. Successful agent polls
verify current bytes and avoid repeated writes and reports. Smoke failures restore
previous bytes and permissions and suppress retries of that release until an
operator clears its rejection receipt.

## Integrations and limits

Adapters use only APIs or CLI argument vectors:

- GitHub REST plus a read-only GraphQL query for review-thread resolution.
- `panewire emit` for notifications, with stdout and no-op alternatives.
- `handoffkeep tasks show`, `comment`, `transition`, and `doc put` for task recovery
  and rollout records; a no-op sink is available.
- Optional `scopefuel gate -m <profile>` before a configured reviewer runs.

The current handoffkeep API does not reopen terminal `merged` or `dropped` tasks.
mergewake records a recovery comment, surfaces the failed transition, and reports
the limitation in the notification. It does not claim that the task reopened.
Supporting terminal reopen requires a task sink with that capability or an upstream
handoffkeep API change.

This initial release supports linear squash history, existing CI evidence,
regular Git-backed files, and operator-managed session runners. Missing CI,
overlapping later edits during a revert, and unsupported Git file types stop the
action. There is no any-ref CI dispatcher, binary artifact builder, service
manager, fleet transport, or durable notification outbox. See
[DESIGN.md](DESIGN.md) for failure handling and [docs/config.md](docs/config.md)
for every configuration key and recovery procedure.

## Development

```sh
go vet ./...
go test -race ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/mergewake-linux-amd64 ./cmd/mergewake
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o dist/mergewake-linux-arm64 ./cmd/mergewake
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o dist/mergewake-darwin-amd64 ./cmd/mergewake
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o dist/mergewake-darwin-arm64 ./cmd/mergewake
```

CI runs vet, the race-enabled test suite, and all four CGO-disabled cross-builds.
Tests use fake GitHub, task sinks, notifiers, and local runners; they do not invoke
models or fleet services. No Go dependency is imported from an integration project.
The only direct external dependency is `gopkg.in/yaml.v3`.

Inspired by Anthropic's [Redesigning source control](https://www.anthropic.com/research/redesigning-source-control):
post-merge verification, derived data outside the source tree, review of intent,
and component-focused monitors. mergewake applies those ideas without replacing
Git or running a source-control server.

Licensed under [Apache-2.0](LICENSE).
