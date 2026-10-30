# Container image

`.github/workflows/image.yml` builds the daemon image from the repository
`Dockerfile`. Pull requests build linux/amd64 and smoke-test it; a push to
`main` also pushes linux/amd64 and linux/arm64 to
`ghcr.io/<owner>/mergewake` tagged with the full commit SHA and `main`. The
workflow has no deploy step: rolling the image out is the mergewake agent's
job (a `kind: container` install, see [examples/container.yaml](../examples/container.yaml)).

The image holds `mergewake` (`mergewake commit` prints the built SHA), `git`,
`sh`, `ssh` and CA certificates, and runs as uid/gid `65532` with
`HOME=/home/mergewake`. Its default command is `mergewake daemon`, reading
`~/.config/mergewake/config.yaml`. Nothing secret is in the image; everything
below is supplied at run time.

## What the container needs

The container is meant for the GitHub-only roles with
`review.executor: runtug` (see [config.md](config.md#runtug-executor)): it
needs no reviewer CLI logins. Roles that act on the host itself — command or
container installs (`rollout`), and `reap` of local panes through `herdr` — still
belong to a host install.

| Mount or variable | Container path | Mode | Purpose |
| --- | --- | --- | --- |
| `GITHUB_TOKEN` (or the name in `token_env`) | environment | — | GitHub API. Required: the image has no `gh` for the token fallback. Pass it from a secret store or an env file outside the repository. |
| mergewake config | `/home/mergewake/.config/mergewake/config.yaml` | ro | Host config. Paths in it are container paths. |
| state dir | `/home/mergewake/.local/state/mergewake` (default `state_dir`) | rw, persistent | Ledgers, audit log, sent-notice records and the review checkouts (`review.runtug.checkouts`). |
| `runtug`, `panewire`, `handoffkeep` | `/usr/local/bin/<name>` | ro | Static linux binaries for the image's architecture, mounted from the host install. |
| runtug config | `/home/mergewake/.config/wrk/hosts.toml` | ro | Host table and placement; remote hosts are reached over ssh. |
| hub, lane and quota-cache credential files that config names | the same paths under `/home/mergewake` | ro | panewire `--hub-token-env` files, handoffkeep's `config.env`; mode 0600, owned by uid 65532. |
| ssh key and `known_hosts` | `/home/mergewake/.ssh/` | ro | runtug placement on remote hosts and `reap` of configured `hosts`. |
| runtug jobs dir | the path in `review.runtug.jobs` | rw | Reviewer briefs, findings and reports. The pane writes its findings file here, so the pane's host must see the same directory. |
| repository checkouts | each `repos[].root` | rw | Clones the runtug executor fetches PR heads into and adds review worktrees from. Their `origin` must be fetchable with the mounted credentials. |

Example (paths are placeholders):

```sh
docker run -d --name mergewake --restart unless-stopped \
  --env-file /path/outside/repo/mergewake.env \
  -v /srv/mergewake/config.yaml:/home/mergewake/.config/mergewake/config.yaml:ro \
  -v /srv/mergewake/state:/home/mergewake/.local/state/mergewake \
  -v /srv/mergewake/bin/runtug:/usr/local/bin/runtug:ro \
  -v /srv/mergewake/bin/panewire:/usr/local/bin/panewire:ro \
  -v /srv/mergewake/bin/handoffkeep:/usr/local/bin/handoffkeep:ro \
  -v /srv/mergewake/wrk:/home/mergewake/.config/wrk:ro \
  -v /srv/mergewake/ssh:/home/mergewake/.ssh:ro \
  -v /srv/mergewake/jobs:/srv/mergewake/jobs \
  -v /srv/mergewake/repos:/srv/mergewake/repos \
  ghcr.io/<owner>/mergewake:<sha>
```

Mounted directories must be readable (and, where marked rw, writable) by uid
65532. Pin the image by digest when rolling it out.
