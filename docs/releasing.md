# Releasing mergewake

## Cut a release

1. Merge the change to `main`.
2. Fetch `main` so that `origin/main` is the merge commit, then tag it and
   push the tag:
   ```sh
   git fetch origin main
   git tag vMAJOR.MINOR.PATCH origin/main
   git push origin vMAJOR.MINOR.PATCH
   ```
3. `.github/workflows/release.yml` builds CGO-disabled (`CGO_ENABLED=0`, `-trimpath`) binaries named `mergewake-<os>-<arch>`. It builds four targets: linux/amd64,
   linux/arm64, darwin/amd64 and darwin/arm64. It stamps the tag as the
   version and writes `SHA256SUMS`. It checks that `mergewake --version`
   prints the tag. It runs the tests. Before publishing, it waits for the tag
   commit's checks named by `.mergewake/policy.yaml` at the tag
   (`deploy_checks`, else `watch_checks`, else `required_checks`) and publishes
   only when every one is green; a name may be a check run or a commit status.
   A failing check at the tag fails the release with its name and URL. While
   tag checks are pending, a newer release tag on main that contains the tag
   commit supersedes this run, so it stops without publishing. Also, once the
   tag commit's checks are green, or main's head is green and contains it, a
   higher stable release tag on the tag commit itself supersedes this run: versions
   compare by number, and a lower or equal tag or a `-suffix` tag never does. If main has no
   newer release tag and its head contains the tag commit, green policy checks
   on that head let this run publish. Pending checks that reach the bound
   (`RELEASE_CHECKS_TIMEOUT_SECONDS`, default 60 minutes) fail with what is
   missing, so a later rerun can publish. Then it publishes the GitHub Release
   with `GITHUB_TOKEN`.

Pushing a test tag `v0.0.0-<anything>` (for example `v0.0.0-test.1`) runs
the same build and checks on a real tag, publishes nothing and does not wait.

Every pull request runs the same build and checks for the dry-run tag
`v0.0.0-dryrun.<run>`, and publishes nothing. To run the same check locally:

```sh
sh scripts/build-release.sh v0.0.0-dryrun.1 dist/release
sh scripts/verify-release.sh v0.0.0-dryrun.1 dist/release
```

A tag with a `-suffix` (`v1.2.3-rc.1`) builds the same way and is published
as a *pre-release*. Hosts that follow `latest` skip pre-releases. Pin the tag
on a host to try it there.

## How hosts get it

mergewake does not deploy. No workflow connects to a host, and this repository
holds no updater: each host installs a release asset by its own process.

## Roll back one host

Copy the previous binary to a temporary file in the same directory, then rename
it over `mergewake`. A rename works while the service still runs the old file,
whereas copying onto a running executable fails on Linux with `ETXTBSY`:

```sh
cp -p ~/.local/bin/mergewake.prev ~/.local/bin/mergewake.new
mv -f ~/.local/bin/mergewake.new ~/.local/bin/mergewake
```

Restart the owning service and check that it is active and that the version
command prints the old tag. Then keep that tag pinned in the host's own updater,
so the next run does not reinstall the bad release.
