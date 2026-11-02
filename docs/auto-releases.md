# Automatic release boundary

Each code-bearing push event to protected main gets a next-patch tag and release.
A normal protected-branch merge produces its own push event. Distinct commit
SHAs use distinct Actions groups, so a later push cannot replace another merge's
pending run. Remote atomic ref reservation and bounded refetch/retry handle
concurrent tag writers without moving any tag.

A single push containing multiple commits releases the complete tip snapshot.
Its intermediate commits do not have independent push CI results, so this flow
does not backfill independently deployable releases for those transient states.
All their changes are in the tip release. Documentation-only pushes may skip;
unknown/missing push-range evidence conservatively releases the tip. This
optimization never replaces the deployment controller's separate fail-closed
previous-deployment..release comparison and exact-release CI gate.

Only the event's checked-out SHA can be tagged. Reusing a tag requires it to
resolve to that same SHA, so its release workflow is the same Git tree as the
checked-out workflow, including the publish input. This flow does not dispatch
new inputs at unrelated historical tags.

GitHub App/PAT merges trigger push normally. A caller merging with an Actions
GITHUB_TOKEN must explicitly dispatch auto-tag.yml at main afterward; that event
is supported without introducing another credential. Both routes reject PR refs
and other branches. Repeating dispatch is safe: the tag is reused, publication is
serialized by tag, existing assets are verified byte-for-byte, and only missing
assets are uploaded. Different existing bytes fail without replacement. Checksums
are uploaded after missing binaries. Initial/zero before SHA may create a release
because docs-only skipping is optional when the range is unavailable.

## Recover an interrupted publication

Re-run only the failed publish job first: it reuses the original successful build
artifact, avoiding a rebuild with a newer Go patch. The repair verifies existing
bytes before uploading missing files, then publishes a completed draft. Do not
clobber assets or move a release tag to make a retry pass.

If the artifact has expired and a rebuild differs, use the original run's Go
version, dependency lockfiles, source commit, release tag and build flags, and
require the existing-asset comparisons to pass. If exact reproduction is not
possible, the operator must quarantine the incomplete release as a draft and
publish a fresh patch tag with a complete build. Preserve the old bytes and
receipts for audit; do not teach automatic retries to replace them silently.
