#!/bin/sh
# Resume interrupted publication without replacing any existing release bytes.
set -eu
tag=$1
dir=$2
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
# Treat the build manifest as the complete immutable artifact contract.
python3 - "$dir" > "$tmp/expected" <<'PYASSETS'
import hashlib
from pathlib import Path
import re
import sys
root = Path(sys.argv[1])
expected = {'SHA256SUMS'}
for line in (root / 'SHA256SUMS').read_text().splitlines():
    match = re.fullmatch(r'([0-9a-fA-F]{64})  ([A-Za-z0-9_.-]+)', line)
    if not match or match[2] in expected:
        raise SystemExit('invalid or duplicate checksum entry')
    digest, name = match.groups()
    expected.add(name)
    asset = root / name
    if not asset.is_file() or asset.is_symlink() or hashlib.sha256(asset.read_bytes()).hexdigest() != digest.lower():
        raise SystemExit('missing or corrupt build asset: ' + name)
if len(expected) == 1 or {p.name for p in root.iterdir()} != expected:
    raise SystemExit('build assets do not match checksum manifest')
print('\n'.join(sorted(expected)))
PYASSETS
existing=$(gh release view "$tag" --repo "$GITHUB_REPOSITORY" --json assets --jq '.assets[].name')
# Refuse unrelated remote assets before any upload or publication.
printf '%s\n' "$existing" | while IFS= read -r name; do
  [ -z "$name" ] && continue
  grep -Fxq "$name" "$tmp/expected" || { echo "unexpected release asset: $name" >&2; exit 1; }
done
# Validate all existing expected assets before uploading anything.
for file in "$dir"/*; do
  name=${file##*/}
  if printf '%s\n' "$existing" | grep -Fxq "$name"; then
    gh release download "$tag" --repo "$GITHUB_REPOSITORY" --pattern "$name" --dir "$tmp"
    cmp -s "$file" "$tmp/$name" || { echo "existing release asset differs: $name" >&2; exit 1; }
  fi
done
# Checksums appear only after every binary exists. Existing assets are immutable.
for file in "$dir"/*; do
  name=${file##*/}
  [ "$name" = SHA256SUMS ] && continue
  if ! printf '%s\n' "$existing" | grep -Fxq "$name"; then
    gh release upload "$tag" "$file" --repo "$GITHUB_REPOSITORY"
  fi
done
if ! printf '%s\n' "$existing" | grep -Fxq SHA256SUMS; then
  gh release upload "$tag" "$dir/SHA256SUMS" --repo "$GITHUB_REPOSITORY"
fi

# Re-read the remote set after uploads and require exact completeness.
actual=$(gh release view "$tag" --repo "$GITHUB_REPOSITORY" --json assets --jq '.assets[].name')
printf '%s\n' "$actual" | LC_ALL=C sort > "$tmp/actual"
LC_ALL=C sort "$tmp/expected" > "$tmp/sorted-expected"
cmp -s "$tmp/sorted-expected" "$tmp/actual" || { echo "release asset set is incomplete" >&2; exit 1; }

# An interrupted gh release create may leave a draft. Publish only after the
# complete expected asset set has been verified or uploaded successfully.
draft=$(gh release view "$tag" --repo "$GITHUB_REPOSITORY" --json isDraft --jq .isDraft)
case "$draft" in
  true) gh release edit "$tag" --repo "$GITHUB_REPOSITORY" --draft=false ;;
  false) ;;
  *) echo "release draft state is unavailable" >&2; exit 1 ;;
esac
