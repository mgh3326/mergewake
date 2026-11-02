#!/bin/sh
# Resume interrupted publication without replacing any existing release bytes.
set -eu
tag=$1
dir=$2
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
existing=$(gh release view "$tag" --repo "$GITHUB_REPOSITORY" --json assets --jq '.assets[].name')
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
