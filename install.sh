#!/bin/sh
# Download a public release, verify its checksum, and atomically install it.
set -eu

tag=latest
bin_dir="${HOME}/.local/bin"
usage() { echo 'Usage: install.sh [TAG|latest] [--tag TAG] [--bin-dir DIR]'; }
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --tag) [ "$#" -ge 2 ] || { usage >&2; exit 1; }; tag=$2; shift 2 ;;
    --bin-dir) [ "$#" -ge 2 ] || { usage >&2; exit 1; }; bin_dir=$2; shift 2 ;;
    --*) usage >&2; exit 1 ;;
    *) tag=$1; shift ;;
  esac
done
case "$tag" in ''|*[!A-Za-z0-9._-]*) echo 'Invalid release tag' >&2; exit 1 ;; esac
case "$(uname -s)" in Linux) os=linux ;; Darwin) os=darwin ;; *) echo 'Unsupported OS' >&2; exit 1 ;; esac
case "$(uname -m)" in x86_64|amd64) arch=amd64 ;; aarch64|arm64) arch=arm64 ;; *) echo 'Unsupported architecture' >&2; exit 1 ;; esac
asset="mergewake-$os-$arch"
base=https://github.com/mgh3326/mergewake/releases
if [ "$tag" = latest ]; then url="$base/latest/download"; else url="$base/download/$tag"; fi
scratch=$(mktemp -d)
staged=
trap 'rm -rf "$scratch"; if [ -n "$staged" ]; then rm -f "$staged"; fi' EXIT
trap 'exit 1' HUP INT TERM
curl --fail --silent --show-error --location --proto '=https' "$url/SHA256SUMS" -o "$scratch/SHA256SUMS"
curl --fail --silent --show-error --location --proto '=https' "$url/$asset" -o "$scratch/$asset"
expected=$(awk -v name="$asset" '($2==name || $2=="*"name) {n++; value=$1} END {if(n!=1) exit 1; print value}' "$scratch/SHA256SUMS")
case "$expected" in *[!a-fA-F0-9]*|'') echo 'Invalid checksum' >&2; exit 1 ;; esac
[ "${#expected}" -eq 64 ] || { echo 'Invalid checksum length' >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$scratch/$asset" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$scratch/$asset" | awk '{print $1}')
fi
[ "$actual" = "$expected" ] || { echo 'Checksum mismatch; install aborted' >&2; exit 1; }
mkdir -p "$bin_dir"
staged=$(mktemp "$bin_dir/.mergewake.XXXXXX")
cat "$scratch/$asset" > "$staged"
chmod 0755 "$staged"
mv -f "$staged" "$bin_dir/mergewake"
staged=
printf 'Installed %s (%s/%s) to %s/mergewake\n' "$tag" "$os" "$arch" "$bin_dir"
