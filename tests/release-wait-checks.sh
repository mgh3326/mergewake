#!/usr/bin/env bash
# Tests scripts/release-wait-checks.sh against a stubbed gh on PATH.
#
#   bash tests/release-wait-checks.sh
#
# Every case writes a policy and check-run / commit-status fixtures, then runs
# the script as a child. The stub answers the two read endpoints from files.
set -u

here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
script=$root/scripts/release-wait-checks.sh
sha=5b78e56bd34e19b77ab93bbc16d45b68ce1a93be

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

stub=$tmp/stub
mkdir -p "$stub"
cat > "$stub/gh" <<'STUB'
#!/bin/sh
# Stub gh: answer the two API reads from the fixtures in $STUB_DIR.
for arg in "$@"; do
	case $arg in
	*/check-runs*) cat "$STUB_DIR/check-runs.tsv"; exit 0 ;;
	*/commits/*/status*) cat "$STUB_DIR/statuses.tsv"; exit 0 ;;
	esac
done
echo "stub gh: unexpected request: $*" >&2
exit 1
STUB
chmod +x "$stub/gh"

case_dir=$tmp/case
mkdir -p "$case_dir/.mergewake"
: > "$case_dir/check-runs.tsv"
: > "$case_dir/statuses.tsv"

failures=0
pass() { printf 'PASS: %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*"; failures=$((failures + 1)); }

reset() {
	: > "$case_dir/check-runs.tsv"
	: > "$case_dir/statuses.tsv"
}

policy() { printf '%s\n' "$1" > "$case_dir/.mergewake/policy.yaml"; }

run() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$case_dir/check-runs.tsv"; }
status_line() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$case_dir/statuses.tsv"; }

run_checks() { # timeout_seconds
	output=$(cd "$case_dir" &&
		PATH="$stub:$PATH" \
		STUB_DIR="$case_dir" \
		GITHUB_REPOSITORY="example/repo" \
		RELEASE_CHECKS_TIMEOUT_SECONDS="$1" \
		RELEASE_CHECKS_POLL_SECONDS=0.1 \
		bash "$script" "$sha" 2>&1)
	status=$?
}

has() { printf '%s' "$output" | grep -Fq -- "$1"; }

# (a) Every named check is green.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok, deploy-ok]'
run ci-ok completed success https://ci.example/ci-ok
run deploy-ok completed success https://ci.example/deploy-ok
run_checks 10
if [ "$status" -eq 0 ] && has 'ci-ok' && has 'deploy-ok'; then
	pass "(a) all named checks success -> exit 0"
else
	fail "(a) all named checks success -> exit $status: $output"
fi

# (b) A failing check fails the wait and is named with its URL.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok, deploy-ok]'
run ci-ok completed failure https://ci.example/ci-ok
run deploy-ok completed success https://ci.example/deploy-ok
run_checks 10
if [ "$status" -ne 0 ] && has 'ci-ok' && has 'https://ci.example/ci-ok'; then
	pass "(b) failing check -> exit $status naming it and its URL"
else
	fail "(b) failing check -> exit $status: $output"
fi

# (c) A check still pending at the bound fails the wait and names it.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run ci-ok in_progress '' https://ci.example/ci-ok
run_checks 1
if [ "$status" -ne 0 ] && has 'timed out' && has 'ci-ok'; then
	pass "(c) pending past the bound -> exit $status naming the missing check"
else
	fail "(c) pending past the bound -> exit $status: $output"
fi

# (d) deploy_checks wins over the main set.
reset
policy 'version: 1
required_checks: [vet]
deploy_checks:
  - deploy-ok
watch_checks: [ci-ok]'
run ci-ok completed success https://ci.example/ci-ok
run_checks 1
if [ "$status" -ne 0 ] && has 'deploy-ok' && ! has 'ci-ok'; then
	pass "(d) deploy_checks selects deploy-ok, not the green ci-ok"
else
	fail "(d) deploy_checks selection -> exit $status: $output"
fi

# (d) without deploy_checks, main_checks is the set.
reset
policy 'version: 1
required_checks: [vet]
main_checks: [ci-ok]'
run deploy-ok completed success https://ci.example/deploy-ok
run_checks 1
if [ "$status" -ne 0 ] && has 'ci-ok' && ! has 'deploy-ok'; then
	pass "(d) main_checks selects ci-ok, not the green deploy-ok"
else
	fail "(d) main_checks selection -> exit $status: $output"
fi

# (d) without main_checks, watch_checks is the set.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run ci-ok completed success https://ci.example/ci-ok
run_checks 10
if [ "$status" -eq 0 ] && has 'ci-ok'; then
	pass "(d) watch_checks fallback -> exit 0"
else
	fail "(d) watch_checks fallback -> exit $status: $output"
fi

# (e) A commit status and a check run are both accepted.
reset
policy 'version: 1
required_checks: [vet]
main_checks: [ci-ok, build-linux-amd64]'
status_line ci-ok success https://ci.example/status/ci-ok
run build-linux-amd64 completed success https://ci.example/run/build-linux-amd64
run_checks 10
if [ "$status" -eq 0 ] && has 'ci-ok' && has 'build-linux-amd64'; then
	pass "(e) commit status and check run both accepted -> exit 0"
else
	fail "(e) commit status and check run -> exit $status: $output"
fi

if [ "$failures" -ne 0 ]; then
	printf '%d case(s) failed\n' "$failures"
	exit 1
fi
printf 'all cases passed\n'
