#!/usr/bin/env bash
# Tests scripts/release-wait-checks.sh against a stubbed gh on PATH.
#
#   bash tests/release-wait-checks.sh
#
# Every case writes a policy and per-SHA check-run / commit-status fixtures,
# then runs the script as a child. The stub also models main, compare and tags;
# those reads are refused by default so cases (a) to (l) prove they are not
# needed when the tag commit is already green or is itself main's head.
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
# Stub gh: answer API reads from fixtures in $STUB_DIR. Reads must paginate.
paginate=0
endpoint=
for arg in "$@"; do
	case $arg in repos/*) endpoint=$arg ;; esac
done
for arg in "$@"; do
	[ "$arg" = --paginate ] && paginate=1
done
for arg in "$@"; do
	case $arg in
	*/check-runs*)
		[ "$paginate" = 1 ] || { echo "stub gh: check-runs read is not paginated" >&2; exit 1; }
		sha=${endpoint#*/commits/}
		sha=${sha%%/*}
		if [ -n "${STUB_FAIL_RUNS_ONCE:-}" ] && [ ! -f "$STUB_DIR/failed-once" ]; then
			: > "$STUB_DIR/failed-once"
			echo "stub gh: transient failure" >&2
			exit 1
		fi
		fixture=$STUB_DIR/check-runs-$sha.tsv
		[ -f "$fixture" ] || fixture=$STUB_DIR/check-runs.tsv
		cat "$fixture"
		exit 0
		;;
	*/commits/*/statuses*)
		[ "$paginate" = 1 ] || { echo "stub gh: status read is not paginated" >&2; exit 1; }
		sha=${endpoint#*/commits/}
		sha=${sha%%/*}
		fixture=$STUB_DIR/statuses-$sha.tsv
		[ -f "$fixture" ] || fixture=$STUB_DIR/statuses.tsv
		cat "$fixture"
		exit 0
		;;
	*/branches/main)
		printf '%s\n' "${STUB_MAIN_SHA:?}"
		exit 0
		;;
	*/compare/*...*)
		[ "${STUB_ALLOW_COMPARE:-}" = 1 ] || { echo "stub gh: unexpected compare request: $*" >&2; exit 1; }
		[ "$paginate" = 1 ] || { echo "stub gh: compare read is not paginated" >&2; exit 1; }
		printf '%s\t%s\t%s\n' "$STUB_COMPARE_STATUS" "$STUB_TOTAL_COMMITS" "$STUB_COMPARE_SHAS"
		exit 0
		;;
	*/tags)
		[ "${STUB_ALLOW_TAGS:-}" = 1 ] || { echo "stub gh: unexpected tags request: $*" >&2; exit 1; }
		[ "$paginate" = 1 ] || { echo "stub gh: tags read is not paginated" >&2; exit 1; }
		cat "$STUB_DIR/tags.tsv"
		exit 0
		;;
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
	: > "$case_dir/tags.tsv"
	rm -f "$case_dir"/check-runs-*.tsv "$case_dir"/statuses-*.tsv
	rm -f "$case_dir/failed-once"
	main_sha=$sha
	compare_status=ahead
	total_commits=1
	compare_shas=0123456789abcdef0123456789abcdef01234567
	allow_compare=
	allow_tags=
}

policy() { printf '%s\n' "$1" > "$case_dir/.mergewake/policy.yaml"; }

# run ID NAME STATUS CONCLUSION URL
run() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >> "$case_dir/check-runs.tsv"; }
# status ID CONTEXT STATE URL
status_line() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$case_dir/statuses.tsv"; }

run_checks() { # timeout_seconds [stub_fail_runs_once]
	output=$(cd "$case_dir" &&
		PATH="$stub:$PATH" \
		STUB_DIR="$case_dir" \
		STUB_FAIL_RUNS_ONCE="${2:-}" \
		STUB_MAIN_SHA="$main_sha" \
		STUB_ALLOW_COMPARE="$allow_compare" \
		STUB_ALLOW_TAGS="$allow_tags" \
		STUB_COMPARE_STATUS="$compare_status" \
		STUB_TOTAL_COMMITS="$total_commits" \
		STUB_COMPARE_SHAS="$compare_shas" \
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
run 1 ci-ok completed success https://ci.example/ci-ok
run 2 deploy-ok completed success https://ci.example/deploy-ok
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
run 1 ci-ok completed failure https://ci.example/ci-ok
run 2 deploy-ok completed success https://ci.example/deploy-ok
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
run 1 ci-ok in_progress '' https://ci.example/ci-ok
run_checks 1
if [ "$status" -ne 0 ] && has 'timed out' && has 'ci-ok'; then
	pass "(c) pending past the bound -> exit $status naming the missing check"
else
	fail "(c) pending past the bound -> exit $status: $output"
fi

# (d) deploy_checks wins over the main set. The list is indentless, which YAML
# accepts and an ad-hoc parser would silently drop.
reset
policy 'version: 1
required_checks: [vet]
deploy_checks:
- deploy-ok
watch_checks: [ci-ok]'
run 1 ci-ok completed success https://ci.example/ci-ok
run_checks 1
if [ "$status" -ne 0 ] && has 'deploy-ok' && ! has 'ci-ok'; then
	pass "(d) deploy_checks selects deploy-ok, not the green ci-ok"
else
	fail "(d) deploy_checks selection -> exit $status: $output"
fi

# (d) A leftover main_checks key is ignored: with no deploy_checks or
# watch_checks, required_checks is the set, so the failing ci-ok is not read.
reset
policy 'version: 1
required_checks: [vet]
main_checks: [ci-ok]'
run 1 ci-ok completed failure https://ci.example/ci-ok
run 2 vet completed success https://ci.example/vet
run_checks 10
if [ "$status" -eq 0 ] && has 'vet' && ! has 'ci-ok'; then
	pass "(d) main_checks is ignored, required_checks selects vet, not the failing ci-ok"
else
	fail "(d) main_checks fall-through -> exit $status: $output"
fi

# (d) without deploy_checks, watch_checks is the set.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run 1 ci-ok completed success https://ci.example/ci-ok
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
watch_checks: [ci-ok, build-linux-amd64]'
status_line 1 ci-ok success https://ci.example/status/ci-ok
run 1 build-linux-amd64 completed success https://ci.example/run/build-linux-amd64
run_checks 10
if [ "$status" -eq 0 ] && has 'ci-ok' && has 'build-linux-amd64'; then
	pass "(e) commit status and check run both accepted -> exit 0"
else
	fail "(e) commit status and check run -> exit $status: $output"
fi

# (f) A rerun's newer success clears the same name's older failure.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run 1 ci-ok completed failure https://ci.example/old
run 2 ci-ok completed success https://ci.example/new
run_checks 10
if [ "$status" -eq 0 ] && has 'https://ci.example/new'; then
	pass "(f) newer success clears an older failure -> exit 0"
else
	fail "(f) rerun fold -> exit $status: $output"
fi

# (g) A newer in-flight run keeps the name pending even after an older success.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run 1 ci-ok completed success https://ci.example/old
run 2 ci-ok in_progress '' https://ci.example/new
run_checks 1
if [ "$status" -ne 0 ] && has 'timed out' && has 'ci-ok'; then
	pass "(g) newer pending run keeps the name pending -> exit $status"
else
	fail "(g) pending fold -> exit $status: $output"
fi

# (h) The newest status per context decides, so a rerun status can go green.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
status_line 1 ci-ok failure https://ci.example/old
status_line 2 ci-ok success https://ci.example/new
run_checks 10
if [ "$status" -eq 0 ] && has 'https://ci.example/new'; then
	pass "(h) newest commit status per context decides -> exit 0"
else
	fail "(h) status fold -> exit $status: $output"
fi

# (i) YAML anchors and a multiline flow list are decoded, not rejected.
reset
policy 'version: 1
required_checks: [vet]
deploy_checks: &d [ci-ok,
  deploy-ok]
watch_checks: *d'
run 1 ci-ok completed success https://ci.example/ci-ok
run 2 deploy-ok completed success https://ci.example/deploy-ok
run_checks 10
if [ "$status" -eq 0 ] && has 'ci-ok' && has 'deploy-ok'; then
	pass "(i) anchors and multiline flow lists decode -> exit 0"
else
	fail "(i) YAML decode -> exit $status: $output"
fi

# (j) A transient API failure is retried instead of ending the release.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run 1 ci-ok completed success https://ci.example/ci-ok
run_checks 10 1
if [ "$status" -eq 0 ] && has 'ci-ok'; then
	pass "(j) transient read failure retried -> exit 0"
else
	fail "(j) transient retry -> exit $status: $output"
fi

# (k) A status success must not settle a run of the same name that is in flight.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run 1 ci-ok in_progress '' https://ci.example/run
status_line 1 ci-ok success https://ci.example/status
run_checks 1
if [ "$status" -ne 0 ] && has 'timed out' && has 'ci-ok'; then
	pass "(k) status success never settles a pending run -> exit $status"
else
	fail "(k) pending run vs status success -> exit $status: $output"
fi

# (l) A status failure still fails the release while the run is pending.
reset
policy 'version: 1
required_checks: [vet]
watch_checks: [ci-ok]'
run 1 ci-ok in_progress '' https://ci.example/run
status_line 1 ci-ok failure https://ci.example/status
run_checks 10
if [ "$status" -ne 0 ] && has 'failure' && has 'https://ci.example/status'; then
	pass "(l) status failure still fails a pending run -> exit $status"
else
	fail "(l) pending run vs status failure -> exit $status: $output"
fi

# (m) The tag commit is unsettled, but green checks on a containing main head
# cover it when no newer release tag carries a commit in the compare list.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed cancelled https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed success https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
run_checks 10
if [ "$status" -eq 0 ] && has "$main_sha" && has 'contains tag commit'; then
	pass "(m) green checks on containing main head -> exit 0 naming the head"
else
	fail "(m) green containing head -> exit $status: $output"
fi

# (n) A newer release tag on the compare commits supersedes this release.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed cancelled https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed success https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
printf 'v1.2.4\t%s\n' "$compare_shas" > "$case_dir/tags.tsv"
run_checks 10
if [ "$status" -eq 3 ] && has 'v1.2.4' && has "$compare_shas" && has "$sha"; then
	pass "(n) newer release tag supersedes -> exit 3 naming both commits"
else
	fail "(n) newer release tag -> exit $status: $output"
fi

# (o) A non-ahead compare cannot use the head's green checks to pass.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed cancelled https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
compare_status=diverged
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed success https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
run_checks 1
if [ "$status" -eq 1 ] && has 'timed out'; then
	pass "(o) diverged head is not accepted -> exit 1 at bound"
else
	fail "(o) diverged compare -> exit $status: $output"
fi

# (p) A tag commit failure still fails even when main is green.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed failure https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed success https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
run_checks 10
if [ "$status" -eq 1 ] && has 'tag commit' && has 'failure'; then
	pass "(p) tag commit failure wins over green head -> exit 1"
else
	fail "(p) tag commit failed, head green -> exit $status: $output"
fi

# (q) A capped compare list is unknown and cannot pass via head checks.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed cancelled https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
total_commits=2
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed success https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
run_checks 1
if [ "$status" -eq 1 ] && has 'timed out'; then
	pass "(q) incomplete compare list is unknown -> exit 1 at bound"
else
	fail "(q) incomplete compare list -> exit $status: $output"
fi

# (r) A red head does not fail the tag release immediately; it remains pending.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed cancelled https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed failure https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
run_checks 1
if [ "$status" -eq 1 ] && has 'timed out'; then
	pass "(r) red containing head remains pending -> exit 1 at bound"
else
	fail "(r) red head -> exit $status: $output"
fi

# (s) A pre-release tag in the compare list does not supersede.
reset
policy 'version: 1
watch_checks: [ci-ok]'
run 1 ci-ok completed cancelled https://ci.example/tag
main_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
allow_compare=1
allow_tags=1
printf '%s\t%s\t%s\t%s\t%s\n' 2 ci-ok completed success https://ci.example/head > "$case_dir/check-runs-$main_sha.tsv"
printf 'v1.2.4-rc.1\t%s\n' "$compare_shas" > "$case_dir/tags.tsv"
run_checks 10
if [ "$status" -eq 0 ] && has "$main_sha" && ! has 'superseded'; then
	pass "(s) pre-release tag does not supersede -> exit 0"
else
	fail "(s) pre-release tag -> exit $status: $output"
fi

if [ "$failures" -ne 0 ]; then
	printf '%d case(s) failed\n' "$failures"
	exit 1
fi
printf 'all cases passed\n'
