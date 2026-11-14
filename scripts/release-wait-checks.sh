#!/bin/sh
# Fail publication unless every check named by the repository policy at SHA is
# green, or main contains SHA and has green policy checks. Exit 3 means a newer
# release tag on main already contains SHA, or a higher stable release tag points
# at SHA; either supersedes this release.
#
# The names are read from POLICY_FILE at the tag by decoding the YAML with the
# repository's own gopkg.in/yaml.v3, then selected in mergewake's DeploySet
# order: deploy_checks, else watch_checks, else required_checks. A name may be reported as a check run or as a commit status;
# both sources are folded the way internal/github's gate folds them: the latest
# result per name wins, so a rerun clears an older failure and an in-flight run
# keeps the name pending.
#
# usage: scripts/release-wait-checks.sh SHA [POLICY_FILE] (exit 0 green, 1
# failed or timed out, 2 usage, 3 superseded by a release tag)
#        scripts/release-wait-checks.sh --recheck-supersession SHA (exit 0
#        not superseded, 1 the tags read failed, 2 usage, 3 superseded)
#
# GITHUB_REPOSITORY must name the repository (owner/name) and GH_TOKEN
# authenticates gh. RELEASE_CHECKS_TIMEOUT_SECONDS bounds the wait (default
# 3600) and RELEASE_CHECKS_POLL_SECONDS sets the poll interval (default 30).
# RELEASE_TAG names the tag being released: vMAJOR.MINOR.PATCH with an optional
# -suffix. A -suffix tag is a pre-release and skips the same-commit check.
# The script must run from the repository checkout: it compiles a small helper
# with `go run` so the policy is read with the repository's YAML library.
set -eu

usage() {
	echo "usage: $0 SHA [POLICY_FILE]" >&2
	exit 2
}

fail() {
	echo "release-wait-checks: $*" >&2
	exit 1
}

mode="wait"
if [ "${1:-}" = --recheck-supersession ]; then
	mode=recheck
	shift
fi
if [ "$mode" = recheck ]; then
	[ "$#" -eq 1 ] || usage
elif [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
	usage
fi
sha=$1
policy=${2:-.mergewake/policy.yaml}
repo=${GITHUB_REPOSITORY:-}
timeout=${RELEASE_CHECKS_TIMEOUT_SECONDS:-3600}
poll=${RELEASE_CHECKS_POLL_SECONDS:-30}
release_tag=${RELEASE_TAG:-}

[ -n "$repo" ] || fail "GITHUB_REPOSITORY is not set"
[ -n "$release_tag" ] || fail "RELEASE_TAG is not set"
printf '%s\n' "$release_tag" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$' ||
	fail "RELEASE_TAG must be vMAJOR.MINOR.PATCH with an optional -suffix: $release_tag"
if [ "$mode" = wait ]; then
	[ -f "$policy" ] || fail "policy file not found: $policy"
fi
case $timeout in
	'' | *[!0-9]*) fail "RELEASE_CHECKS_TIMEOUT_SECONDS must be a whole number of seconds" ;;
esac
[ "$timeout" -gt 0 ] || fail "RELEASE_CHECKS_TIMEOUT_SECONDS must be positive"
if [ "$mode" = wait ]; then
	command -v go >/dev/null 2>&1 || fail "go is required to decode $policy"
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
module_root=$(dirname "$script_dir")
policy_abs=
if [ "$mode" = wait ]; then
	policy_abs=$(cd "$(dirname "$policy")" && pwd)/$(basename "$policy")
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM

# The helper reads the policy with yaml.v3 and folds the two evidence files the
# same way internal/github/client.go foldChecks does. It prints one
# "name<TAB>state<TAB>url" record per selected check, in policy order.
cat > "$work/verdict.go" <<'GOVERDICT'
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type policy struct {
	Deploy   []string `yaml:"deploy_checks"`
	Watch    []string `yaml:"watch_checks"`
	Required []string `yaml:"required_checks"`
}

type checkRun struct {
	id         int64
	name       string
	status     string
	conclusion string
	url        string
}

type commitStatus struct {
	id      int64
	context string
	state   string
	url     string
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func parseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func records(path string) ([][]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out [][]string
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		out = append(out, strings.Split(line, "\t"))
	}
	return out, nil
}

func readRuns(path string) ([]checkRun, error) {
	rows, err := records(path)
	if err != nil {
		return nil, err
	}
	runs := make([]checkRun, 0, len(rows))
	for _, f := range rows {
		if len(f) < 4 {
			return nil, fmt.Errorf("%s: malformed check-run record %q", path, strings.Join(f, "\\t"))
		}
		r := checkRun{id: parseInt(f[0]), name: f[1], status: f[2], conclusion: f[3]}
		if len(f) > 4 {
			r.url = f[4]
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func readStatuses(path string) ([]commitStatus, error) {
	rows, err := records(path)
	if err != nil {
		return nil, err
	}
	out := make([]commitStatus, 0, len(rows))
	for _, f := range rows {
		if len(f) < 3 {
			return nil, fmt.Errorf("%s: malformed commit-status record %q", path, strings.Join(f, "\\t"))
		}
		s := commitStatus{id: parseInt(f[0]), context: f[1], state: f[2]}
		if len(f) > 3 {
			s.url = f[3]
		}
		out = append(out, s)
	}
	return out, nil
}

// fold mirrors internal/github/client.go foldChecks with preserveVerdicts
// false: a skipped or neutral run never overrides a sibling that ran, the
// highest run ID decides a name, the highest status ID decides a context, and
// two decisive sources that disagree are an error, not a guess. Release is
// stricter in one place: a status success never settles a run that is still in
// flight, because publishing over a pending run is the race this gate exists
// to prevent.
func fold(runs []checkRun, statuses []commitStatus) (map[string]string, map[string]string, error) {
	states := map[string]string{}
	urls := map[string]string{}
	ids := map[string]int64{}
	ran := map[string]bool{}
	for _, r := range runs {
		idle := r.status == "completed" && (r.conclusion == "skipped" || r.conclusion == "neutral")
		holdsNothing := r.conclusion == "skipped" || r.conclusion == "neutral"
		if idle && (ran[r.name] || (holdsNothing && states[r.name] == "cancelled")) {
			continue
		}
		if !idle && !ran[r.name] {
			ran[r.name] = true
			ids[r.name] = 0
		}
		if r.id < ids[r.name] {
			continue
		}
		ids[r.name] = r.id
		state := "pending"
		if r.status == "completed" {
			switch r.conclusion {
			case "success":
				state = "success"
			case "cancelled":
				state = "cancelled"
			case "skipped", "neutral":
				state = "skipped"
			case "failure", "timed_out", "action_required", "startup_failure", "stale":
				state = "failure"
			}
		}
		states[r.name] = state
		urls[r.name] = r.url
	}

	latest := map[string]commitStatus{}
	for _, s := range statuses {
		if latest[s.context].id < s.id {
			latest[s.context] = s
		}
	}
	for name, s := range latest {
		state := "pending"
		switch s.state {
		case "success":
			state = "success"
		case "failure", "error":
			state = "failure"
		}
		if old, ok := states[name]; ok && old != state {
			decisive := func(x string) bool { return x == "success" || x == "failure" }
			if decisive(old) && decisive(state) {
				return nil, nil, fmt.Errorf("check %q: run and status disagree", name)
			}
			if decisive(old) {
				continue
			}
			// A run still in flight is newer evidence than a status with the
			// same name: a status success must not publish over a pending run.
			// A cancelled or skipped run holds no verdict, so a status may
			// settle it as the gate allows, and a status failure still fails.
			if old == "pending" && state == "success" {
				continue
			}
		}
		states[name] = state
		urls[name] = s.url
	}
	return states, urls, nil
}

func main() {
	if len(os.Args) != 4 {
		fatal("usage: verdict POLICY RUNS STATUSES")
	}
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		fatal("%v", err)
	}
	var p policy
	if err := yaml.Unmarshal(b, &p); err != nil {
		fatal("%s: %v", os.Args[1], err)
	}
	names := p.Deploy
	if len(names) == 0 {
		names = p.Watch
	}
	if len(names) == 0 {
		names = p.Required
	}
	if len(names) == 0 {
		fatal("%s: no deploy_checks, watch_checks or required_checks", os.Args[1])
	}
	seen := map[string]bool{}
	for _, name := range names {
		if strings.TrimSpace(name) == "" || seen[name] {
			fatal("%s: check names must be nonempty and unique", os.Args[1])
		}
		seen[name] = true
	}
	runs, err := readRuns(os.Args[2])
	if err != nil {
		fatal("%v", err)
	}
	statuses, err := readStatuses(os.Args[3])
	if err != nil {
		fatal("%v", err)
	}
	states, urls, err := fold(runs, statuses)
	if err != nil {
		fatal("%v", err)
	}
	for _, name := range names {
		state := "absent"
		if s, ok := states[name]; ok {
			switch s {
			case "success":
				state = "success"
			case "failure":
				state = "failure"
			default:
				state = "pending"
			}
		}
		fmt.Printf("%s\t%s\t%s\n", name, state, urls[name])
	}
}
GOVERDICT

# read_tags fills $work/tags with "name<TAB>sha" for every tag. On failure the
# last line of $work/error names the tags read, so the caller reports it.
read_tags() {
	if gh api --paginate "repos/$repo/tags" \
		--jq '.[] | [.name, .commit.sha] | @tsv' \
		> "$work/tags" 2> "$work/error"; then
		return 0
	fi
	err=$(tail -n 1 "$work/error" 2>/dev/null || true)
	printf 'cannot read the tags: %s\n' "${err:-gh api failed}" > "$work/error"
	return 1
}

# strip_zeros N: print decimal N without leading zeros; an all-zero N prints 0.
strip_zeros() {
	s=$1
	while [ "${#s}" -gt 1 ] && [ "${s#0}" != "$s" ]; do
		s=${s#0}
	done
	printf '%s\n' "$s"
}

# num_eq A B and num_gt A B compare decimal strings of any length, so a tag
# component past the shell's integer range still orders. Non-digit input is
# never equal or greater, the same refusal the integer tests gave.
num_eq() {
	case $1 in '' | *[!0-9]*) return 1 ;; esac
	case $2 in '' | *[!0-9]*) return 1 ;; esac
	[ "$(strip_zeros "$1")" = "$(strip_zeros "$2")" ]
}

num_gt() {
	case $1 in '' | *[!0-9]*) return 1 ;; esac
	case $2 in '' | *[!0-9]*) return 1 ;; esac
	a=$(strip_zeros "$1")
	b=$(strip_zeros "$2")
	if [ "${#a}" -gt "${#b}" ]; then
		return 0
	fi
	if [ "${#a}" -lt "${#b}" ]; then
		return 1
	fi
	# shellcheck disable=SC3012 # dash, bash and ash all order equal-length strings with \>
	[ "$a" \> "$b" ]
}

# version_gt TAG BASE: exit 0 when stable vMAJOR.MINOR.PATCH TAG is higher than
# BASE, compared by number, so v1.10.0 is higher than v1.9.0.
version_gt() {
	IFS=. read -r a1 a2 a3 <<EOF
${1#v}
EOF
	IFS=. read -r b1 b2 b3 <<EOF
${2#v}
EOF
	num_gt "$a1" "$b1" && return 0
	num_eq "$a1" "$b1" || return 1
	num_gt "$a2" "$b2" && return 0
	num_eq "$a2" "$b2" || return 1
	num_gt "$a3" "$b3"
}

# check_same_commit exits 3 when a stable tag higher than RELEASE_TAG points at
# the tag commit. A -suffix RELEASE_TAG is a pre-release and skips this check.
# It reads $work/tags, which read_tags filled.
check_same_commit() {
	case $release_tag in *-*) return 0 ;; esac
	while IFS="$(printf '\t')" read -r tag tag_sha; do
		[ "$tag_sha" = "$sha" ] || continue
		printf '%s\n' "$tag" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' || continue
		version_gt "$tag" "$release_tag" || continue
		printf 'release-wait-checks: superseded by %s at %s; tag commit is %s\n' \
			"$tag" "$tag_sha" "$sha"
		exit 3
	done < "$work/tags"
}

# --recheck-supersession reads the tags once and runs check_same_commit, for
# the publish step to repeat the supersession check right before creating the
# release. A failed tags read fails; it is never read as "not superseded".
if [ "$mode" = recheck ]; then
	if ! read_tags; then
		fail "$(cat "$work/error")"
	fi
	check_same_commit
	exit 0
fi

check_main_coverage() {
	if ! gh api "repos/$repo/branches/main" --jq '.commit.sha' > "$work/head" 2> "$work/error"; then
		return 2
	fi
	head=$(cat "$work/head")
	[ "$head" != "$sha" ] || return 1

	if ! gh api --paginate --slurp "repos/$repo/compare/$sha...$head" \
		--jq '[.[0].status, (.[0].total_commits | tostring), ([.[].commits[]?.sha] | join(","))] | @tsv' \
		> "$work/compare" 2> "$work/error"; then
		return 2
	fi
	compare_status=
	total_commits=
	compare_shas=
	IFS="$(printf '\t')" read -r compare_status total_commits compare_shas < "$work/compare" || true
	compare_count=0
	if [ -n "${compare_shas:-}" ]; then
		compare_count=$(printf '%s\n' "$compare_shas" | awk -F, '{print NF}')
	fi
	case ${total_commits:-} in
		'' | *[!0-9]*) return 1 ;;
		*) [ "$compare_count" -ge "$total_commits" ] || return 1 ;;
	esac
	[ "${compare_status:-}" = ahead ] || return 1

	if ! read_tags; then
		return 2
	fi
	while IFS="$(printf '\t')" read -r tag tag_sha; do
		case $tag in
			v0.0.0 | *-*) continue ;;
			*) ;;
		esac
		printf '%s\n' "$tag" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' || continue
		[ "$tag_sha" != "$sha" ] || continue
		case ,${compare_shas:-}, in
			*,"$tag_sha",*)
				printf 'release-wait-checks: superseded by %s at %s; tag commit is %s\n' \
					"$tag" "$tag_sha" "$sha"
				exit 3
				;;
		esac
	done < "$work/tags"

	if ! gh api --paginate "repos/$repo/commits/$head/check-runs?filter=latest&per_page=100" \
		--jq '.check_runs[] | [.id, .name, .status, (.conclusion // ""), (.details_url // "")] | @tsv' \
		> "$work/head-runs" 2> "$work/error" ||
		! gh api --paginate "repos/$repo/commits/$head/statuses?per_page=100" \
			--jq '.[] | [.id, .context, .state, (.target_url // "")] | @tsv' \
			> "$work/head-statuses" 2>> "$work/error"; then
		return 2
	fi
	if ! (cd "$module_root" && go run "$work/verdict.go" "$policy_abs" "$work/head-runs" "$work/head-statuses" > "$work/head-verdicts" 2>> "$work/error"); then
		return 3
	fi
	head_green=1
	while IFS="$(printf '\t')" read -r name state url; do
		[ "$state" = success ] || head_green=0
	done < "$work/head-verdicts"
	if [ "$head_green" -eq 1 ]; then
		check_same_commit
		printf 'release-wait-checks: main head %s contains tag commit %s; all %s policy checks are green\n' \
			"$head" "$sha" "$(wc -l < "$work/head-verdicts" | tr -d ' ')"
		exit 0
	fi
	return 1
}

start=$(date +%s)
last_error=
missing=
while :; do
	: > "$work/error"
	coverage_error=
	if gh api --paginate "repos/$repo/commits/$sha/check-runs?filter=latest&per_page=100" \
		--jq '.check_runs[] | [.id, .name, .status, (.conclusion // ""), (.details_url // "")] | @tsv' \
		> "$work/runs" 2> "$work/error" &&
		gh api --paginate "repos/$repo/commits/$sha/statuses?per_page=100" \
			--jq '.[] | [.id, .context, .state, (.target_url // "")] | @tsv' \
			> "$work/statuses" 2>> "$work/error"; then
		last_error=
		if ! (cd "$module_root" && go run "$work/verdict.go" "$policy_abs" "$work/runs" "$work/statuses" > "$work/verdicts" 2>> "$work/error"); then
			fail "cannot judge the checks in $policy: $(cat "$work/error")"
		fi
		failed=
		missing=
		while IFS="$(printf '\t')" read -r name state url; do
			case $state in
			success)
				printf 'release-wait-checks: %s: success%s\n' "$name" "${url:+ ($url)}"
				;;
			failure)
				printf 'release-wait-checks: %s: failure%s\n' "$name" "${url:+ ($url)}" >&2
				failed=1
				;;
			*)
				missing="$missing $name ($state)"
				;;
			esac
		done < "$work/verdicts"
		[ -z "$failed" ] || fail "the tag commit's checks are not green for $sha"
		if [ -z "$missing" ]; then
			# All tag checks are green: publish unless a higher release tag
			# already points at this commit. A failed tags read is retried,
			# never read as a pass.
			if read_tags; then
				check_same_commit
				printf 'release-wait-checks: all %s policy checks are green for %s\n' \
					"$(wc -l < "$work/verdicts" | tr -d ' ')" "$sha"
				exit 0
			fi
			last_error=$(tail -n 1 "$work/error" 2>/dev/null || true)
		# Only pending tag checks enter the main coverage path. A failed API
		# read leaves the result unknown for this poll and is retried next time.
		elif check_main_coverage; then
			:
		else
			coverage_result=$?
			case $coverage_result in
				2) coverage_error=$(tail -n 1 "$work/error" 2>/dev/null || true) ;;
				3) coverage_error="cannot judge main-head checks: $(cat "$work/error" 2>/dev/null || true)" ;;
			esac
			[ -n "$coverage_error" ] || [ "$coverage_result" -eq 1 ] || coverage_error="gh api failed"
		fi
	else
		# A transient API failure is retried until the bound, never read as a
		# missing check or as permission to publish stale verdicts.
		last_error=$(tail -n 1 "$work/error" 2>/dev/null || true)
		[ -n "$last_error" ] || last_error="gh api failed"
	fi
	now=$(date +%s)
	if [ $((now - start)) -ge "$timeout" ]; then
		if [ -n "$last_error" ]; then
			fail "could not read the checks for $sha within ${timeout}s: $last_error"
		fi
		if [ -n "$coverage_error" ]; then
			fail "timed out after ${timeout}s waiting for:$missing; main coverage error: $coverage_error"
		fi
		fail "timed out after ${timeout}s waiting for:$missing"
	fi
	sleep "$poll"
done
