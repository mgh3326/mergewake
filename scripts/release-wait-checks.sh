#!/bin/sh
# Fail publication unless every check named by the repository policy at SHA is
# green.
#
# The names are read from POLICY_FILE at the tag, in the same order as
# mergewake's DeploySet: deploy_checks, else main_checks, else watch_checks,
# else required_checks. A name may be reported as a check run or as a commit
# status; either is accepted.
#
# usage: scripts/release-wait-checks.sh SHA [POLICY_FILE]
#
# GITHUB_REPOSITORY must name the repository (owner/name) and GH_TOKEN
# authenticates gh. RELEASE_CHECKS_TIMEOUT_SECONDS bounds the wait (default
# 3600) and RELEASE_CHECKS_POLL_SECONDS sets the poll interval (default 30).
set -eu

usage() {
	echo "usage: $0 SHA [POLICY_FILE]" >&2
	exit 2
}

fail() {
	echo "release-wait-checks: $*" >&2
	exit 1
}

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
	usage
fi
sha=$1
policy=${2:-.mergewake/policy.yaml}
repo=${GITHUB_REPOSITORY:-}
timeout=${RELEASE_CHECKS_TIMEOUT_SECONDS:-3600}
poll=${RELEASE_CHECKS_POLL_SECONDS:-30}

[ -n "$repo" ] || fail "GITHUB_REPOSITORY is not set"
[ -f "$policy" ] || fail "policy file not found: $policy"
case $timeout in
	'' | *[!0-9]*) fail "RELEASE_CHECKS_TIMEOUT_SECONDS must be a whole number of seconds" ;;
esac
[ "$timeout" -gt 0 ] || fail "RELEASE_CHECKS_TIMEOUT_SECONDS must be positive"

# The policy file is simple YAML; read only the check-name lists so the script
# needs no YAML library on the runner.
names=$(python3 - "$policy" <<'PYPOLICY'
import re
import sys

path = sys.argv[1]
keys = ('deploy_checks', 'main_checks', 'watch_checks', 'required_checks')
top_re = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*):(.*)$')
item_re = re.compile(r'^\s+-\s*(.*)$')


def unquote(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def strip_comment(line):
    out = []
    quote = None
    for ch in line:
        if quote is not None:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
            out.append(ch)
        elif ch == '#' and (not out or out[-1] in ' \t'):
            break
        else:
            out.append(ch)
    return ''.join(out).rstrip()


def split_flow(text):
    items = []
    item = []
    quote = None
    for ch in text:
        if quote is not None:
            item.append(ch)
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
            item.append(ch)
        elif ch == ',':
            items.append(''.join(item))
            item = []
        else:
            item.append(ch)
    items.append(''.join(item))
    return [name for name in (unquote(i) for i in items) if name]


lists = {}
current = None
with open(path, encoding='utf-8') as policy:
    for raw in policy:
        line = strip_comment(raw.rstrip('\r\n'))
        if not line.strip():
            continue
        top = top_re.match(line) if line[0] not in ' \t' else None
        if top:
            key, rest = top.group(1), top.group(2).strip()
            if key not in keys:
                current = None
                continue
            current = key
            if rest.startswith('['):
                if not rest.endswith(']'):
                    raise SystemExit('%s: unterminated list for %s' % (path, key))
                lists[key] = split_flow(rest[1:-1])
            elif rest == '':
                lists[key] = []
            else:
                raise SystemExit('%s: %s must be a list' % (path, key))
            continue
        item = item_re.match(line)
        if current is not None and item:
            name = unquote(item.group(1))
            if name:
                lists[current].append(name)

selected = []
for key in keys:
    if lists.get(key):
        selected = lists[key]
        break
if not selected:
    raise SystemExit(
        '%s: no deploy_checks, main_checks, watch_checks or required_checks' % path)
for name in selected:
    print(name)
PYPOLICY
) || fail "cannot read the check names from $policy"

[ -n "$names" ] || fail "$policy names no checks"
count=$(printf '%s\n' "$names" | wc -l | tr -d ' ')

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
start=$(date +%s)
while :; do
	gh api --paginate "repos/$repo/commits/$sha/check-runs?filter=latest&per_page=100" \
		--jq '.check_runs[] | [.name, .status, (.conclusion // ""), (.html_url // "")] | @tsv' \
		> "$work/runs" || fail "cannot read check runs for $sha"
	gh api "repos/$repo/commits/$sha/status?per_page=100" \
		--jq '.statuses[] | [.context, .state, (.target_url // "")] | @tsv' \
		> "$work/statuses" || fail "cannot read commit statuses for $sha"

	# One name-state-url record per piece of evidence. A completed run that is
	# skipped or neutral holds no verdict, and a cancelled run is pending, the
	# same reading mergewake's gate uses.
	awk -F '\t' -v OFS='\t' '
		{
			state = "pending"
			if ($2 == "completed") {
				if ($3 == "success") state = "success"
				else if ($3 == "failure" || $3 == "timed_out" || $3 == "action_required" || $3 == "startup_failure" || $3 == "stale") state = "failure"
				else if ($3 == "skipped" || $3 == "neutral") state = "other"
			}
			if ($1 != "") print $1, state, ($4 == "" ? "" : $4)
		}' "$work/runs" > "$work/evidence"
	awk -F '\t' -v OFS='\t' '
		{
			state = "other"
			if ($2 == "success") state = "success"
			else if ($2 == "failure" || $2 == "error") state = "failure"
			else if ($2 == "pending") state = "pending"
			if ($1 != "") print $1, state, ($3 == "" ? "" : $3)
		}' "$work/statuses" >> "$work/evidence"

	# A name is green only when it has a success and no failure. Conflicting
	# decisive evidence fails: the release must not guess which run decided.
	awk -F '\t' -v names="$names" '
		BEGIN {
			n = split(names, order, "\n")
			for (i = 1; i <= n; i++) wanted[order[i]] = 1
		}
		{
			if (!($1 in wanted)) next
			if ($3 != "") url[$1] = $3
			if ($2 == "success") success[$1] = 1
			else if ($2 == "failure") failure[$1] = 1
			else if ($2 == "pending") pending[$1] = 1
			else other[$1] = 1
		}
		END {
			for (i = 1; i <= n; i++) {
				name = order[i]
				if (success[name] && failure[name]) print name "\tfailure\t" url[name]
				else if (failure[name]) print name "\tfailure\t" url[name]
				else if (success[name]) print name "\tsuccess\t" url[name]
				else if (pending[name] || other[name]) print name "\tpending\t" url[name]
				else print name "\tabsent\t"
			}
		}' "$work/evidence" > "$work/verdicts"

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
		printf 'release-wait-checks: all %s policy checks are green for %s\n' "$count" "$sha"
		exit 0
	fi
	now=$(date +%s)
	if [ $((now - start)) -ge "$timeout" ]; then
		fail "timed out after ${timeout}s waiting for:$missing"
	fi
	sleep "$poll"
done
