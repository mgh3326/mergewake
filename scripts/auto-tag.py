#!/usr/bin/env python3
"""Reserve one patch tag per main push and explicitly dispatch publishing."""
import os
import re
import subprocess


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def next_tag(tags):
    versions = [tuple(map(int, t[1:].split('.'))) for t in tags if re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', t)]
    major, minor, patch = max(versions, default=(0, 0, 0))
    return f'v{major}.{minor}.{patch + 1}'


def main():
    if os.environ.get('GITHUB_REF') != 'refs/heads/main' or os.environ.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch'):
        raise SystemExit('auto-tag requires main and a push or explicit dispatch')
    sha = os.environ['GITHUB_SHA']
    if run('git', 'rev-parse', 'HEAD') != sha:
        raise SystemExit('checkout does not match push commit')
    before = os.environ.get('BEFORE_SHA', '')
    if re.fullmatch(r'[0-9a-f]{40}', before) and before != '0' * 40:
        try:
            changed = run('git', 'diff', '--no-renames', '--name-only', before, sha).splitlines()
        except subprocess.CalledProcessError:
            # Docs-only skipping is an optimization; unknown content releases
            # normally. Deployment CI/range approval remains fail closed.
            changed = ['unknown']
        if not changed or all(p.startswith('docs/') or p.endswith('.md') for p in changed):
            print('unchanged or docs-only: skipped')
            return
    # Each SHA has its own Actions group, so pushes cannot displace another
    # commit's pending release. Atomic remote ref creation arbitrates versions.
    for _ in range(20):
        run('git', 'fetch', 'origin', '--tags')
        tags = run('git', 'tag', '--list').splitlines()
        existing = [t for t in tags if t != 'v0.0.0' and re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', t) and run('git', 'rev-list', '-n', '1', t) == sha]
        if existing:
            tag = max(existing, key=lambda t: tuple(map(int, t[1:].split('.'))))
            break
        tag = next_tag(tags)
        run('git', 'tag', tag, sha)
        try:
            run('git', 'push', 'origin', 'refs/tags/' + tag)
            break
        except subprocess.CalledProcessError:
            # Only remove the unpushed local reservation; never force a ref.
            # An ambiguous successful push is rediscovered on the next fetch.
            run('git', 'tag', '--delete', tag)
    else:
        raise SystemExit('could not reserve a patch tag after concurrent pushes')
    # Tag and checkout resolve to this exact commit, including the release
    # workflow's publish input. GITHUB_TOKEN pushes do not start workflows.
    run('gh', 'workflow', 'run', 'release.yml', '--ref', tag, '-f', 'publish=true')
    print(tag)


if __name__ == '__main__':
    main()
