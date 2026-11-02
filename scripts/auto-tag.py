#!/usr/bin/env python3
"""Create one patch tag per main commit and explicitly dispatch release publishing."""
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
    if os.environ.get('GITHUB_REF') != 'refs/heads/main' or os.environ.get('GITHUB_EVENT_NAME') != 'push':
        raise SystemExit('auto-tag requires a push to main')
    sha = os.environ['GITHUB_SHA']
    if run('git', 'rev-parse', 'HEAD') != sha:
        raise SystemExit('checkout does not match push commit')
    run('git', 'fetch', 'origin', '--tags')
    tags = run('git', 'tag', '--list').splitlines()
    existing = [t for t in tags if re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', t) and run('git', 'rev-list', '-n', '1', t) == sha]
    if existing:
        tag = max(existing, key=lambda t: tuple(map(int, t[1:].split('.'))))
    else:
        before = os.environ.get('BEFORE_SHA', '')
        if re.fullmatch(r'[0-9a-f]{40}', before) and before != '0' * 40:
            try:
                changed = run('git', 'diff', '--name-only', before, sha).splitlines()
            except subprocess.CalledProcessError:
                changed = ['unknown']
            if changed and all(p.startswith('docs/') or p.endswith('.md') for p in changed):
                print('docs-only: skipped')
                return
        tag = next_tag(tags)
        run('git', 'tag', tag, sha)
        # Never force or move a tag. A concurrent publisher fails safely.
        run('git', 'push', 'origin', 'refs/tags/' + tag)
    # GITHUB_TOKEN tag pushes do not start workflows. Dispatch the release
    # workflow at this immutable tag explicitly, including on a retry.
    run('gh', 'workflow', 'run', 'release.yml', '--ref', tag, '-f', 'publish=true')
    print(tag)


if __name__ == '__main__':
    main()
