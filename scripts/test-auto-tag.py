import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('auto_tag', Path(__file__).with_name('auto-tag.py'))
auto = importlib.util.module_from_spec(spec)
spec.loader.exec_module(auto)


class AutoTagTests(unittest.TestCase):
    def test_next_patch(self):
        self.assertEqual(auto.next_tag([]), 'v0.0.1')
        self.assertEqual(auto.next_tag(['v1.9.9', 'v2.0.3', 'v9.0.0-rc1', 'junk']), 'v2.0.4')

    def test_not_main(self):
        for ref,event in [('refs/pull/1/merge','pull_request'),('refs/heads/feature','push')]:
            with self.subTest(ref=ref,event=event), patch.dict(os.environ, {'GITHUB_REF':ref,'GITHUB_EVENT_NAME':event}), patch.object(auto,'run') as git:
                with self.assertRaises(SystemExit):
                    auto.main()
                git.assert_not_called()

    def test_tag_dispatch_retry_and_docs(self):
        with tempfile.TemporaryDirectory() as d:
            old = os.getcwd()
            try:
                os.chdir(d)
                def git(*args):
                    return subprocess.check_output(['git', *args], text=True, stderr=subprocess.DEVNULL).strip()
                git('init', '--bare', 'remote.git')
                git('clone', 'remote.git', 'repo')
                os.chdir('repo')
                git('config', 'user.name', 'Workflow test')
                git('config', 'user.email', 'test@example.invalid')
                git('checkout', '-b', 'main')
                Path('code.go').write_text('package main')
                git('add', '.'); git('commit', '-m', 'code'); git('push', 'origin', 'main')
                sha = git('rev-parse', 'HEAD')
                calls = []
                real_run = auto.run
                def run(*args):
                    if args[0] == 'gh':
                        calls.append(args)
                        return ''
                    return real_run(*args)
                with patch.dict(os.environ, {'GITHUB_REF': 'refs/heads/main', 'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': sha, 'BEFORE_SHA': '0' * 40}), patch.object(auto, 'run', run):
                    auto.main()
                    os.environ['GITHUB_EVENT_NAME'] = 'workflow_dispatch'
                    auto.main()
                    os.environ['GITHUB_EVENT_NAME'] = 'push'
                    self.assertEqual(git('tag', '--list'), 'v0.0.1')
                    self.assertIn('refs/tags/v0.0.1', git('ls-remote', 'origin', 'refs/tags/v0.0.1'))
                    self.assertEqual(calls, [('gh', 'workflow', 'run', 'release.yml', '--ref', 'v0.0.1', '-f', 'publish=true')] * 2)
                    Path('README.md').write_text('docs')
                    git('add', '.'); git('commit', '-m', 'docs')
                    os.environ['BEFORE_SHA'] = sha
                    os.environ['GITHUB_SHA'] = git('rev-parse', 'HEAD')
                    auto.main()
                    self.assertEqual(len(calls), 2)
                    self.assertEqual(git('tag', '--list'), 'v0.0.1')
            finally:
                os.chdir(old)

    def test_concurrent_reservation_retries_without_force(self):
        sha = 'a' * 40
        calls = []
        collided = False
        def run(*args):
            nonlocal collided
            calls.append(args)
            if args == ('git', 'rev-parse', 'HEAD'): return sha
            if args[:3] == ('git', 'tag', '--list'): return 'v0.0.1' if collided else ''
            if args[:3] == ('git', 'rev-list', '-n'): return 'b' * 40
            if args[:3] == ('git', 'push', 'origin') and args[-1] == 'refs/tags/v0.0.1':
                collided = True
                raise subprocess.CalledProcessError(1, args)
            return ''
        with patch.dict(os.environ, {'GITHUB_REF': 'refs/heads/main', 'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': sha, 'BEFORE_SHA': '0' * 40}), patch.object(auto, 'run', run):
            auto.main()
        self.assertIn(('git', 'tag', '--delete', 'v0.0.1'), calls)
        self.assertIn(('git', 'push', 'origin', 'refs/tags/v0.0.2'), calls)
        self.assertIn(('gh', 'workflow', 'run', 'release.yml', '--ref', 'v0.0.2', '-f', 'publish=true'), calls)
        self.assertFalse(any('--force' in c for c in calls))


if __name__ == '__main__':
    unittest.main()
