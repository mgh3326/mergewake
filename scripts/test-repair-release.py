import hashlib
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class RepairReleaseTests(unittest.TestCase):
    def test_publish_job_checks_out_recovery_script(self):
        workflow = Path(__file__).resolve().parents[1] / '.github/workflows/release.yml'
        publish = workflow.read_text().split('\n  publish:\n', 1)[1]
        checkout = re.search(r'      - uses: actions/checkout@[^\n]+\n        with:\n((?:          [^\n]+\n)+)', publish)
        self.assertIsNotNone(checkout, 'publish job must acquire the recovery script')
        self.assertIn('ref: ${{ github.sha }}', checkout[1])
        self.assertIn('persist-credentials: false', checkout[1])
        self.assertLess(checkout.end(), publish.index('sh scripts/repair-release-assets.sh'))

    def test_partial_retry_is_immutable_and_complete(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            assets = root / 'built'; assets.mkdir()
            remote = root / 'remote'; remote.mkdir()
            names = ['tool-linux-amd64', 'tool-linux-arm64', 'expected']
            for name in names:
                (assets / name).write_bytes(name.encode())
            (assets / 'SHA256SUMS').write_text(''.join(
                hashlib.sha256((assets / name).read_bytes()).hexdigest() + '  ' + name + '\n'
                for name in names))
            (remote / 'tool-linux-amd64').write_bytes(b'tool-linux-amd64')
            (remote / 'expected').write_bytes(b'expected')
            (root / 'draft').write_text('draft')
            fake = root / 'gh'
            fake.write_text('#!' + sys.executable + '\n' + '''import os, sys, shutil
from pathlib import Path
args = sys.argv[1:]
remote = Path(os.environ['FAKE_REMOTE'])
if args[:2] == ['release', 'view']:
    if args[args.index('--json') + 1] == 'isDraft':
        print('true' if (remote.parent / 'draft').exists() else 'false')
    else:
        print('\\n'.join(p.name for p in remote.iterdir()))
elif args[:2] == ['release', 'edit']:
    if '--draft=false' not in args: raise SystemExit('unexpected edit')
    expected_flag = '--prerelease=' + os.environ.get('FAKE_PRERELEASE', 'false')
    if expected_flag not in args: raise SystemExit('wrong prerelease metadata')
    (remote.parent / 'draft').unlink()
elif args[:2] == ['release', 'download']:
    name = args[args.index('--pattern') + 1]
    shutil.copyfile(remote / name, Path(args[args.index('--dir') + 1]) / name)
elif args[:2] == ['release', 'upload']:
    source = Path(args[3]); target = remote / source.name
    if target.exists(): raise SystemExit('refusing overwrite')
    shutil.copyfile(source, target)
    with open(os.environ['FAKE_LOG'], 'a') as f: f.write(source.name + '\\n')
else: raise SystemExit('unexpected gh command')
''')
            fake.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                       FAKE_REMOTE=str(remote), FAKE_LOG=str(root / 'uploads'), GITHUB_REPOSITORY='example/tool')
            script = str(Path(__file__).with_name('repair-release-assets.sh').resolve())
            # Missing local artifacts and unexpected remote assets must leave the draft untouched.
            missing = assets / 'tool-linux-arm64'
            content = missing.read_bytes(); missing.unlink()
            result = subprocess.run(['sh', script, 'v1.0.0', str(assets)], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((root / 'draft').exists())
            self.assertFalse((root / 'uploads').exists())
            missing.write_bytes(content)
            (remote / 'unexpected').write_bytes(b'extra')
            result = subprocess.run(['sh', script, 'v1.0.0', str(assets)], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((root / 'draft').exists())
            self.assertFalse((root / 'uploads').exists())
            (remote / 'unexpected').unlink()
            subprocess.run(['sh', script, 'v1.0.0', str(assets)], env=env, check=True)
            self.assertEqual((root / 'uploads').read_text().splitlines(), ['tool-linux-arm64', 'SHA256SUMS'])
            self.assertFalse((root / 'draft').exists(), 'repaired draft was not published')
            subprocess.run(['sh', script, 'v1.0.0', str(assets)], env=env, check=True)
            self.assertEqual(len((root / 'uploads').read_text().splitlines()), 2)
            (remote / 'tool-linux-amd64').write_bytes(b'corrupt')
            result = subprocess.run(['sh', script, 'v1.0.0', str(assets)], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((remote / 'tool-linux-amd64').read_bytes(), b'corrupt')
            self.assertEqual(len((root / 'uploads').read_text().splitlines()), 2)
            (remote / 'tool-linux-amd64').write_bytes(b'tool-linux-amd64')
            (root / 'draft').write_text('draft')
            subprocess.run(['sh', script, 'v1.0.1-rc.1', str(assets)],
                           env=dict(env, FAKE_PRERELEASE='true'), check=True)
            self.assertFalse((root / 'draft').exists())


if __name__ == '__main__':
    unittest.main()
