import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class RepairReleaseTests(unittest.TestCase):
    def test_partial_retry_is_immutable_and_complete(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            assets = root / 'built'; assets.mkdir()
            remote = root / 'remote'; remote.mkdir()
            for name in ['tool-linux-amd64', 'tool-linux-arm64', 'SHA256SUMS']:
                (assets / name).write_bytes(name.encode())
            (remote / 'tool-linux-amd64').write_bytes(b'tool-linux-amd64')
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


if __name__ == '__main__':
    unittest.main()
