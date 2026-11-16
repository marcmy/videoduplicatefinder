import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "resolutions", Path(__file__).with_name("resolve-reviewed-sync-conflicts.py"))
resolutions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resolutions)


class ReviewedResolutionTests(unittest.TestCase):
    def test_package_resolution_preserves_the_perf_only_dependency(self):
        text = ('<Project><ItemGroup>\n<<<<<<< HEAD\n'
                '  <PackageReference Include="Vortice.Direct3D11" Version="3.8.3" />\n'
                '  <PackageReference Include="System.Numerics.Tensors" Version="10.0.11" />\n'
                '=======\n'
                '  <PackageReference Include="System.Numerics.Tensors" Version="11.0.0" />\n'
                '>>>>>>> master\n</ItemGroup></Project>\n')
        expected = ('<Project><ItemGroup>\n'
                    '  <PackageReference Include="Vortice.Direct3D11" Version="3.8.3" />\n'
                    '  <PackageReference Include="System.Numerics.Tensors" Version="11.0.0" />\n'
                    '</ItemGroup></Project>\n').encode()
        rule = {"kind": "project", "preserve_package": "Vortice.Direct3D11",
                "result": resolutions.blob_sha(expected)}
        self.assertEqual(resolutions.render_resolution(text, rule, Path('.')), expected)

    def test_rejects_a_changed_result_instead_of_silently_resolving_it(self):
        rule = {"kind": "project", "result": "0" * 40}
        with self.assertRaises(ValueError):
            resolutions.render_resolution('<Project />\n', rule, Path('.'))

    def test_windows_template_line_endings_match_the_reviewed_git_blob(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'reviewed.cs').write_bytes(b'class Example {\r\n}\r\n')
            expected = b'class Example {\n}\n'
            rule = {'kind': 'template', 'template': 'reviewed.cs',
                    'result': resolutions.blob_sha(expected)}
            self.assertEqual(resolutions.render_resolution('', rule, root), expected)

    def test_changed_input_blobs_stay_unmerged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args, check=True):
                return subprocess.run(['git', '-C', directory, *args], check=check,
                                      capture_output=True, text=True)
            git('init', '--quiet', '--initial-branch=master')
            git('config', 'user.name', 'Resolution tests')
            git('config', 'user.email', 'tests@example.invalid')
            target = root / 'example.txt'
            target.write_text('base\n')
            git('add', '.')
            git('commit', '--quiet', '-m', 'base')
            git('checkout', '--quiet', '-b', 'fork')
            target.write_text('fork\n')
            git('commit', '--quiet', '-am', 'fork')
            git('checkout', '--quiet', 'master')
            target.write_text('upstream\n')
            git('commit', '--quiet', '-am', 'upstream')
            self.assertNotEqual(git('merge', 'fork', check=False).returncode, 0)
            manifest = root / '.github/sync-resolutions/net11.json'
            manifest.parent.mkdir(parents=True)
            manifest.write_text(json.dumps({'example.txt': {
                'kind': 'hunks', 'ours': '0' * 40, 'theirs': '0' * 40, 'result': '0' * 40}}))
            before = target.read_bytes()
            self.assertEqual(resolutions.resolve(root), [])
            self.assertEqual(target.read_bytes(), before)
            self.assertTrue(git('ls-files', '-u').stdout)


if __name__ == '__main__':
    unittest.main()
