import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "resolutions", Path(__file__).with_name("resolve-reviewed-sync-conflicts.py"))
resolutions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resolutions)


class ReviewedResolutionTests(unittest.TestCase):
    def test_net11_patch_uses_utf8_for_locales_and_is_idempotent(self):
        patch_spec = importlib.util.spec_from_file_location(
            "net11_patch", Path(__file__).with_name("patch-perf-net11.py"))
        net11_patch = importlib.util.module_from_spec(patch_spec)
        patch_spec.loader.exec_module(net11_patch)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {
                'VDF.Core/VDF.Core.csproj': '<Project><TargetFramework>net11.0</TargetFramework></Project>',
                'VDF.GUI/VDF.GUI.csproj': '<Project><PackageReference Include="Avalonia" Version="12.1.6" /></Project>',
                'VDF.GUI.HeadlessTests/VDF.GUI.HeadlessTests.csproj': '<Project>\n    <PackageReference Include="Avalonia.Headless" Version="12.1.6" />\n</Project>',
                'VDF.GUI.HeadlessTests/HeadlessUi.cs': 'AppBuilder.Configure<App>()\n.UseSkia();',
                'VDF.GUI/Assets/Locales/en.json': '{"message":"Deleted {0} — done"}',
                'VDF.GUI/Assets/Locales/ru.json': '{"existing":"Сохранить"}',
                '.github/sync-resolutions/ru-perf.json': '{"message":"Удалено {0} — готово"}',
            }
            for relative, text in files.items():
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text, encoding='utf-8')
            original_read_text = Path.read_text
            def require_explicit_encoding(path, *args, **kwargs):
                self.assertIn('encoding', kwargs, f'Locale-dependent read: {path}')
                return original_read_text(path, *args, **kwargs)
            with patch.object(net11_patch, 'ROOT', root), patch.object(Path, 'read_text', require_explicit_encoding):
                net11_patch.main()
                first = {relative: (root / relative).read_bytes() for relative in files}
                net11_patch.main()
                self.assertEqual(first, {relative: (root / relative).read_bytes() for relative in files})
            russian = json.loads((root / 'VDF.GUI/Assets/Locales/ru.json').read_text(encoding='utf-8'))
            self.assertEqual(russian['existing'], 'Сохранить')
            self.assertEqual(russian['message'], 'Удалено {0} — готово')
            self.assertIn(b'DefaultFamilyName = "fonts:Inter#Inter"', first['VDF.GUI.HeadlessTests/HeadlessUi.cs'])

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
