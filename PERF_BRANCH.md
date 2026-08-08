# Active performance branch

`perf/4.1-native-hwaccel` is the actively maintained VideoDuplicateFinder performance branch.

It receives upstream updates from `master` and publishes the GUI-only Windows x64 Native AOT `4.1.x` release used by the `videoduplicatefinder-perf` Scoop package.

The release intentionally contains only `GUI-win-x64.zip`; CLI, Web, bundles, symbols, and the retired full package are not produced.

Before FFmpeg integration and Native AOT packaging, the release workflow runs the end-to-end extraction performance probe against the pinned known-good `perf/4.1-baseline` branch on the same Windows runner. The baseline and candidate reuse the exact same generated H.264, HEVC 10-bit, and VP9 corpus bytes and the same verified shared FFmpeg build. Publication is blocked when any hosted `process` or `native-cpu` case loses more than 15% median throughput. Baseline/current JSON reports and logs are retained for 14 days.

`perf/4.1-baseline` is advanced only after an intentional performance change has been separately verified and accepted. It is not moved automatically by a successful or failed release run, so a regression cannot silently become the next baseline.

GitHub-hosted Windows runners do not expose usable D3D11VA hardware. D3D11 performance remains measurable through `VDF.Benchmarks` and `.github/scripts/perf-regression-gate.ps1` on a capable local or self-hosted Windows GPU system, while hosted CI continues to provide functional fallback coverage.

Failed FFmpeg integration runs retain their detailed console log and TRX results for seven days so release regressions can be diagnosed without weakening the validation gate.

The Scoop manifest tracks the fork's `4.1.x` tag through checkver/autoupdate. Scoop Excavator runs whenever bucket manifests change and on its normal schedule; the release workflow also requests an immediate run when a cross-repository token is configured.

`perf/native-hwaccel-from-crashfix` is retained as historical reference and no longer receives automated syncs or release builds.
