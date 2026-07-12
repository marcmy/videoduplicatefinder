# Active performance branch

`perf/4.1-native-hwaccel` is the actively maintained VideoDuplicateFinder performance branch.

It receives upstream updates from `master` and publishes the GUI-only Windows x64 Native AOT `4.1.x` release used by the `videoduplicatefinder-perf` Scoop package.

The release intentionally contains only `GUI-win-x64.zip`; CLI, Web, bundles, symbols, and the retired full package are not produced.

Failed FFmpeg integration runs retain their detailed console log and TRX results for seven days so release regressions can be diagnosed without weakening the validation gate.

The Scoop manifest tracks the fork's `4.1.x` tag through checkver/autoupdate. Scoop Excavator runs whenever bucket manifests change and on its normal schedule; the release workflow also requests an immediate run when a cross-repository token is configured.

`perf/native-hwaccel-from-crashfix` is retained as historical reference and no longer receives automated syncs or release builds.
