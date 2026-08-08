# Evil-media regression corpus

The integration suite deliberately generates a small pathological media corpus at runtime.
The recipes live in `VDF.TestSupport/EvilMediaGenerator.cs`; generated files go to a unique
temporary directory and are deleted after the test class finishes.

This keeps the corpus reproducible, reviewable, and tiny instead of committing opaque binary
videos whose provenance or exact encoding parameters are hard to reconstruct.

## Generated pathological fixtures

| Fixture | Shape | Regression contract |
| --- | --- | --- |
| `h264_long_gop.mp4` | 4 s H.264, 100-frame closed GOP | Late seeking must complete and native/process gray frames must remain equivalent. |
| `h264_vfr.mp4` | 12 fps section followed by 30 fps section, preserved as VFR | Timestamp-based native/process extraction must return a valid frame without assuming CFR. |
| `odd_321x241_ffv1.mkv` | 321x241 FFV1/yuv444p | Conversion/scaling must not assume even 4:2:0 dimensions. |
| `h264_120ms.mp4` | ~120 ms H.264 | Very short media must still yield a sample without over-seeking past EOF. |
| `h264_rotation_90.mp4` | H.264 remux with a real 90-degree display matrix | Rotation side data must not crash either extraction path. |
| `h264_faststart_truncated.mp4` | Valid fast-start MP4 with the media tail physically removed | Late native extraction must fail/recover within a bounded time rather than entering a long retry ladder. |
| `h264_resolution_change.mkv` | Two independently encoded H.264 Matroska segments concatenated losslessly, 320x240 -> 640x360, with continuous timestamps | Sequential native sampling must reconfigure from decoded-frame metadata instead of stale open-time dimensions; normal process seeking must also remain valid. |

`EvilMediaRegressionTests` checks process/native survivability, targeted parity where frame
selection is deterministic, bounded corrupt handling, and the mid-stream resolution-change
batch path.

## Existing generated coverage

The normal `FfmpegFixture` already generates and tests several other important corpus members:

- H.264 8-bit baseline
- HEVC 10-bit
- VP9
- anamorphic H.264 / non-1:1 SAR
- lightly corrupted H.264
- fully corrupted H.264 for corrupt-file fast-fail
- JPEG and PNG still images
- container creation-time metadata

Those remain in their existing focused test classes instead of being duplicated here.

## HEIC / tiled HEIC

`sample.heic` is a tiny checked-in single-image HEIC because FFmpeg can decode HEIC but cannot
mux one for the normal fixture. It is **not** an Apple tile-grid/stream-group file.

True tiled iPhone HEIC remains opt-in through `VDF_TEST_TILED_HEIC`; `TiledHeicTests` exercises
the native refusal and `[0:g:0]` process-grid fallback when such a file is supplied. We do not
commit a personal iPhone photo merely to make that test unconditional.

If a redistributable, provenance-clear tiled HEIC fixture becomes available, add it here and
remove the environment-variable dependency.

## Rules for adding corpus cases

1. Prefer a deterministic FFmpeg/libav recipe over a checked-in binary.
2. Prove the recipe actually creates the intended pathology, not merely a file with a suggestive name.
3. Keep files short and low-resolution so release CI remains fast.
4. Test a behavioral invariant (no crash, bounded failure, parity, reconfiguration, etc.), not just "FFmpeg opened it."
5. When a real-world bug produces a new media shape, reduce it to the smallest synthetic case possible and add it permanently.
