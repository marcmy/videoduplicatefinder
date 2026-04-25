# VideoDuplicateFinder Agent Notes

## Current Snapshot

- 2026-04-21: Branch `perf/native-hwaccel-from-crashfix`, based on native swscale destination-buffer padding crash fix.
- Task: improve native FFmpeg HW-accel graybyte extraction without long forward decodes across sparse samples.
- Caveat: GPU decode may lose when full HW frames must download before CPU 32x32 conversion. Use sequential decode only for dense sample windows; otherwise seek per sample with a reused decoder.
- 2026-04-22: Native gray32 uses configured HW device by default, including D3D11VA. Set `VDF_FORCE_NATIVE_GRAYBYTE_CPU=1` only to benchmark faster CPU graybyte path with higher CPU load.
- Verify: `dotnet test VDF.Core.Tests/VDF.Core.Tests.csproj -c Release`; `dotnet test VDF.IntegrationTests/VDF.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~FFTools"`; `dotnet build VideoDuplicateFinder.sln -c Release`.
