// /*
//     Copyright (C) 2026 0x90d
//     This file is part of VideoDuplicateFinder
//     VideoDuplicateFinder is free software: you can redistribute it and/or modify
//     it under the terms of the GNU Affero General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
//     VideoDuplicateFinder is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU Affero General Public License for more details.
//     You should have received a copy of the GNU Affero General Public License
//     along with VideoDuplicateFinder.  If not, see <http://www.gnu.org/licenses/>.
// */
//

using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.IntegrationTests.Fixtures;
using Vortice.Direct3D11;
using D3DDevice = Vortice.Direct3D11.ID3D11Device;
using D3DDeviceContext = Vortice.Direct3D11.ID3D11DeviceContext;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// D3D11VA decoders share one process-wide device instead of creating one per file.
/// </summary>
[Collection("Ffmpeg")]
public class SharedHardwareDeviceTests {
	readonly FfmpegFixture _fixture;

	public SharedHardwareDeviceTests(FfmpegFixture fixture) => _fixture = fixture;

	static readonly List<TimeSpan> Positions = new() { TimeSpan.FromSeconds(0.5), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1.5) };

	void SkipIfD3D11Unavailable() {
		Skip.If(!OperatingSystem.IsWindows(), "D3D11VA is Windows-only");
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");
		try {
			using var decoder = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		}
		catch (Exception ex) {
			Skip.If(true, $"D3D11VA decode unavailable on this machine: {ex.Message}");
		}
	}

	sealed class EnvVarScope : IDisposable {
		readonly string _name;
		readonly string? _previous;
		public EnvVarScope(string name, string? value) {
			_name = name;
			_previous = Environment.GetEnvironmentVariable(name);
			Environment.SetEnvironmentVariable(name, value);
		}
		public void Dispose() => Environment.SetEnvironmentVariable(_name, _previous);
	}

	static List<byte[]> GpuGray(string file) {
		using var decoder = new VideoStreamDecoder(file, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		using var scaler = new D3D11VideoProcessorGrayByteScaler();
		var results = new List<byte[]>();
		Assert.True(decoder.TryDecodeFrames(Positions, (_, frame, _) => results.Add(scaler.ScaleToGray32(frame, out _)), out _, FrameTransferMode.KeepHardwareFrame));
		Assert.Equal(Positions.Count, results.Count);
		return results;
	}

	[SkippableFact]
	public void D3D11Decoders_ShareOneDevice() {
		SkipIfD3D11Unavailable();
		using var envScope = new EnvVarScope(SharedHardwareDevice.DisableEnvVar, null);
		using var first = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		int creations = SharedHardwareDevice.SharedCreations;
		using var second = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		Assert.NotEqual(0, first.D3D11DevicePointer);
		Assert.Equal(first.D3D11DevicePointer, second.D3D11DevicePointer);
		Assert.Equal(creations, SharedHardwareDevice.SharedCreations);
	}

	[SkippableFact]
	public void OptOut_CreatesADevicePerDecoder() {
		SkipIfD3D11Unavailable();
		using var envScope = new EnvVarScope(SharedHardwareDevice.DisableEnvVar, "1");
		using var first = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		using var second = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		Assert.NotEqual(0, first.D3D11DevicePointer);
		Assert.NotEqual(first.D3D11DevicePointer, second.D3D11DevicePointer);
	}

	[SkippableFact]
	public void SharedDevice_IsMultithreadProtected() {
		SkipIfD3D11Unavailable();
		using var envScope = new EnvVarScope(SharedHardwareDevice.DisableEnvVar, null);
		using var decoder = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		using var device = new D3DDevice(decoder.D3D11DevicePointer);
		device.AddRef();
		Assert.Equal(0, (int)(device.CreationFlags & DeviceCreationFlags.Singlethreaded));
		using D3DDeviceContext context = device.ImmediateContext;
		using ID3D11Multithread multithread = context.QueryInterface<ID3D11Multithread>();
		Assert.True(multithread.GetMultithreadProtected());
	}

	[SkippableFact]
	public void Invalidate_NextDecoderGetsANewDevice_AndOpenDecodersKeepWorking() {
		SkipIfD3D11Unavailable();
		using var envScope = new EnvVarScope(SharedHardwareDevice.DisableEnvVar, null);
		List<byte[]> expected = GpuGray(_fixture.H264_8bit!);

		using var before = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		int creations = SharedHardwareDevice.SharedCreations;
		SharedHardwareDevice.Invalidate();
		using var after = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		Assert.Equal(creations + 1, SharedHardwareDevice.SharedCreations);

		// The dropped device stays alive for the decoder that still holds it.
		using var scaler = new D3D11VideoProcessorGrayByteScaler();
		var stillWorking = new List<byte[]>();
		Assert.True(before.TryDecodeFrames(Positions, (_, frame, _) => stillWorking.Add(scaler.ScaleToGray32(frame, out _)), out _, FrameTransferMode.KeepHardwareFrame));
		Assert.Equal(expected, stillWorking);
		Assert.Equal(expected, GpuGray(_fixture.H264_8bit!));
	}

	[SkippableFact]
	public void ConcurrentDecodersOnSharedDevice_MatchSequentialOutput() {
		SkipIfD3D11Unavailable();
		using var envScope = new EnvVarScope(SharedHardwareDevice.DisableEnvVar, null);
		string[] files = new[] { _fixture.H264_8bit, _fixture.HEVC_10bit, _fixture.H264_Different }
			.Where(f => f != null).Select(f => f!).ToArray();
		Dictionary<string, List<byte[]>> expected = files.ToDictionary(f => f, GpuGray);

		var work = Enumerable.Range(0, 8).SelectMany(_ => files).ToArray();
		var failures = new System.Collections.Concurrent.ConcurrentBag<string>();
		Parallel.ForEach(work, new ParallelOptions { MaxDegreeOfParallelism = 8 }, file => {
			List<byte[]> actual = GpuGray(file);
			for (int i = 0; i < actual.Count; i++)
				if (!actual[i].AsSpan().SequenceEqual(expected[file][i]))
					failures.Add($"{Path.GetFileName(file)} sample {i}");
		});
		Assert.Empty(failures);
	}
}
