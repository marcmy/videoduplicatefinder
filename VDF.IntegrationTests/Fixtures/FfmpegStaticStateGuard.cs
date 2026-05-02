// /*
//     Copyright (C) 2025 0x90d
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

using VDF.Core.FFTools;

namespace VDF.IntegrationTests.Fixtures;

/// <summary>
/// Snapshots and restores FfmpegEngine static fields so tests don't leak state.
/// </summary>
sealed class FfmpegStaticStateGuard : IDisposable {
	readonly bool _useNativeBinding;
	readonly FFHardwareAccelerationMode _hwMode;
	readonly string _customArgs;
	readonly string? _forceNativeGrayByteCpu;
	readonly string? _disableNativeGrayByteGpuScale;

	public FfmpegStaticStateGuard() {
		_useNativeBinding = FfmpegEngine.UseNativeBinding;
		_hwMode = FfmpegEngine.HardwareAccelerationMode;
		_customArgs = FfmpegEngine.CustomFFArguments;
		_forceNativeGrayByteCpu = Environment.GetEnvironmentVariable("VDF_FORCE_NATIVE_GRAYBYTE_CPU");
		_disableNativeGrayByteGpuScale = Environment.GetEnvironmentVariable("VDF_DISABLE_NATIVE_GRAYBYTE_GPU_SCALE");
	}

	public void Dispose() {
		FfmpegEngine.UseNativeBinding = _useNativeBinding;
		FfmpegEngine.HardwareAccelerationMode = _hwMode;
		FfmpegEngine.CustomFFArguments = _customArgs;
		Environment.SetEnvironmentVariable("VDF_FORCE_NATIVE_GRAYBYTE_CPU", _forceNativeGrayByteCpu);
		Environment.SetEnvironmentVariable("VDF_DISABLE_NATIVE_GRAYBYTE_GPU_SCALE", _disableNativeGrayByteGpuScale);
	}
}
