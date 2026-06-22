// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Reflection;
using FFmpeg.AutoGen;
using VDF.Core.FFTools;

namespace VDF.Core.Tests.FFTools;

public class HardwareAccelDeviceTypeTests {
	[Fact]
	public void Vulkan_DowngradesToSoftwareForNativeBinding() {
		FFHardwareAccelerationMode oldMode = FfmpegEngine.HardwareAccelerationMode;
		try {
			FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.vulkan;
			Assert.Equal(AVHWDeviceType.AV_HWDEVICE_TYPE_NONE, GetConfiguredHardwareDeviceType());
		}
		finally {
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Theory]
	[InlineData(FFHardwareAccelerationMode.cuda, AVHWDeviceType.AV_HWDEVICE_TYPE_CUDA)]
	[InlineData(FFHardwareAccelerationMode.vaapi, AVHWDeviceType.AV_HWDEVICE_TYPE_VAAPI)]
	[InlineData(FFHardwareAccelerationMode.qsv, AVHWDeviceType.AV_HWDEVICE_TYPE_QSV)]
	[InlineData(FFHardwareAccelerationMode.none, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE)]
	public void OtherModes_MapThrough(FFHardwareAccelerationMode mode, AVHWDeviceType expected) {
		FFHardwareAccelerationMode oldMode = FfmpegEngine.HardwareAccelerationMode;
		try {
			FfmpegEngine.HardwareAccelerationMode = mode;
			Assert.Equal(expected, GetConfiguredHardwareDeviceType());
		}
		finally {
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	static AVHWDeviceType GetConfiguredHardwareDeviceType() {
		MethodInfo? method = typeof(FfmpegEngine).GetMethod(
			"GetConfiguredHardwareDeviceType",
			BindingFlags.Static | BindingFlags.NonPublic);
		Assert.NotNull(method);
		return (AVHWDeviceType)method.Invoke(null, new object[] { true })!;
	}
}
