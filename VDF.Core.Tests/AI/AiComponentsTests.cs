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

using System.Runtime.InteropServices;
using VDF.Core.AI;
using VDF.TestSupport;

namespace VDF.Core.Tests.AI;

public class AiComponentsTests {

	[Fact]
	public void RuntimeDownloadPlan_TargetsPinnedVersionForCurrentPlatform() {
		(Uri url, string archive, string? nativeDirectory) = AiComponents.GetRuntimeDownloadPlan();

		Assert.Contains(AiComponents.RuntimeVersion, archive);
		Assert.StartsWith("https://api.nuget.org/v3-flatcontainer/", url.AbsoluteUri);
		Assert.EndsWith(".nupkg", archive);
		Assert.StartsWith("runtimes/", nativeDirectory);
		Assert.EndsWith("/native", nativeDirectory);
		if (AiComponents.UsesDirectMLRuntime) {
			Assert.Equal(AiComponents.DirectMLRuntimeVersion, AiComponents.RuntimeVersion);
			Assert.Contains("microsoft.ml.onnxruntime.directml", url.AbsoluteUri);
			Assert.EndsWith("-directml", AiComponents.RuntimeVersionMarker);
		}
		else if (AiComponents.UsesCudaRuntime) {
			Assert.Contains(".gpu.", url.AbsoluteUri);
			Assert.EndsWith("-cuda", AiComponents.RuntimeVersionMarker);
		}
		else {
			Assert.Contains("/microsoft.ml.onnxruntime/", url.AbsoluteUri);
			Assert.EndsWith("-cpu", AiComponents.RuntimeVersionMarker);
		}
	}

	[Fact]
	public void RuntimeVersionMarker_FromCpuRuntime_IsNotCurrentWhenDirectMLIsExpected() {
		// Upgrading Windows installs from the CPU-only runtime must re-download (the GUI
		// prompts once at the next AI scan), not keep running inference on the CPU.
		if (!AiComponents.UsesDirectMLRuntime)
			return;
		Assert.NotEqual(AiComponents.CpuRuntimeVersion, AiComponents.RuntimeVersionMarker);
	}

	[Fact]
	public async Task DownloadedNativeRuntime_ExecutesModel_OptIn() {
		// Opt-in only: run in a separate dotnet test process.
		if (Environment.GetEnvironmentVariable("VDF_TEST_ONNX_DOWNLOAD") != "1") return;
		string dir = Directory.CreateTempSubdirectory("vdf-onnx-smoke").FullName;
		try {
			AiComponents.TestAiFolder.Value = dir;
			File.Copy(TestModels.TinyEmbedderPath, AiComponents.ModelPath);
			await AiComponents.DownloadAsync(null, CancellationToken.None);
			AiComponents.EnsureReady();
			bool gpu = Environment.GetEnvironmentVariable("VDF_TEST_ONNX_GPU") == "1";
			using var embedder = new OnnxEmbedder(AiComponents.ModelPath,
				gpu ? AiInferenceDevice.Gpu : AiInferenceDevice.Cpu);
			Assert.Equal(gpu, embedder.UsesGpu);
			var result = embedder.EmbedBatch(new[] { new byte[OnnxEmbedder.InputSide * OnnxEmbedder.InputSide * 3] });
			Assert.Single(result);
			Assert.Equal(EmbeddingMath.Dimensions, result[0].Length);
			Assert.All(result[0], x => Assert.True(float.IsFinite(x)));
		}
		finally {
			AiComponents.TestAiFolder.Value = null;
			// Windows keeps the native ONNX DLL loaded until the test host exits.
			// Do not mask the real ONNX/CUDA failure with a cleanup exception.
			// An unloaded test folder can be removed after the host process exits.
			try { Directory.Delete(dir, recursive: true); }
			catch (IOException) { }
			catch (UnauthorizedAccessException) { }
		}
	}

	[Fact]
	public void EmbeddingSidecars_KeepLegacyInt8AndIsolateFutureModels() {
		// Existing users retain their unmodified untagged INT8 cache files.
		Assert.Equal("UnionEmbeddings.db",
			AiComponents.EmbeddingSidecarFileName("UnionEmbeddings.db"));
		Assert.Equal("DenseEmbeddings.db",
			AiComponents.EmbeddingSidecarFileName("DenseEmbeddings.db"));

		const string futureHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
		string union = AiComponents.EmbeddingSidecarFileNameForModel("UnionEmbeddings.db", futureHash, 1);
		string dense = AiComponents.EmbeddingSidecarFileNameForModel("DenseEmbeddings.db", futureHash, 1);
		Assert.Equal($"UnionEmbeddings.{futureHash}.p1.db", union);
		Assert.Equal($"DenseEmbeddings.{futureHash}.p1.db", dense);
		Assert.NotEqual(union, AiComponents.EmbeddingSidecarFileNameForModel("UnionEmbeddings.db", futureHash, 2));
		Assert.NotEqual("UnionEmbeddings.db",
			AiComponents.EmbeddingSidecarFileNameForModel("UnionEmbeddings.db",
				AiComponents.LegacyInt8ModelSha256, 2));
		Assert.Throws<ArgumentException>(() =>
			AiComponents.EmbeddingSidecarFileNameForModel("UnionEmbeddings.db", "bad", 1));
		Assert.Throws<ArgumentOutOfRangeException>(() =>
			AiComponents.EmbeddingSidecarFileNameForModel("UnionEmbeddings.db", futureHash, 0));
	}

	[Fact]
	public void ModelSha256_IsPinned() {
		// The download verifies against this constant — a typo would brick the feature.
		Assert.Equal(64, AiComponents.ModelSha256.Length);
		Assert.All(AiComponents.ModelSha256, c => Assert.True(Uri.IsHexDigit(c)));
	}

	[Theory]
	[InlineData(false, false, false)]
	[InlineData(true, false, true)]
	[InlineData(false, true, true)]
	[InlineData(true, true, true)]
	public void NeedsAiComponents_TracksEveryAiFeature(bool union, bool partial, bool expected) {
		var settings = new Settings { UseAiMatching = union, EnableAiPartialDetection = partial };
		Assert.Equal(expected, settings.NeedsAiComponents);
	}

	[Fact]
	public void NeedsAiComponents_IsNotSerializedIntoSettingsJson() {
		// Computed gate only — persisting it would add a dead key that a hand-edited
		// settings file could set to a value the getter then silently contradicts.
		string json = System.Text.Json.JsonSerializer.Serialize(
			new Settings { UseAiMatching = true }, VDF.Core.Utils.CoreJsonContext.Default.Settings);
		Assert.DoesNotContain("NeedsAiComponents", json);
	}

	[Fact]
	public async Task RunDownloadsAsync_RunsDownloadsConcurrently() {
		// Each download only completes once the other has started — sequential
		// execution would deadlock here and trip the timeout.
		var aStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var bStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var downloads = new List<Func<CancellationToken, Task>> {
			async ct => { aStarted.SetResult(); await bStarted.Task.WaitAsync(ct); },
			async ct => { bStarted.SetResult(); await aStarted.Task.WaitAsync(ct); },
		};

		await AiComponents.RunDownloadsAsync(downloads, new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
	}

	[Fact]
	public async Task RunDownloadsAsync_FailureCancelsSiblingAndSurfacesTheRealError() {
		var siblingCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var downloads = new List<Func<CancellationToken, Task>> {
			// The sibling: runs "forever" until the failure cancels it. It is FIRST in
			// the list, so a naive Task.WhenAll await would rethrow its cancellation
			// instead of the real error below.
			async ct => {
				try { await Task.Delay(Timeout.Infinite, ct); }
				finally { siblingCanceled.TrySetResult(); }
			},
			ct => Task.FromException(new IOException("model corrupt")),
		};

		var ex = await Assert.ThrowsAsync<IOException>(() =>
			AiComponents.RunDownloadsAsync(downloads, CancellationToken.None));
		Assert.Equal("model corrupt", ex.Message);
		await siblingCanceled.Task.WaitAsync(TimeSpan.FromSeconds(10));
	}

	[Fact]
	public async Task RunDownloadsAsync_UserCancellationSurfacesAsCancellation() {
		using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
		var downloads = new List<Func<CancellationToken, Task>> {
			ct => Task.Delay(Timeout.Infinite, ct),
			ct => Task.Delay(Timeout.Infinite, ct),
		};

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
			AiComponents.RunDownloadsAsync(downloads, cts.Token));
	}

	/// <summary>A folder holding a runtime library file that will not load.</summary>
	static string BrokenRuntimeFolder() {
		string folder = Directory.CreateTempSubdirectory("vdf-ai-broken").FullName;
		string name = OperatingSystem.IsWindows() ? "onnxruntime.dll"
			: $"libonnxruntime.so.{AiComponents.RuntimeVersion}";
		File.WriteAllBytes(Path.Combine(folder, name), new byte[4096]);
		return folder;
	}

	/// <summary>
	/// The reported crash: the downloaded runtime would not load (its Visual C++ runtime
	/// was missing), the resolver fell through to default probing, and that bound Windows'
	/// own older System32\onnxruntime.dll, which then failed with "Unable to find an entry
	/// point named 'OrtGetApiBase'" (or an access violation on newer Windows builds).
	/// </summary>
	[Fact]
	public void ResolveRuntimeImport_DownloadThatWillNotLoad_FailsInsteadOfFallingBackToAnotherOnnxRuntime() {
		string folder = BrokenRuntimeFolder();
		try {
			string lib = Directory.GetFiles(folder).Single();

			var ex = Assert.Throws<InvalidOperationException>(() => AiComponents.ResolveRuntimeImport("onnxruntime", lib));

			Assert.Contains(lib, ex.Message);
			Assert.Contains(AiComponents.RuntimeVersion, ex.Message);
		}
		finally {
			Directory.Delete(folder, true);
		}
	}

	[Fact]
	public void ResolveRuntimeImport_NothingDownloaded_FallsThroughToDefaultProbing() {
		// Test runs (and a runtime placed next to the executable) rely on default probing.
		Assert.Equal(IntPtr.Zero, AiComponents.ResolveRuntimeImport("onnxruntime", null));
		Assert.Equal(IntPtr.Zero, AiComponents.ResolveRuntimeImport("kernel32", @"C:\does\not\matter.dll"));
	}

	[Fact]
	public void EnsureReady_RuntimeThatWillNotLoad_FailsBeforeTheScanStarts() {
		string folder = BrokenRuntimeFolder();
		File.WriteAllText(Path.Combine(folder, "runtime.version"), AiComponents.RuntimeVersionMarker);
		File.WriteAllBytes(Path.Combine(folder, AiComponents.ModelFileName), new byte[16]);
		try {
			AiComponents.TestAiFolder.Value = folder;
			// Every file is in place, so the settings page says Ready...
			Assert.Equal(AiComponentsState.Ready, AiComponents.GetState());

			// ...but the scan start (PrepareSearch / PrepareCompare) must stop with the reason.
			var ex = Assert.Throws<InvalidOperationException>(AiComponents.EnsureReady);
			Assert.Contains("could not be loaded", ex.Message);
		}
		finally {
			AiComponents.TestAiFolder.Value = null;
			Directory.Delete(folder, true);
		}
	}

	[Fact]
	public void DescribeRuntimeLoadFailure_NamesTheMissingVcRuntimeAndWhereToGetIt() {
		string message = AiComponents.DescribeRuntimeLoadFailure(@"C:\vdf\ai\onnxruntime.dll", "The specified module could not be found.",
			new[] { "VCRUNTIME140_1.dll", "MSVCP140_1.dll" });

		Assert.Contains(@"C:\vdf\ai\onnxruntime.dll", message);
		Assert.Contains("The specified module could not be found.", message);
		Assert.Contains("Visual C++ Redistributable", message);
		Assert.Contains("VCRUNTIME140_1.dll, MSVCP140_1.dll", message);
		Assert.Contains("https://aka.ms/vc14/vc_redist.", message);
	}

	[Fact]
	public void DescribeRuntimeLoadFailure_DoesNotBlameTheVcRuntimeWhenItIsInstalled() {
		string message = AiComponents.DescribeRuntimeLoadFailure("/state/ai/libonnxruntime.so.1.23.2", "invalid ELF header", Array.Empty<string>());

		Assert.Contains("invalid ELF header", message);
		Assert.DoesNotContain("Visual C++", message);
	}

	[Fact]
	public void MissingVcRuntimeLibraries_ReportsExactlyWhatTheSystemLacks() {
		List<string> missing = AiComponents.MissingVcRuntimeLibraries();

		if (!OperatingSystem.IsWindows()) {
			Assert.Empty(missing);
			return;
		}
		// No false alarms where the redistributable is installed (a misspelled name would
		// show up here), and every absent one is named.
		string[] expected = new[] { "VCRUNTIME140.dll", "VCRUNTIME140_1.dll", "MSVCP140.dll", "MSVCP140_1.dll" }
			.Where(dll => !File.Exists(Path.Combine(Environment.SystemDirectory, dll)))
			.ToArray();
		Assert.Equal(expected, missing);
	}

	[Fact]
	public void EnsureReady_HonorsTestOverride() {
		string? prev = AiComponents.TestOverrideModelPath;
		try {
			AiComponents.TestOverrideModelPath = @"D:\some\model.onnx";
			AiComponents.EnsureReady(); // must not throw
			Assert.Equal(@"D:\some\model.onnx", AiComponents.ModelPath);
		}
		finally {
			AiComponents.TestOverrideModelPath = prev;
		}
	}
}
