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

using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.ML.OnnxRuntime;
using VDF.Core.Utils;

namespace VDF.Core.AI {
	public enum AiComponentsState { Missing, RuntimeMissing, ModelMissing, Ready }

	public readonly record struct AiDownloadProgress(string Step, long BytesDone, long? BytesTotal);

	/// <summary>
	/// Locates and downloads the two native components AI matching needs: the ONNX Runtime
	/// library (per-RID archive from the official microsoft/onnxruntime GitHub release,
	/// pinned version) and the DINOv2-small embedding model (SHA256-pinned). Nothing is
	/// bundled with VDF releases — the FFmpeg pattern: opt-in download on first use into
	/// <c>{StateFolder}/ai</c>. Under Native AOT the OnnxRuntime *Managed* wrapper is pure
	/// P/Invoke; a DllImport resolver points its "onnxruntime" import at the downloaded
	/// library, so no native lib has to sit next to the executable.
	/// </summary>
	public static class AiComponents {
		/// <summary>
		/// CPU ONNX Runtime 1.32.0 uses the older 1.24.4 managed API.
		/// Compatibility is gated by downloaded-native inference tests.
		/// </summary>
		/// <summary>CPU-only ONNX Runtime (NuGet native package) used on Linux and forced-CPU Windows.</summary>
		public const string CpuRuntimeVersion = "1.32.0";
		/// <summary>
		/// ONNX Runtime with the DirectML execution provider, used on Windows so AI
		/// inference runs on any D3D12 GPU instead of burning CPU (≈160 ms of CPU time per
		/// frame on CPU vs ≈1 ms on DirectML). Published only as a NuGet package; it uses
		/// the system DirectML.dll (1.15+, in-box on current Windows 11) and falls back to
		/// the CPU provider when no usable GPU/DirectML is present. Paired with managed 1.24.4.
		/// </summary>
		public const string DirectMLRuntimeVersion = "1.24.4";
		internal const string ForceCpuRuntimeEnvVar = "VDF_AI_CPU_RUNTIME";
		internal const string ProviderEnvVar = "VDF_AI_PROVIDER";
		internal static bool UsesCudaRuntime =>
			(OperatingSystem.IsWindows() || OperatingSystem.IsLinux())
			&& RuntimeInformation.ProcessArchitecture == Architecture.X64
			&& !IsForceCpuRuntimeRequested()
			&& string.Equals(Environment.GetEnvironmentVariable(ProviderEnvVar), "cuda", StringComparison.OrdinalIgnoreCase);

		/// <summary>The ONNX Runtime version this platform installs.</summary>
		public static string RuntimeVersion => UsesDirectMLRuntime ? DirectMLRuntimeVersion : CpuRuntimeVersion;

		/// <summary>Runtime version plus its accelerator, for status text and diagnostics.</summary>
		public static string RuntimeDescription => (UsesCudaRuntime ? $"{CpuRuntimeVersion}, CUDA GPU" : UsesDirectMLRuntime ? $"{DirectMLRuntimeVersion}, DirectML GPU" : $"{CpuRuntimeVersion}, CPU")
			+ (IsFp16Selected ? ", experimental FP16 model" : "");

		/// <summary>Content of the runtime.version marker for the runtime this platform installs.</summary>
		internal static string RuntimeVersionMarker => UsesCudaRuntime ? $"{CpuRuntimeVersion}-cuda"
			: UsesDirectMLRuntime ? $"{DirectMLRuntimeVersion}-directml" : $"{CpuRuntimeVersion}-cpu";

		internal static bool UsesDirectMLRuntime =>
			OperatingSystem.IsWindows()
			&& RuntimeInformation.ProcessArchitecture is Architecture.X64 or Architecture.Arm64
			&& !IsForceCpuRuntimeRequested()
			&& !UsesCudaRuntime;

		static bool IsForceCpuRuntimeRequested() {
			string? value = Environment.GetEnvironmentVariable(ForceCpuRuntimeEnvVar);
			return value is "1" || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
		}
		public const string ModelFileName = "dinov2-small-int8.onnx";
		/// <summary>SHA256 of the model file (Xenova/dinov2-small ONNX export, quantized, Apache-2.0).</summary>
		public const string ModelSha256 = "3afdc8bc63b50558d6e5770f5b799bb82455c2311183a2de43803f343a29d917";
		// Test-only opt-in via the launcher. The default shipped model remains INT8.
		internal const string ModelVariantEnvVar = "VDF_AI_MODEL";
		internal const string Fp16ModelFileName = "dinov2-small-fp16.onnx";
		internal const string Fp16ModelSha256 = "4e9ea6fe106e2225e28ee3c1c3d53b5b92aa4af62142f6ed6b66b6a92213cf04";
		internal const string Fp16ModelUrl = "https://huggingface.co/Xenova/dinov2-small/resolve/main/onnx/model_fp16.onnx";

		internal static string ParseModelVariant(string? value) =>
			value?.Trim().ToLowerInvariant() switch {
				null or "" or "int8" => "int8",
				"fp16" => "fp16",
				_ => throw new ArgumentException("VDF_AI_MODEL must be int8 or fp16.", ModelVariantEnvVar)
			};
		internal static string SelectedModelVariant => ParseModelVariant(Environment.GetEnvironmentVariable(ModelVariantEnvVar));
		internal static bool IsFp16Selected => SelectedModelVariant == "fp16";
		internal static string SelectedModelFileName => IsFp16Selected ? Fp16ModelFileName : ModelFileName;
		internal static string SelectedModelSha256 => IsFp16Selected ? Fp16ModelSha256 : ModelSha256;

		// The original INT8 DirectML/CPU caches predate model identification.
		// New model/provider/optimization combinations MUST NOT reuse them.
		internal static string? SelectedCacheExecutionProfile {
			get {
				if (!IsFp16Selected && !UsesCudaRuntime)
					return null; // preserve original INT8 cache filenames
				if (UsesCudaRuntime) {
					var optimization = OnnxEmbedder.ParseCudaGraphOptimization(
						Environment.GetEnvironmentVariable(OnnxEmbedder.CudaGraphOptimizationEnvVar));
					string mode = optimization switch {
						GraphOptimizationLevel.ORT_ENABLE_EXTENDED => "extended",
						GraphOptimizationLevel.ORT_ENABLE_ALL => "all",
						GraphOptimizationLevel.ORT_DISABLE_ALL => "disabled",
						_ => "basic"
					};
					return $"cuda-{CpuRuntimeVersion}-{mode}";
				}
				return UsesDirectMLRuntime ? $"directml-{DirectMLRuntimeVersion}" : $"cpu-{CpuRuntimeVersion}";
			}
		}
		// The legacy VDF sidecar files have no model fingerprint in their header.
		// These exact bytes are known to have generated their contents. Keep this
		// constant independent of ModelSha256 if the production model changes.
		internal const string LegacyInt8ModelSha256 = "3afdc8bc63b50558d6e5770f5b799bb82455c2311183a2de43803f343a29d917";
		// Bump when changing preprocessing, CLS/output selection or quantization
		// semantics even if the model ONNX file has not changed.
		internal const int EmbeddingPreprocessingRevision = 1;

		/// <summary>
		/// Sidecar cache names are scoped by the model hash and preprocessing revision.
		/// Keep existing untagged INT8 caches intact; a different model uses a new
		/// sidecar, avoiding comparisons between incompatible embedding spaces.
		/// This is a filename partition, not a migration or destructive cleanup.
		/// </summary>
		internal static string EmbeddingSidecarFileName(string legacyName) =>
			EmbeddingSidecarFileNameForModel(legacyName, SelectedModelSha256,
				EmbeddingPreprocessingRevision, SelectedCacheExecutionProfile);

		internal static string EmbeddingSidecarFileNameForModel(string legacyName,
			string modelSha256, int preprocessingRevision, string? executionProfile = null) {
			if (modelSha256.Length != 64 || !modelSha256.All(Uri.IsHexDigit))
				throw new ArgumentException("Expected the model's 64-character SHA256.", nameof(modelSha256));
			if (preprocessingRevision <= 0)
				throw new ArgumentOutOfRangeException(nameof(preprocessingRevision));
			if (string.Equals(modelSha256, LegacyInt8ModelSha256, StringComparison.OrdinalIgnoreCase)
				&& preprocessingRevision == 1 && string.IsNullOrEmpty(executionProfile))
				return legacyName;

			string suffix = "";
			if (!string.IsNullOrEmpty(executionProfile)) {
				if (!executionProfile.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '.'))
					throw new ArgumentException("Invalid execution profile cache tag.", nameof(executionProfile));
				suffix = $".{executionProfile.ToLowerInvariant()}";
			}
			string filename = Path.GetFileNameWithoutExtension(legacyName);
			string extension = Path.GetExtension(legacyName);
			return $"{filename}.{modelSha256.ToLowerInvariant()}.p{preprocessingRevision}{suffix}{extension}";
		}
		// Primary: mirrored release asset on the VDF repo (stable bytes, hash-pinned).
		// Fallback: the upstream HuggingFace export the mirror was taken from.
		const string ModelPrimaryUrl = "https://github.com/0x90d/videoduplicatefinder/releases/download/ai-models-v1/dinov2-small-int8.onnx";
		const string ModelFallbackUrl = "https://huggingface.co/Xenova/dinov2-small/resolve/main/onnx/model_quantized.onnx";
		const string VersionMarkerFileName = "runtime.version";

		public static string AiFolder => TestAiFolder.Value ?? Path.Combine(CoreUtils.StateFolder, "ai");
		public static string ModelPath => TestOverrideModelPath ?? Path.Combine(AiFolder, SelectedModelFileName);

		/// <summary>
		/// Test hook: points ModelPath at the checked-in tiny embedder and makes
		/// EnsureReady succeed, so suites exercise the AI pipeline without downloads
		/// (the native runtime comes from the test projects' full OnnxRuntime package).
		/// </summary>
		internal static string? TestOverrideModelPath;

		/// <summary>
		/// Test hook: redirects AiFolder for the calling test's async flow only. AsyncLocal
		/// rather than a static field because OnnxEmbedderTests load the real runtime in
		/// parallel and must not resolve it from another test's folder.
		/// </summary>
		internal static readonly AsyncLocal<string?> TestAiFolder = new();

		static bool resolverInstalled;
		static readonly object resolverLock = new();

		/// <summary>
		/// The Visual C++ runtime the pinned Windows build links dynamically (dumpbin
		/// /dependents of onnxruntime.dll 1.23.2). VDF itself is Native AOT and its FFmpeg
		/// is static, so machines without the redistributable run VDF fine until AI matching
		/// tries to load the runtime. Recheck when bumping RuntimeVersion.
		/// </summary>
		static readonly string[] VcRuntimeLibraries = { "VCRUNTIME140.dll", "VCRUNTIME140_1.dll", "MSVCP140.dll", "MSVCP140_1.dll" };

		public static AiComponentsState GetState() {
			bool runtime = FindRuntimeLibrary() != null && HasCurrentRuntimeVersion();
			bool model = IsSelectedModelValid();
			if (runtime && model) return AiComponentsState.Ready;
			if (runtime) return AiComponentsState.ModelMissing;
			if (model) return AiComponentsState.RuntimeMissing;
			return AiComponentsState.Missing;
		}

		public static bool IsReady => GetState() == AiComponentsState.Ready;

		/// <summary>
		/// Throws with an actionable message when the components are not present, or when
		/// the runtime is present but cannot be loaded (the scan should fail at its start,
		/// not after the file list has been built).
		/// </summary>
		public static void EnsureReady() {
			if (TestOverrideModelPath != null)
				return;
			AiComponentsState state = GetState();
			if (state != AiComponentsState.Ready)
				throw new InvalidOperationException(
					$"AI matching components are not available ({state}). " +
					$"Download them in Settings → Matching, or place onnxruntime {RuntimeVersion} and {SelectedModelFileName} into '{AiFolder}'.");
			LoadRuntime();
		}

		// For experimental FP16, validate even a pre-existing file: a mismatched
		// model must never silently be used merely because the filename exists.
		// Preserve legacy INT8's existing cheap readiness checks.
		internal static bool IsSelectedModelValid() {
			if (!File.Exists(ModelPath)) return false;
			if (!IsFp16Selected || TestOverrideModelPath != null) return true;
			try {
				using var stream = File.OpenRead(ModelPath);
				return Convert.ToHexString(SHA256.HashData(stream))
					.Equals(SelectedModelSha256, StringComparison.OrdinalIgnoreCase);
			}
			catch (IOException) { return false; }
			catch (UnauthorizedAccessException) { return false; }
		}

		static bool HasCurrentRuntimeVersion() {
			try {
				string marker = Path.Combine(AiFolder, VersionMarkerFileName);
				return File.Exists(marker) && File.ReadAllText(marker).Trim() == RuntimeVersionMarker;
			}
			catch { return false; }
		}

		/// <summary>The downloaded ONNX Runtime library file, or null when absent.</summary>
		internal static string? FindRuntimeLibrary() {
			try {
				if (!Directory.Exists(AiFolder)) return null;
				// Windows: onnxruntime.dll — Linux: libonnxruntime.so.<ver>
				return Directory.EnumerateFiles(AiFolder)
					.Where(f => {
						string name = Path.GetFileName(f);
						return name.Contains("onnxruntime", StringComparison.OrdinalIgnoreCase) &&
							!name.Contains("providers", StringComparison.OrdinalIgnoreCase) &&
							(name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) ||
							 name.Contains(".so", StringComparison.OrdinalIgnoreCase) ||
							 name.EndsWith(".dylib", StringComparison.OrdinalIgnoreCase));
					})
					.OrderByDescending(f => Path.GetFileName(f).Length) // prefer the fully-versioned real file over a bare symlink copy
					.FirstOrDefault();
			}
			catch { return null; }
		}

		/// <summary>
		/// Routes the Managed wrapper's "onnxruntime" DllImport to the downloaded library.
		/// Must run before the first OnnxRuntime native call (OnnxEmbedder does so).
		/// SetDllImportResolver may only be called once per assembly, hence the guard.
		/// </summary>
		internal static void EnsureResolverInstalled() {
			if (resolverInstalled) return;
			lock (resolverLock) {
				if (resolverInstalled) return;
				NativeLibrary.SetDllImportResolver(typeof(InferenceSession).Assembly,
					(name, _, _) => ResolveRuntimeImport(name, FindRuntimeLibrary()));
				resolverInstalled = true;
			}
		}

		/// <summary>
		/// Loads the downloaded runtime before anything touches an OnnxRuntime type. A load
		/// failure inside the wrapper's static constructor would surface as a
		/// TypeInitializationException that stays cached for the rest of the process, so
		/// installing the missing piece would not help until VDF is restarted.
		/// </summary>
		internal static void LoadRuntime() {
			EnsureResolverInstalled();
			string? lib = FindRuntimeLibrary();
			if (lib != null)
				LoadRuntimeLibrary(lib);
		}

		/// <summary>
		/// The DllImport resolver. Nothing downloaded falls through to default probing (test
		/// runs ship the runtime next to their binaries). A download that will not load must
		/// NOT fall through: Windows has its own onnxruntime.dll in System32 (Windows ML), and
		/// default probing binds to that one. It is an older version, so the wrapper died with
		/// "Unable to find an entry point named 'OrtGetApiBase'" on older Windows builds and
		/// with an access violation on newer ones.
		/// </summary>
		internal static IntPtr ResolveRuntimeImport(string libraryName, string? downloadedLibrary) {
			if (!libraryName.Contains("onnxruntime", StringComparison.OrdinalIgnoreCase) || downloadedLibrary == null)
				return IntPtr.Zero;
			return LoadRuntimeLibrary(downloadedLibrary);
		}

		// Loading an already loaded path just returns the same module, so no caching needed.
		static IntPtr LoadRuntimeLibrary(string lib) {
			try {
				return NativeLibrary.Load(lib);
			}
			catch (Exception e) when (e is DllNotFoundException or BadImageFormatException) {
				throw new InvalidOperationException(DescribeRuntimeLoadFailure(lib, e.Message, MissingVcRuntimeLibraries()), e);
			}
		}

		internal static string DescribeRuntimeLoadFailure(string lib, string reason, IReadOnlyList<string> missingVcRuntime) {
			string message = $"ONNX Runtime {RuntimeVersion} could not be loaded from '{lib}': {reason}";
			if (missingVcRuntime.Count == 0)
				return message;
			string arch = RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "arm64" : "x64";
			return message + Environment.NewLine +
				$"The Microsoft Visual C++ Redistributable is missing or outdated (not found: {string.Join(", ", missingVcRuntime)}). " +
				$"Install the current one from https://aka.ms/vc14/vc_redist.{arch}.exe and restart VDF.";
		}

		/// <summary>The VC++ runtime libraries the Windows runtime needs that the OS cannot find.</summary>
		internal static List<string> MissingVcRuntimeLibraries() {
			var missing = new List<string>();
			if (!OperatingSystem.IsWindows())
				return missing;
			foreach (string dll in VcRuntimeLibraries) {
				if (NativeLibrary.TryLoad(dll, out IntPtr handle))
					NativeLibrary.Free(handle);
				else
					missing.Add(dll);
			}
			return missing;
		}

		/// <summary>
		/// Where to fetch the runtime for this platform: the archive URL, its file name and
		/// the directory inside the archive holding the native libraries (null = the
		/// archive's <c>lib/</c> directory, as in the GitHub release archives).
		/// </summary>
		internal static (Uri Url, string ArchiveFileName, string? NativeDirectory) GetRuntimeDownloadPlan() {
			string os =
				RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "win" :
				RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "linux" :
				throw new PlatformNotSupportedException("This VDF fork supports only Windows and Linux.");
			if (UsesDirectMLRuntime) {
				string rid = RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "win-arm64" : "win-x64";
				const string package = "microsoft.ml.onnxruntime.directml";
				string nupkg = $"{package}.{DirectMLRuntimeVersion}.nupkg";
				return (new Uri($"https://api.nuget.org/v3-flatcontainer/{package}/{DirectMLRuntimeVersion}/{nupkg}"),
					nupkg, $"runtimes/{rid}/native");
			}
			string nativeRid = RuntimeInformation.ProcessArchitecture switch {
				Architecture.X64 => $"{os}-x64",
				Architecture.Arm64 => $"{os}-arm64",
				_ => throw new PlatformNotSupportedException($"AI matching is not supported on {RuntimeInformation.ProcessArchitecture}.")
			};
			string nativePackage = UsesCudaRuntime
				? (os == "win" ? "microsoft.ml.onnxruntime.gpu.windows" : "microsoft.ml.onnxruntime.gpu.linux")
				: "microsoft.ml.onnxruntime";
			string file = $"{nativePackage}.{CpuRuntimeVersion}.nupkg";
			return (new Uri($"https://api.nuget.org/v3-flatcontainer/{nativePackage}/{CpuRuntimeVersion}/{file}"),
				file, $"runtimes/{nativeRid}/native");
		}

		/// <summary>Downloads whatever is missing (runtime and/or model). Safe to call when Ready.</summary>
		public static async Task DownloadAsync(IProgress<AiDownloadProgress>? progress, CancellationToken token) {
			Directory.CreateDirectory(AiFolder);
			using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(15) };

			var downloads = new List<Func<CancellationToken, Task>>(2);
			if (FindRuntimeLibrary() == null || !HasCurrentRuntimeVersion())
				downloads.Add(ct => DownloadRuntimeAsync(http, progress, ct));
			if (!IsSelectedModelValid())
				downloads.Add(ct => DownloadModelAsync(http, progress, ct));
			// The two archives come from different hosts (onnxruntime GitHub release vs
			// the model mirror), so fetching them concurrently roughly halves the
			// first-use wait. Consumers already key progress off AiDownloadProgress.Step.
			await RunDownloadsAsync(downloads, token);
		}

		/// <summary>
		/// Runs the given downloads concurrently. The first failure cancels the
		/// remaining downloads (no point finishing a 100 MB sibling once the feature
		/// cannot become ready) and its exception surfaces — specifically NOT the
		/// sibling's induced OperationCanceledException, which Task.WhenAll would
		/// otherwise rethrow when the canceled task comes first in the list.
		/// </summary>
		internal static async Task RunDownloadsAsync(IReadOnlyList<Func<CancellationToken, Task>> downloads, CancellationToken token) {
			if (downloads.Count == 0)
				return;
			if (downloads.Count == 1) {
				await downloads[0](token);
				return;
			}
			using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token);
			List<Task> tasks = downloads.Select(RunOne).ToList();
			async Task RunOne(Func<CancellationToken, Task> download) {
				try {
					await download(linkedCts.Token);
				}
				catch {
					linkedCts.Cancel();
					throw;
				}
			}
			try {
				await Task.WhenAll(tasks);
			}
			catch when (!token.IsCancellationRequested) {
				foreach (Task t in tasks) {
					Exception? real = t.Exception?.InnerExceptions.FirstOrDefault(e => e is not OperationCanceledException);
					if (real != null)
						System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(real).Throw();
				}
				throw;
			}
		}

		static async Task DownloadRuntimeAsync(HttpClient http, IProgress<AiDownloadProgress>? progress, CancellationToken token) {
			(Uri url, string archiveName, string? nativeDirectory) = GetRuntimeDownloadPlan();
			// Per-attempt temp dir: a fixed shared path let two VDF processes (GUI + CLI,
			// or Web + CLI in one container) clobber each other's in-progress download —
			// FileShare.None collisions, or one process's cleanup deleting the other's files.
			string tempRoot = Path.Combine(Path.GetTempPath(), $"VDF.AiDownload.{Guid.NewGuid():N}");
			Directory.CreateDirectory(tempRoot);
			string archivePath = Path.Combine(tempRoot, archiveName);
			try {
				string runtimeStep = $"ONNX Runtime {RuntimeVersion}";
				await DownloadUtils.DownloadFileAsync(http, url, archivePath, runtimeStep,
					(done, total) => progress?.Report(new AiDownloadProgress(runtimeStep, done, total)), token);

				string extractDir = Path.Combine(tempRoot, "extracted");
				Directory.CreateDirectory(extractDir);
				ArchiveUtils.Extract(archivePath, extractDir,
					archiveName.EndsWith(".tgz", StringComparison.OrdinalIgnoreCase) ? ArchiveKind.TarGz : ArchiveKind.Zip);

				// Purge any previously installed runtime BEFORE copying: the Linux
				// library names carry the version (libonnxruntime.so.1.23.2), so upgraded
				// installs would otherwise accumulate versions and FindRuntimeLibrary's
				// tie-break could keep loading the stale one while the marker claims the
				// new version — with GetState() reporting Ready, only deleting the ai
				// folder by hand would have recovered.
				foreach (string old in Directory.EnumerateFiles(AiFolder)) {
					string oldName = Path.GetFileName(old);
					if (oldName.Contains("onnxruntime", StringComparison.OrdinalIgnoreCase))
						try { File.Delete(old); } catch { /* loaded/locked — overwritten below */ }
				}

				// NuGet packages lay out the provider and core binaries in
				// runtimes/<rid>/native. Keep them together in AiFolder.
				string? libDir = nativeDirectory != null
					? Path.Combine(extractDir, nativeDirectory.Replace('/', Path.DirectorySeparatorChar))
					: Directory.EnumerateDirectories(extractDir, "lib", SearchOption.AllDirectories).FirstOrDefault();
				if (libDir == null || !Directory.Exists(libDir))
					throw new IOException($"Downloaded ONNX Runtime archive has no {nativeDirectory ?? "lib/"} directory ({archiveName}).");
				foreach (string file in Directory.EnumerateFiles(libDir)) {
					string name = Path.GetFileName(file);
					if (name.EndsWith(".pdb", StringComparison.OrdinalIgnoreCase) || name.EndsWith(".lib", StringComparison.OrdinalIgnoreCase)) continue;
					File.Copy(file, Path.Combine(AiFolder, name), overwrite: true);
				}
				await File.WriteAllTextAsync(Path.Combine(AiFolder, VersionMarkerFileName), RuntimeVersionMarker, token);
				if (FindRuntimeLibrary() == null)
					throw new IOException("ONNX Runtime archive extracted but no runtime library was found in it.");
			}
			finally {
				try { Directory.Delete(tempRoot, true); } catch { }
			}
		}

		static async Task DownloadModelAsync(HttpClient http, IProgress<AiDownloadProgress>? progress, CancellationToken token) {
			// Unique temp name for the same two-process reason as the runtime download;
			// the final File.Move is atomic either way.
			string tempPath = ModelPath + $".{Guid.NewGuid():N}.download";
			Action<long, long?> onProgress = (done, total) => progress?.Report(new AiDownloadProgress("AI model", done, total));
			try {
				if (IsFp16Selected) {
					await DownloadUtils.DownloadFileAsync(http, new Uri(Fp16ModelUrl),
						tempPath, "AI FP16 model (experimental)", onProgress, token);
				}
				else {
					try {
						await DownloadUtils.DownloadFileAsync(http, new Uri(ModelPrimaryUrl), tempPath, "AI model", onProgress, token);
					}
					catch (Exception e) when (e is not OperationCanceledException) {
						Logger.Instance.Info($"AI model mirror unavailable ({e.Message}), falling back to upstream source.");
						await DownloadUtils.DownloadFileAsync(http, new Uri(ModelFallbackUrl), tempPath, "AI model", onProgress, token);
					}
				}

				using (var stream = File.OpenRead(tempPath)) {
					string hash = Convert.ToHexString(SHA256.HashData(stream));
					if (!hash.Equals(SelectedModelSha256, StringComparison.OrdinalIgnoreCase))
						throw new IOException($"AI model download failed the integrity check (SHA256 {hash}, expected {SelectedModelSha256}).");
				}
				// Close the verification stream before replacing the file on Windows.
				File.Move(tempPath, ModelPath, overwrite: true);
			}
			finally {
				try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
			}
		}

	}
}
