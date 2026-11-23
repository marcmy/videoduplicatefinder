// Real DINOv2 benchmark. Deliberately opt-in: downloads the SHA-pinned model
// and versioned ONNX native library; GPU validation requires a local GPU runner.
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using VDF.Core.AI;
using Xunit.Abstractions;

namespace VDF.Core.Tests.AI;

public sealed class OnnxRealModelBenchmarkTests {
	private readonly ITestOutputHelper output;
	public OnnxRealModelBenchmarkTests(ITestOutputHelper output) => this.output = output;

	[Fact]
	public async Task DINOv2_RealModel_Benchmark_OptIn() {
		if (Environment.GetEnvironmentVariable("VDF_ONNX_BENCHMARK") != "1") return;
		string? target = Environment.GetEnvironmentVariable("VDF_ONNX_BENCH_OUTPUT");
		if (string.IsNullOrWhiteSpace(target))
			throw new ArgumentException("Set VDF_ONNX_BENCH_OUTPUT to the JSON report path.");

		string reportPath = Path.GetFullPath(target);
		Directory.CreateDirectory(Path.GetDirectoryName(reportPath)!);
		string componentDir = Path.Combine(Path.GetTempPath(), "VDF.OnnxBenchmark", "components");
		Directory.CreateDirectory(componentDir);
		// Each backend runs as a separate dotnet test process. ONNX native DLLs
		// remain loaded for the lifetime of that process on Windows.
		AiComponents.TestAiFolder.Value = componentDir;
		try {
			await AiComponents.DownloadAsync(progress: null, CancellationToken.None);
			AiComponents.EnsureReady();
			using (var stream = File.OpenRead(AiComponents.ModelPath)) {
				string hash = Convert.ToHexString(SHA256.HashData(stream));
				Assert.Equal(AiComponents.ModelSha256, hash, ignoreCase: true);
			}

			bool cuda = AiComponents.UsesCudaRuntime;
			bool directML = AiComponents.UsesDirectMLRuntime;
			string provider = cuda ? "cuda" : directML ? "directml" : "cpu";
			AiInferenceDevice device = cuda || directML ? AiInferenceDevice.Gpu : AiInferenceDevice.Cpu;
			byte[][] samples = MakeSamples();
			float[][] embeddings;
			double[] singleMs;
			double[] batchMs;
			string description;
			var batch = new[] {
				samples[0], samples[1], samples[2], samples[3],
				samples[4], samples[5], samples[0], samples[2]
			};
			using (var embedder = new OnnxEmbedder(AiComponents.ModelPath, device)) {
				description = embedder.Description;
				Assert.Equal(cuda || directML, embedder.UsesGpu);
				// Warm up the native kernels and memory allocators before measuring.
				for (int i = 0; i < 3; i++) embedder.EmbedBatch(new[] { samples[0] });
				for (int i = 0; i < 2; i++) embedder.EmbedBatch(batch);
				embeddings = embedder.EmbedBatch(samples);
				Assert.Equal(samples.Length, embeddings.Length);
				foreach (float[] vector in embeddings) {
					Assert.Equal(EmbeddingMath.Dimensions, vector.Length);
					Assert.All(vector, v => Assert.True(float.IsFinite(v)));
					double norm = Math.Sqrt(vector.Sum(v => (double)v * v));
					Assert.InRange(norm, 0.999, 1.001);
				}
				singleMs = Measure(embedder, new[] { samples[0] }, 12);
				batchMs = Measure(embedder, batch, 6);
			}

			// Enable ONNX profiling only in a SECOND session. Profiling can add
			// overhead and would contaminate the timing measurements above.
			string? oldPrefix = Environment.GetEnvironmentVariable("VDF_AI_PROFILE_PREFIX");
			string? profilePath;
			try {
				string prefix = Path.Combine(Path.GetDirectoryName(reportPath)!,
					$"onnx-profile-{provider}-{Environment.ProcessId}");
				Environment.SetEnvironmentVariable("VDF_AI_PROFILE_PREFIX", prefix);
				using var embedder = new OnnxEmbedder(AiComponents.ModelPath, device);
				embedder.EmbedBatch(batch);
				profilePath = embedder.EndProfiling();
			}
			finally {
				Environment.SetEnvironmentVariable("VDF_AI_PROFILE_PREFIX", oldPrefix);
			}
			Assert.False(string.IsNullOrWhiteSpace(profilePath));
			Assert.True(File.Exists(profilePath), "ONNX profiling output was not written.");

			byte[][] q = embeddings.Select(EmbeddingMath.QuantizeUnitVector).ToArray();
			var pairs = (from i in Enumerable.Range(0, samples.Length)
						 from j in Enumerable.Range(i + 1, samples.Length - i - 1)
						 select new {
							 a = i, b = j,
							 rawCosine = Cosine(embeddings[i], embeddings[j]),
							 quantizedSimilarity = EmbeddingMath.CosineSimilarity(q[i], q[j]),
						 }).ToArray();
			var profile = ProfileSummary(profilePath!);
			var report = new {
				formatVersion = 1,
				provider,
				description,
				nativeRuntime = AiComponents.RuntimeVersion,
				managedRuntime = "1.24.4",
				modelSha256 = AiComponents.ModelSha256,
				operatingSystem = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
				architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
				processorCount = Environment.ProcessorCount,
				generatedUtc = DateTimeOffset.UtcNow,
				singleMs,
				batch8Ms = batchMs,
				singleMedianMs = Median(singleMs),
				batch8MedianMs = Median(batchMs),
				embeddings,
				pairs,
				profilePath,
				profile,
			};
			await File.WriteAllTextAsync(reportPath, JsonSerializer.Serialize(report,
				new JsonSerializerOptions { WriteIndented = true }));
			output.WriteLine($"DINOv2 {provider}: 1-frame median {Median(singleMs):F2} ms, 8-frame median {Median(batchMs):F2} ms");
			output.WriteLine($"Profile: {profilePath}; GPU nodes: {profile.gpuNodes}, CPU nodes: {profile.cpuNodes}");
			output.WriteLine($"JSON report: {reportPath}");
		}
		finally {
			AiComponents.TestAiFolder.Value = null;
		}
	}

	private static double[] Measure(OnnxEmbedder model, byte[][] frames, int iterations) {
		var durations = new double[iterations];
		for (int i = 0; i < iterations; i++) {
			long start = Stopwatch.GetTimestamp();
			model.EmbedBatch(frames);
			durations[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
		}
		return durations;
	}

	private static double Median(double[] timings) {
		double[] sorted = timings.Order().ToArray();
		int mid = sorted.Length / 2;
		return sorted.Length % 2 == 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
	}

	private static float Cosine(float[] a, float[] b) {
		double dot = 0;
		for (int i = 0; i < a.Length; i++) dot += (double)a[i] * b[i];
		return (float)Math.Clamp(dot, -1, 1);
	}

	private static (int gpuNodes, int cpuNodes, int otherNodes) ProfileSummary(string profilePath) {
		using var doc = JsonDocument.Parse(File.ReadAllText(profilePath));
		int gpu = 0, cpu = 0, other = 0;
		foreach (var ev in doc.RootElement.EnumerateArray()) {
			if (!ev.TryGetProperty("cat", out var cat) || cat.GetString() != "Node") continue;
			if (!ev.TryGetProperty("args", out var args) ||
				!args.TryGetProperty("provider", out var provider)) continue;
			string p = provider.GetString() ?? "";
			if (p.Contains("CUDA", StringComparison.OrdinalIgnoreCase) ||
				p.Contains("Dml", StringComparison.OrdinalIgnoreCase)) gpu++;
			else if (p.Contains("CPU", StringComparison.OrdinalIgnoreCase)) cpu++;
			else other++;
		}
		return (gpu, cpu, other);
	}

	// Identical on every backend: textured gradient, a lightly altered version,
	// spatial shift, stripes, a second texture, and an exact duplicate.
	// These exercise output stability; they are NOT a substitute for real video
	// samples around VDF's 94% and 89% decision thresholds.
	private static byte[][] MakeSamples() {
		int side = OnnxEmbedder.InputSide;
		byte[] gradient = new byte[side * side * 3];
		byte[] altered = new byte[gradient.Length];
		byte[] shifted = new byte[gradient.Length];
		byte[] stripes = new byte[gradient.Length];
		byte[] texture = new byte[gradient.Length];
		for (int y = 0; y < side; y++)
		for (int x = 0; x < side; x++) {
			int i = (y * side + x) * 3;
			int checker = ((x / 11 + y / 13) % 2) * 45;
			gradient[i] = (byte)((x + checker) % 256);
			gradient[i + 1] = (byte)((y * 2 + checker) % 256);
			gradient[i + 2] = (byte)((x + y + checker) % 256);
			for (int c = 0; c < 3; c++)
				altered[i + c] = (byte)Math.Clamp(gradient[i + c] + (x + y) % 7 - 3, 0, 255);
			stripes[i] = (byte)((x / 9 % 2) * 230);
			stripes[i + 1] = (byte)((y / 17 % 2) * 200);
			stripes[i + 2] = (byte)((x / 9 + y / 17) % 2 * 255);
			int hash = unchecked((x * 73856093) ^ (y * 19349663) ^ 12345);
			texture[i] = (byte)hash;
			texture[i + 1] = (byte)(hash >> 8);
			texture[i + 2] = (byte)(hash >> 16);
		}
		for (int y = 0; y < side; y++)
		for (int x = 0; x < side; x++) {
			int from = (Math.Clamp(y - 3, 0, side - 1) * side +
				Math.Clamp(x - 5, 0, side - 1)) * 3;
			Array.Copy(gradient, from, shifted, (y * side + x) * 3, 3);
		}
		return new[] { gradient, altered, shifted, stripes, texture, (byte[])gradient.Clone() };
	}
}
