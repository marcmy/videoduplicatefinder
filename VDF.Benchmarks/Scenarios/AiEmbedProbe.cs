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

using System.Diagnostics;
using VDF.Core.AI;

namespace VDF.Benchmarks.Scenarios;

/// <summary>
/// Measures AI embedding throughput and process CPU time per execution provider, and how
/// closely GPU embeddings agree with CPU embeddings for the same frames.
/// Usage: --probe-ai-embed [--ai-folder DIR] [--model PATH] [--reference-model PATH] [--frames N] [--batch N] [--devices cpu,gpu]
/// </summary>
public static class AiEmbedProbe {
	public static int Run(string[] args) {
		string? aiFolder = null;
		string? model = null;
		string? referenceModel = null;
		int frames = 256;
		int batch = OnnxEmbedder.MaxBatch;
		string[] devices = { "cpu", "gpu" };
		for (int i = 1; i < args.Length; i++) {
			string Next() => ++i < args.Length ? args[i] : throw new ArgumentException($"Missing value after {args[i - 1]}.");
			switch (args[i]) {
				case "--ai-folder": aiFolder = Next(); break;
				case "--model": model = Next(); break;
				case "--reference-model": referenceModel = Next(); break;
				case "--frames": frames = int.Parse(Next()); break;
				case "--batch": batch = Math.Clamp(int.Parse(Next()), 1, 64); break;
				case "--devices": devices = Next().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries); break;
				default:
					Console.WriteLine("Usage: --probe-ai-embed [--ai-folder DIR] [--model PATH] [--reference-model PATH] [--frames N] [--batch N] [--devices cpu,gpu]");
					return args[i] is "--help" or "-h" ? 0 : 1;
			}
		}

		if (aiFolder != null)
			AiComponents.TestAiFolder.Value = Path.GetFullPath(aiFolder);
		string modelPath = model != null ? Path.GetFullPath(model) : AiComponents.ModelPath;
		Console.WriteLine($"AI folder: {AiComponents.AiFolder}");
		Console.WriteLine($"Model: {modelPath}");

		var random = new Random(1234);
		var inputs = new List<byte[]>(frames);
		for (int f = 0; f < frames; f++) {
			// Smooth synthetic images (gradients + blobs), closer to video frames than noise.
			byte[] img = new byte[OnnxEmbedder.InputSide * OnnxEmbedder.InputSide * 3];
			int cx = random.Next(224), cy = random.Next(224), phase = random.Next(256);
			for (int y = 0; y < 224; y++)
				for (int x = 0; x < 224; x++) {
					int d = ((x - cx) * (x - cx) + (y - cy) * (y - cy)) / 64;
					int o = (y * 224 + x) * 3;
					img[o] = (byte)(x + phase);
					img[o + 1] = (byte)(y * 2 - d);
					img[o + 2] = (byte)(d + phase * 3);
				}
			inputs.Add(img);
		}

		Dictionary<string, float[][]> results = new();
		foreach (string device in devices) {
			AiInferenceDevice requested = device.Equals("gpu", StringComparison.OrdinalIgnoreCase) ? AiInferenceDevice.Gpu : AiInferenceDevice.Cpu;
			try {
				var loadSw = Stopwatch.StartNew();
				using var embedder = new OnnxEmbedder(modelPath, requested);
				long loadMs = loadSw.ElapsedMilliseconds;
				embedder.EmbedBatch(inputs.Take(batch).ToList()); // warm-up
				TimeSpan cpuBefore = Process.GetCurrentProcess().TotalProcessorTime;
				var sw = Stopwatch.StartNew();
				var embeddings = new List<float[]>(frames);
				for (int start = 0; start < frames; start += batch)
					embeddings.AddRange(embedder.EmbedBatch(inputs.Skip(start).Take(batch).ToList()));
				sw.Stop();
				TimeSpan cpu = Process.GetCurrentProcess().TotalProcessorTime - cpuBefore;
				results[device] = embeddings.ToArray();
				Console.WriteLine($"{device,-4} [{embedder.Description}] load={loadMs}ms, {frames} frames in {sw.ElapsedMilliseconds}ms = {sw.Elapsed.TotalMilliseconds / frames:0.00} ms/frame, {frames / sw.Elapsed.TotalSeconds:0.0} frames/s, process CPU={cpu.TotalMilliseconds:0}ms ({cpu.TotalMilliseconds / frames:0.0} ms/frame)");
			}
			catch (Exception e) {
				Console.WriteLine($"{device,-4} unavailable: {e.Message}");
			}
		}

		if (results.TryGetValue("cpu", out float[][]? cpuEmb) && results.TryGetValue("gpu", out float[][]? gpuEmb))
			PrintSimilarity("CPU vs GPU", cpuEmb, gpuEmb);

		if (referenceModel != null && results.Count > 0) {
			// Agreement with embeddings from another model (e.g. the int8 model behind
			// existing caches) on the same frames, through the stored int8 quantization.
			using var reference = new OnnxEmbedder(Path.GetFullPath(referenceModel), AiInferenceDevice.Cpu);
			var referenceEmbeddings = new List<float[]>(frames);
			for (int start = 0; start < frames; start += batch)
				referenceEmbeddings.AddRange(reference.EmbedBatch(inputs.Skip(start).Take(batch).ToList()));
			foreach ((string device, float[][] embeddings) in results)
				PrintSimilarity($"{device} vs reference model", embeddings, referenceEmbeddings.ToArray());
		}
		return 0;
	}

	static void PrintSimilarity(string label, float[][] a, float[][] b) {
		double min = 1, sum = 0;
		for (int i = 0; i < a.Length; i++) {
			double dot = 0;
			for (int k = 0; k < a[i].Length; k++)
				dot += a[i][k] * b[i][k];
			min = Math.Min(min, dot);
			sum += dot;
		}
		Console.WriteLine($"{label} embedding cosine similarity: mean={sum / a.Length:0.00000}, min={min:0.00000}");
	}
}
