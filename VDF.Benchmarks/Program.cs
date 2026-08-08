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

using BenchmarkDotNet.Running;
using VDF.Benchmarks.Scenarios;

namespace VDF.Benchmarks;

internal static class Program {
	static int Main(string[] args) {
		if (args.Length > 0 && args[0] == "--list-probes") {
			PrintProbeList();
			return 0;
		}

		// Direct phase-timing probe (bypasses BDN so we can split open vs decode cost).
		if (args.Length > 0 && args[0] == "--probe-decoder-reuse")
			return DecoderReuseProbe.Run(args);

		// Synthetic compare-phase probe (ScanForDuplicates + HighlightBestMatches).
		if (args.Length > 0 && args[0] == "--probe-compare")
			return ComparePhaseProbe.Run(args);

		// Repeatable JSON regression probe for the real extraction paths.
		if (args.Length > 0 && args[0] == "--probe-regression")
			return RegressionProbe.Run(args);

		// Full search+compare pipeline probe: discovery -> analysis/hashing -> comparison/finalization.
		if (args.Length > 0 && args[0] == "--probe-scan-pipeline")
			return ScanPipelineProbe.Run(args);

		// BenchmarkSwitcher routes CLI args (--filter, --list, --job, --exporters …)
		// to BDN. With no args, prints the menu of available benchmarks.
		BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
		return 0;
	}

	static void PrintProbeList() {
		Console.WriteLine("Direct VDF performance probes:");
		Console.WriteLine("  --probe-decoder-reuse   Native decoder open/seek/decode phase timing");
		Console.WriteLine("  --probe-compare         Synthetic duplicate matching/highlight phases");
		Console.WriteLine("  --probe-regression      Repeatable extraction-path JSON regression probe");
		Console.WriteLine("  --probe-scan-pipeline   Full discovery -> analysis -> compare/finalize scan profile");
		Console.WriteLine();
		Console.WriteLine("Pass --help after a probe name for probe-specific options where supported.");
	}
}
