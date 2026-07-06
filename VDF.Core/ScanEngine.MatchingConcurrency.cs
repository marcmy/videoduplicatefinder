// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

namespace VDF.Core {
	public sealed partial class ScanEngine {
		/// <summary>
		/// Resolves comparison concurrency independently from media extraction.
		/// A positive configured value is honored up to the machine's logical CPU
		/// count. Non-positive values reserve one or two logical processors and
		/// cap matching at roughly 80% of the machine for UI/system headroom.
		/// </summary>
		internal static int CalculateMatchingMaxDegreeOfParallelism(
			int configured,
			int processorCount) {
			processorCount = Math.Max(1, processorCount);
			if (configured > 0)
				return Math.Min(configured, processorCount);

			int reservedProcessors = processorCount >= 8 ? 2 : processorCount >= 2 ? 1 : 0;
			int reserveCap = Math.Max(1, processorCount - reservedProcessors);
			int percentageCap = Math.Max(1, (int)Math.Ceiling(processorCount * 0.80d));
			return Math.Min(reserveCap, percentageCap);
		}

		internal int GetMatchingMaxDegreeOfParallelism() =>
			CalculateMatchingMaxDegreeOfParallelism(
				Settings.MatchingMaxDegreeOfParallelism,
				Environment.ProcessorCount);
	}
}
