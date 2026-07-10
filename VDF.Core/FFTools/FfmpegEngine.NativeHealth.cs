using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		static bool ShouldUseNativeBinding =>
			UseNativeBinding
			&& !IsNativeBindingDisabledForSessionForTests
			&& FFmpegHelper.CanLoadNativeLibraries;

		internal static bool ShouldAttemptNativeBinding => ShouldUseNativeBinding;

		internal static bool IsNativeBindingDisabledForSessionForTests =>
			Volatile.Read(ref NativeDisabledForSession) != 0;

		internal static void ResetNativeBindingHealthForTests() =>
			ResetNativeBindingHealth();

		static void ResetNativeBindingHealth() {
			Volatile.Write(ref NativeDisabledForSession, 0);
			Volatile.Write(ref VulkanNativeWarningLogged, 0);
		}

		static void RecordNativeSuccess() {
			// Successful native work needs no health counter. File-specific
			// failures never disable the native binding globally.
		}

		static IEnumerable<Exception> EnumerateExceptionChain(
			Exception exception) {
			for (Exception? current = exception;
				current != null;
				current = current.InnerException) {
				yield return current;
			}
		}

		static bool IsNativeBindingAutoGenFailure(Exception e) =>
			EnumerateExceptionChain(e).Any(current =>
				current is NotSupportedException &&
				(current.Message.Contains(
					"Specified method is not supported",
					StringComparison.OrdinalIgnoreCase) ||
					(current.StackTrace?.Contains(
						"FFmpeg.AutoGen.DynamicallyLoadedBindings",
						StringComparison.Ordinal) ?? false)));

		static bool IsNativeBindingInfrastructureFailure(Exception e) =>
			EnumerateExceptionChain(e).Any(current =>
				current is DllNotFoundException or
					EntryPointNotFoundException or
					BadImageFormatException);

		internal static bool IsNativeBindingLoadFailure(Exception e) =>
			IsNativeBindingAutoGenFailure(e) ||
			IsNativeBindingInfrastructureFailure(e);

		static void DisableNativeBindingForSession(
			string file,
			Exception e,
			string prefix) {
			if (Interlocked.Exchange(
				ref NativeDisabledForSession,
				1) != 0) {
				return;
			}

			Logger.Instance.Info(
				$"{prefix}; using process mode for the rest of this " +
				$"session. Last error on '{file}': " +
				$"{e.GetType().Name}: {e.Message}." +
				$"{BuildNativeFailureDetail(e)} If this persists, " +
				$"disable 'Use native FFmpeg binding' or install " +
				$"matching shared FFmpeg libraries.");
		}

		internal static void RecordNativeFailure(
			string file,
			Exception e) {
			if (IsNativeBindingInfrastructureFailure(e)) {
				DisableNativeBindingForSession(
					file,
					e,
					"Native FFmpeg binding libraries are unavailable " +
					"or ABI-incompatible");
				return;
			}

			if (IsNativeBindingAutoGenFailure(e)) {
				DisableNativeBindingForSession(
					file,
					e,
					"Native FFmpeg binding could not call the loaded " +
					"FFmpeg libraries");
			}

			// Decode, seek, conversion, malformed-file, and other media
			// failures stay local to the current operation. They must never
			// disable native mode for unrelated files.
		}


		/// <summary>
		/// Builds the extra diagnostic suffix for a native failure: FFmpeg log lines
		/// captured on this thread plus a plain-language hint about the likely cause.
		/// </summary>
		static string BuildNativeFailureDetail(Exception e) {
			string diagnostics = FfmpegLogCapture.GetRecent();
			string? hint = FfmpegErrorClassifier.Classify(
				diagnostics.Length > 0 ? $"{diagnostics} {e.Message}" : e.Message);
			string detail = string.Empty;
			if (diagnostics.Length > 0)
				detail += $" FFmpeg log: {diagnostics}.";
			if (hint != null)
				detail += $" Hint: {hint}";
			return detail;
		}
	}
}
