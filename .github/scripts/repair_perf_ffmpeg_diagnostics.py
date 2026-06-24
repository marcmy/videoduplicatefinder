from pathlib import Path

path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
text = path.read_text(encoding="utf-8")

helper = '''
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

'''
marker = "\t\tstatic void LogNativeTiming("
if "static string BuildNativeFailureDetail(Exception e)" not in text:
    if text.count(marker) != 1:
        raise SystemExit("Could not locate native timing insertion point exactly once.")
    text = text.replace(marker, helper + marker, 1)

old_disable = '''			Logger.Instance.Info($"{prefix}; using process mode for the rest of this session. Last error on '{file}': {e.GetType().Name}: {e.Message}. If this persists, disable 'Use native FFmpeg binding' or install matching shared FFmpeg libraries.");'''
new_disable = '''			Logger.Instance.Info($"{prefix}; using process mode for the rest of this session. Last error on '{file}': {e.GetType().Name}: {e.Message}.{BuildNativeFailureDetail(e)} If this persists, disable 'Use native FFmpeg binding' or install matching shared FFmpeg libraries.");'''
if old_disable in text:
    text = text.replace(old_disable, new_disable, 1)
elif new_disable not in text:
    raise SystemExit("Could not locate session-disable native failure log.")

old_observed = '''			Logger.Instance.Info($"Native FFmpeg binding failure observed on '{file}' ({failures}/{NativeFailureThreshold}); keeping native enabled until repeated failures. Reason: {e.GetType().Name}: {e.Message}");'''
new_observed = '''			Logger.Instance.Info($"Native FFmpeg binding failure observed on '{file}' ({failures}/{NativeFailureThreshold}); keeping native enabled until repeated failures. Reason: {e.GetType().Name}: {e.Message}{BuildNativeFailureDetail(e)}");'''
if old_observed in text:
    text = text.replace(old_observed, new_observed, 1)
elif new_observed not in text:
    raise SystemExit("Could not locate observed native failure log.")

text = text.replace(
    'Exception: {e}");',
    'Exception: {e}{BuildNativeFailureDetail(e)}");',
)

lines = text.splitlines(keepends=True)
output: list[str] = []
inserted = 0
for line in lines:
    if "new VideoStreamDecoder(" in line:
        previous = next((item.strip() for item in reversed(output) if item.strip()), "")
        if previous != "FfmpegLogCapture.Reset();":
            indent = line[: len(line) - len(line.lstrip())]
            newline = "\r\n" if line.endswith("\r\n") else "\n"
            output.append(f"{indent}FfmpegLogCapture.Reset();{newline}")
            inserted += 1
    output.append(line)
text = "".join(output)

if inserted == 0 and "FfmpegLogCapture.Reset();" not in text:
    raise SystemExit("No decoder construction was instrumented.")
if any(marker in text for marker in ("<<<<<<<", "=======", ">>>>>>>")):
    raise SystemExit("Conflict markers remain in FfmpegEngine.cs.")

path.write_text(text, encoding="utf-8", newline="")
print(f"Inserted FFmpeg log reset before {inserted} decoder construction(s).")
