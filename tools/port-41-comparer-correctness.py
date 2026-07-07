from pathlib import Path
import subprocess

REFERENCE = "origin/perf/native-hwaccel-from-crashfix"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


# This control is architecturally unchanged in 4.1; use the fully-tested fixed version.
zoom_path = Path("VDF.GUI/Data/ZoomPanPresenter.cs")
zoom_path.write_bytes(subprocess.check_output([
    "git", "show", f"{REFERENCE}:VDF.GUI/Data/ZoomPanPresenter.cs"
]))

path = Path("VDF.GUI/ViewModels/ThumbnailComparerVM.cs")
text = path.read_text(encoding="utf-8")

text = replace_once(
    text,
    "\t\tvoid UpdateFrameImages() {\n"
    "\t\t\tvar baseIdx = BaseThumbnailIndex;\n",
    "\t\tvoid UpdateFrameImages() {\n"
    "\t\t\t// Invalidate work started for the previous selection, base frame, or offsets.\n"
    "\t\t\t// A completed stale extraction may populate its cache, but must not update the UI.\n"
    "\t\t\t_frameExtractCts?.Cancel();\n"
    "\t\t\t_frameExtractCts = null;\n"
    "\t\t\tIsExtractingFrame = false;\n\n"
    "\t\t\tvar baseIdx = BaseThumbnailIndex;\n",
    "frame request invalidation",
)

text = replace_once(
    text,
    "\t\t\t_frameExtractCts?.Cancel();\n"
    "\t\t\tvar cts = new CancellationTokenSource();\n",
    "\t\t\tvar cts = new CancellationTokenSource();\n",
    "remove late cancellation",
)

text = replace_once(
    text,
    "\t\t\t_frameExtractCts = cts;\n"
    "\t\t\tIsExtractingFrame = true;\n\n"
    "\t\t\t_ = Task.Run(() => {\n",
    "\t\t\t_frameExtractCts = cts;\n"
    "\t\t\tIsExtractingFrame = true;\n\n"
    "\t\t\tbool IsStillCurrentRequest() =>\n"
    "\t\t\t\t!cts.IsCancellationRequested &&\n"
    "\t\t\t\tReferenceEquals(_frameExtractCts, cts) &&\n"
    "\t\t\t\tReferenceEquals(SelectedItemA, itemA) &&\n"
    "\t\t\t\tReferenceEquals(SelectedItemB, itemB) &&\n"
    "\t\t\t\tBaseThumbnailIndex == baseIdx &&\n"
    "\t\t\t\tStepA == stepA &&\n"
    "\t\t\t\tStepB == stepB;\n\n"
    "\t\t\t_ = Task.Run(() => {\n",
    "current request guard",
)

text = replace_once(
    text,
    "\t\t\t\t\tRxSchedulers.MainThreadScheduler.Schedule(() => {\n"
    "\t\t\t\t\t\tif (cts.IsCancellationRequested) return;\n"
    "\t\t\t\t\t\tif (needExtractA && bmpA != null)\n"
    "\t\t\t\t\t\t\tImageA = bmpA;\n"
    "\t\t\t\t\t\tif (needExtractB && bmpB != null)\n"
    "\t\t\t\t\t\t\tImageB = bmpB;\n"
    "\t\t\t\t\t\tthis.RaisePropertyChanged(nameof(ImageSingle));\n"
    "\t\t\t\t\t\tIsExtractingFrame = false;\n"
    "\t\t\t\t\t\tUpdateFrameLabels();\n"
    "\t\t\t\t\t});\n",
    "\t\t\t\t\tRxSchedulers.MainThreadScheduler.Schedule(() => {\n"
    "\t\t\t\t\t\tif (!IsStillCurrentRequest())\n"
    "\t\t\t\t\t\t\treturn;\n"
    "\t\t\t\t\t\tif (needExtractA && bmpA != null)\n"
    "\t\t\t\t\t\t\tImageA = bmpA;\n"
    "\t\t\t\t\t\tif (needExtractB && bmpB != null)\n"
    "\t\t\t\t\t\t\tImageB = bmpB;\n"
    "\t\t\t\t\t\tthis.RaisePropertyChanged(nameof(ImageSingle));\n"
    "\t\t\t\t\t\t_frameExtractCts = null;\n"
    "\t\t\t\t\t\tIsExtractingFrame = false;\n"
    "\t\t\t\t\t\tUpdateFrameLabels();\n"
    "\t\t\t\t\t});\n",
    "guard async UI update",
)

text = replace_once(
    text,
    "\t\t\t\tcatch {\n"
    "\t\t\t\t\tRxSchedulers.MainThreadScheduler.Schedule(() => IsExtractingFrame = false);\n"
    "\t\t\t\t}\n",
    "\t\t\t\tcatch {\n"
    "\t\t\t\t\tRxSchedulers.MainThreadScheduler.Schedule(() => {\n"
    "\t\t\t\t\t\tif (!ReferenceEquals(_frameExtractCts, cts))\n"
    "\t\t\t\t\t\t\treturn;\n"
    "\t\t\t\t\t\t_frameExtractCts = null;\n"
    "\t\t\t\t\t\tIsExtractingFrame = false;\n"
    "\t\t\t\t\t});\n"
    "\t\t\t\t}\n",
    "guard stale failure callback",
)

path.write_text(text, encoding="utf-8")
print("Ported 4.1 comparer stale-frame, zoom, pan, and swipe-coordinate fixes.")
