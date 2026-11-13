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
// A stopped scan can finish with what it found so far (Settings.KeepPartialResultsOnStop)
// instead of discarding it. These drive the real StartCompare over a prepared database:
// three pairs of identical videos, the pairs far apart from each other. The linear video
// compare handles one entry per step and checks the token between steps, so stopping on
// the first published progress step (entry 0 done) leaves exactly entry 0's pair found.

using VDF.Core.Utils;
using VDF.Core.ViewModels;

namespace VDF.Core.Tests;

[Collection("DatabaseUtils")] // StartCompare reads and saves the shared static database
public sealed class StoppedScanPartialResultsTests : IDisposable {

	const int GraySize = GrayBytesUtils.Side * GrayBytesUtils.Side;
	readonly string dbDir = Directory.CreateTempSubdirectory("vdf-partial-results").FullName;
	readonly List<FileEntry> added = new();

	public StoppedScanPartialResultsTests() {
		// The final save must land in a throwaway folder, never the real database.
		DatabaseUtils.CustomDatabaseFolder = dbDir;
		DatabaseUtils.InvalidateDatabaseFolder();
		string root = @"X:\vdf-partial-" + Guid.NewGuid().ToString("N") + @"\";
		byte[] fills = { 20, 128, 235 };
		for (int pair = 0; pair < fills.Length; pair++)
			for (int copy = 0; copy < 2; copy++)
				AddVideo(root, $"pair{pair}-{copy}.mp4", fills[pair]);
	}

	public void Dispose() {
		foreach (var e in added)
			DatabaseUtils.Database.Remove(e);
		DatabaseUtils.CustomDatabaseFolder = null;
		DatabaseUtils.InvalidateDatabaseFolder();
		try { Directory.Delete(dbDir, true); } catch { /* best effort */ }
	}

	void AddVideo(string root, string name, byte fill) {
		var entry = new FileEntry {
			_Path = root + name,
			Folder = root,
			FileSize = 1000,
			mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(4), Streams = Array.Empty<MediaInfo.StreamInfo>() },
			invalid = false,
		};
		var gray = new byte[GraySize];
		Array.Fill(gray, fill);
		entry.grayBytes[4d * 0.5f] = gray;
		DatabaseUtils.Database.Add(entry);
		added.Add(entry);
	}

	ScanEngine NewEngine(bool keepPartial) {
		var engine = new ScanEngine {
			Settings = new Settings {
				ThumbnailCount = 1,
				Percent = 96f,
				KeepPartialResultsOnStop = keepPartial,
				CustomDatabaseFolder = dbDir,
			}
		};
		engine.positionList.Add(0.5f);
		return engine;
	}

	/// <summary>Stops the scan on the first progress step past the compare's start.</summary>
	static void StopAfterFirstCompareStep(ScanEngine engine) {
		string compareStage = LanguageService.Instance.Get(engine.Settings.LanguageCode, "Scan.Stage.ComparingDuplicates");
		int stopped = 0;
		engine.Progress += (_, e) => {
			if (e.CurrentStage == compareStage && e.CurrentPosition >= 1 && e.CurrentPosition < e.MaxPosition
				&& Interlocked.Exchange(ref stopped, 1) == 0)
				engine.Stop();
		};
	}

	enum Outcome { Done, Aborted, TimedOut }

	static async Task<Outcome> RunCompare(ScanEngine engine) {
		var done = new TaskCompletionSource<Outcome>(TaskCreationOptions.RunContinuationsAsynchronously);
		engine.ScanDone += (_, _) => done.TrySetResult(Outcome.Done);
		engine.ScanAborted += (_, _) => done.TrySetResult(Outcome.Aborted);
		engine.StartCompare();
		var winner = await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(30)));
		return winner == done.Task ? done.Task.Result : Outcome.TimedOut;
	}

	[Fact]
	public async Task StopWhileComparing_WithKeepPartial_FinishesWithTheGroupsFoundSoFar() {
		var engine = NewEngine(keepPartial: true);
		StopAfterFirstCompareStep(engine);

		Assert.Equal(Outcome.Done, await RunCompare(engine));
		Assert.True(engine.ResultsArePartial);
		// Only entry 0 was compared before the stop: its pair, and nothing else.
		Assert.Equal(2, engine.Duplicates.Count);
		Assert.Single(engine.Duplicates.Select(d => d.GroupId).Distinct());
	}

	[Fact]
	public async Task StopWhileComparing_WithoutKeepPartial_StillAborts() {
		var engine = NewEngine(keepPartial: false);
		StopAfterFirstCompareStep(engine);

		Assert.Equal(Outcome.Aborted, await RunCompare(engine));
		Assert.False(engine.ResultsArePartial);
	}

	[Fact]
	public async Task CompareThatIsNotStopped_IsNotPartial() {
		var engine = NewEngine(keepPartial: true);

		Assert.Equal(Outcome.Done, await RunCompare(engine));
		Assert.False(engine.ResultsArePartial);
		Assert.Equal(6, engine.Duplicates.Count);
		Assert.Equal(3, engine.Duplicates.Select(d => d.GroupId).Distinct().Count());
	}

	/// <summary>
	/// Stop on a live, unpaused scan must only cancel. It used to raise ScanAborted on the
	/// spot (an else that drifted onto the pause check), so the UI reset while workers were
	/// still winding down — and a stop that keeps partial results was reported as aborted
	/// before its ScanDone arrived.
	/// </summary>
	[Fact]
	public async Task StopOnALiveScan_DoesNotRaiseScanAbortedBeforeTheScanEnds() {
		var engine = NewEngine(keepPartial: true);
		StopAfterFirstCompareStep(engine);
		int abortedCount = 0;
		engine.ScanAborted += (_, _) => Interlocked.Increment(ref abortedCount);

		Assert.Equal(Outcome.Done, await RunCompare(engine));
		Assert.Equal(0, abortedCount);
	}

	/// <summary>
	/// A search stopped while fingerprinting finishes by comparing what it has — and nothing
	/// more. The compare gets a fresh token, so the follow-up passes used to run anyway, one
	/// phase after another, after the user had pressed Stop.
	/// </summary>
	[Fact]
	public async Task FinishingAStoppedSearch_SkipsTheFollowUpPasses() {
		var engine = NewEngine(keepPartial: true);
		engine.Settings.EnablePartialClipDetection = true;
		var done = new TaskCompletionSource<Outcome>(TaskCreationOptions.RunContinuationsAsynchronously);
		engine.ScanDone += (_, _) => done.TrySetResult(Outcome.Done);
		engine.ScanAborted += (_, _) => done.TrySetResult(Outcome.Aborted);
		engine.StartCompareFinishingStoppedScan();
		var winner = await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(30)));

		Assert.True(winner == done.Task, "the compare never finished");
		Assert.Equal(Outcome.Done, done.Task.Result);
		Assert.True(engine.ResultsArePartial);
		Assert.Equal(6, engine.Duplicates.Count); // the compare itself ran in full
		Assert.Equal(0, engine.FollowUpPassInvocations); // no follow-up was dispatched by this engine
	}

	/// <summary>
	/// Control for the test above: an ordinary compare dispatches follow-up passes.
	/// Check the engine itself rather than the shared logger, which other concurrent tests use.
	/// </summary>
	[Fact]
	public async Task OrdinaryCompare_RunsTheFollowUpPasses() {
		var engine = NewEngine(keepPartial: true);
		engine.Settings.EnablePartialClipDetection = true;

		Assert.Equal(Outcome.Done, await RunCompare(engine));
		Assert.Equal(1, engine.FollowUpPassInvocations);
	}

	/// <summary>
	/// Thumbnail loading runs on the scan's token. A scan stopped while comparing leaves it
	/// cancelled, and the loading quit before the first item — partial results had no
	/// thumbnails. Missing files (these entries have none on disk) get the placeholder, so a
	/// placeholder on every result proves each one was visited.
	/// </summary>
	[Fact]
	public async Task ThumbnailsLoad_AfterAScanStoppedWhileComparing() {
		var engine = NewEngine(keepPartial: true);
		engine.Settings.IncludeNonExistingFiles = true;
		engine.NoThumbnailImage = new byte[] { 1 };
		StopAfterFirstCompareStep(engine);
		Assert.Equal(Outcome.Done, await RunCompare(engine));
		Assert.NotEmpty(engine.Duplicates);

		var retrieved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		engine.ThumbnailsRetrieved += (_, _) => retrieved.TrySetResult();
		engine.RetrieveThumbnails();
		var winner = await Task.WhenAny(retrieved.Task, Task.Delay(TimeSpan.FromSeconds(30)));

		Assert.True(winner == retrieved.Task, "thumbnail loading never finished");
		Assert.All(engine.Duplicates, d => Assert.Single(d.ImageList));
	}

	[Theory]
	// stopped, searchAndCompare, fingerprintingStarted, keepSetting, expected
	[InlineData(true, true, true, true, true)]    // the one case that compares what it has
	[InlineData(false, true, true, true, false)]  // not stopped: the normal chain, not this path
	[InlineData(true, false, true, true, false)]  // search-only callers drive the compare themselves
	[InlineData(true, true, false, true, false)]  // stopped during the file walk: nothing to compare
	[InlineData(true, true, true, false, false)]  // setting off: a stop aborts as before
	public void ComparesAfterStoppedFingerprinting_OnlyWhenEveryConditionHolds(
		bool stopped, bool searchAndCompare, bool fingerprintingStarted, bool keepSetting, bool expected) =>
		Assert.Equal(expected, ScanEngine.ComparesAfterStoppedFingerprinting(stopped, searchAndCompare, fingerprintingStarted, keepSetting));
}
