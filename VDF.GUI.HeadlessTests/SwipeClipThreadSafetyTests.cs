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

using System.Reflection;
using Avalonia;
using Avalonia.Media;
using SkiaSharp;
using VDF.GUI.ViewModels;

namespace VDF.GUI.HeadlessTests;

/// <summary>
/// Dragging the comparer's swipe line closed the app without a word: an access violation in
/// libSkiaSharp (SkPathBuilder::snapshot). Every drag step builds a new clip geometry. Avalonia.Skia
/// turns it into <c>new SKPath(); AddRect(...)</c>, and SkiaSharp 4 only queues AddRect in a lazy
/// builder that the first read of the path's Handle flushes, without a lock. The render thread
/// (Canvas.ClipPath) and the UI thread's hit test (Clip.FillContains) both read a new clip first,
/// and when they flush it together one disposes the builder the other is still detaching.
/// The real Skia backend runs here, so the race below is the app's own. Without the fix it takes the
/// test host down with 0xC0000005 or a NullReferenceException in SKPath.FlushBuilder, usually in
/// the first iterations.
/// </summary>
public class SwipeClipThreadSafetyTests {

	[Fact]
	public Task SwipeClip_SurvivesRenderThreadAndHitTestReadingItAtOnce() => HeadlessUi.Run(() => {
		const int iterations = 5_000;
		using var surface = SKSurface.Create(new SKImageInfo(64, 64));
		var canvas = surface.Canvas;
		SKPath? shared = null;
		using var turn = new Barrier(2);

		var render = new Thread(() => {
			for (int i = 0; i < iterations; i++) {
				turn.SignalAndWait();
				// DrawingContextImpl.PushGeometryClip
				canvas.Save();
				canvas.ClipPath(shared!, SKClipOperation.Intersect, true);
				canvas.Restore();
				turn.SignalAndWait();
			}
		}) { IsBackground = true, Name = "fake render thread" };
		render.Start();

		for (int i = 0; i < iterations; i++) {
			var clip = ThumbnailComparerVM.CreateSwipeClip(new Rect(0, 0, 10 + i % 7, 10));
			shared = FillPathOf(clip);
			turn.SignalAndWait();
			clip.FillContains(new Point(1, 1)); // hit testing against Visual.Clip on the UI thread
			turn.SignalAndWait();
		}
		render.Join();
	});

	// Geometry.PlatformImpl is internal; Avalonia.Skia's GeometryImpl exposes the SKPath it hands
	// the render thread as FillPath.
	static SKPath FillPathOf(Geometry geometry) {
		var impl = typeof(Geometry).GetProperty("PlatformImpl", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(geometry);
		Assert.NotNull(impl);
		var path = impl.GetType().GetProperty("FillPath", BindingFlags.Instance | BindingFlags.Public)?.GetValue(impl) as SKPath;
		Assert.NotNull(path);
		return path;
	}
}
