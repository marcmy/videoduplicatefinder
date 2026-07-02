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
using Avalonia;
using Avalonia.Media;
using Avalonia.Media.Imaging;
using Avalonia.Platform;
using VDF.Core.FFTools;

namespace VDF.GUI.Utils {
	public enum ImageOrientationTransform {
		Normal,
		MirrorHorizontal,
		FlipVertical,
		Rotate180,
		Rotate90,
		Rotate90MirrorHorizontal,
		Rotate270,
		Rotate270MirrorHorizontal
	}

	public readonly record struct ImageOrientationMatch(
		ImageOrientationTransform Transform,
		double Score,
		double NormalScore,
		double SecondBestScore);

	static class ImageUtils {
		// Anything beyond this gets downscaled to keep the composite inside
		// Avalonia/UI texture limits.
		public const int MaxDisplayableCompositeWidth = 4096;
		// Hard sanity cap for the intermediate compose buffer (BGRA bytes).
		const long MaxCompositeBufferBytes = 256_000_000;

		/// <summary>
		/// Composes <paramref name="encodedImages"/> (JPEG/PNG bytes) into a horizontal-strip
		/// thumbnail and returns a Bitmap for immediate UI use. If <paramref name="jpegOut"/> is
		/// supplied, the strip JPEG is written there FIRST, before the UI bitmap is built — that
		/// way a failure on the Avalonia side still produces a valid cache entry (issue #751).
		/// Decoding uses Avalonia/Skia; the strip JPEG is encoded via FFmpeg.
		/// </summary>
		public static unsafe Bitmap? JoinImages(IReadOnlyList<byte[]> encodedImages, Stream? jpegOut = null) {
			if (encodedImages == null || encodedImages.Count == 0) return null;

			var parts = new List<WriteableBitmap>(encodedImages.Count);
			try {
				int totalWidth = 0, maxHeight = 0;
				foreach (var bytes in encodedImages) {
					if (bytes == null || bytes.Length == 0) continue;
					using var ms = new MemoryStream(bytes);
					var part = WriteableBitmap.Decode(ms);
					parts.Add(part);
					totalWidth += part.PixelSize.Width;
					maxHeight = Math.Max(maxHeight, part.PixelSize.Height);
				}
				if (parts.Count == 0 || totalWidth <= 0 || maxHeight <= 0) return null;
				if ((long)totalWidth * maxHeight * 4 > MaxCompositeBufferBytes) return null;

				// Compose raw BGRA strip (rows shorter than maxHeight stay transparent).
				byte[] strip = new byte[totalWidth * maxHeight * 4];
				int xOffset = 0;
				foreach (var part in parts) {
					using var fb = part.Lock();
					int w = fb.Size.Width, h = fb.Size.Height;
					bool isRgba = fb.Format == PixelFormat.Rgba8888;
					if (fb.Format != PixelFormat.Bgra8888 && !isRgba)
						return null; // unexpected decoder output — give up rather than show garbage
					byte* src = (byte*)fb.Address;
					for (int y = 0; y < h; y++) {
						var srcRow = new ReadOnlySpan<byte>(src + (long)y * fb.RowBytes, w * 4);
						var dstRow = strip.AsSpan(((y * totalWidth) + xOffset) * 4, w * 4);
						if (!isRgba) {
							srcRow.CopyTo(dstRow);
						}
						else {
							for (int x = 0; x < w * 4; x += 4) {
								dstRow[x] = srcRow[x + 2];     // B
								dstRow[x + 1] = srcRow[x + 1]; // G
								dstRow[x + 2] = srcRow[x];     // R
								dstRow[x + 3] = srcRow[x + 3]; // A
							}
						}
					}
					xOffset += part.PixelSize.Width;
				}

				// Encode the strip via FFmpeg. The cache write happens before the UI bitmap is
				// decoded, preserving the cache-first guarantee.
				byte[]? jpeg = FfmpegEngine.EncodeJpegFromBgra(strip, totalWidth, maxHeight, MaxDisplayableCompositeWidth);
				if (jpeg != null) {
					if (jpegOut != null) {
						try {
							jpegOut.Write(jpeg, 0, jpeg.Length);
							try { jpegOut.Flush(); } catch { /* ignore */ }
							if (jpegOut.CanSeek) { try { jpegOut.Position = 0; } catch { /* ignore */ } }
						}
						catch { /* the cache write is best-effort; UI bitmap below is independent */ }
					}
					using var jpegMs = new MemoryStream(jpeg);
					return new Bitmap(jpegMs);
				}

				// FFmpeg encode failed — still give the UI something (no cache entry written).
				if (totalWidth > MaxDisplayableCompositeWidth) return null;
				var fallback = new WriteableBitmap(
					new PixelSize(totalWidth, maxHeight),
					new Vector(96, 96),
					PixelFormat.Bgra8888,
					AlphaFormat.Unpremul);
				using (var fb = fallback.Lock()) {
					byte* dest = (byte*)fb.Address;
					int rowBytes = totalWidth * 4;
					fixed (byte* src = strip) {
						for (int y = 0; y < maxHeight; y++)
							Buffer.MemoryCopy(src + (long)y * rowBytes, dest + (long)y * fb.RowBytes, rowBytes, rowBytes);
					}
				}
				return fallback;
			}
			catch {
				return null;
			}
			finally {
				foreach (var part in parts)
					part.Dispose();
			}
		}
		public static unsafe Bitmap? JoinImages(IReadOnlyList<Bitmap> images, Stream? jpegOut = null) {
			if (images == null || images.Count == 0) return null;

			int h = images[0].PixelSize.Height;
			int w = 0; for (int i = 0; i < images.Count; i++) w += images[i].PixelSize.Width;

			RenderTargetBitmap rtb = new(new PixelSize(w, h));

			using var dc = rtb.CreateDrawingContext();
			//dc.FillRectangle(Brushes.Transparent, new Rect(0, 0, w, h));

			double x = 0;
			foreach (var bmp in images) {
				var src = new Rect(0, 0, bmp.PixelSize.Width, bmp.PixelSize.Height);
				var dst = new Rect(x, 0, bmp.PixelSize.Width, bmp.PixelSize.Height);
				dc.DrawImage(bmp, src, dst);
				x += bmp.PixelSize.Width;
			}
			return rtb;
		}

		public static byte[] ToByteArray(this Bitmap image) {
			using MemoryStream ms = new();
			image.Save(ms);
			return ms.ToArray();
		}

		public static Bitmap? TransformBitmap(
			Bitmap? source,
			ImageOrientationTransform transform) {
			if (source == null)
				return null;
			if (transform == ImageOrientationTransform.Normal)
				return source;

			try {
				(PixelSize size, Matrix matrix) =
					GetOrientationRenderInfo(source.PixelSize, transform);
				return RenderTransformed(source, matrix, size);
			}
			catch {
				return source;
			}
		}

		public static byte[]? TransformGraySquare(
			byte[]? source,
			int sideLength,
			ImageOrientationTransform transform) {
			if (source == null)
				return null;
			if (transform == ImageOrientationTransform.Normal)
				return source;
			if (sideLength <= 0 ||
				source.Length != sideLength * sideLength) {
				return source;
			}

			int max = sideLength - 1;
			var transformed = new byte[source.Length];

			for (int y = 0; y < sideLength; y++) {
				for (int x = 0; x < sideLength; x++) {
					int sourceIndex = transform switch {
						ImageOrientationTransform.MirrorHorizontal =>
							(y * sideLength) + (max - x),
						ImageOrientationTransform.FlipVertical =>
							((max - y) * sideLength) + x,
						ImageOrientationTransform.Rotate180 =>
							((max - y) * sideLength) + (max - x),
						ImageOrientationTransform.Rotate90 =>
							((max - x) * sideLength) + y,
						ImageOrientationTransform.Rotate90MirrorHorizontal =>
							(x * sideLength) + y,
						ImageOrientationTransform.Rotate270 =>
							(x * sideLength) + (max - y),
						ImageOrientationTransform.Rotate270MirrorHorizontal =>
							((max - x) * sideLength) + (max - y),
						_ =>
							(y * sideLength) + x,
					};

					transformed[(y * sideLength) + x] =
						source[sourceIndex];
				}
			}

			return transformed;
		}

		public static ImageOrientationMatch? FindBestOrientation(
			Bitmap reference,
			Bitmap candidate,
			int sampleSize = 64) {
			byte[]? referenceGray =
				RenderGraySample(
					reference,
					ImageOrientationTransform.Normal,
					sampleSize);
			if (referenceGray == null)
				return null;

			var scores = new List<(ImageOrientationTransform Transform, double Score)>();
			foreach (ImageOrientationTransform transform in
				Enum.GetValues<ImageOrientationTransform>()) {
				byte[]? candidateGray =
					RenderGraySample(candidate, transform, sampleSize);
				if (candidateGray == null)
					continue;

				scores.Add((
					transform,
					MeanAbsoluteDifference(referenceGray, candidateGray)));
			}

			if (scores.Count == 0)
				return null;

			var ordered = scores
				.OrderBy(score => score.Score)
				.ToArray();
			double normalScore = scores
				.FirstOrDefault(score =>
					score.Transform == ImageOrientationTransform.Normal)
				.Score;
			double secondBestScore =
				ordered.Length > 1 ? ordered[1].Score : ordered[0].Score;

			return new ImageOrientationMatch(
				ordered[0].Transform,
				ordered[0].Score,
				normalScore,
				secondBestScore);
		}

		static double MeanAbsoluteDifference(
			byte[] reference,
			byte[] candidate) {
			if (reference.Length != candidate.Length)
				return double.MaxValue;

			long difference = 0;
			for (int i = 0; i < reference.Length; i++)
				difference += Math.Abs(reference[i] - candidate[i]);

			return difference / (255d * reference.Length);
		}

		static unsafe byte[]? RenderGraySample(
			Bitmap source,
			ImageOrientationTransform transform,
			int sampleSize) {
			sampleSize = Math.Clamp(sampleSize, 16, 256);

			(PixelSize naturalSize, Matrix naturalMatrix) =
				GetOrientationRenderInfo(source.PixelSize, transform);
			var targetSize = new PixelSize(sampleSize, sampleSize);
			double sx = targetSize.Width / (double)naturalSize.Width;
			double sy = targetSize.Height / (double)naturalSize.Height;
			Matrix scaledMatrix = new(
				naturalMatrix.M11 * sx,
				naturalMatrix.M12 * sy,
				naturalMatrix.M21 * sx,
				naturalMatrix.M22 * sy,
				naturalMatrix.M31 * sx,
				naturalMatrix.M32 * sy);

			using Bitmap rendered =
				RenderTransformed(source, scaledMatrix, targetSize);
			using var readable = new WriteableBitmap(
				targetSize,
				new Vector(96, 96),
				PixelFormat.Bgra8888,
				AlphaFormat.Unpremul);

			using (var fb = readable.Lock()) {
				rendered.CopyPixels(fb);

				byte[] gray = new byte[targetSize.Width * targetSize.Height];
				byte* src = (byte*)fb.Address;
				for (int y = 0; y < targetSize.Height; y++) {
					byte* row = src + (long)y * fb.RowBytes;
					for (int x = 0; x < targetSize.Width; x++) {
						int i = x * 4;
						int b = row[i];
						int g = row[i + 1];
						int r = row[i + 2];
						gray[(y * targetSize.Width) + x] =
							(byte)((r * 299 + g * 587 + b * 114) / 1000);
					}
				}

				return gray;
			}
		}

		static RenderTargetBitmap RenderTransformed(
			Bitmap source,
			Matrix matrix,
			PixelSize targetSize) {
			var rendered = new RenderTargetBitmap(targetSize);
			using DrawingContext dc = rendered.CreateDrawingContext();
			using (dc.PushTransform(matrix)) {
				dc.DrawImage(
					source,
					new Rect(
						0,
						0,
						source.PixelSize.Width,
						source.PixelSize.Height));
			}

			return rendered;
		}

		static (PixelSize Size, Matrix Matrix) GetOrientationRenderInfo(
			PixelSize sourceSize,
			ImageOrientationTransform transform) {
			int w = sourceSize.Width;
			int h = sourceSize.Height;

			return transform switch {
				ImageOrientationTransform.Normal => (
					new PixelSize(w, h),
					Matrix.Identity),
				ImageOrientationTransform.MirrorHorizontal => (
					new PixelSize(w, h),
					new Matrix(-1, 0, 0, 1, w, 0)),
				ImageOrientationTransform.FlipVertical => (
					new PixelSize(w, h),
					new Matrix(1, 0, 0, -1, 0, h)),
				ImageOrientationTransform.Rotate180 => (
					new PixelSize(w, h),
					new Matrix(-1, 0, 0, -1, w, h)),
				ImageOrientationTransform.Rotate90 => (
					new PixelSize(h, w),
					new Matrix(0, 1, -1, 0, h, 0)),
				ImageOrientationTransform.Rotate90MirrorHorizontal => (
					new PixelSize(h, w),
					new Matrix(0, 1, 1, 0, 0, 0)),
				ImageOrientationTransform.Rotate270 => (
					new PixelSize(h, w),
					new Matrix(0, -1, 1, 0, 0, w)),
				ImageOrientationTransform.Rotate270MirrorHorizontal => (
					new PixelSize(h, w),
					new Matrix(0, -1, -1, 0, h, w)),
				_ => (
					new PixelSize(w, h),
					Matrix.Identity),
			};
		}
	}
}
