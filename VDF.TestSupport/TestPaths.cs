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

namespace VDF.TestSupport;

public static class TestPaths {
	/// <summary>
	/// A Windows-style test path (<c>D:\media\clip.mp4</c>) in the running OS's own form.
	/// Unchanged on Windows; elsewhere the drive becomes a top-level folder
	/// (<c>/D/media/clip.mp4</c>), so the path stays rooted and <see cref="Path"/>
	/// splits it into folder and file name as it does on Windows.
	/// </summary>
	public static string Native(string windowsPath) {
		if (OperatingSystem.IsWindows() || windowsPath.Length < 2 || windowsPath[1] != ':' || !char.IsAsciiLetter(windowsPath[0]))
			return windowsPath;
		return "/" + windowsPath[0] + windowsPath[2..].Replace('\\', '/');
	}
}
