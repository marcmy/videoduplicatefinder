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

using VDF.Core;

namespace VDF.Core.Tests;

public class ScanEngineTests {
	[Fact]
	public void IsReparsePoint_NormalFile_ReturnsFalse() {
		string path = Path.Combine(Directory.GetCurrentDirectory(), $"{Guid.NewGuid():N}.tmp");
		try {
			File.WriteAllText(path, "not a link");

			Assert.False(ScanEngine.IsReparsePoint(path));
		}
		finally {
			File.Delete(path);
		}
	}

	[Fact]
	public void IsReparsePoint_MissingFile_ReturnsFalse() {
		string path = Path.Combine(Directory.GetCurrentDirectory(), $"{Guid.NewGuid():N}.tmp");

		Assert.False(ScanEngine.IsReparsePoint(path));
	}
}
