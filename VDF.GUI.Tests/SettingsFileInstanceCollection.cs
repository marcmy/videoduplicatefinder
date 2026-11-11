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

namespace VDF.GUI.Tests;

/// <summary>
/// Test classes that read or mutate the process-wide <c>SettingsFile.Instance</c>.
/// <c>SettingsFile.LoadSettings</c> resets that singleton, so running these classes in
/// parallel let one test observe another's values (e.g. ResultsPreviewWidth read back as
/// the 160 default right after being set to 1200). They run serially, apart from the
/// rest of the suite.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SettingsFileInstanceCollection {
	public const string Name = "SettingsFile.Instance";
}
