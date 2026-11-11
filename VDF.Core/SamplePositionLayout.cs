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

namespace VDF.Core {
	/// <summary>
	/// Where in a video the frames are sampled. One layout applies to the whole database:
	/// comparing frame j of two files only means something when both were sampled at the
	/// same relative position.
	/// </summary>
	public enum SamplePositionLayout {
		/// <summary>k / (n + 1), float-accumulated: evenly spaced, but a different set for every count.</summary>
		Legacy = 0,
		/// <summary>The first n terms of the base-2 van der Corput sequence, sorted: a smaller count's positions are a subset of a larger count's.</summary>
		Nested = 1,
	}
}
