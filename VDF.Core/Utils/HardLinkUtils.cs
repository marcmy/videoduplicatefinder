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

using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace VDF.Core.Utils {
	/// <summary>
	/// Credits to David-Maisonave for his original windows implementation
	/// </summary>
	internal static partial class HardLinkUtils {
		[LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
		private static partial IntPtr FindFirstFileNameW(string lpFileName, uint dwFlags, ref int StringLength, char[] LinkName);

		[LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
		[return: MarshalAs(UnmanagedType.Bool)]
		private static partial bool FindNextFileNameW(IntPtr hFindStream, ref int StringLength, char[] LinkName);

		[LibraryImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		private static partial bool FindClose(IntPtr hFindFile);

		[LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
		[return: MarshalAs(UnmanagedType.Bool)]
		private static partial bool GetVolumePathNameW(string lpszFileName, [Out] char[] lpszVolumePathName, int cchBufferLength);


		[LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
		private static partial SafeFileHandle CreateFileW(
			string lpFileName,
			uint dwDesiredAccess,
			FileShare dwShareMode,
			IntPtr lpSecurityAttributes,
			FileMode dwCreationDisposition,
			uint dwFlagsAndAttributes,
			IntPtr hTemplateFile);

		[LibraryImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		private static partial bool GetFileInformationByHandle(SafeFileHandle hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);

		[LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16, EntryPoint = "CreateHardLinkW")]
		[return: MarshalAs(UnmanagedType.Bool)]
		private static partial bool CreateHardLinkW(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

		/// <summary>
		/// Creates a hard link at <paramref name="linkPath"/> referencing the same data as
		/// <paramref name="existingFilePath"/>. Throws <see cref="IOException"/> on failure
		/// (most commonly: the two paths are on different volumes, or the filesystem
		/// doesn't support hard links).
		/// </summary>
		public static void CreateHardLink(string linkPath, string existingFilePath) {
			if (CoreUtils.IsWindows) {
				if (!CreateHardLinkW(ToWin32Path(linkPath), ToWin32Path(existingFilePath), IntPtr.Zero))
					throw new IOException($"Failed to create hard link '{linkPath}' -> '{existingFilePath}' (Win32 error {Marshal.GetLastPInvokeError()}). Note: hard links require both paths to be on the same volume.");
				return;
			}
			if (Mono.Unix.Native.Syscall.link(existingFilePath, linkPath) != 0)
				throw new IOException($"Failed to create hard link '{linkPath}' -> '{existingFilePath}' (errno {Mono.Unix.Native.Stdlib.GetLastError()}). Note: hard links require both paths to be on the same filesystem.");
		}

		[StructLayout(LayoutKind.Sequential)]
		struct BY_HANDLE_FILE_INFORMATION {
			public uint FileAttributes;
			public FILETIME CreationTime;
			public FILETIME LastAccessTime;
			public FILETIME LastWriteTime;
			public uint VolumeSerialNumber;
			public uint FileSizeHigh;
			public uint FileSizeLow;
			public uint NumberOfLinks;
			public uint FileIndexHigh;
			public uint FileIndexLow;
		}
		[StructLayout(LayoutKind.Sequential)]
		struct FILETIME {
			public uint dwLowDateTime;
			public uint dwHighDateTime;
		}

		const IntPtr INVALID_HANDLE_VALUE = -1;
		const int ERROR_MORE_DATA = 234;
		const uint FILE_READ_ATTRIBUTES = 0x0080;
		const uint FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;

		/// <summary>
		/// The identity of the data a path points at: two paths with equal identities are
		/// hard links of one file. Windows: volume serial plus the 64-bit file index from
		/// GetFileInformationByHandle. Elsewhere: st_dev plus st_ino.
		/// </summary>
		internal readonly record struct FileIdentity(ulong Volume, ulong FileIndex);

		public static bool AreSameFile(string filepath, string otherFilepath) {
			if (CoreUtils.IsWindows)
				return AreSameFileWindows(filepath, otherFilepath);
			return AreSameFilePosix(filepath, otherFilepath);
		}

		/// <summary>
		/// Reads the physical identity of <paramref name="filepath"/>. Returns false when it
		/// cannot be read (missing file, no permission, or a filesystem that reports no file
		/// index); the caller must then treat the file as unique.
		/// </summary>
		public static bool TryGetFileIdentity(string filepath, out FileIdentity identity) {
			identity = default;
			if (!CoreUtils.IsWindows) {
				if (Mono.Unix.Native.Syscall.stat(filepath, out var st) != 0)
					return false;
				identity = new FileIdentity(st.st_dev, st.st_ino);
				return true;
			}
			if (!TryGetFileIdWindows(filepath, out var id))
				return false;
			// Some providers (network redirectors, FUSE-style drivers) return zeros: unknown.
			if ((id.VolumeSerial, id.FileIndexHigh, id.FileIndexLow) == (0u, 0u, 0u))
				return false;
			identity = new FileIdentity(id.VolumeSerial, ((ulong)id.FileIndexHigh << 32) | id.FileIndexLow);
			return true;
		}

		static bool AreSameFilePosix(string filepath, string otherFilepath) {
			if (Mono.Unix.Native.Syscall.stat(filepath, out var statA) != 0)
				return false;
			if (Mono.Unix.Native.Syscall.stat(otherFilepath, out var statB) != 0)
				return false;
			return statA.st_ino == statB.st_ino && statA.st_dev == statB.st_dev;
		}

		static bool AreSameFileWindows(string filepath, string otherFilepath) {
			if (TryGetFileIdentity(filepath, out var fileA) && TryGetFileIdentity(otherFilepath, out var fileB))
				return fileA == fileB;
			return FallbackWindows(filepath, otherFilepath);
		}
		static bool FallbackWindows(string filepath, string otherFilepath) {
			// GetHardLinksWindows rebuilds each link as volume + path. NTFS names are
			// case-insensitive, and either side may carry the extended-length prefix.
			string other = StripExtendedPrefix(otherFilepath);
			foreach (var link in GetHardLinksWindows(filepath))
				if (string.Equals(StripExtendedPrefix(link), other, StringComparison.OrdinalIgnoreCase))
					return true;
			return false;
		}

		const string ExtendedPrefix = @"\\?\";
		const string ExtendedUncPrefix = @"\\?\UNC\";

		static string StripExtendedPrefix(string path) {
			if (path.StartsWith(ExtendedUncPrefix, StringComparison.Ordinal))
				return @"\\" + path.Substring(ExtendedUncPrefix.Length);
			if (path.StartsWith(ExtendedPrefix, StringComparison.Ordinal))
				return path.Substring(ExtendedPrefix.Length);
			return path;
		}

		/// <summary>
		/// The raw Win32 calls here bypass .NET's own long-path handling, so a path at or past
		/// MAX_PATH failed to open and its file silently counted as "not a hard link". Long
		/// paths are passed in the extended-length form, which has no such limit.
		/// </summary>
		internal static string ToWin32Path(string path) {
			if (path.Length < 248 || path.StartsWith(ExtendedPrefix, StringComparison.Ordinal) || path.StartsWith(@"\\.\", StringComparison.Ordinal))
				return path;
			string full = Path.GetFullPath(path);
			if (full.StartsWith(@"\\", StringComparison.Ordinal))
				return ExtendedUncPrefix + full.Substring(2);
			return ExtendedPrefix + full;
		}

		static bool TryGetFileIdWindows(string filepath, out (uint VolumeSerial, uint FileIndexHigh, uint FileIndexLow) fileId) {
			fileId = default;
			try {
				using SafeFileHandle handle = CreateFileW(
					ToWin32Path(filepath),
					FILE_READ_ATTRIBUTES,
					FileShare.ReadWrite | FileShare.Delete,
					IntPtr.Zero,
					FileMode.Open,
					FILE_FLAG_BACKUP_SEMANTICS,
					IntPtr.Zero);
				if (handle.IsInvalid)
					return false;
				if (!GetFileInformationByHandle(handle, out var info))
					return false;
				fileId = (info.VolumeSerialNumber, info.FileIndexHigh, info.FileIndexLow);
				return true;
			}
			catch (Exception ex) {
				Logger.Instance.Warn($"Failed getting file id for file: {filepath}, reason: {ex.Message}");
				return false;
			}
		}

		static IEnumerable<string> GetHardLinksWindows(string filepath) {
			filepath = ToWin32Path(filepath);
			char[] buffer = ArrayPool<char>.Shared.Rent(512);
			try {
				int stringLength = buffer.Length;
				buffer.AsSpan().Clear();

				if (!GetVolumePathNameW(filepath, buffer, stringLength))
					return Array.Empty<string>();

				Span<char> volume = buffer.AsSpan().TrimEnd('\0').TrimEnd('\\');
				List<string> links = new();

				while (true) {
					buffer.AsSpan().Clear();
					IntPtr findHandle = FindFirstFileNameW(filepath, 0, ref stringLength, buffer);
					if (findHandle == INVALID_HANDLE_VALUE) {
						if (Marshal.GetLastPInvokeError() == ERROR_MORE_DATA) {
							ArrayPool<char>.Shared.Return(buffer);
							buffer = ArrayPool<char>.Shared.Rent(stringLength);
							continue;
						}
						else {
							break;
						}
					}
					links.Add(string.Concat(volume, buffer.AsSpan().TrimEnd('\0')));
					while (true) {
						buffer.AsSpan().Clear();
						bool success = FindNextFileNameW(findHandle, ref stringLength, buffer);

						if (!success) {
							if (Marshal.GetLastPInvokeError() == ERROR_MORE_DATA) {
								ArrayPool<char>.Shared.Return(buffer);
								buffer = ArrayPool<char>.Shared.Rent(stringLength);
								continue;
							}
							else {
								FindClose(findHandle);
								break;
							}
						}
						links.Add(string.Concat(volume, buffer.AsSpan().TrimEnd('\0')));
					}
					break;
				}

				return links;
			}
			catch (Exception ex) {
				Logger.Instance.Warn($"Failed getting hard links of file: {filepath}, reason: {ex.Message}");
				return Array.Empty<string>();
			}
			finally {
				ArrayPool<char>.Shared.Return(buffer);
			}
		}
	}
}
