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
			int result = OperatingSystem.IsMacOS() ? LinkMac(existingFilePath, linkPath) : LinkLinux(existingFilePath, linkPath);
			if (result != 0) {
				int errno = Marshal.GetLastPInvokeError();
				throw new IOException($"Failed to create hard link '{linkPath}' -> '{existingFilePath}' ({Marshal.GetPInvokeErrorMessage(errno)}, errno {errno}). Note: hard links require both paths to be on the same filesystem.");
			}
		}

		// POSIX: link(2) makes the hard link, and two paths are one file when device and inode
		// number match. struct stat differs per OS, architecture and glibc version (glibc before
		// 2.33 does not even export a "stat" symbol), which is why Mono.Posix went through a native
		// helper library of its own, one without an arm64 build for macOS. Linux uses statx(2)
		// instead, whose struct is the same on every architecture (glibc 2.28+, musl 1.2.5+), and
		// macOS its 64-bit-inode stat, which is the same on x64 and arm64.
		const string LinuxLibc = "libc";
		const string MacLibc = "/usr/lib/libc.dylib";

		[LibraryImport(LinuxLibc, EntryPoint = "link", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
		private static partial int LinkLinux(string existingPath, string newPath);

		[LibraryImport(MacLibc, EntryPoint = "link", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
		private static partial int LinkMac(string existingPath, string newPath);

		[LibraryImport(LinuxLibc, EntryPoint = "statx", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
		private static partial int Statx(int dirFd, string path, int flags, uint mask, out StatxBuffer buffer);

		// x64 macOS keeps the old 32-bit-inode struct under the plain name; arm64 only has the 64-bit one.
		[LibraryImport(MacLibc, EntryPoint = "stat$INODE64", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
		private static partial int StatMacX64(string path, out DarwinStat buffer);

		[LibraryImport(MacLibc, EntryPoint = "stat", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
		private static partial int StatMacArm64(string path, out DarwinStat buffer);

		const int AT_FDCWD = -100;
		const uint STATX_INO = 0x100;
		const uint STATX_NLINK = 0x4;

		/// <summary>struct statx (linux/stat.h): 256 bytes on every architecture; device is always filled.</summary>
		[StructLayout(LayoutKind.Explicit, Size = 256)]
		struct StatxBuffer {
			[FieldOffset(0)] public uint Mask;
			[FieldOffset(16)] public uint LinkCount;
			[FieldOffset(32)] public ulong Ino;
			[FieldOffset(136)] public uint DevMajor;
			[FieldOffset(140)] public uint DevMinor;
		}

		/// <summary>macOS struct stat with 64-bit inodes (sys/stat.h): 144 bytes, sized up as headroom.</summary>
		[StructLayout(LayoutKind.Explicit, Size = 256)]
		struct DarwinStat {
			[FieldOffset(0)] public int Dev;
			[FieldOffset(6)] public ushort LinkCount;
			[FieldOffset(8)] public ulong Ino;
		}

		/// <summary>glibc's makedev, so <see cref="TryGetFileIdPosix"/> reports the st_dev that stat(1) prints.</summary>
		static ulong MakeDev(uint major, uint minor) =>
			((ulong)(major & 0xfffff000u) << 32) | ((ulong)(major & 0xfffu) << 8) |
			((ulong)(minor & 0xffffff00u) << 12) | (minor & 0xffu);

		static bool posixFileIdUnavailableLogged;

		/// <summary>
		/// The device and inode number of <paramref name="path"/> (following symlinks, like stat), or
		/// false when the file cannot be read or the C library lacks the call.
		/// </summary>
		internal static bool TryGetFileIdPosix(string path, out (ulong Device, ulong Inode) id) =>
			TryGetFileIdPosix(path, out id, out _);

		static bool TryGetFileIdPosix(string path, out (ulong Device, ulong Inode) id, out uint linkCount) {
			id = default;
			linkCount = 0;
			try {
				if (OperatingSystem.IsMacOS()) {
					DarwinStat st;
					int result = RuntimeInformation.ProcessArchitecture == Architecture.X64
						? StatMacX64(path, out st)
						: StatMacArm64(path, out st);
					if (result != 0)
						return false;
					id = ((uint)st.Dev, st.Ino);
					linkCount = st.LinkCount;
					return true;
				}
				if (Statx(AT_FDCWD, path, 0, STATX_INO | STATX_NLINK, out var stx) != 0 || (stx.Mask & STATX_INO) == 0)
					return false;
				id = (MakeDev(stx.DevMajor, stx.DevMinor), stx.Ino);
				linkCount = (stx.Mask & STATX_NLINK) != 0 ? stx.LinkCount : 0;
				return true;
			}
			catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException) {
				if (!posixFileIdUnavailableLogged) {
					posixFileIdUnavailableLogged = true;
					Logger.Instance.Warn($"Hard links cannot be recognized on this system, treating them as separate files: {ex.Message}");
				}
				return false;
			}
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

		/// <summary>Reads a physical identity; unreadable paths remain unique.</summary>
		public static bool TryGetFileIdentity(string filepath, out FileIdentity identity) =>
			TryGetLinkInfo(filepath, out identity, out _);

		/// <summary>
		/// Reads the physical identity and total name count, including links outside the results.
		/// The count is needed to decide whether a deletion frees the underlying file's data.
		/// </summary>
		public static bool TryGetLinkInfo(string filepath, out FileIdentity identity, out uint linkCount) {
			identity = default;
			linkCount = 0;
			if (!CoreUtils.IsWindows) {
				if (!TryGetFileIdPosix(filepath, out var posixId, out linkCount) || linkCount == 0)
					return false;
				identity = new FileIdentity(posixId.Device, posixId.Inode);
				return true;
			}
			if (!TryGetFileIdWindows(filepath, out var id))
				return false;
			if ((id.VolumeSerial, id.FileIndexHigh, id.FileIndexLow) == (0u, 0u, 0u))
				return false;
			identity = new FileIdentity(id.VolumeSerial, ((ulong)id.FileIndexHigh << 32) | id.FileIndexLow);
			linkCount = id.NumberOfLinks;
			return true;
		}

		static bool AreSameFilePosix(string filepath, string otherFilepath) =>
			TryGetFileIdPosix(filepath, out var fileA) &&
			TryGetFileIdPosix(otherFilepath, out var fileB) &&
			fileA == fileB;

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

		static bool TryGetFileIdWindows(string filepath, out (uint VolumeSerial, uint FileIndexHigh, uint FileIndexLow, uint NumberOfLinks) fileId) {
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
				fileId = (info.VolumeSerialNumber, info.FileIndexHigh, info.FileIndexLow, info.NumberOfLinks);
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
