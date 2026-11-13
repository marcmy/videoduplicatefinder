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
using System.Text;
using System.Text.RegularExpressions;

namespace VDF.Core.Utils {

	/// <summary>What a <see cref="LogRedactor"/> treats as identifying besides media file paths.</summary>
	public sealed record LogRedactorOptions {
		/// <summary>The user's home folder; replaced with <see cref="ProfileToken"/>.</summary>
		public string? UserProfile { get; init; }
		/// <summary>Whole-word occurrences are replaced with <c>&lt;user&gt;</c>.</summary>
		public string? UserName { get; init; }
		/// <summary>Whole-word occurrences are replaced with <c>&lt;machine&gt;</c>.</summary>
		public string? MachineName { get; init; }
		/// <summary>
		/// Folders that are identifying even when no media file inside them shows up in the
		/// text, such as the scan's include and exclude folders.
		/// </summary>
		public IReadOnlyCollection<string> SensitiveFolders { get; init; } = Array.Empty<string>();
		/// <summary><c>%USERPROFILE%</c> on Windows, <c>~</c> elsewhere.</summary>
		public string ProfileToken { get; init; } = OperatingSystem.IsWindows() ? "%USERPROFILE%" : "~";

		public static LogRedactorOptions ForCurrentUser(IEnumerable<string>? sensitiveFolders = null) => new() {
			UserProfile = SafeGet(() => Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)),
			UserName = SafeGet(() => Environment.UserName),
			MachineName = SafeGet(() => Environment.MachineName),
			SensitiveFolders = sensitiveFolders?.ToArray() ?? Array.Empty<string>(),
		};

		static string? SafeGet(Func<string> get) {
			try { return get(); }
			catch { return null; }
		}
	}

	/// <summary>
	/// Replaces video and image paths and names in log text with stable tokens, so a log
	/// can be shared without exposing the library. A C# take on the Redact-VideoPaths
	/// script: <c>D:\Movies\Some Film (2020).mkv</c> becomes <c>D:\video_0001.mkv</c>, and
	/// the same path (case-insensitively) always gets the same number for the lifetime of
	/// the instance, so a reader can still follow one file through the log.
	///
	/// Kept readable: the drive letter (it ties a file to the "Drive 'D:\'" concurrency
	/// line and is not identifying), the extension, whether the file was on a network
	/// share, and everything that is not a path (codecs, timings, FFmpeg messages).
	///
	/// Also replaced: the folders those files live in (<c>D:\folder_0001</c>), any
	/// <see cref="LogRedactorOptions.SensitiveFolders"/>, the user's profile folder, and the
	/// user and machine names. A bare file name with spaces that never appears with its
	/// folder and is not quoted or after a <c>[Tag] </c> can only be caught from its last
	/// space-free word onwards.
	/// </summary>
	public sealed class LogRedactor {
		// The script's list plus VDF's own video and image extensions.
		static readonly string[] VideoExtensions = {
			"mkv", "mk3d", "mp4", "m4v", "avi", "mpg", "mpeg", "m2v", "mov", "qt", "wmv", "flv", "webm",
			"ts", "mts", "m2ts", "m2t", "rm", "rmvb", "3gp", "3g2", "vob", "ogv", "ogm", "divx", "asf",
			"mpv", "mxf", "f4v", "bik", "smk", "nut", "y4m", "evo", "dvr-ms",
		};
		static readonly string[] ImageExtensions = {
			"jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff", "webp", "heic", "heif", "avif",
		};
		static readonly HashSet<string> ImageExtensionSet = new(ImageExtensions, StringComparer.OrdinalIgnoreCase);

		const RegexOptions Options = RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture;
		static readonly string Ext = "(?<ext>" + string.Join("|", VideoExtensions.Concat(ImageExtensions).OrderByDescending(e => e.Length).Select(Regex.Escape)) + ")(?![\\w.])";
		// Characters Windows forbids in a path segment (the script's set).
		const string Seg = "[^\\\\/:*?\"<>|\\r\\n]";

		// D:\dir\name.ext, also D:/dir/name.ext as FFmpeg sometimes echoes it.
		static readonly string DrivePath = $"(?<![\\w])(?<drive>[A-Za-z]):[\\\\/](?<dir>(?:{Seg}+[\\\\/])*)(?<name>{Seg}+?)\\.{Ext}";
		// \\host\share\dir\name.ext
		static readonly string UncPath = $"(?<![\\\\\\w])\\\\\\\\(?<host>[^\\\\/:*?\"<>|\\s]+)\\\\(?<dir>(?:{Seg}+\\\\)*)(?<name>{Seg}+?)\\.{Ext}";
		// /home/me/dir/name.ext — not part of a URL or a Windows path.
		static readonly string UnixPath = $"(?<![\\w:/\\\\.~])/(?<dir>(?:[^/\\r\\n\"'<>|:*?]+/)*)(?<name>[^/\\r\\n\"'<>|:*?]+?)\\.{Ext}";
		// 'name.ext' or "name.ext"
		static readonly string QuotedName = $"(?<=['\"])(?<name>[^'\"\\\\/:*?<>|\\r\\n]+?)\\.{Ext}(?=['\"])";
		// [ChromaprintEngine] name with spaces.ext
		static readonly string TaggedName = $"(?<=\\]\\s)(?<name>{Seg}+?)\\.{Ext}";
		// name.ext as one word
		static readonly string WordName = $"(?<=^|[\\s'\"=,;(\\[{{>])(?<name>[^\\s\\\\/:*?\"<>|'=,;(\\[{{][^\\s\\\\/:*?\"<>|'=,;]*?)\\.{Ext}";

		static readonly Regex PathFinder = new($"(?<d>{DrivePath})|(?<u>{UncPath})|(?<x>{UnixPath})", Options);

		readonly LogRedactorOptions options;
		readonly string? profile;
		readonly Dictionary<string, int> fileNumbers = new(StringComparer.OrdinalIgnoreCase);
		readonly Dictionary<string, int> nameNumbers = new(StringComparer.OrdinalIgnoreCase);
		readonly Dictionary<string, int> folderNumbers = new(StringComparer.OrdinalIgnoreCase);
		readonly HashSet<string> knownFolders = new(StringComparer.OrdinalIgnoreCase);
		int nextFile, nextFolder;
		Regex? replacer;
		bool replacerStale = true;

		public LogRedactor(LogRedactorOptions? options = null) {
			this.options = options ?? new LogRedactorOptions();
			profile = TrimSeparators(this.options.UserProfile);
			if (profile is { Length: < 3 }) profile = null;
			foreach (var folder in this.options.SensitiveFolders)
				RegisterFolder(folder);
		}

		/// <summary>Distinct media files (by full path or bare name) replaced so far.</summary>
		public int RedactedFileCount => nextFile;
		/// <summary>Distinct folders replaced so far.</summary>
		public int RedactedFolderCount => nextFolder;

		/// <summary>
		/// Redacts <paramref name="text"/>. Every full path in it is numbered first, in the
		/// order it appears, so a bare file name earlier in the text still gets the number
		/// of its full path.
		/// </summary>
		public string Redact(string text) {
			if (string.IsNullOrEmpty(text))
				return text;
			foreach (Match m in PathFinder.Matches(text))
				RegisterPath(m);
			var regex = GetReplacer();
			var sb = new StringBuilder(text.Length);
			// Line by line keeps the regex's work per match bounded by the line length.
			int start = 0;
			while (start <= text.Length) {
				int nl = text.IndexOf('\n', start);
				int end = nl < 0 ? text.Length : nl;
				sb.Append(regex.Replace(text.Substring(start, end - start), Replace));
				if (nl < 0) break;
				sb.Append('\n');
				start = nl + 1;
			}
			return sb.ToString();
		}

		/// <summary>Redacts a text file line by line into another file.</summary>
		public void RedactFile(string inputPath, string outputPath) {
			if (string.Equals(Path.GetFullPath(inputPath), Path.GetFullPath(outputPath), StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException("The output path must differ from the input path.", nameof(outputPath));
			string text = ReadShared(inputPath);
			File.WriteAllText(outputPath, Redact(text), new UTF8Encoding(false));
		}

		/// <summary>Reads a file another process (the logger) may be appending to.</summary>
		public static string ReadShared(string path) {
			using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
			using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
			return reader.ReadToEnd();
		}

		void RegisterPath(Match m) {
			string full = m.Value;
			if (fileNumbers.ContainsKey(full))
				return;
			int number = ++nextFile;
			fileNumbers[full] = number;
			string fileName = m.Groups["name"].Value + "." + m.Groups["ext"].Value;
			nameNumbers.TryAdd(fileName, number);
			replacerStale = true;

			// Every folder above the file identifies the library too.
			char separator = m.Groups["x"].Success ? '/' : '\\';
			string current = m.Groups["d"].Success ? m.Groups["drive"].Value + ":"
				: m.Groups["u"].Success ? "\\\\" + m.Groups["host"].Value
				: string.Empty;
			foreach (var part in m.Groups["dir"].Value.Split(new[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries)) {
				current = current + separator + part;
				RegisterFolder(current);
			}
		}

		void RegisterFolder(string? folder) {
			folder = TrimSeparators(folder);
			if (string.IsNullOrEmpty(folder) || folder.Length < 3)
				return;
			// A bare drive root ("D:") or the profile and its parents ("C:\Users") stay as they are;
			// the profile gets its own token.
			if (folder.Length <= 3 && folder[1] == ':')
				return;
			if (profile != null && (profile.Equals(folder, StringComparison.OrdinalIgnoreCase) ||
				profile.StartsWith(folder + "\\", StringComparison.OrdinalIgnoreCase) ||
				profile.StartsWith(folder + "/", StringComparison.OrdinalIgnoreCase)))
				return;
			if (knownFolders.Add(folder))
				replacerStale = true;
		}

		static string? TrimSeparators(string? path) {
			if (string.IsNullOrWhiteSpace(path)) return null;
			path = path.Trim();
			// Keep "\\" and "/" alone; strip a trailing separator otherwise.
			while (path.Length > 1 && (path.EndsWith('\\') || path.EndsWith('/')) && !(path.Length == 3 && path[1] == ':'))
				path = path[..^1];
			return path;
		}

		Regex GetReplacer() {
			if (replacer != null && !replacerStale)
				return replacer;
			const string FolderEnd = "(?=[\\\\/'\"\\s,;:)\\]]|$)";
			var alternatives = new List<string> {
				$"(?<d>{DrivePath})",
				$"(?<u>{UncPath})",
				$"(?<x>{UnixPath})",
			};
			if (knownFolders.Count > 0)
				alternatives.Add("(?<f>" + string.Join("|", knownFolders.OrderByDescending(f => f.Length).Select(Regex.Escape)) + ")" + FolderEnd);
			if (profile != null)
				alternatives.Add("(?<p>" + Regex.Escape(profile) + ")" + FolderEnd);
			if (nameNumbers.Count > 0)
				alternatives.Add("(?<![\\w.\\-])(?<k>" + string.Join("|", nameNumbers.Keys.OrderByDescending(n => n.Length).Select(Regex.Escape)) + ")(?![\\w.])");
			alternatives.Add($"(?<q>{QuotedName})");
			alternatives.Add($"(?<t>{TaggedName})");
			alternatives.Add($"(?<w>{WordName})");
			if (IsUsefulWord(options.UserName))
				alternatives.Add("(?<![\\w])(?<user>" + Regex.Escape(options.UserName!) + ")(?![\\w])");
			if (IsUsefulWord(options.MachineName))
				alternatives.Add("(?<![\\w])(?<machine>" + Regex.Escape(options.MachineName!) + ")(?![\\w])");
			replacer = new Regex(string.Join("|", alternatives), Options);
			replacerStale = false;
			return replacer;
		}

		static bool IsUsefulWord(string? word) => word is { Length: >= 3 };

		string Replace(Match m) {
			if (m.Groups["d"].Success)
				return $"{m.Groups["drive"].Value.ToUpperInvariant()}:\\{FileToken(m.Value, m.Groups["ext"].Value)}";
			if (m.Groups["u"].Success)
				return $"\\\\server\\{FileToken(m.Value, m.Groups["ext"].Value)}";
			if (m.Groups["x"].Success)
				return $"/{FileToken(m.Value, m.Groups["ext"].Value)}";
			if (m.Groups["f"].Success)
				return FolderToken(m.Value);
			if (m.Groups["p"].Success)
				return options.ProfileToken;
			if (m.Groups["k"].Success || m.Groups["q"].Success || m.Groups["t"].Success || m.Groups["w"].Success) {
				string name = m.Value;
				string ext = name[(name.LastIndexOf('.') + 1)..];
				if (!nameNumbers.TryGetValue(name, out int number)) {
					number = ++nextFile;
					nameNumbers[name] = number;
				}
				return Token(number, ext);
			}
			if (m.Groups["user"].Success)
				return "<user>";
			if (m.Groups["machine"].Success)
				return "<machine>";
			return m.Value;
		}

		string FileToken(string fullPath, string ext) {
			if (!fileNumbers.TryGetValue(fullPath, out int number)) {
				number = ++nextFile;
				fileNumbers[fullPath] = number;
			}
			return Token(number, ext);
		}

		static string Token(int number, string ext) =>
			$"{(ImageExtensionSet.Contains(ext) ? "image" : "video")}_{number:D4}.{ext}";

		string FolderToken(string folder) {
			if (!folderNumbers.TryGetValue(folder, out int number)) {
				number = ++nextFolder;
				folderNumbers[folder] = number;
			}
			string token = $"folder_{number:D4}";
			if (folder.StartsWith("\\\\", StringComparison.Ordinal))
				return $"\\\\server\\{token}";
			if (folder.Length >= 2 && folder[1] == ':')
				return $"{char.ToUpperInvariant(folder[0])}:\\{token}";
			return $"/{token}";
		}
	}
}
