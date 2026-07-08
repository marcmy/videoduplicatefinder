// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Reflection;

namespace VDF.Core.Tests;

public class PHashSampleQuorumTests {
    [Fact]
    public void PHashUsesAllSamples_NotJustFirst() {
        var scanner = Scanner(percent: 90f, ratio: 0.6f, 0.25f, 0.5f);
        var entry = Entry((1d, 0UL), (2d, 0UL));
        var other = Entry((1d, 0UL), (2d, ulong.MaxValue));

        Assert.False(Check(scanner, entry, other, out _));
    }

    [Fact]
    public void PHashDifferenceAveragesMatchingSamples() {
        var scanner = Scanner(percent: 75f, ratio: 1f, 0.25f, 0.5f);
        var entry = Entry((1d, 0UL), (2d, 0UL));
        var other = Entry((1d, 0UL), (2d, 0xFFFFUL));

        Assert.True(Check(scanner, entry, other, out float difference));
        Assert.InRange(difference, 0.124f, 0.126f);
    }

    [Fact]
    public void PHashRequiresConfiguredSampleQuorum() {
        var scanner = Scanner(percent: 75f, ratio: 0.6f, 0.25f, 0.5f, 0.75f, 1f, 1.25f);
        var entry = Entry((1d, 0UL), (2d, 0UL), (3d, 0UL), (4d, 0UL), (5d, 0UL));
        var other = Entry((1d, 0UL), (2d, 0UL), (3d, 0xFFFFFUL), (4d, 0xFFFFFUL), (5d, 0xFFFFFUL));

        Assert.False(Check(scanner, entry, other, out _));

        scanner.Settings.PHashRequiredMatchingSampleRatio = 0.4f;
        Assert.True(Check(scanner, entry, other, out _));
    }

    static ScanEngine Scanner(float percent, float ratio, params float[] positions) {
        var scanner = new ScanEngine {
            Settings = new Settings {
                UsePHashing = true,
                Percent = percent,
                PHashRequiredMatchingSampleRatio = ratio,
                ThumbnailCount = positions.Length,
            }
        };
        var field = typeof(ScanEngine).GetField("positionList", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var list = (List<float>)field.GetValue(scanner)!;
        list.Clear();
        list.AddRange(positions);
        return scanner;
    }

    static FileEntry Entry(params (double Index, ulong PHash)[] samples) {
        var entry = new FileEntry {
            _Path = @"X:\video.mp4",
            Folder = @"X:\",
            mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(4), Streams = [] },
            invalid = false,
        };
        foreach ((double index, ulong hash) in samples) {
            entry.grayBytes[index] = new byte[1024];
            entry.PHashes[index] = hash;
        }
        return entry;
    }

    static bool Check(ScanEngine scanner, FileEntry entry, FileEntry other, out float difference) {
        var build = typeof(ScanEngine).GetMethod("TryBuildCompareSnapshot", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Assert.True((bool)build.Invoke(scanner, [entry, true])!);
        Assert.True((bool)build.Invoke(scanner, [other, true])!);

        var check = typeof(ScanEngine).GetMethod("CheckIfDuplicate", BindingFlags.Instance | BindingFlags.NonPublic)!;
        object?[] args = [entry, null, null, other, 0f];
        bool result = (bool)check.Invoke(scanner, args)!;
        difference = (float)args[4]!;
        return result;
    }
}
