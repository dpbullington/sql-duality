using System.Globalization;

namespace SqlDuality.Core.Logging;

/// <summary>
/// One on-disk log file holding a contiguous range of LSNs. A segment's file name encodes the LSN
/// of its first record (Kafka-style), so the set of segments can be ordered and addressed without
/// opening them.
/// </summary>
/// <remarks>
/// Instances are mutable only by the single writer loop in <see cref="DurableLog"/> (via
/// <see cref="RecordAppended"/>); readers access <see cref="BaseLsn"/>, <see cref="NextLsn"/> and
/// <see cref="TryGetPosition"/> under the log's index lock.
/// </remarks>
internal sealed class LogSegment
{
    public const string FileExtension = ".log";

    /// <summary>LSN of the first record this segment will hold.</summary>
    public Lsn BaseLsn { get; }

    /// <summary>Absolute path to the segment file.</summary>
    public string Path { get; }

    /// <summary>Byte position of record (BaseLsn + i) within the file, indexed by i.</summary>
    private readonly List<long> _positions = new();

    /// <summary>Current size of the segment file in bytes (as written so far).</summary>
    public long SizeBytes { get; private set; }

    public LogSegment(Lsn baseLsn, string path, long initialSizeBytes = 0)
    {
        BaseLsn = baseLsn;
        Path = path;
        SizeBytes = initialSizeBytes;
    }

    /// <summary>Number of records currently in the segment.</summary>
    public int Count => _positions.Count;

    /// <summary>True if the segment holds no records.</summary>
    public bool IsEmpty => _positions.Count == 0;

    /// <summary>One past the last LSN in this segment (i.e. the LSN a new record here would take).</summary>
    public Lsn NextLsn => new(BaseLsn.Value + (ulong)_positions.Count);

    /// <summary>Records that a framed record for <paramref name="lsn"/> was written at <paramref name="position"/>.</summary>
    public void RecordAppended(Lsn lsn, long position, int framedLength)
    {
        if (lsn != NextLsn)
        {
            throw new InvalidOperationException(
                $"Out-of-order append to segment {BaseLsn}: expected {NextLsn}, got {lsn}.");
        }
        _positions.Add(position);
        SizeBytes = position + framedLength;
    }

    /// <summary>Resolves the file position of <paramref name="lsn"/> if it lives in this segment.</summary>
    public bool TryGetPosition(Lsn lsn, out long position)
    {
        if (lsn >= BaseLsn && lsn < NextLsn)
        {
            position = _positions[(int)(lsn.Value - BaseLsn.Value)];
            return true;
        }
        position = 0;
        return false;
    }

    /// <summary>Formats the canonical, zero-padded file name for a segment beginning at <paramref name="baseLsn"/>.</summary>
    public static string FileNameFor(Lsn baseLsn) =>
        baseLsn.Value.ToString("D20", CultureInfo.InvariantCulture) + FileExtension;

    /// <summary>Parses a segment base LSN from a file name, or returns <c>false</c> if it is not a segment file.</summary>
    public static bool TryParseBaseLsn(string fileName, out Lsn baseLsn)
    {
        baseLsn = Lsn.None;
        if (!fileName.EndsWith(FileExtension, StringComparison.Ordinal))
        {
            return false;
        }
        string stem = fileName[..^FileExtension.Length];
        if (!ulong.TryParse(stem, NumberStyles.None, CultureInfo.InvariantCulture, out ulong value))
        {
            return false;
        }
        baseLsn = new Lsn(value);
        return true;
    }
}
