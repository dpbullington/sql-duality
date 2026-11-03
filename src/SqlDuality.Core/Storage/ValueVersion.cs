using SqlDuality.Core.Logging;

namespace SqlDuality.Core.Storage;

/// <summary>
/// One immutable version of a key's value, forming a singly-linked chain ordered by
/// <see cref="CommitLsn"/> descending (newest first). MVCC snapshot reads walk from the head to the
/// first version whose <see cref="CommitLsn"/> is at or below the reader's snapshot LSN.
/// </summary>
/// <remarks>
/// Immutability makes reads lock-free: a reader atomically grabs the head reference and walks the
/// chain while the writer prepends newer versions without mutating existing nodes.
/// </remarks>
public sealed class ValueVersion
{
    /// <summary>The commit LSN at which this version became visible.</summary>
    public Lsn CommitLsn { get; }

    /// <summary>The value bytes, or <c>null</c> if this version is a tombstone (delete).</summary>
    public byte[]? Value { get; }

    /// <summary>True if this version represents a deletion.</summary>
    public bool IsTombstone => Value is null;

    /// <summary>The previous (older) version of the same key, or <c>null</c> at the end of the chain.</summary>
    public ValueVersion? Previous { get; }

    public ValueVersion(Lsn commitLsn, byte[]? value, ValueVersion? previous)
    {
        CommitLsn = commitLsn;
        Value = value;
        Previous = previous;
    }

    /// <summary>Returns the newest version visible at <paramref name="snapshot"/>, or <c>null</c> if none.</summary>
    public ValueVersion? VisibleAt(Lsn snapshot)
    {
        ValueVersion? v = this;
        while (v is not null && v.CommitLsn > snapshot)
        {
            v = v.Previous;
        }
        return v;
    }
}
