namespace SqlDuality.Core.Logging;

/// <summary>
/// A Log Sequence Number: the logical, monotonically increasing identity of a record in the
/// <see cref="DurableLog"/>. LSNs are the engine's universal cursor — they address records for
/// reads, anchor MVCC read snapshots, mark checkpoints, and serve as CDC/replication offsets.
/// </summary>
/// <remarks>
/// <see cref="None"/> (value 0) is the reserved "before the beginning" sentinel; the first real
/// record is <c>Lsn 1</c>. LSNs are dense and contiguous within a single log's lifetime.
/// </remarks>
public readonly record struct Lsn(ulong Value) : IComparable<Lsn>
{
    /// <summary>The reserved sentinel meaning "no record" / "before the first record".</summary>
    public static readonly Lsn None = new(0);

    /// <summary>The first valid LSN a log will ever assign.</summary>
    public static readonly Lsn First = new(1);

    /// <summary>True for any assigned (non-sentinel) LSN.</summary>
    public bool IsValid => Value != 0;

    /// <summary>The next LSN in sequence.</summary>
    public Lsn Next() => new(Value + 1);

    public int CompareTo(Lsn other) => Value.CompareTo(other.Value);

    public static bool operator <(Lsn a, Lsn b) => a.Value < b.Value;
    public static bool operator >(Lsn a, Lsn b) => a.Value > b.Value;
    public static bool operator <=(Lsn a, Lsn b) => a.Value <= b.Value;
    public static bool operator >=(Lsn a, Lsn b) => a.Value >= b.Value;

    public override string ToString() => Value.ToString();
}
