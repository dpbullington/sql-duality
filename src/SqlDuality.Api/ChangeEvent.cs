using SqlDuality.Core.Logging;

namespace SqlDuality;

/// <summary>The kind of change a <see cref="ChangeEvent"/> reports.</summary>
public enum ChangeKind
{
    /// <summary>A key was inserted or updated.</summary>
    Upsert,

    /// <summary>A key was deleted.</summary>
    Delete,
}

/// <summary>
/// A single change-data-capture event derived from the durable log. Because the log is the source
/// of truth, CDC is exact and replayable: a consumer can resume from any <see cref="Lsn"/>.
/// </summary>
/// <param name="Lsn">The commit LSN of the transaction that produced this change.</param>
/// <param name="TimestampUnixMs">Commit time, Unix epoch milliseconds (UTC).</param>
/// <param name="Collection">The collection the key belongs to.</param>
/// <param name="Key">The user key bytes (collection prefix stripped).</param>
/// <param name="Kind">Whether the change was an upsert or a delete.</param>
/// <param name="Value">The new value for an upsert; <c>null</c> for a delete.</param>
public sealed record ChangeEvent(
    Lsn Lsn,
    long TimestampUnixMs,
    string Collection,
    byte[] Key,
    ChangeKind Kind,
    byte[]? Value);
