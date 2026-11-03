namespace SqlDuality.Core.Logging;

/// <summary>
/// Classifies the intent of a <see cref="LogRecord"/>. The log layer treats the payload as opaque
/// bytes; the record type lets higher layers (storage, checkpoints, CDC) route a record without
/// deserializing its payload.
/// </summary>
/// <remarks>
/// Values are explicit and must remain stable across versions — they are persisted on disk.
/// Reserve new values by appending; never renumber existing ones.
/// </remarks>
public enum LogRecordType : byte
{
    /// <summary>Unspecified / invalid. Never written deliberately.</summary>
    Unknown = 0,

    /// <summary>
    /// A committed transaction batch: one or more key/value mutations that must be applied
    /// all-or-nothing. Atomicity is guaranteed by the single-record CRC frame.
    /// </summary>
    Transaction = 1,

    /// <summary>
    /// A checkpoint marker. Signals that durable materialized state up to this record's LSN has
    /// been snapshotted to disk, so earlier log segments may be truncated.
    /// </summary>
    Checkpoint = 2,

    /// <summary>
    /// A no-op / heartbeat record. Useful for advancing offsets and probing liveness without
    /// mutating state. Not produced in Phase 1 but reserved so readers tolerate it.
    /// </summary>
    Noop = 3,
}
