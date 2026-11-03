namespace SqlDuality.Core.Logging;

/// <summary>
/// A single logical entry in the <see cref="DurableLog"/>. The <see cref="Payload"/> is opaque to
/// the log layer; higher layers interpret it according to <see cref="Type"/>.
/// </summary>
/// <param name="Lsn">The record's assigned logical sequence number.</param>
/// <param name="Type">The record's intent.</param>
/// <param name="TimestampUnixMs">Append time, Unix epoch milliseconds (UTC).</param>
/// <param name="Payload">The record's opaque body.</param>
public sealed record LogRecord(
    Lsn Lsn,
    LogRecordType Type,
    long TimestampUnixMs,
    ReadOnlyMemory<byte> Payload);
