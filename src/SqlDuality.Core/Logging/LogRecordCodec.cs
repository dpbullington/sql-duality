using System.Buffers;
using System.Buffers.Binary;

namespace SqlDuality.Core.Logging;

/// <summary>
/// Serializes and deserializes the on-disk framing of a <see cref="LogRecord"/>.
/// </summary>
/// <remarks>
/// <para>Frame layout (all integers little-endian):</para>
/// <code>
///   uint32  BodyLength        // number of bytes following this field (header + payload + crc)
///   ── CRC-covered body starts ──
///   byte    FormatVersion     // = 1
///   byte    RecordType        // LogRecordType
///   uint64  Lsn
///   int64   TimestampUnixMs
///   uint32  PayloadLength
///   byte[]  Payload
///   ── body ends ──
///   uint32  Crc32C            // CRC-32C over the body (FormatVersion .. end of Payload)
/// </code>
/// <para>
/// The leading <c>BodyLength</c> lets a reader know exactly how many bytes to consume before it
/// can validate the trailing CRC. A torn write at the tail manifests as either a short read
/// (fewer than <c>BodyLength</c> bytes available) or a CRC mismatch; both are treated as the
/// end of valid data during recovery.
/// </para>
/// </remarks>
public static class LogRecordCodec
{
    /// <summary>The current on-disk frame format version.</summary>
    public const byte CurrentFormatVersion = 1;

    // Field sizes within the body.
    private const int VersionSize = sizeof(byte);
    private const int TypeSize = sizeof(byte);
    private const int LsnSize = sizeof(ulong);
    private const int TimestampSize = sizeof(long);
    private const int PayloadLenSize = sizeof(uint);
    private const int CrcSize = sizeof(uint);

    /// <summary>Bytes of the leading length prefix that precedes the CRC-covered body.</summary>
    public const int LengthPrefixSize = sizeof(uint);

    /// <summary>Body bytes that are fixed regardless of payload size (everything but the payload).</summary>
    public const int FixedBodyOverhead =
        VersionSize + TypeSize + LsnSize + TimestampSize + PayloadLenSize + CrcSize;

    /// <summary>Total framed size on disk for a record carrying <paramref name="payloadLength"/> payload bytes.</summary>
    public static int FramedSize(int payloadLength) =>
        LengthPrefixSize + FixedBodyOverhead + payloadLength;

    /// <summary>
    /// Writes the fully framed record (length prefix + body + CRC) into <paramref name="destination"/>,
    /// which must be at least <see cref="FramedSize(int)"/> bytes. Returns the number of bytes written.
    /// </summary>
    public static int Write(
        Span<byte> destination,
        Lsn lsn,
        LogRecordType type,
        long timestampUnixMs,
        ReadOnlySpan<byte> payload)
    {
        int framed = FramedSize(payload.Length);
        if (destination.Length < framed)
        {
            throw new ArgumentException(
                $"Destination buffer too small: need {framed}, have {destination.Length}.",
                nameof(destination));
        }

        int bodyLength = FixedBodyOverhead + payload.Length;

        // Length prefix (not covered by CRC).
        BinaryPrimitives.WriteUInt32LittleEndian(destination, (uint)bodyLength);

        Span<byte> body = destination.Slice(LengthPrefixSize, bodyLength);
        int o = 0;
        body[o++] = CurrentFormatVersion;
        body[o++] = (byte)type;
        BinaryPrimitives.WriteUInt64LittleEndian(body[o..], lsn.Value);
        o += LsnSize;
        BinaryPrimitives.WriteInt64LittleEndian(body[o..], timestampUnixMs);
        o += TimestampSize;
        BinaryPrimitives.WriteUInt32LittleEndian(body[o..], (uint)payload.Length);
        o += PayloadLenSize;
        payload.CopyTo(body[o..]);
        o += payload.Length;

        // CRC covers the body from FormatVersion through the end of the payload.
        uint crc = Crc32C.Compute(body[..o]);
        BinaryPrimitives.WriteUInt32LittleEndian(body[o..], crc);

        return framed;
    }

    /// <summary>
    /// Attempts to decode one record from <paramref name="source"/> starting at offset 0.
    /// </summary>
    /// <param name="source">A buffer positioned at the start of a record (its length prefix).</param>
    /// <param name="record">The decoded record on success.</param>
    /// <param name="consumed">Total bytes the record occupies on disk on success.</param>
    /// <returns>
    /// The outcome. Any result other than <see cref="DecodeStatus.Ok"/> means the record is the
    /// end of valid data (truncated or corrupt) and recovery should stop/truncate here.
    /// </returns>
    public static DecodeStatus TryRead(
        ReadOnlySpan<byte> source,
        out LogRecord? record,
        out int consumed)
    {
        record = null;
        consumed = 0;

        if (source.Length < LengthPrefixSize)
        {
            return DecodeStatus.Incomplete;
        }

        uint bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(source);

        // A zero or implausibly small body cannot be a real record (pre-allocated zero-fill, or junk).
        if (bodyLength < FixedBodyOverhead)
        {
            return DecodeStatus.Corrupt;
        }

        long framed = (long)LengthPrefixSize + bodyLength;
        if (source.Length < framed)
        {
            return DecodeStatus.Incomplete; // torn write at the tail
        }

        ReadOnlySpan<byte> body = source.Slice(LengthPrefixSize, (int)bodyLength);
        int crcCoveredLength = (int)bodyLength - CrcSize;

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(body[crcCoveredLength..]);
        uint actualCrc = Crc32C.Compute(body[..crcCoveredLength]);
        if (storedCrc != actualCrc)
        {
            return DecodeStatus.Corrupt;
        }

        int o = 0;
        byte version = body[o++];
        if (version != CurrentFormatVersion)
        {
            // Unknown/newer format: treat as corrupt for Phase 1 (forward-compat is a later concern).
            return DecodeStatus.Corrupt;
        }

        var type = (LogRecordType)body[o++];
        ulong lsn = BinaryPrimitives.ReadUInt64LittleEndian(body[o..]);
        o += LsnSize;
        long timestamp = BinaryPrimitives.ReadInt64LittleEndian(body[o..]);
        o += TimestampSize;
        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(body[o..]);
        o += PayloadLenSize;

        // Internal consistency: the declared payload length must match the framed body.
        if (crcCoveredLength - o != (int)payloadLen)
        {
            return DecodeStatus.Corrupt;
        }

        var payload = body.Slice(o, (int)payloadLen).ToArray();
        record = new LogRecord(new Lsn(lsn), type, timestamp, payload);
        consumed = (int)framed;
        return DecodeStatus.Ok;
    }

    /// <summary>Rents a buffer, frames the record into it, and returns the writable slice + the owner to return.</summary>
    internal static (byte[] Buffer, int Length) Rent(
        Lsn lsn, LogRecordType type, long timestampUnixMs, ReadOnlySpan<byte> payload)
    {
        int framed = FramedSize(payload.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(framed);
        int written = Write(buffer, lsn, type, timestampUnixMs, payload);
        return (buffer, written);
    }
}

/// <summary>Outcome of attempting to decode a single framed record.</summary>
public enum DecodeStatus
{
    /// <summary>A complete, checksum-valid record was decoded.</summary>
    Ok,

    /// <summary>Not enough bytes are present to form a complete record (torn tail write).</summary>
    Incomplete,

    /// <summary>Enough bytes were present but the record failed structural or CRC validation.</summary>
    Corrupt,
}
