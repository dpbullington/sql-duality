using System.Buffers.Binary;
using System.Text;
using SqlDuality.Core.Logging;

namespace SqlDuality.Core.Storage;

/// <summary>One live key/value entry captured in a checkpoint.</summary>
/// <param name="Key">The key bytes.</param>
/// <param name="CommitLsn">The commit LSN of the captured version.</param>
/// <param name="Value">The value bytes (tombstones are not captured).</param>
public readonly record struct CheckpointEntry(byte[] Key, Lsn CommitLsn, byte[] Value);

/// <summary>
/// The on-disk, key-sorted snapshot of materialized state as of a checkpoint LSN. Loading a
/// checkpoint and replaying only the log tail above its LSN is how recovery avoids replaying the
/// entire log, and is what makes log truncation safe.
/// </summary>
/// <remarks>
/// <para>File layout (little-endian):</para>
/// <code>
///   byte[8] Magic = "SQLDCKP1"
///   uint64  CheckpointLsn
///   uint64  EntryCount
///   repeat EntryCount (sorted by Key, lexicographic unsigned):
///     uint32 KeyLength,   Key
///     uint64 CommitLsn
///     uint32 ValueLength, Value
///   uint32  Crc32C         // over every preceding byte
/// </code>
/// <para>Writes go to a temp file that is flushed and atomically renamed over the target, so a
/// crash never leaves a partially written checkpoint in place.</para>
/// </remarks>
public static class CheckpointFile
{
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("SQLDCKP1");

    /// <summary>Atomically writes a checkpoint containing <paramref name="entries"/> to <paramref name="path"/>.</summary>
    public static async Task WriteAsync(
        string path,
        Lsn checkpointLsn,
        IReadOnlyList<CheckpointEntry> entries,
        CancellationToken cancellationToken = default)
    {
        string tempPath = path + ".tmp";

        await using (var fs = new FileStream(
            tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            uint crc = 0;

            void Emit(ReadOnlySpan<byte> bytes)
            {
                crc = Crc32C.Append(crc, bytes);
                fs.Write(bytes);
            }

            Emit(Magic);

            Span<byte> u64 = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(u64, checkpointLsn.Value);
            Emit(u64);
            BinaryPrimitives.WriteUInt64LittleEndian(u64, (ulong)entries.Count);
            Emit(u64);

            Span<byte> u32 = stackalloc byte[4];
            foreach (CheckpointEntry e in entries)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(u32, (uint)e.Key.Length);
                Emit(u32);
                Emit(e.Key);
                BinaryPrimitives.WriteUInt64LittleEndian(u64, e.CommitLsn.Value);
                Emit(u64);
                BinaryPrimitives.WriteUInt32LittleEndian(u32, (uint)e.Value.Length);
                Emit(u32);
                Emit(e.Value);
            }

            BinaryPrimitives.WriteUInt32LittleEndian(u32, crc);
            fs.Write(u32); // CRC itself is not part of the CRC

            fs.Flush(flushToDisk: true);
        }

        File.Move(tempPath, path, overwrite: true);
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Reads a checkpoint file. Returns <c>false</c> if the file is missing or fails integrity
    /// validation (in which case recovery should fall back to replaying from the log start).
    /// </summary>
    public static bool TryRead(string path, out Lsn checkpointLsn, out IReadOnlyList<CheckpointEntry> entries)
    {
        checkpointLsn = Lsn.None;
        entries = Array.Empty<CheckpointEntry>();

        if (!File.Exists(path))
        {
            return false;
        }

        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length < Magic.Length + 8 + 8 + 4)
        {
            return false;
        }

        var span = bytes.AsSpan();
        if (!span[..Magic.Length].SequenceEqual(Magic))
        {
            return false;
        }

        int crcCovered = bytes.Length - 4;
        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(span[crcCovered..]);
        if (Crc32C.Compute(span[..crcCovered]) != storedCrc)
        {
            return false;
        }

        int o = Magic.Length;
        ulong lsn = BinaryPrimitives.ReadUInt64LittleEndian(span[o..]);
        o += 8;
        ulong count = BinaryPrimitives.ReadUInt64LittleEndian(span[o..]);
        o += 8;

        var list = new List<CheckpointEntry>((int)count);
        for (ulong i = 0; i < count; i++)
        {
            uint keyLen = BinaryPrimitives.ReadUInt32LittleEndian(span[o..]);
            o += 4;
            byte[] key = span.Slice(o, (int)keyLen).ToArray();
            o += (int)keyLen;
            ulong commitLsn = BinaryPrimitives.ReadUInt64LittleEndian(span[o..]);
            o += 8;
            uint valueLen = BinaryPrimitives.ReadUInt32LittleEndian(span[o..]);
            o += 4;
            byte[] value = span.Slice(o, (int)valueLen).ToArray();
            o += (int)valueLen;
            list.Add(new CheckpointEntry(key, new Lsn(commitLsn), value));
        }

        checkpointLsn = new Lsn(lsn);
        entries = list;
        return true;
    }
}
