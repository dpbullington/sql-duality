using System.Buffers.Binary;
using System.Text;

namespace SqlDuality;

/// <summary>
/// Encodes a (collection, key) pair into the single flat key space of the underlying key-value
/// store, and decodes it back. Layout: <c>[uint16 nameLength][name UTF-8][user key bytes]</c>.
/// </summary>
/// <remarks>
/// The length-prefixed collection name guarantees that two collections never collide regardless of
/// the user key bytes, and lets a change-capture consumer recover which collection a raw store key
/// belonged to.
/// </remarks>
internal static class CollectionKey
{
    /// <summary>Maximum encoded length of a collection name in bytes.</summary>
    public const int MaxNameBytes = ushort.MaxValue;

    public static byte[] Encode(string collection, ReadOnlySpan<byte> key)
    {
        int nameByteCount = Encoding.UTF8.GetByteCount(collection);
        if (nameByteCount > MaxNameBytes)
        {
            throw new ArgumentException($"Collection name exceeds {MaxNameBytes} bytes.", nameof(collection));
        }

        var buffer = new byte[sizeof(ushort) + nameByteCount + key.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(buffer, (ushort)nameByteCount);
        Encoding.UTF8.GetBytes(collection, buffer.AsSpan(sizeof(ushort), nameByteCount));
        key.CopyTo(buffer.AsSpan(sizeof(ushort) + nameByteCount));
        return buffer;
    }

    public static bool TryDecode(ReadOnlySpan<byte> composite, out string collection, out byte[] key)
    {
        collection = string.Empty;
        key = Array.Empty<byte>();
        if (composite.Length < sizeof(ushort))
        {
            return false;
        }

        int nameLen = BinaryPrimitives.ReadUInt16LittleEndian(composite);
        int keyStart = sizeof(ushort) + nameLen;
        if (composite.Length < keyStart)
        {
            return false;
        }

        collection = Encoding.UTF8.GetString(composite.Slice(sizeof(ushort), nameLen));
        key = composite[keyStart..].ToArray();
        return true;
    }
}
