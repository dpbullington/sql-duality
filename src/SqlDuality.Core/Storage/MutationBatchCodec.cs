using System.Buffers;
using System.Buffers.Binary;

namespace SqlDuality.Core.Storage;

/// <summary>
/// Serializes a batch of <see cref="Mutation"/>s into (and out of) the opaque payload of a
/// <see cref="Logging.LogRecordType.Transaction"/> log record. One committed transaction is one
/// such payload, so the single-record CRC gives the whole batch all-or-nothing atomicity.
/// </summary>
/// <remarks>
/// <para>Payload layout (little-endian):</para>
/// <code>
///   byte    PayloadVersion   // = 1
///   uint32  MutationCount
///   repeat MutationCount:
///     byte    MutationKind
///     uint32  KeyLength
///     byte[]  Key
///     uint32  ValueLength     // 0 for Delete
///     byte[]  Value
/// </code>
/// </remarks>
public static class MutationBatchCodec
{
    /// <summary>Current payload format version.</summary>
    public const byte CurrentVersion = 1;

    /// <summary>Computes the serialized size of <paramref name="mutations"/>.</summary>
    public static int ComputeSize(IReadOnlyList<Mutation> mutations)
    {
        int size = sizeof(byte) + sizeof(uint);
        foreach (Mutation m in mutations)
        {
            size += sizeof(byte) + sizeof(uint) + m.Key.Length + sizeof(uint) + m.Value.Length;
        }
        return size;
    }

    /// <summary>Serializes <paramref name="mutations"/> into <paramref name="destination"/> and returns bytes written.</summary>
    public static int Write(Span<byte> destination, IReadOnlyList<Mutation> mutations)
    {
        int o = 0;
        destination[o++] = CurrentVersion;
        BinaryPrimitives.WriteUInt32LittleEndian(destination[o..], (uint)mutations.Count);
        o += sizeof(uint);

        foreach (Mutation m in mutations)
        {
            destination[o++] = (byte)m.Kind;
            BinaryPrimitives.WriteUInt32LittleEndian(destination[o..], (uint)m.Key.Length);
            o += sizeof(uint);
            m.Key.Span.CopyTo(destination[o..]);
            o += m.Key.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(destination[o..], (uint)m.Value.Length);
            o += sizeof(uint);
            m.Value.Span.CopyTo(destination[o..]);
            o += m.Value.Length;
        }
        return o;
    }

    /// <summary>Serializes <paramref name="mutations"/> to a freshly allocated array.</summary>
    public static byte[] Serialize(IReadOnlyList<Mutation> mutations)
    {
        var buffer = new byte[ComputeSize(mutations)];
        Write(buffer, mutations);
        return buffer;
    }

    /// <summary>Deserializes a mutation batch previously produced by <see cref="Write"/>.</summary>
    public static IReadOnlyList<Mutation> Read(ReadOnlySpan<byte> payload)
    {
        int o = 0;
        byte version = payload[o++];
        if (version != CurrentVersion)
        {
            throw new InvalidDataException($"Unsupported mutation batch version {version}.");
        }

        uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]);
        o += sizeof(uint);

        var mutations = new List<Mutation>((int)count);
        for (uint i = 0; i < count; i++)
        {
            var kind = (MutationKind)payload[o++];
            uint keyLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]);
            o += sizeof(uint);
            byte[] key = payload.Slice(o, (int)keyLen).ToArray();
            o += (int)keyLen;
            uint valueLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]);
            o += sizeof(uint);
            byte[] value = payload.Slice(o, (int)valueLen).ToArray();
            o += (int)valueLen;
            mutations.Add(new Mutation(kind, key, value));
        }
        return mutations;
    }
}
