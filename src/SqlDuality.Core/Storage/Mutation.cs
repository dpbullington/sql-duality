namespace SqlDuality.Core.Storage;

/// <summary>The kind of change a <see cref="Mutation"/> represents.</summary>
public enum MutationKind : byte
{
    /// <summary>Unspecified / invalid.</summary>
    Unknown = 0,

    /// <summary>Insert or overwrite a key with a value.</summary>
    Put = 1,

    /// <summary>Remove a key. Recorded as a tombstone version so deletes are visible to snapshots and CDC.</summary>
    Delete = 2,
}

/// <summary>A single key-level change within a transaction.</summary>
/// <param name="Kind">Whether the key is written or deleted.</param>
/// <param name="Key">The key bytes.</param>
/// <param name="Value">The value bytes for <see cref="MutationKind.Put"/>; empty for a delete.</param>
public sealed record Mutation(MutationKind Kind, ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value)
{
    /// <summary>Creates a put mutation.</summary>
    public static Mutation Put(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value) =>
        new(MutationKind.Put, key, value);

    /// <summary>Creates a delete mutation (tombstone).</summary>
    public static Mutation Delete(ReadOnlyMemory<byte> key) =>
        new(MutationKind.Delete, key, ReadOnlyMemory<byte>.Empty);
}
