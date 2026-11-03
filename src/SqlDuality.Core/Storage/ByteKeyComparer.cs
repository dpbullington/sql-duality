namespace SqlDuality.Core.Storage;

/// <summary>
/// Content-based equality and ordering for <see cref="byte"/>[] keys. Equality drives the in-memory
/// index; ordering (lexicographic, unsigned) drives the sorted on-disk checkpoint.
/// </summary>
public sealed class ByteKeyComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
{
    /// <summary>Shared instance.</summary>
    public static readonly ByteKeyComparer Instance = new();

    private ByteKeyComparer() { }

    /// <inheritdoc/>
    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return x.AsSpan().SequenceEqual(y);
    }

    /// <inheritdoc/>
    public int GetHashCode(byte[] obj)
    {
        var hash = new HashCode();
        hash.AddBytes(obj);
        return hash.ToHashCode();
    }

    /// <inheritdoc/>
    public int Compare(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.AsSpan().SequenceCompareTo(y);
    }
}
