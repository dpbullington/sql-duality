namespace SqlDuality;

/// <summary>
/// Converts a strongly-typed value to and from its stored byte representation. Keeping serialization
/// pluggable (rather than baking in reflection-based JSON) keeps the API AOT/trim-clean and lets
/// callers supply a source-generated serializer for <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">The document/value type.</typeparam>
public interface IValueSerializer<T>
{
    /// <summary>Serializes <paramref name="value"/> to bytes.</summary>
    byte[] Serialize(T value);

    /// <summary>Deserializes a value from <paramref name="data"/>.</summary>
    T Deserialize(ReadOnlySpan<byte> data);
}

/// <summary>Identity serializer for raw binary values.</summary>
public sealed class ByteArraySerializer : IValueSerializer<byte[]>
{
    /// <summary>Shared instance.</summary>
    public static readonly ByteArraySerializer Instance = new();

    /// <inheritdoc/>
    public byte[] Serialize(byte[] value) => value;

    /// <inheritdoc/>
    public byte[] Deserialize(ReadOnlySpan<byte> data) => data.ToArray();
}
