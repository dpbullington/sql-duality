using System.Text;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Transactions;

namespace SqlDuality;

/// <summary>
/// A strongly-typed document collection with string keys. Values are (de)serialized via the
/// supplied <see cref="IValueSerializer{T}"/>, keeping the type AOT-friendly and letting callers
/// plug in a source-generated serializer.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class Collection<T>
{
    private readonly BinaryCollection _inner;
    private readonly IValueSerializer<T> _serializer;

    internal Collection(BinaryCollection inner, IValueSerializer<T> serializer)
    {
        _inner = inner;
        _serializer = serializer;
    }

    /// <summary>The collection's name.</summary>
    public string Name => _inner.Name;

    /// <summary>The underlying binary collection, for advanced key/value access.</summary>
    public BinaryCollection Binary => _inner;

    private static byte[] KeyBytes(string id) => Encoding.UTF8.GetBytes(id);

    /// <summary>Reads the document with id <paramref name="id"/>, or <c>default</c> if absent.</summary>
    public async ValueTask<T?> GetAsync(string id, CancellationToken cancellationToken = default)
    {
        byte[]? data = await _inner.GetAsync(KeyBytes(id), cancellationToken).ConfigureAwait(false);
        return data is null ? default : _serializer.Deserialize(data);
    }

    /// <summary>Inserts or updates the document with id <paramref name="id"/>.</summary>
    public Task<Lsn> PutAsync(string id, T value, CancellationToken cancellationToken = default) =>
        _inner.PutAsync(KeyBytes(id), _serializer.Serialize(value), cancellationToken);

    /// <summary>Deletes the document with id <paramref name="id"/>.</summary>
    public Task<Lsn> DeleteAsync(string id, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(KeyBytes(id), cancellationToken);

    // ----- Transaction-scoped operations -----

    /// <summary>Reads the document with id <paramref name="id"/> within <paramref name="transaction"/>.</summary>
    public T? Get(Transaction transaction, string id)
    {
        byte[]? data = _inner.Get(transaction, KeyBytes(id));
        return data is null ? default : _serializer.Deserialize(data);
    }

    /// <summary>Stages a put within <paramref name="transaction"/>.</summary>
    public void Put(Transaction transaction, string id, T value) =>
        _inner.Put(transaction, KeyBytes(id), _serializer.Serialize(value));

    /// <summary>Stages a delete within <paramref name="transaction"/>.</summary>
    public void Delete(Transaction transaction, string id) =>
        _inner.Delete(transaction, KeyBytes(id));
}
