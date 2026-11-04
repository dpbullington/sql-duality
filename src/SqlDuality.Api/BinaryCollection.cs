using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;
using SqlDuality.Core.Transactions;

namespace SqlDuality;

/// <summary>
/// A key-value collection over raw binary keys and values. Keys are namespaced into the shared store
/// by the collection's name, so collections are isolated from one another.
/// </summary>
/// <remarks>
/// The async methods auto-commit a single-mutation transaction. To group several writes atomically,
/// use <see cref="Database.BeginTransaction"/> and the transaction-scoped overloads.
/// </remarks>
public sealed class BinaryCollection
{
    private readonly KeyValueStore _store;

    /// <summary>The collection's name.</summary>
    public string Name { get; }

    internal BinaryCollection(KeyValueStore store, string name)
    {
        _store = store;
        Name = name;
    }

    // ----- Auto-commit operations -----

    /// <summary>Reads the current value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        byte[] composite = CollectionKey.Encode(Name, key.Span);
        return _store.TryGet(composite, out byte[] value)
            ? new ValueTask<byte[]?>(value)
            : new ValueTask<byte[]?>((byte[]?)null);
    }

    /// <summary>Inserts or updates <paramref name="key"/> and returns the commit LSN.</summary>
    public Task<Lsn> PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        byte[] composite = CollectionKey.Encode(Name, key.Span);
        var mutations = new[] { Mutation.Put(composite, value) };
        // Blind, unconditional write (last-writer-wins): skip conflict detection with Lsn.Max.
        return _store.CommitAsync(mutations, Lsn.Max, cancellationToken);
    }

    /// <summary>Deletes <paramref name="key"/> and returns the commit LSN.</summary>
    public Task<Lsn> DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        byte[] composite = CollectionKey.Encode(Name, key.Span);
        var mutations = new[] { Mutation.Delete(composite) };
        return _store.CommitAsync(mutations, Lsn.Max, cancellationToken);
    }

    // ----- Transaction-scoped operations -----

    /// <summary>Reads <paramref name="key"/> within <paramref name="transaction"/> (honors buffered writes).</summary>
    public byte[]? Get(Transaction transaction, ReadOnlyMemory<byte> key)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        byte[] composite = CollectionKey.Encode(Name, key.Span);
        return transaction.TryGet(composite, out byte[] value) ? value : null;
    }

    /// <summary>Stages a put within <paramref name="transaction"/>.</summary>
    public void Put(Transaction transaction, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        transaction.Put(CollectionKey.Encode(Name, key.Span), value);
    }

    /// <summary>Stages a delete within <paramref name="transaction"/>.</summary>
    public void Delete(Transaction transaction, ReadOnlyMemory<byte> key)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        transaction.Delete(CollectionKey.Encode(Name, key.Span));
    }
}
