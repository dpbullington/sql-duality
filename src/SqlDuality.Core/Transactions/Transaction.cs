using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;

namespace SqlDuality.Core.Transactions;

/// <summary>
/// A snapshot-isolated unit of work. Reads observe a stable snapshot taken at <see cref="Begin"/>;
/// writes are buffered and applied atomically at <see cref="CommitAsync"/> (as a single log record).
/// </summary>
/// <remarks>
/// <para>Reads see the transaction's own uncommitted writes (read-your-writes). A transaction that
/// only reads never produces a log record.</para>
/// <para>Not thread-safe: a single transaction is intended to be used by one logical flow at a time.</para>
/// </remarks>
public sealed class Transaction : IDisposable
{
    private readonly KeyValueStore _store;
    private readonly List<Mutation> _buffer = new();
    private readonly Dictionary<byte[], int> _writeIndex = new(ByteKeyComparer.Instance); // key -> latest buffer slot
    private bool _completed;
    private bool _snapshotReleased;

    internal Transaction(KeyValueStore store, Lsn readSnapshot)
    {
        _store = store;
        ReadSnapshot = readSnapshot;
    }

    /// <summary>The snapshot LSN this transaction reads at.</summary>
    public Lsn ReadSnapshot { get; }

    /// <summary>True once the transaction has committed or aborted.</summary>
    public bool IsCompleted => _completed;

    /// <summary>Begins a transaction reading at the store's current snapshot.</summary>
    public static Transaction Begin(KeyValueStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        Lsn snapshot = store.Snapshot();
        // Register the snapshot so garbage collection retains versions this transaction may still read.
        store.RegisterSnapshot(snapshot);
        return new Transaction(store, snapshot);
    }

    /// <summary>Reads a key, honoring this transaction's buffered writes then its read snapshot.</summary>
    public bool TryGet(ReadOnlyMemory<byte> key, out byte[] value)
    {
        EnsureActive();
        if (_writeIndex.TryGetValue(key.ToArray(), out int slot))
        {
            Mutation m = _buffer[slot];
            if (m.Kind == MutationKind.Delete)
            {
                value = Array.Empty<byte>();
                return false;
            }
            value = m.Value.ToArray();
            return true;
        }
        return _store.TryGet(key, ReadSnapshot, out value);
    }

    /// <summary>Buffers a put.</summary>
    public void Put(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        EnsureActive();
        Stage(Mutation.Put(key, value));
    }

    /// <summary>Buffers a delete.</summary>
    public void Delete(ReadOnlyMemory<byte> key)
    {
        EnsureActive();
        Stage(Mutation.Delete(key));
    }

    private void Stage(Mutation mutation)
    {
        byte[] key = mutation.Key.ToArray();
        if (_writeIndex.TryGetValue(key, out int slot))
        {
            _buffer[slot] = mutation; // collapse repeated writes to the same key
        }
        else
        {
            _writeIndex[key] = _buffer.Count;
            _buffer.Add(mutation);
        }
    }

    /// <summary>
    /// Commits buffered writes atomically. Throws <see cref="WriteConflictException"/> if a mutated
    /// key changed after <see cref="ReadSnapshot"/>. Returns the commit LSN (or the current snapshot
    /// for a read-only transaction).
    /// </summary>
    public async Task<Lsn> CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureActive();
        _completed = true;
        try
        {
            return await _store.CommitAsync(_buffer, ReadSnapshot, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseSnapshot();
        }
    }

    /// <summary>Abandons buffered writes.</summary>
    public void Abort()
    {
        if (_completed)
        {
            return;
        }
        _completed = true;
        _buffer.Clear();
        _writeIndex.Clear();
        ReleaseSnapshot();
    }

    private void ReleaseSnapshot()
    {
        if (!_snapshotReleased)
        {
            _snapshotReleased = true;
            _store.ReleaseSnapshot(ReadSnapshot);
        }
    }

    private void EnsureActive()
    {
        if (_completed)
        {
            throw new InvalidOperationException("The transaction has already completed.");
        }
    }

    /// <summary>Aborts the transaction if it has not already completed.</summary>
    public void Dispose()
    {
        if (!_completed)
        {
            Abort();
        }
    }
}
