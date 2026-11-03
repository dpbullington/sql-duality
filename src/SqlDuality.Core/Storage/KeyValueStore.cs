using System.Collections.Concurrent;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Transactions;

namespace SqlDuality.Core.Storage;

/// <summary>
/// A multi-version key-value store whose durable source of truth is a <see cref="DurableLog"/>.
/// The in-memory index is a materialized projection of the log, rebuilt on open from the latest
/// checkpoint plus the log tail.
/// </summary>
/// <remarks>
/// <para><b>Reads</b> are lock-free and operate against an immutable version chain per key; a read
/// supplies a snapshot LSN and sees the newest version at or below it. Readers never block.</para>
///
/// <para><b>Commits</b> are serialized through a single gate in Phase 1: conflict check → append to
/// the log → apply to the index, all under one lock, so the index's version chains are built in
/// strict LSN order. This trades concurrent-writer throughput for simplicity and obvious
/// correctness; lock-free optimistic commit is a later phase. Readers are unaffected either way.</para>
/// </remarks>
public sealed class KeyValueStore : IAsyncDisposable
{
    /// <summary>Default file name of the checkpoint within the database directory.</summary>
    public const string CheckpointFileName = "checkpoint.sqld";

    private readonly DurableLog _log;
    private readonly string _checkpointPath;
    private readonly ConcurrentDictionary<byte[], ValueVersion> _index =
        new(ByteKeyComparer.Instance);

    private readonly SemaphoreSlim _commitGate = new(1, 1);
    private long _currentLsn; // highest applied commit LSN; read via Volatile

    private KeyValueStore(DurableLog log, string checkpointPath)
    {
        _log = log;
        _checkpointPath = checkpointPath;
    }

    /// <summary>The underlying durable log (exposed for CDC subscriptions and stream processing).</summary>
    public DurableLog Log => _log;

    /// <summary>The highest commit LSN currently visible — the default read snapshot.</summary>
    public Lsn CurrentLsn => new((ulong)Volatile.Read(ref _currentLsn));

    /// <summary>Captures the current snapshot LSN for a consistent sequence of reads.</summary>
    public Lsn Snapshot() => CurrentLsn;

    /// <summary>Opens (and recovers) a key-value store rooted at <paramref name="directory"/>.</summary>
    public static async Task<KeyValueStore> OpenAsync(
        string directory, LogOptions? logOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        Directory.CreateDirectory(directory);

        DurableLog log = await DurableLog.OpenAsync(
            Path.Combine(directory, "log"), logOptions, cancellationToken).ConfigureAwait(false);

        var store = new KeyValueStore(log, Path.Combine(directory, CheckpointFileName));
        try
        {
            await store.RecoverAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await store.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return store;
    }

    /// <summary>Reads the value of <paramref name="key"/> as visible at <paramref name="snapshot"/>.</summary>
    /// <returns><c>true</c> with the value if a non-tombstone version exists at the snapshot.</returns>
    public bool TryGet(ReadOnlyMemory<byte> key, Lsn snapshot, out byte[] value)
    {
        value = Array.Empty<byte>();
        if (!_index.TryGetValue(key.ToArray(), out ValueVersion? head))
        {
            return false;
        }

        ValueVersion? v = head.VisibleAt(snapshot);
        if (v is null || v.IsTombstone)
        {
            return false;
        }

        value = v.Value!;
        return true;
    }

    /// <summary>Reads the value of <paramref name="key"/> at the current snapshot.</summary>
    public bool TryGet(ReadOnlyMemory<byte> key, out byte[] value) => TryGet(key, CurrentLsn, out value);

    /// <summary>
    /// Commits <paramref name="mutations"/> atomically. <paramref name="readSnapshot"/> is the LSN the
    /// transaction observed; the commit is rejected with a <see cref="WriteConflictException"/> if any
    /// mutated key changed after it.
    /// </summary>
    /// <returns>The commit LSN assigned to the batch.</returns>
    public async Task<Lsn> CommitAsync(
        IReadOnlyList<Mutation> mutations, Lsn readSnapshot, CancellationToken cancellationToken = default)
    {
        if (mutations.Count == 0)
        {
            return CurrentLsn; // nothing to do; no log record produced
        }

        await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // 1. Conflict check against the write set.
            foreach (Mutation m in mutations)
            {
                byte[] keyArr = m.Key.ToArray();
                if (_index.TryGetValue(keyArr, out ValueVersion? head) && head.CommitLsn > readSnapshot)
                {
                    throw new WriteConflictException(
                        $"Key was modified at LSN {head.CommitLsn} after snapshot {readSnapshot}.", keyArr);
                }
            }

            // 2. Durably append the batch (one record => atomic commit).
            byte[] payload = MutationBatchCodec.Serialize(mutations);
            Lsn commitLsn = await _log
                .AppendAsync(LogRecordType.Transaction, payload, cancellationToken)
                .ConfigureAwait(false);

            // 3. Apply to the in-memory index in LSN order.
            Apply(mutations, commitLsn);
            Volatile.Write(ref _currentLsn, (long)commitLsn.Value);
            return commitLsn;
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>
    /// Writes a checkpoint capturing all live state as of the current LSN, then truncates log
    /// segments below it. Safe to call concurrently with reads; briefly blocks new commits.
    /// </summary>
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        Lsn checkpointLsn;
        List<CheckpointEntry> entries;

        await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            checkpointLsn = CurrentLsn;
            entries = new List<CheckpointEntry>(_index.Count);
            foreach (KeyValuePair<byte[], ValueVersion> kv in _index)
            {
                ValueVersion? v = kv.Value.VisibleAt(checkpointLsn);
                if (v is { IsTombstone: false })
                {
                    entries.Add(new CheckpointEntry(kv.Key, v.CommitLsn, v.Value!));
                }
            }
        }
        finally
        {
            _commitGate.Release();
        }

        entries.Sort(static (a, b) => ByteKeyComparer.Instance.Compare(a.Key, b.Key));
        await CheckpointFile.WriteAsync(_checkpointPath, checkpointLsn, entries, cancellationToken)
            .ConfigureAwait(false);

        // Everything at or below the checkpoint is now captured on disk; reclaim log segments.
        _log.TruncateBefore(checkpointLsn);
    }

    // ----- Recovery -----

    private async Task RecoverAsync(CancellationToken cancellationToken)
    {
        Lsn replayFrom = Lsn.First;

        if (CheckpointFile.TryRead(_checkpointPath, out Lsn checkpointLsn, out IReadOnlyList<CheckpointEntry> entries))
        {
            foreach (CheckpointEntry e in entries)
            {
                _index[e.Key] = new ValueVersion(e.CommitLsn, e.Value, previous: null);
            }
            Volatile.Write(ref _currentLsn, (long)checkpointLsn.Value);
            replayFrom = checkpointLsn.Next();
        }

        // Replay transaction records above the checkpoint to rebuild the tail of state.
        Lsn firstAvailable = _log.FirstAvailableLsn;
        if (firstAvailable > replayFrom)
        {
            replayFrom = firstAvailable;
        }

        await foreach (LogRecord record in _log.ReadFromAsync(replayFrom, follow: false, cancellationToken)
            .ConfigureAwait(false))
        {
            if (record.Type != LogRecordType.Transaction)
            {
                continue;
            }
            IReadOnlyList<Mutation> mutations = MutationBatchCodec.Read(record.Payload.Span);
            Apply(mutations, record.Lsn);
            Volatile.Write(ref _currentLsn, (long)record.Lsn.Value);
        }
    }

    private void Apply(IReadOnlyList<Mutation> mutations, Lsn commitLsn)
    {
        foreach (Mutation m in mutations)
        {
            byte[] key = m.Key.ToArray();
            _index.AddOrUpdate(
                key,
                _ => new ValueVersion(commitLsn, ValueOf(m), previous: null),
                (_, existing) => new ValueVersion(commitLsn, ValueOf(m), existing));
        }

        static byte[]? ValueOf(Mutation m) => m.Kind == MutationKind.Delete ? null : m.Value.ToArray();
    }

    /// <summary>Flushes and closes the store and its log.</summary>
    public async ValueTask DisposeAsync()
    {
        _commitGate.Dispose();
        await _log.DisposeAsync().ConfigureAwait(false);
    }
}
