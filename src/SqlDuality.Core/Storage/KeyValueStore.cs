using System.Collections.Concurrent;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Transactions;

namespace SqlDuality.Core.Storage;

/// <summary>
/// A multi-version key-value store whose durable source of truth is a <see cref="DurableLog"/>.
/// The in-memory index is a materialized projection of the log, maintained in strict LSN order by
/// the log's own writer loop and rebuilt on open from the latest checkpoint plus the log tail.
/// </summary>
/// <remarks>
/// <para><b>Reads</b> are lock-free: a read supplies a snapshot LSN and walks an immutable per-key
/// version chain to the newest version at or below it. Readers never block and never take a lock.</para>
///
/// <para><b>Writes are concurrent (Phase 2).</b> A commit holds a short, I/O-free "oracle" critical
/// section only long enough to validate the write set against a conflict registry, enqueue the log
/// append, and claim its commit LSN. The <c>fsync</c> happens outside that section, so many
/// concurrent commits coalesce into a single group-commit flush. The memtable is updated by the log
/// writer as each record becomes durable (see <see cref="ApplyDurableRecord"/>), which keeps version
/// chains ordered without any per-key apply lock.</para>
///
/// <para><b>Isolation</b> is snapshot isolation with first-committer-wins conflict detection: a
/// commit is rejected (<see cref="WriteConflictException"/>) if any mutated key changed after the
/// transaction's read snapshot.</para>
///
/// <para><b>Memory</b> is bounded at checkpoint time: versions older than the oldest in-flight
/// snapshot are garbage-collected and the conflict registry is pruned to the same watermark.</para>
/// </remarks>
public sealed class KeyValueStore : IAsyncDisposable
{
    /// <summary>Default file name of the checkpoint within the database directory.</summary>
    public const string CheckpointFileName = "checkpoint.sqld";

    private DurableLog _log = null!; // assigned immediately after construction (see OpenAsync)
    private readonly string _checkpointPath;
    private readonly ConcurrentDictionary<byte[], ValueVersion> _index = new(ByteKeyComparer.Instance);
    private readonly SnapshotTracker _snapshots = new();

    // Oracle: guards conflict validation, commit-LSN assignment, and the conflict registry. Held
    // only for in-memory work (no I/O), so it is not the throughput bottleneck the Phase 1 gate was.
    private readonly object _oracleLock = new();
    private readonly Dictionary<byte[], ulong> _lastCommit = new(ByteKeyComparer.Instance);
    private ulong _commitCounter; // next commit LSN, kept in lock-step with the log's LSN assignment

    private long _currentLsn; // highest applied (materialized) commit LSN; read via Volatile

    // Observability counters.
    private long _commits, _emptyCommits, _conflicts, _checkpoints, _versionsReclaimed, _keysReclaimed;

    private KeyValueStore(string checkpointPath) => _checkpointPath = checkpointPath;

    /// <summary>The underlying durable log (exposed for CDC subscriptions and stream processing).</summary>
    public DurableLog Log => _log;

    /// <summary>The highest commit LSN currently materialized — the default read snapshot.</summary>
    public Lsn CurrentLsn => new((ulong)Volatile.Read(ref _currentLsn));

    /// <summary>Captures the current snapshot LSN for a consistent sequence of reads.</summary>
    public Lsn Snapshot() => CurrentLsn;

    /// <summary>A point-in-time snapshot of operational counters.</summary>
    public KeyValueStoreStats Stats => new(
        Interlocked.Read(ref _commits),
        Interlocked.Read(ref _emptyCommits),
        Interlocked.Read(ref _conflicts),
        Interlocked.Read(ref _checkpoints),
        Interlocked.Read(ref _versionsReclaimed),
        Interlocked.Read(ref _keysReclaimed));

    /// <summary>Opens (and recovers) a key-value store rooted at <paramref name="directory"/>.</summary>
    public static async Task<KeyValueStore> OpenAsync(
        string directory, LogOptions? logOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        Directory.CreateDirectory(directory);

        string logDir = Path.Combine(directory, "log");
        string checkpointPath = Path.Combine(directory, CheckpointFileName);

        // Construct first so the log can project durable records straight into this store's index.
        // The callback is never invoked during the log's own recovery (that only scans files), so it
        // is safe to wire before the store has fully recovered its state below.
        var store = new KeyValueStore(checkpointPath);
        store._log = await DurableLog
            .OpenAsync(logDir, logOptions, store.ApplyDurableRecord, cancellationToken)
            .ConfigureAwait(false);

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

    // ----- Reads -----

    /// <summary>Reads the value of <paramref name="key"/> as visible at <paramref name="snapshot"/>.</summary>
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
    /// Diagnostic: the number of retained MVCC versions for <paramref name="key"/> (0 if absent).
    /// Useful for observing garbage collection; not a hot-path API.
    /// </summary>
    public int GetVersionChainLength(ReadOnlyMemory<byte> key)
    {
        if (!_index.TryGetValue(key.ToArray(), out ValueVersion? node))
        {
            return 0;
        }
        int count = 0;
        for (ValueVersion? v = node; v is not null; v = v.Previous)
        {
            count++;
        }
        return count;
    }

    // ----- Commit (concurrent OCC) -----

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
            Interlocked.Increment(ref _emptyCommits);
            return CurrentLsn; // nothing to do; no log record produced
        }

        // Serialize outside the lock; the oracle section does only validation + enqueue.
        byte[] payload = MutationBatchCodec.Serialize(mutations);

        Task<Lsn> durable;
        Lsn commitLsn;
        lock (_oracleLock)
        {
            // 1. Conflict check against the write set.
            foreach (Mutation m in mutations)
            {
                byte[] keyArr = m.Key.ToArray();
                if (_lastCommit.TryGetValue(keyArr, out ulong last) && last > readSnapshot.Value)
                {
                    Interlocked.Increment(ref _conflicts);
                    throw new WriteConflictException(
                        $"Key was modified at LSN {last} after snapshot {readSnapshot}.", keyArr);
                }
            }

            // 2. Enqueue the durable append (no I/O here) and claim the matching commit LSN. Because
            //    the enqueue happens under this lock, the LSN the log assigns equals ++_commitCounter.
            durable = _log.EnqueueAppendOwned(LogRecordType.Transaction, payload);
            commitLsn = new Lsn(++_commitCounter);

            // 3. Record the write set so concurrent/later commits can detect conflicts against it.
            foreach (Mutation m in mutations)
            {
                _lastCommit[m.Key.ToArray()] = commitLsn.Value;
            }
        }

        // 4. Await durability + projection outside the lock (this is where the fsync batches).
        Lsn assigned = await durable.ConfigureAwait(false);
        if (assigned != commitLsn)
        {
            throw new InvalidOperationException(
                $"Commit LSN desync: predicted {commitLsn}, log assigned {assigned}. " +
                "All log appends must flow through the commit path.");
        }

        Interlocked.Increment(ref _commits);
        return commitLsn;
    }

    // ----- Projection (materialized view maintained by the log writer) -----

    /// <summary>
    /// Applies a durable record to the in-memory index. Invoked by the log's writer loop in strict
    /// LSN order, so version chains are built newest-on-top without any per-key apply lock.
    /// </summary>
    private void ApplyDurableRecord(LogRecord record)
    {
        if (record.Type != LogRecordType.Transaction)
        {
            return;
        }

        foreach (Mutation m in MutationBatchCodec.Read(record.Payload.Span))
        {
            byte[] key = m.Key.ToArray();
            byte[]? value = m.Kind == MutationKind.Delete ? null : m.Value.ToArray();
            _index.AddOrUpdate(
                key,
                _ => new ValueVersion(record.Lsn, value, previous: null),
                (_, existing) => new ValueVersion(record.Lsn, value, existing));
        }

        Volatile.Write(ref _currentLsn, (long)record.Lsn.Value);
    }

    // ----- Checkpoint + garbage collection -----

    /// <summary>
    /// Writes a checkpoint capturing all live state as of the current LSN, truncates log segments
    /// below it, then garbage-collects versions and conflict-registry entries older than the oldest
    /// in-flight snapshot. Safe to run concurrently with reads and commits.
    /// </summary>
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        Lsn checkpointLsn = CurrentLsn;
        Lsn watermark;

        lock (_oracleLock)
        {
            watermark = _snapshots.Oldest(checkpointLsn);
            PruneRegistry(watermark);
        }

        // Gather live entries lock-free: versions at/below checkpointLsn are immutable and present.
        var entries = new List<CheckpointEntry>(_index.Count);
        foreach (KeyValuePair<byte[], ValueVersion> kv in _index)
        {
            ValueVersion? v = kv.Value.VisibleAt(checkpointLsn);
            if (v is { IsTombstone: false })
            {
                entries.Add(new CheckpointEntry(kv.Key, v.CommitLsn, v.Value!));
            }
        }
        entries.Sort(static (a, b) => ByteKeyComparer.Instance.Compare(a.Key, b.Key));

        await CheckpointFile.WriteAsync(_checkpointPath, checkpointLsn, entries, cancellationToken)
            .ConfigureAwait(false);

        _log.TruncateBefore(checkpointLsn);
        CollectGarbage(watermark);
        Interlocked.Increment(ref _checkpoints);
    }

    private void PruneRegistry(Lsn watermark)
    {
        // Entries at/below the watermark can never cause a conflict: every current and future
        // snapshot is at least the watermark, so only commits strictly newer than it matter.
        if (_lastCommit.Count == 0)
        {
            return;
        }
        var stale = new List<byte[]>();
        foreach (KeyValuePair<byte[], ulong> kv in _lastCommit)
        {
            if (kv.Value <= watermark.Value)
            {
                stale.Add(kv.Key);
            }
        }
        foreach (byte[] key in stale)
        {
            _lastCommit.Remove(key);
        }
    }

    private void CollectGarbage(Lsn watermark)
    {
        foreach (KeyValuePair<byte[], ValueVersion> kv in _index)
        {
            ValueVersion head = kv.Value;
            (ValueVersion? pruned, int dropped, bool removable) = Prune(head, watermark);
            if (dropped == 0)
            {
                continue;
            }

            // CAS so a concurrent commit that prepended a newer version is never clobbered.
            bool applied;
            if (removable)
            {
                applied = _index.TryRemove(new KeyValuePair<byte[], ValueVersion>(kv.Key, head));
                if (applied)
                {
                    Interlocked.Increment(ref _keysReclaimed);
                }
            }
            else
            {
                applied = _index.TryUpdate(kv.Key, pruned!, head);
            }

            if (applied)
            {
                Interlocked.Add(ref _versionsReclaimed, dropped);
            }
        }
    }

    /// <summary>
    /// Produces a pruned copy of a version chain that keeps every version newer than
    /// <paramref name="watermark"/> plus the single newest version at or below it (what a snapshot at
    /// the watermark would read). Older versions are dropped.
    /// </summary>
    private static (ValueVersion? Pruned, int Dropped, bool Removable) Prune(ValueVersion head, Lsn watermark)
    {
        // Collect the versions to keep, top (newest) down to and including the first <= watermark.
        var keep = new List<ValueVersion>();
        ValueVersion? node = head;
        ValueVersion? boundary = null;
        while (node is not null)
        {
            keep.Add(node);
            if (node.CommitLsn <= watermark)
            {
                boundary = node;
                node = node.Previous; // everything below the boundary is reclaimable
                break;
            }
            node = node.Previous;
        }

        // Count how many nodes hang below the boundary (those are what we drop).
        int dropped = 0;
        for (ValueVersion? below = node; below is not null; below = below.Previous)
        {
            dropped++;
        }

        if (dropped == 0)
        {
            return (head, 0, false); // nothing below the boundary; chain already minimal
        }

        // If all we would keep is a single tombstone, the key is observably absent — remove it.
        if (boundary is not null && keep.Count == 1 && boundary.IsTombstone)
        {
            return (null, dropped, true);
        }

        // Rebuild the kept prefix as a fresh immutable chain terminating at the boundary.
        ValueVersion? rebuilt = null;
        for (int i = keep.Count - 1; i >= 0; i--)
        {
            rebuilt = new ValueVersion(keep[i].CommitLsn, keep[i].Value, rebuilt);
        }
        return (rebuilt, dropped, false);
    }

    // ----- Snapshot tracking (for GC watermark) -----

    internal void RegisterSnapshot(Lsn snapshot) => _snapshots.Acquire(snapshot);

    internal void ReleaseSnapshot(Lsn snapshot) => _snapshots.Release(snapshot);

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

        Lsn firstAvailable = _log.FirstAvailableLsn;
        if (firstAvailable > replayFrom)
        {
            replayFrom = firstAvailable;
        }

        // Replay the log tail through the same projection path used for live appends.
        await foreach (LogRecord record in _log.ReadFromAsync(replayFrom, follow: false, cancellationToken)
            .ConfigureAwait(false))
        {
            ApplyDurableRecord(record);
        }

        // The commit counter must continue exactly where the durable log left off.
        _commitCounter = (ulong)Volatile.Read(ref _currentLsn);
    }

    /// <summary>Flushes and closes the store and its log.</summary>
    public async ValueTask DisposeAsync() => await _log.DisposeAsync().ConfigureAwait(false);
}
