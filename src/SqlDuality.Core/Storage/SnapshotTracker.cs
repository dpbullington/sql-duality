using SqlDuality.Core.Logging;

namespace SqlDuality.Core.Storage;

/// <summary>
/// Tracks the set of read snapshots currently held by in-flight transactions so the store knows the
/// oldest version any active reader might still need. Garbage collection and conflict-registry
/// pruning use this watermark: anything strictly older can be reclaimed, because no current reader —
/// and no future reader, whose snapshot can only be newer — can observe it.
/// </summary>
internal sealed class SnapshotTracker
{
    private readonly object _lock = new();
    private readonly SortedDictionary<ulong, int> _active = new(); // snapshot LSN -> reference count

    /// <summary>Registers a held snapshot.</summary>
    public void Acquire(Lsn snapshot)
    {
        lock (_lock)
        {
            _active.TryGetValue(snapshot.Value, out int count);
            _active[snapshot.Value] = count + 1;
        }
    }

    /// <summary>Releases a previously held snapshot.</summary>
    public void Release(Lsn snapshot)
    {
        lock (_lock)
        {
            if (_active.TryGetValue(snapshot.Value, out int count))
            {
                if (count <= 1)
                {
                    _active.Remove(snapshot.Value);
                }
                else
                {
                    _active[snapshot.Value] = count - 1;
                }
            }
        }
    }

    /// <summary>
    /// The oldest snapshot still held, or <paramref name="fallback"/> if no snapshots are active
    /// (in which case nothing older than <paramref name="fallback"/> is observable).
    /// </summary>
    public Lsn Oldest(Lsn fallback)
    {
        lock (_lock)
        {
            foreach (ulong key in _active.Keys)
            {
                return new Lsn(key); // first key = smallest, SortedDictionary is ordered
            }
            return fallback;
        }
    }
}
