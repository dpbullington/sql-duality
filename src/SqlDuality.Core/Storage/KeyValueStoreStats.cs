namespace SqlDuality.Core.Storage;

/// <summary>
/// A point-in-time snapshot of a <see cref="KeyValueStore"/>'s operational counters. Cheap to read;
/// intended as a seed for metrics/observability exporters.
/// </summary>
/// <param name="Commits">Transactions that produced a durable log record.</param>
/// <param name="EmptyCommits">Commits with no mutations (no record written).</param>
/// <param name="Conflicts">Commits rejected by write-write conflict detection.</param>
/// <param name="Checkpoints">Checkpoints written.</param>
/// <param name="VersionsReclaimed">Old MVCC versions dropped by garbage collection.</param>
/// <param name="KeysReclaimed">Keys removed entirely (GC collapsed them to a single tombstone).</param>
public readonly record struct KeyValueStoreStats(
    long Commits,
    long EmptyCommits,
    long Conflicts,
    long Checkpoints,
    long VersionsReclaimed,
    long KeysReclaimed);
