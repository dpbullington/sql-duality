using System.Collections.Concurrent;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;
using SqlDuality.Core.Transactions;
using SqlDuality.StreamProcessing;

namespace SqlDuality;

/// <summary>Options for opening a <see cref="Database"/>.</summary>
public sealed record DatabaseOptions
{
    /// <summary>Durable-log tunables. If omitted, defaults are used.</summary>
    public LogOptions? Log { get; init; }
}

/// <summary>
/// The embedded database: a thin façade over the durable log, the materialized key-value state, and
/// the stream-processing engine. It exposes collection-style data access, ACID transactions,
/// resumable change-data-capture, and the ability to run custom stream processors — all over the
/// same log-as-source-of-truth core.
/// </summary>
public sealed class Database : IAsyncDisposable
{
    private readonly KeyValueStore _store;
    private readonly ConcurrentDictionary<string, BinaryCollection> _collections = new(StringComparer.Ordinal);

    private Database(KeyValueStore store)
    {
        _store = store;
        Changes = new Changes(store);
    }

    /// <summary>The change-data-capture surface.</summary>
    public Changes Changes { get; }

    /// <summary>The highest committed LSN currently visible.</summary>
    public Lsn CurrentLsn => _store.CurrentLsn;

    /// <summary>Opens (creating/recovering as needed) the database rooted at <paramref name="path"/>.</summary>
    public static async Task<Database> OpenAsync(
        string path, DatabaseOptions? options = null, CancellationToken cancellationToken = default)
    {
        KeyValueStore store = await KeyValueStore
            .OpenAsync(path, options?.Log, cancellationToken)
            .ConfigureAwait(false);
        return new Database(store);
    }

    /// <summary>Gets (or lazily creates) a binary key-value collection named <paramref name="name"/>.</summary>
    public BinaryCollection GetBinaryCollection(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return _collections.GetOrAdd(name, n => new BinaryCollection(_store, n));
    }

    /// <summary>Gets (or lazily creates) a strongly-typed collection named <paramref name="name"/>.</summary>
    public Collection<T> GetCollection<T>(string name, IValueSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return new Collection<T>(GetBinaryCollection(name), serializer);
    }

    /// <summary>Begins a snapshot-isolated transaction spanning any collections.</summary>
    public Transaction BeginTransaction() => Transaction.Begin(_store);

    /// <summary>
    /// Runs a custom processor over the change stream (the primary way to submit user-authored
    /// stream components). In <see cref="ExecutionMode.Continuous"/> the result keeps producing as
    /// new changes commit, until cancelled.
    /// </summary>
    public IAsyncEnumerable<TOut> RunProcessor<TOut>(
        IStreamOperator<ChangeEvent, TOut> processor,
        Lsn from = default,
        ExecutionMode mode = ExecutionMode.OneShot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(processor);
        IAsyncEnumerable<ChangeEvent> source = Changes.WatchAsync(
            from, follow: mode == ExecutionMode.Continuous, cancellationToken);
        return StreamExecutionEngine.Run(source, processor, cancellationToken);
    }

    /// <summary>Runs a custom processor over the raw durable log (records, not decoded changes).</summary>
    public IAsyncEnumerable<TOut> RunLogProcessor<TOut>(
        IStreamOperator<LogRecord, TOut> processor,
        Lsn from = default,
        ExecutionMode mode = ExecutionMode.OneShot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(processor);
        Lsn start = from.IsValid ? from : Lsn.First;
        IAsyncEnumerable<LogRecord> source = LogStream.Read(_store.Log, start, mode, cancellationToken);
        return StreamExecutionEngine.Run(source, processor, cancellationToken);
    }

    /// <summary>
    /// Submits a SQL statement. The SQL engine is a later-phase deliverable; this currently throws
    /// <see cref="NotSupportedException"/> so the surface exists and is discoverable.
    /// </summary>
    public Task<IAsyncEnumerable<object>> QueryAsync(string sql, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            "The SQL query engine is not implemented in Phase 1. Use RunProcessor for custom stream " +
            "processors, or the collection/transaction APIs for direct access.");

    /// <summary>Writes a checkpoint and truncates the log below it.</summary>
    public Task CheckpointAsync(CancellationToken cancellationToken = default) =>
        _store.CheckpointAsync(cancellationToken);

    /// <summary>Flushes and closes the database.</summary>
    public ValueTask DisposeAsync() => _store.DisposeAsync();
}
