using SqlDuality.Core.Logging;

namespace SqlDuality.StreamProcessing;

/// <summary>How far a stream source runs.</summary>
public enum ExecutionMode
{
    /// <summary>Read everything currently durable, then complete.</summary>
    OneShot,

    /// <summary>Read to the current tail, then keep following newly appended records until cancelled.</summary>
    Continuous,
}

/// <summary>Adapts a <see cref="DurableLog"/> into the <see cref="IAsyncEnumerable{T}"/> source that operators consume.</summary>
public static class LogStream
{
    /// <summary>
    /// Produces log records with LSN &gt;= <paramref name="from"/>. In
    /// <see cref="ExecutionMode.Continuous"/> the stream follows the tail until cancelled.
    /// </summary>
    public static IAsyncEnumerable<LogRecord> Read(
        DurableLog log,
        Lsn from,
        ExecutionMode mode = ExecutionMode.OneShot,
        CancellationToken cancellationToken = default) =>
        log.ReadFromAsync(from, follow: mode == ExecutionMode.Continuous, cancellationToken);
}
