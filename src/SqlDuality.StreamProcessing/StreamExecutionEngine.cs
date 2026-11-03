using System.Runtime.CompilerServices;

namespace SqlDuality.StreamProcessing;

/// <summary>
/// Drives an <see cref="IStreamOperator{TIn,TOut}"/> against a source stream. The engine is
/// deliberately thin: it wires a source to an operator and manages one-shot vs. continuous
/// consumption. The same engine runs SQL plan operators and user-authored processors.
/// </summary>
public static class StreamExecutionEngine
{
    /// <summary>Runs <paramref name="op"/> over <paramref name="source"/>, yielding results lazily.</summary>
    public static IAsyncEnumerable<TOut> Run<TIn, TOut>(
        IAsyncEnumerable<TIn> source,
        IStreamOperator<TIn, TOut> op,
        CancellationToken cancellationToken = default) =>
        op.ProcessAsync(source, cancellationToken);

    /// <summary>Runs <paramref name="op"/> to completion and collects its output into a list (one-shot).</summary>
    public static async Task<List<TOut>> CollectAsync<TIn, TOut>(
        IAsyncEnumerable<TIn> source,
        IStreamOperator<TIn, TOut> op,
        CancellationToken cancellationToken = default)
    {
        var results = new List<TOut>();
        await foreach (TOut item in op.ProcessAsync(source, cancellationToken)
            .WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            results.Add(item);
        }
        return results;
    }

    /// <summary>
    /// Runs <paramref name="op"/> and invokes <paramref name="onEach"/> for every output element.
    /// Intended for continuous processors (subscriptions); returns when the source completes or is cancelled.
    /// </summary>
    public static async Task ForEachAsync<TIn, TOut>(
        IAsyncEnumerable<TIn> source,
        IStreamOperator<TIn, TOut> op,
        Func<TOut, CancellationToken, ValueTask> onEach,
        CancellationToken cancellationToken = default)
    {
        await foreach (TOut item in op.ProcessAsync(source, cancellationToken)
            .WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await onEach(item, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Wraps an enumerable as an async source (useful for running operators over in-memory data).</summary>
    public static async IAsyncEnumerable<T> AsAsync<T>(
        IEnumerable<T> source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (T item in source)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
            await Task.Yield();
        }
    }
}
