using System.Runtime.CompilerServices;

namespace SqlDuality.StreamProcessing;

/// <summary>
/// Built-in building-block operators and composition helpers. These demonstrate the operator model
/// and are enough to express simple projections, filters, and side effects; SQL operators and user
/// processors implement the same <see cref="IStreamOperator{TIn,TOut}"/> contract.
/// </summary>
public static class StreamOperators
{
    /// <summary>Creates an operator from a per-element transform.</summary>
    public static IStreamOperator<TIn, TOut> Map<TIn, TOut>(Func<TIn, TOut> selector) =>
        new DelegateOperator<TIn, TOut>((source, ct) => MapImpl(source, selector, ct));

    /// <summary>Creates an operator that forwards only elements matching <paramref name="predicate"/>.</summary>
    public static IStreamOperator<T, T> Filter<T>(Func<T, bool> predicate) =>
        new DelegateOperator<T, T>((source, ct) => FilterImpl(source, predicate, ct));

    /// <summary>Creates an operator from an arbitrary stream transform function.</summary>
    public static IStreamOperator<TIn, TOut> Create<TIn, TOut>(
        Func<IAsyncEnumerable<TIn>, CancellationToken, IAsyncEnumerable<TOut>> transform) =>
        new DelegateOperator<TIn, TOut>(transform);

    /// <summary>Composes two operators so the output of <paramref name="first"/> feeds <paramref name="second"/>.</summary>
    public static IStreamOperator<TIn, TOut> Then<TIn, TMid, TOut>(
        this IStreamOperator<TIn, TMid> first, IStreamOperator<TMid, TOut> second) =>
        new DelegateOperator<TIn, TOut>((source, ct) =>
            second.ProcessAsync(first.ProcessAsync(source, ct), ct));

    private static async IAsyncEnumerable<TOut> MapImpl<TIn, TOut>(
        IAsyncEnumerable<TIn> source, Func<TIn, TOut> selector,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (TIn item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            yield return selector(item);
        }
    }

    private static async IAsyncEnumerable<T> FilterImpl<T>(
        IAsyncEnumerable<T> source, Func<T, bool> predicate,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (T item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            if (predicate(item))
            {
                yield return item;
            }
        }
    }

    private sealed class DelegateOperator<TIn, TOut> : IStreamOperator<TIn, TOut>
    {
        private readonly Func<IAsyncEnumerable<TIn>, CancellationToken, IAsyncEnumerable<TOut>> _transform;

        public DelegateOperator(Func<IAsyncEnumerable<TIn>, CancellationToken, IAsyncEnumerable<TOut>> transform) =>
            _transform = transform;

        public IAsyncEnumerable<TOut> ProcessAsync(IAsyncEnumerable<TIn> source, CancellationToken cancellationToken) =>
            _transform(source, cancellationToken);
    }
}
