namespace SqlDuality.StreamProcessing;

/// <summary>
/// The fundamental unit of computation in the engine: a transformation from an input stream to an
/// output stream. Both SQL query plans and user-supplied C# processors are expressed as graphs of
/// operators, and both consume the same kind of source — records flowing out of the durable log or
/// a materialized collection.
/// </summary>
/// <typeparam name="TIn">The element type consumed from upstream.</typeparam>
/// <typeparam name="TOut">The element type produced downstream.</typeparam>
/// <remarks>
/// Operators are pull-based and asynchronous: an operator reads from its source at its own pace and
/// yields results lazily. Composition is just function composition — feed one operator's output as
/// another's source (see <c>StreamOperators.Then</c>).
/// </remarks>
public interface IStreamOperator<TIn, TOut>
{
    /// <summary>Transforms <paramref name="source"/> into an output stream.</summary>
    IAsyncEnumerable<TOut> ProcessAsync(IAsyncEnumerable<TIn> source, CancellationToken cancellationToken);
}
