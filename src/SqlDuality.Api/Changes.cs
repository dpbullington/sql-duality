using System.Runtime.CompilerServices;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;

namespace SqlDuality;

/// <summary>
/// The change-data-capture surface of a <see cref="Database"/>. Subscribers consume an ordered,
/// resumable stream of <see cref="ChangeEvent"/>s decoded straight from the durable log.
/// </summary>
public sealed class Changes
{
    private readonly KeyValueStore _store;

    internal Changes(KeyValueStore store) => _store = store;

    /// <summary>
    /// Watches for changes with commit LSN &gt;= <paramref name="from"/>.
    /// </summary>
    /// <param name="from">
    /// The inclusive starting LSN. Use <see cref="Lsn.None"/>/<see cref="Lsn.First"/> to replay from
    /// the earliest available change, or a previously observed LSN to resume exactly where a consumer
    /// left off.
    /// </param>
    /// <param name="follow">
    /// When <c>true</c> (default), the stream keeps emitting new changes as they are committed until
    /// cancelled. When <c>false</c>, it ends once current changes are drained.
    /// </param>
    /// <param name="cancellationToken">Stops watching.</param>
    public async IAsyncEnumerable<ChangeEvent> WatchAsync(
        Lsn from = default,
        bool follow = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Lsn start = from.IsValid ? from : Lsn.First;

        await foreach (LogRecord record in _store.Log
            .ReadFromAsync(start, follow, cancellationToken)
            .ConfigureAwait(false))
        {
            if (record.Type != LogRecordType.Transaction)
            {
                continue;
            }

            foreach (Mutation m in MutationBatchCodec.Read(record.Payload.Span))
            {
                if (!CollectionKey.TryDecode(m.Key.Span, out string collection, out byte[] userKey))
                {
                    continue; // not a collection-scoped key; skip
                }

                yield return m.Kind == MutationKind.Delete
                    ? new ChangeEvent(record.Lsn, record.TimestampUnixMs, collection, userKey, ChangeKind.Delete, null)
                    : new ChangeEvent(record.Lsn, record.TimestampUnixMs, collection, userKey, ChangeKind.Upsert, m.Value.ToArray());
            }
        }
    }
}
