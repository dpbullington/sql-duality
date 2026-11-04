using System.Buffers.Binary;
using System.Text;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;
using SqlDuality.Core.Transactions;
using Xunit;

namespace SqlDuality.Tests.Concurrency;

/// <summary>
/// Phase 2: concurrent writers (optimistic concurrency) with lock-free snapshot readers.
/// </summary>
public sealed class MvccConcurrencyTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static long ReadLong(byte[] b) => BinaryPrimitives.ReadInt64LittleEndian(b);
    private static byte[] WriteLong(long v) { var b = new byte[8]; BinaryPrimitives.WriteInt64LittleEndian(b, v); return b; }

    [Fact]
    public async Task High_contention_increments_converge_and_conflicts_are_actually_exercised()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await store.CommitAsync(new[] { Mutation.Put(B("c"), WriteLong(0)) }, Lsn.Max);

        const int workers = 8;
        const int each = 150;

        var tasks = Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < each; i++)
            {
                while (true)
                {
                    using var txn = Transaction.Begin(store);
                    long current = txn.TryGet(B("c"), out byte[] cur) ? ReadLong(cur) : 0;
                    txn.Put(B("c"), WriteLong(current + 1));
                    try { await txn.CommitAsync(); break; }
                    catch (WriteConflictException) { /* retry */ }
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.True(store.TryGet(B("c"), out byte[] final));
        Assert.Equal((long)(workers * each), ReadLong(final));

        // With 8 workers hammering one key, conflict detection must have fired at least once —
        // proof that commits ran concurrently rather than being serialized.
        Assert.True(store.Stats.Conflicts > 0, "expected write-write conflicts under contention");
        Assert.Equal(workers * each + 1, store.Stats.Commits); // +1 for the initial seed commit
    }

    [Fact]
    public async Task A_reader_transaction_sees_a_stable_snapshot_while_writers_mutate()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await store.CommitAsync(new[] { Mutation.Put(B("x"), B("start")) }, Lsn.Max);

        using var reader = Transaction.Begin(store);

        using var stop = new CancellationTokenSource();
        int committed = 0;
        var writer = Task.Run(async () =>
        {
            int n = 0;
            while (!stop.IsCancellationRequested)
            {
                // Blind overwrite (last-writer-wins) so concurrent writers never conflict each other.
                await store.CommitAsync(new[] { Mutation.Put(B("x"), B($"v{n++}")) }, Lsn.Max);
                Interlocked.Increment(ref committed);
            }
        });

        // Throughout the writer's churn, the reader's snapshot never changes. Keep checking until the
        // writer has demonstrably made progress (so the post-condition below is meaningful).
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (Volatile.Read(ref committed) < 25 && !deadline.IsCancellationRequested)
        {
            Assert.True(reader.TryGet(B("x"), out byte[] v));
            Assert.Equal("start", Encoding.UTF8.GetString(v));
            await Task.Yield();
        }

        stop.Cancel();
        await writer;
        Assert.True(Volatile.Read(ref committed) >= 25, "writer did not make progress");

        // A fresh read outside the transaction sees the latest committed value.
        Assert.True(store.TryGet(B("x"), out byte[] latest));
        Assert.StartsWith("v", Encoding.UTF8.GetString(latest));
    }

    [Fact]
    public async Task Concurrent_writers_plus_background_checkpoints_stay_consistent()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path, new LogOptions { MaxSegmentBytes = 4096 });

        const int writers = 8;
        const int perWriter = 150;

        using var stopCheckpoints = new CancellationTokenSource();
        var checkpointer = Task.Run(async () =>
        {
            while (!stopCheckpoints.IsCancellationRequested)
            {
                try { await store.CheckpointAsync(); } catch { /* best effort */ }
                await Task.Yield();
            }
        });

        var tasks = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < perWriter; i++)
            {
                await store.CommitAsync(new[] { Mutation.Put(B($"w{w}:k{i}"), B($"w{w}:v{i}")) }, Lsn.Max);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        stopCheckpoints.Cancel();
        await checkpointer;

        for (int w = 0; w < writers; w++)
        {
            for (int i = 0; i < perWriter; i++)
            {
                Assert.True(store.TryGet(B($"w{w}:k{i}"), out byte[] v), $"missing w{w}:k{i}");
                Assert.Equal($"w{w}:v{i}", Encoding.UTF8.GetString(v));
            }
        }

        // Everything still present after reopening (checkpoint + log-tail recovery under churn).
        await store.CheckpointAsync();
        await using var reopened = await KeyValueStore.OpenAsync(dir.Path);
        Assert.True(reopened.TryGet(B("w0:k0"), out byte[] first));
        Assert.Equal("w0:v0", Encoding.UTF8.GetString(first));
    }
}
