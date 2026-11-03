using System.Buffers.Binary;
using System.Text;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;
using SqlDuality.Core.Transactions;
using Xunit;

namespace SqlDuality.Tests.Concurrency;

public sealed class ConcurrentReadWriteTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task Disjoint_writers_all_commit_and_readers_never_see_torn_state()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        const int writers = 12;
        const int perWriter = 100;
        using var done = new CancellationTokenSource();

        // Readers continuously snapshot-read while writes happen; assert monotonic LSN and that
        // any present key has a well-formed value.
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            ulong lastSeen = 0;
            while (!done.IsCancellationRequested)
            {
                Lsn snap = store.Snapshot();
                Assert.True(snap.Value >= lastSeen); // never goes backwards
                lastSeen = snap.Value;

                if (store.TryGet(B("w0:k0"), snap, out byte[] v))
                {
                    Assert.Equal("w0:v0", Encoding.UTF8.GetString(v));
                }
                await Task.Yield();
            }
        })).ToArray();

        var writerTasks = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            for (int i = 0; i < perWriter; i++)
            {
                await store.CommitAsync(
                    new[] { Mutation.Put(B($"w{w}:k{i}"), B($"w{w}:v{i}")) }, store.Snapshot());
            }
        })).ToArray();

        await Task.WhenAll(writerTasks);
        done.Cancel();
        await Task.WhenAll(readers);

        // Every write is present with the correct value.
        for (int w = 0; w < writers; w++)
        {
            for (int i = 0; i < perWriter; i++)
            {
                Assert.True(store.TryGet(B($"w{w}:k{i}"), out byte[] v));
                Assert.Equal($"w{w}:v{i}", Encoding.UTF8.GetString(v));
            }
        }
        Assert.Equal((ulong)(writers * perWriter), store.CurrentLsn.Value);
    }

    [Fact]
    public async Task Contended_counter_stays_correct_with_optimistic_retry()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        static long ReadLong(byte[] b) => BinaryPrimitives.ReadInt64LittleEndian(b);
        static byte[] WriteLong(long v)
        {
            var b = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(b, v);
            return b;
        }

        await store.CommitAsync(new[] { Mutation.Put(B("counter"), WriteLong(0)) }, store.Snapshot());

        const int workers = 8;
        const int incrementsEach = 250;

        var tasks = Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < incrementsEach; i++)
            {
                while (true)
                {
                    using var txn = Transaction.Begin(store);
                    long current = txn.TryGet(B("counter"), out byte[] cur) ? ReadLong(cur) : 0;
                    txn.Put(B("counter"), WriteLong(current + 1));
                    try
                    {
                        await txn.CommitAsync();
                        break;
                    }
                    catch (WriteConflictException)
                    {
                        // Lost the race; retry with a fresh snapshot.
                    }
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.True(store.TryGet(B("counter"), out byte[] final));
        Assert.Equal((long)(workers * incrementsEach), ReadLong(final));
    }
}
