using System.Text;
using SqlDuality.Core.Storage;
using SqlDuality.Core.Transactions;
using Xunit;

namespace SqlDuality.Tests.Storage;

/// <summary>
/// Phase 2: checkpoint-time garbage collection reclaims MVCC versions older than the oldest in-flight
/// snapshot, and preserves everything an active reader can still see.
/// </summary>
public sealed class MvccGarbageCollectionTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static Task Put(KeyValueStore store, string k, string v) =>
        store.CommitAsync(new[] { Mutation.Put(B(k), B(v)) }, store.Snapshot());

    [Fact]
    public async Task Checkpoint_with_no_active_snapshots_collapses_each_chain_to_one_version()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        for (int i = 0; i < 5; i++)
        {
            await Put(store, "k", $"v{i}");
        }
        Assert.Equal(5, store.GetVersionChainLength(B("k")));

        await store.CheckpointAsync();

        Assert.Equal(1, store.GetVersionChainLength(B("k")));
        Assert.Equal(4, store.Stats.VersionsReclaimed);
        Assert.True(store.TryGet(B("k"), out byte[] v));
        Assert.Equal("v4", Encoding.UTF8.GetString(v));
    }

    [Fact]
    public async Task Gc_preserves_versions_an_active_transaction_can_still_read()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await Put(store, "k", "original");

        using (var reader = Transaction.Begin(store)) // holds the oldest snapshot
        {
            for (int i = 0; i < 5; i++)
            {
                await Put(store, "k", $"update{i}");
            }

            // Checkpoint cannot reclaim below the reader's snapshot: its version must survive.
            await store.CheckpointAsync();
            Assert.Equal(0, store.Stats.VersionsReclaimed);

            Assert.True(reader.TryGet(B("k"), out byte[] seen));
            Assert.Equal("original", Encoding.UTF8.GetString(seen)); // still its snapshot's value
        }

        // Once the reader is gone, a checkpoint can reclaim the now-unreachable history.
        await store.CheckpointAsync();
        Assert.True(store.Stats.VersionsReclaimed >= 5);
        Assert.Equal(1, store.GetVersionChainLength(B("k")));
    }

    [Fact]
    public async Task Gc_removes_a_key_that_has_collapsed_to_a_tombstone()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await Put(store, "k", "v");
        await store.CommitAsync(new[] { Mutation.Delete(B("k")) }, store.Snapshot());
        Assert.Equal(2, store.GetVersionChainLength(B("k")));

        await store.CheckpointAsync();

        Assert.Equal(0, store.GetVersionChainLength(B("k"))); // key removed entirely
        Assert.Equal(1, store.Stats.KeysReclaimed);
        Assert.False(store.TryGet(B("k"), out _));
    }

    [Fact]
    public async Task State_survives_gc_and_reopen()
    {
        using var dir = new TempDir();

        await using (var store = await KeyValueStore.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 10; i++)
            {
                await Put(store, $"k{i}", $"v{i}");
                await Put(store, $"k{i}", $"v{i}-final");
            }
            await store.CheckpointAsync(); // GC collapses chains
        }

        await using (var reopened = await KeyValueStore.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 10; i++)
            {
                Assert.True(reopened.TryGet(B($"k{i}"), out byte[] v));
                Assert.Equal($"v{i}-final", Encoding.UTF8.GetString(v));
            }
        }
    }
}
