using System.Text;
using SqlDuality.Core.Logging;
using SqlDuality.Core.Storage;
using SqlDuality.Core.Transactions;
using Xunit;

namespace SqlDuality.Tests.Storage;

public sealed class KeyValueStoreTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string S(byte[] b) => Encoding.UTF8.GetString(b);

    private static async Task<Lsn> PutAsync(KeyValueStore store, string k, string v) =>
        await store.CommitAsync(new[] { Mutation.Put(B(k), B(v)) }, store.Snapshot());

    [Fact]
    public async Task Put_then_get_returns_value()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await PutAsync(store, "a", "apple");
        Assert.True(store.TryGet(B("a"), out byte[] v));
        Assert.Equal("apple", S(v));
        Assert.False(store.TryGet(B("missing"), out _));
    }

    [Fact]
    public async Task Delete_hides_value()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await PutAsync(store, "k", "v");
        await store.CommitAsync(new[] { Mutation.Delete(B("k")) }, store.Snapshot());

        Assert.False(store.TryGet(B("k"), out _));
    }

    [Fact]
    public async Task Snapshot_isolation_hides_later_writes()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await PutAsync(store, "k", "v1");
        Lsn snapshot = store.Snapshot();

        await PutAsync(store, "k", "v2");

        // Reading at the old snapshot still sees v1; current sees v2.
        Assert.True(store.TryGet(B("k"), snapshot, out byte[] oldVal));
        Assert.Equal("v1", S(oldVal));
        Assert.True(store.TryGet(B("k"), out byte[] newVal));
        Assert.Equal("v2", S(newVal));
    }

    [Fact]
    public async Task Write_conflict_is_detected()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        await PutAsync(store, "k", "base");

        using var a = Transaction.Begin(store); // snapshot before B commits
        using var b = Transaction.Begin(store);

        b.Put(B("k"), B("from-b"));
        await b.CommitAsync();

        a.Put(B("k"), B("from-a"));
        WriteConflictException ex = await Assert.ThrowsAsync<WriteConflictException>(() => a.CommitAsync());
        Assert.Equal("k", S(ex.ConflictingKey!));

        // B's write stands.
        Assert.True(store.TryGet(B("k"), out byte[] v));
        Assert.Equal("from-b", S(v));
    }

    [Fact]
    public async Task Transaction_reads_its_own_uncommitted_writes()
    {
        using var dir = new TempDir();
        await using var store = await KeyValueStore.OpenAsync(dir.Path);

        using var txn = Transaction.Begin(store);
        txn.Put(B("k"), B("staged"));

        Assert.True(txn.TryGet(B("k"), out byte[] v));
        Assert.Equal("staged", S(v));

        // Not visible to the store until commit.
        Assert.False(store.TryGet(B("k"), out _));

        await txn.CommitAsync();
        Assert.True(store.TryGet(B("k"), out _));
    }

    [Fact]
    public async Task Checkpoint_then_reopen_recovers_state_and_truncates_log()
    {
        using var dir = new TempDir();

        await using (var store = await KeyValueStore.OpenAsync(dir.Path, new LogOptions { MaxSegmentBytes = 512 }))
        {
            for (int i = 0; i < 100; i++)
            {
                await PutAsync(store, $"k{i}", $"v{i}");
            }
            await store.CheckpointAsync();

            // Writes after the checkpoint exercise the "checkpoint + replay tail" path.
            for (int i = 100; i < 120; i++)
            {
                await PutAsync(store, $"k{i}", $"v{i}");
            }
        }

        string logDir = Path.Combine(dir.Path, "log");
        Assert.True(File.Exists(Path.Combine(dir.Path, KeyValueStore.CheckpointFileName)));

        await using (var reopened = await KeyValueStore.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 120; i++)
            {
                Assert.True(reopened.TryGet(B($"k{i}"), out byte[] v), $"missing k{i}");
                Assert.Equal($"v{i}", S(v));
            }
        }
    }

    [Fact]
    public async Task Recovery_without_checkpoint_replays_entire_log()
    {
        using var dir = new TempDir();

        await using (var store = await KeyValueStore.OpenAsync(dir.Path))
        {
            await PutAsync(store, "x", "1");
            await PutAsync(store, "y", "2");
            await store.CommitAsync(new[] { Mutation.Delete(B("x")) }, store.Snapshot());
        }

        await using (var reopened = await KeyValueStore.OpenAsync(dir.Path))
        {
            Assert.False(reopened.TryGet(B("x"), out _)); // deleted
            Assert.True(reopened.TryGet(B("y"), out byte[] v));
            Assert.Equal("2", S(v));
        }
    }
}
