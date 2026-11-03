using System.Text;
using SqlDuality.Core.Logging;
using SqlDuality.StreamProcessing;
using Xunit;

namespace SqlDuality.Tests.Api;

public sealed class DatabaseTests
{
    private sealed class Utf8StringSerializer : IValueSerializer<string>
    {
        public byte[] Serialize(string value) => Encoding.UTF8.GetBytes(value);
        public string Deserialize(ReadOnlySpan<byte> data) => Encoding.UTF8.GetString(data);
    }

    [Fact]
    public async Task Collection_put_get_delete_roundtrips()
    {
        using var dir = new TempDir();
        await using var db = await Database.OpenAsync(dir.Path);
        Collection<string> users = db.GetCollection("users", new Utf8StringSerializer());

        await users.PutAsync("u1", "Ada");
        await users.PutAsync("u2", "Grace");

        Assert.Equal("Ada", await users.GetAsync("u1"));
        Assert.Equal("Grace", await users.GetAsync("u2"));
        Assert.Null(await users.GetAsync("nope"));

        await users.DeleteAsync("u1");
        Assert.Null(await users.GetAsync("u1"));
    }

    [Fact]
    public async Task Transaction_spans_collections_atomically()
    {
        using var dir = new TempDir();
        await using var db = await Database.OpenAsync(dir.Path);
        Collection<string> accounts = db.GetCollection("accounts", new Utf8StringSerializer());
        Collection<string> audit = db.GetCollection("audit", new Utf8StringSerializer());

        using (var txn = db.BeginTransaction())
        {
            accounts.Put(txn, "a1", "100");
            audit.Put(txn, "a1", "created");
            await txn.CommitAsync();
        }

        Assert.Equal("100", await accounts.GetAsync("a1"));
        Assert.Equal("created", await audit.GetAsync("a1"));
    }

    [Fact]
    public async Task Changes_capture_is_ordered_and_collection_scoped()
    {
        using var dir = new TempDir();
        await using var db = await Database.OpenAsync(dir.Path);
        Collection<string> c = db.GetCollection("c", new Utf8StringSerializer());

        await c.PutAsync("k1", "v1");
        await c.PutAsync("k2", "v2");
        await c.DeleteAsync("k1");

        var events = new List<ChangeEvent>();
        await foreach (ChangeEvent e in db.Changes.WatchAsync(Lsn.First, follow: false))
        {
            events.Add(e);
        }

        Assert.Equal(3, events.Count);
        Assert.All(events, e => Assert.Equal("c", e.Collection));

        Assert.Equal(ChangeKind.Upsert, events[0].Kind);
        Assert.Equal("k1", Encoding.UTF8.GetString(events[0].Key));
        Assert.Equal("v1", Encoding.UTF8.GetString(events[0].Value!));

        Assert.Equal(ChangeKind.Upsert, events[1].Kind);
        Assert.Equal("v2", Encoding.UTF8.GetString(events[1].Value!));

        Assert.Equal(ChangeKind.Delete, events[2].Kind);
        Assert.Equal("k1", Encoding.UTF8.GetString(events[2].Key));
        Assert.Null(events[2].Value);

        // LSNs strictly increase.
        Assert.True(events[0].Lsn < events[1].Lsn && events[1].Lsn < events[2].Lsn);
    }

    [Fact]
    public async Task Custom_stream_processor_runs_over_change_stream()
    {
        using var dir = new TempDir();
        await using var db = await Database.OpenAsync(dir.Path);
        Collection<string> c = db.GetCollection("items", new Utf8StringSerializer());

        for (int i = 0; i < 5; i++)
        {
            await c.PutAsync($"k{i}", $"v{i}");
        }

        // A two-stage processor: keep upserts, then project to "collection/key=value".
        IStreamOperator<ChangeEvent, string> processor =
            StreamOperators.Filter<ChangeEvent>(e => e.Kind == ChangeKind.Upsert)
                .Then(StreamOperators.Map<ChangeEvent, string>(
                    e => $"{e.Collection}/{Encoding.UTF8.GetString(e.Key)}={Encoding.UTF8.GetString(e.Value!)}"));

        var results = new List<string>();
        await foreach (string line in db.RunProcessor(processor, Lsn.First, ExecutionMode.OneShot))
        {
            results.Add(line);
        }

        Assert.Equal(5, results.Count);
        Assert.Equal("items/k0=v0", results[0]);
        Assert.Equal("items/k4=v4", results[4]);
    }

    [Fact]
    public async Task Continuous_cdc_observes_live_changes()
    {
        using var dir = new TempDir();
        await using var db = await Database.OpenAsync(dir.Path);
        Collection<string> c = db.GetCollection("live", new Utf8StringSerializer());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var received = new List<string>();

        var consumer = Task.Run(async () =>
        {
            await foreach (ChangeEvent e in db.Changes.WatchAsync(Lsn.First, follow: true, cts.Token))
            {
                received.Add(Encoding.UTF8.GetString(e.Key));
                if (received.Count == 3)
                {
                    break;
                }
            }
        });

        await c.PutAsync("a", "1");
        await c.PutAsync("b", "2");
        await c.PutAsync("c", "3");

        await consumer;
        Assert.Equal(new[] { "a", "b", "c" }, received);
    }

    [Fact]
    public async Task Sql_query_is_not_yet_supported()
    {
        using var dir = new TempDir();
        await using var db = await Database.OpenAsync(dir.Path);
        await Assert.ThrowsAsync<NotSupportedException>(() => db.QueryAsync("SELECT 1"));
    }
}
