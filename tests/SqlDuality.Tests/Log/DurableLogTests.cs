using System.Text;
using SqlDuality.Core.Logging;
using Xunit;

namespace SqlDuality.Tests.Log;

public sealed class DurableLogTests
{
    private static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task Append_assigns_sequential_lsns_starting_at_one()
    {
        using var dir = new TempDir();
        await using var log = await DurableLog.OpenAsync(dir.Path);

        Lsn a = await log.AppendAsync(LogRecordType.Transaction, Bytes("a"));
        Lsn b = await log.AppendAsync(LogRecordType.Transaction, Bytes("b"));
        Lsn c = await log.AppendAsync(LogRecordType.Transaction, Bytes("c"));

        Assert.Equal(1ul, a.Value);
        Assert.Equal(2ul, b.Value);
        Assert.Equal(3ul, c.Value);
        Assert.Equal(3ul, log.DurableLsn.Value);
    }

    [Fact]
    public async Task Read_returns_records_in_order_with_intact_payloads()
    {
        using var dir = new TempDir();
        await using var log = await DurableLog.OpenAsync(dir.Path);

        for (int i = 0; i < 100; i++)
        {
            await log.AppendAsync(LogRecordType.Transaction, Bytes($"value-{i}"));
        }

        var seen = new List<string>();
        await foreach (LogRecord r in log.ReadFromAsync(Lsn.First))
        {
            seen.Add(Encoding.UTF8.GetString(r.Payload.Span));
        }

        Assert.Equal(100, seen.Count);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal($"value-{i}", seen[i]);
        }
    }

    [Fact]
    public async Task Reopen_recovers_all_durable_records()
    {
        using var dir = new TempDir();

        await using (var log = await DurableLog.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 50; i++)
            {
                await log.AppendAsync(LogRecordType.Transaction, Bytes($"r{i}"));
            }
        }

        await using (var reopened = await DurableLog.OpenAsync(dir.Path))
        {
            Assert.Equal(50ul, reopened.DurableLsn.Value);

            int count = 0;
            await foreach (LogRecord r in reopened.ReadFromAsync(Lsn.First))
            {
                Assert.Equal($"r{count}", Encoding.UTF8.GetString(r.Payload.Span));
                count++;
            }
            Assert.Equal(50, count);

            // The log remains writable after recovery.
            Lsn next = await reopened.AppendAsync(LogRecordType.Transaction, Bytes("after"));
            Assert.Equal(51ul, next.Value);
        }
    }

    [Fact]
    public async Task Follow_mode_observes_records_appended_after_subscription()
    {
        using var dir = new TempDir();
        await using var log = await DurableLog.OpenAsync(dir.Path);

        await log.AppendAsync(LogRecordType.Transaction, Bytes("first"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var received = new List<string>();

        var consumer = Task.Run(async () =>
        {
            await foreach (LogRecord r in log.ReadFromAsync(Lsn.First, follow: true, cts.Token))
            {
                received.Add(Encoding.UTF8.GetString(r.Payload.Span));
                if (received.Count == 3)
                {
                    break;
                }
            }
        });

        await log.AppendAsync(LogRecordType.Transaction, Bytes("second"));
        await log.AppendAsync(LogRecordType.Transaction, Bytes("third"));

        await consumer;
        Assert.Equal(new[] { "first", "second", "third" }, received);
    }

    [Fact]
    public async Task Concurrent_appends_get_unique_contiguous_lsns()
    {
        using var dir = new TempDir();
        await using var log = await DurableLog.OpenAsync(dir.Path);

        const int writers = 16;
        const int perWriter = 200;

        var tasks = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            var lsns = new List<ulong>(perWriter);
            for (int i = 0; i < perWriter; i++)
            {
                Lsn lsn = await log.AppendAsync(LogRecordType.Transaction, Bytes($"{w}:{i}"));
                lsns.Add(lsn.Value);
            }
            return lsns;
        })).ToArray();

        List<ulong>[] results = await Task.WhenAll(tasks);
        var all = results.SelectMany(x => x).OrderBy(x => x).ToList();

        Assert.Equal(writers * perWriter, all.Count);
        // Contiguous 1..N with no duplicates.
        for (int i = 0; i < all.Count; i++)
        {
            Assert.Equal((ulong)(i + 1), all[i]);
        }
    }

    [Fact]
    public async Task Truncation_removes_old_segments_but_keeps_tail_readable()
    {
        using var dir = new TempDir();
        // Tiny segments so many records force many rolls.
        var options = new LogOptions { MaxSegmentBytes = 256 };
        await using var log = await DurableLog.OpenAsync(dir.Path, options);

        for (int i = 0; i < 200; i++)
        {
            await log.AppendAsync(LogRecordType.Transaction, Bytes($"x{i:D4}"));
        }

        int segmentsBefore = Directory.GetFiles(dir.Path, "*.log").Length;
        Assert.True(segmentsBefore > 1, "expected multiple segments");

        // Keep only records from LSN 150 onward.
        var keepFrom = new Lsn(150);
        log.TruncateBefore(keepFrom);

        Assert.True(log.FirstAvailableLsn <= keepFrom);
        Assert.True(Directory.GetFiles(dir.Path, "*.log").Length < segmentsBefore);

        // Remaining records are still readable and correctly ordered.
        var seen = new List<ulong>();
        await foreach (LogRecord r in log.ReadFromAsync(keepFrom))
        {
            seen.Add(r.Lsn.Value);
        }
        Assert.Equal(200ul, seen[^1]);
        Assert.True(seen[0] <= keepFrom.Value);
    }
}
