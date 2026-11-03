using System.Text;
using SqlDuality.Core.Logging;
using Xunit;

namespace SqlDuality.Tests.CrashRecovery;

/// <summary>
/// Simulates power-loss / process-kill at arbitrary points by corrupting the tail of the segment
/// files (torn writes, trailing garbage, mid-record truncation) and asserting that recovery keeps
/// exactly the valid prefix and leaves the log usable.
/// </summary>
public sealed class TornWriteRecoveryTests
{
    private static ReadOnlyMemory<byte> Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string ActiveSegment(string dir) =>
        Directory.GetFiles(dir, "*.log").OrderBy(p => p).Last();

    [Fact]
    public async Task Truncated_tail_record_is_discarded_and_prefix_survives()
    {
        using var dir = new TempDir();

        await using (var log = await DurableLog.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 20; i++)
            {
                await log.AppendAsync(LogRecordType.Transaction, Bytes($"rec-{i}"));
            }
        }

        // Simulate a torn write: lop off the last few bytes of the final record.
        string seg = ActiveSegment(dir.Path);
        long len = new FileInfo(seg).Length;
        using (var fs = new FileStream(seg, FileMode.Open, FileAccess.Write))
        {
            fs.SetLength(len - 3);
        }

        await using (var recovered = await DurableLog.OpenAsync(dir.Path))
        {
            var seen = new List<string>();
            await foreach (LogRecord r in recovered.ReadFromAsync(Lsn.First))
            {
                seen.Add(Encoding.UTF8.GetString(r.Payload.Span));
            }

            // The last (torn) record is gone; the 19 earlier ones survive.
            Assert.Equal(19, seen.Count);
            Assert.Equal("rec-18", seen[^1]);

            // Appends resume cleanly from the recovered tail (LSN 20, reusing the torn slot).
            Lsn next = await recovered.AppendAsync(LogRecordType.Transaction, Bytes("resumed"));
            Assert.Equal(20ul, next.Value);
        }
    }

    [Fact]
    public async Task Trailing_garbage_is_treated_as_end_of_log()
    {
        using var dir = new TempDir();

        await using (var log = await DurableLog.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 10; i++)
            {
                await log.AppendAsync(LogRecordType.Transaction, Bytes($"g{i}"));
            }
        }

        // Append random junk that looks like the start of a record but is not valid.
        string seg = ActiveSegment(dir.Path);
        using (var fs = new FileStream(seg, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(0, SeekOrigin.End);
            // A plausible but bogus length prefix followed by too-few bytes.
            fs.Write(new byte[] { 0x40, 0x00, 0x00, 0x00, 0xFF, 0xFF });
        }

        await using (var recovered = await DurableLog.OpenAsync(dir.Path))
        {
            Assert.Equal(10ul, recovered.DurableLsn.Value);

            int count = 0;
            await foreach (LogRecord _ in recovered.ReadFromAsync(Lsn.First))
            {
                count++;
            }
            Assert.Equal(10, count);
        }
    }

    [Fact]
    public async Task Bitflip_corruption_is_caught_by_crc_and_truncates_there()
    {
        using var dir = new TempDir();

        await using (var log = await DurableLog.OpenAsync(dir.Path))
        {
            for (int i = 0; i < 30; i++)
            {
                await log.AppendAsync(LogRecordType.Transaction, Bytes($"payload-number-{i}"));
            }
        }

        // Flip a byte somewhere in the back third of the file. Everything from the first corrupt
        // record onward must be discarded; everything before it must survive.
        string seg = ActiveSegment(dir.Path);
        long len = new FileInfo(seg).Length;
        long flipAt = len * 2 / 3;
        using (var fs = new FileStream(seg, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(flipAt, SeekOrigin.Begin);
            int b = fs.ReadByte();
            fs.Seek(flipAt, SeekOrigin.Begin);
            fs.WriteByte((byte)(b ^ 0xFF));
        }

        await using (var recovered = await DurableLog.OpenAsync(dir.Path))
        {
            var seen = new List<string>();
            await foreach (LogRecord r in recovered.ReadFromAsync(Lsn.First))
            {
                seen.Add(Encoding.UTF8.GetString(r.Payload.Span));
            }

            // Some prefix survived, the corrupt record and everything after it did not,
            // and the surviving records are an exact in-order prefix of the originals.
            Assert.InRange(seen.Count, 1, 29);
            for (int i = 0; i < seen.Count; i++)
            {
                Assert.Equal($"payload-number-{i}", seen[i]);
            }
        }
    }

    [Fact]
    public async Task Recovery_from_completely_empty_directory_starts_fresh()
    {
        using var dir = new TempDir();
        await using var log = await DurableLog.OpenAsync(dir.Path);

        Assert.Equal(Lsn.None, log.DurableLsn);
        Assert.Equal(1ul, log.NextLsn.Value);

        Lsn first = await log.AppendAsync(LogRecordType.Transaction, Bytes("hello"));
        Assert.Equal(1ul, first.Value);
    }
}
