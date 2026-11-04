using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace SqlDuality.Core.Logging;

/// <summary>
/// An append-only, segmented, checksummed, crash-safe log. This is the engine's source of truth:
/// every committed change is appended here, and all queryable state is derived by replaying it.
/// </summary>
/// <remarks>
/// <para><b>Durability.</b> Appends flow through a single writer loop that batches concurrently
/// enqueued records (group commit), writes them, and issues one <c>fsync</c> per batch before
/// acknowledging. A record is only made visible to readers (its LSN counted in
/// <see cref="NextLsn"/>) <i>after</i> that flush, so readers never observe non-durable data.</para>
///
/// <para><b>Recovery.</b> On open, every segment is scanned sequentially; the first record that is
/// truncated (torn tail write) or fails CRC marks the end of the valid log, and the segment file is
/// truncated to the last good record boundary. LSN continuity is verified across the scan.</para>
///
/// <para><b>Concurrency.</b> One writer, many readers. Readers each use their own file handles and
/// consult a shared position index under a lock; they never block the writer and vice versa.</para>
/// </remarks>
public sealed class DurableLog : IAsyncDisposable
{
    private readonly string _directory;
    private readonly LogOptions _options;

    // Guards _segments, the position indices within them, _durableNextLsn, _firstAvailableLsn.
    private readonly object _indexLock = new();
    private readonly List<LogSegment> _segments = new(); // ordered by BaseLsn ascending

    // Writer-private state (touched only by the writer loop).
    private LogSegment _activeSegment = null!;
    private FileStream _writeStream = null!;
    private long _writePosition;
    private ulong _nextWriteLsn;

    // Durable (reader-visible) horizon. The next LSN that will become visible once flushed.
    private ulong _durableNextLsn;
    private ulong _firstAvailableLsn;

    private readonly Channel<PendingAppend> _appendChannel;
    private readonly Task _writerTask;
    private readonly CancellationTokenSource _shutdownCts = new();

    // Tail-changed signal readers await when following the log.
    private readonly object _tailLock = new();
    private TaskCompletionSource _tailSignal =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private volatile Exception? _faultedWith;
    private int _disposed;

    // Invoked by the writer loop for each record, in strict LSN order, after the batch flush and
    // before that record's append is acknowledged. This is how a materialized view (e.g. the
    // key-value store) stays an exact, in-order projection of the durable log without re-reading it.
    private readonly Action<LogRecord>? _onRecordDurable;

    private DurableLog(string directory, LogOptions options, Action<LogRecord>? onRecordDurable)
    {
        _directory = directory;
        _options = options;
        _onRecordDurable = onRecordDurable;
        _appendChannel = Channel.CreateUnbounded<PendingAppend>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _writerTask = Task.Run(WriterLoopAsync);
    }

    /// <summary>The LSN that will be assigned to the next durably-committed record.</summary>
    public Lsn NextLsn
    {
        get { lock (_indexLock) { return new Lsn(_durableNextLsn); } }
    }

    /// <summary>The highest LSN that is durable on disk, or <see cref="Lsn.None"/> if the log is empty.</summary>
    public Lsn DurableLsn
    {
        get { lock (_indexLock) { return new Lsn(_durableNextLsn - 1); } }
    }

    /// <summary>The lowest LSN still readable (records below this have been truncated after a checkpoint).</summary>
    public Lsn FirstAvailableLsn
    {
        get { lock (_indexLock) { return new Lsn(_firstAvailableLsn); } }
    }

    /// <summary>
    /// Opens (recovering if necessary) the durable log rooted at <paramref name="directory"/>,
    /// creating the directory if it does not exist.
    /// </summary>
    /// <param name="directory">The directory to hold segment files.</param>
    /// <param name="options">Durability tunables, or <c>null</c> for defaults.</param>
    /// <param name="onRecordDurable">
    /// Optional projection callback invoked for each record in LSN order once it is durable, before
    /// its append is acknowledged. Must not block or throw; a throw faults the log.
    /// </param>
    /// <param name="cancellationToken">Cancels recovery.</param>
    public static async Task<DurableLog> OpenAsync(
        string directory,
        LogOptions? options = null,
        Action<LogRecord>? onRecordDurable = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        options ??= new LogOptions();
        Directory.CreateDirectory(directory);

        var log = new DurableLog(directory, options, onRecordDurable);
        try
        {
            await Task.Run(() => log.Recover(cancellationToken), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await log.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return log;
    }

    /// <summary>
    /// Appends a record and returns its assigned LSN once the record is durable on disk.
    /// </summary>
    /// <remarks>The <paramref name="payload"/> is copied synchronously; the caller may reuse its buffer immediately.</remarks>
    public async ValueTask<Lsn> AppendAsync(
        LogRecordType type, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        // Copy so the caller's buffer is not captured across the async boundary, then hand the copy off.
        PendingAppend pending = EnqueueCore(type, payload.ToArray());
        using (cancellationToken.UnsafeRegister(static s => ((PendingAppend)s!).TrySetCanceled(), pending))
        {
            return await pending.Completion.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Synchronously enqueues an append, taking ownership of <paramref name="payload"/> (no copy), and
    /// returns a task that completes with the assigned LSN once the record is durable and projected.
    /// </summary>
    /// <remarks>
    /// Intended for the commit path: callers enqueue under their own ordering lock so that the LSN the
    /// log assigns matches the caller's commit sequence, then await the returned task outside the lock
    /// (keeping the <c>fsync</c> out of the critical section). The enqueue itself cannot be cancelled,
    /// so a committed slot never leaves a gap in the LSN sequence.
    /// </remarks>
    internal Task<Lsn> EnqueueAppendOwned(LogRecordType type, byte[] payload) =>
        EnqueueCore(type, payload).Completion;

    private PendingAppend EnqueueCore(LogRecordType type, byte[] payload)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_faultedWith is { } fault)
        {
            throw new IOException("The durable log is faulted and no longer accepts appends.", fault);
        }
        if (payload.Length > _options.MaxPayloadBytes)
        {
            throw new ArgumentException(
                $"Payload of {payload.Length} bytes exceeds the configured maximum of {_options.MaxPayloadBytes}.",
                nameof(payload));
        }

        var pending = new PendingAppend(type, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), payload);
        if (!_appendChannel.Writer.TryWrite(pending))
        {
            throw new ObjectDisposedException(nameof(DurableLog), "The durable log is closed.");
        }
        return pending;
    }

    /// <summary>
    /// Reads records with LSN &gt;= <paramref name="from"/> in order.
    /// </summary>
    /// <param name="from">
    /// Starting LSN (inclusive). <see cref="Lsn.None"/> or any value below
    /// <see cref="FirstAvailableLsn"/> is clamped up to the first available record.
    /// </param>
    /// <param name="follow">
    /// When <c>true</c>, after reaching the current tail the enumerator waits for new appends and
    /// continues (a durable subscription). When <c>false</c>, it completes at the current tail.
    /// </param>
    /// <param name="cancellationToken">Stops the enumeration.</param>
    public async IAsyncEnumerable<LogRecord> ReadFromAsync(
        Lsn from,
        bool follow = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Lsn next;
        lock (_indexLock)
        {
            next = from.Value < _firstAvailableLsn ? new Lsn(_firstAvailableLsn) : from;
        }

        var readBuffers = new Dictionary<string, FileStream>();
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Drain everything currently durable.
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    string? path;
                    long position;
                    lock (_indexLock)
                    {
                        if (next.Value >= _durableNextLsn)
                        {
                            break; // caught up to the durable tail
                        }
                        Locate(next, out path, out position);
                    }

                    FileStream stream = GetReadStream(readBuffers, path!);
                    LogRecord record = ReadRecordAt(stream, position);
                    yield return record;
                    next = next.Next();
                }

                if (!follow)
                {
                    yield break;
                }

                // Wait for the tail to advance (or cancellation).
                Task signal = CurrentTailSignal();
                lock (_indexLock)
                {
                    if (next.Value < _durableNextLsn)
                    {
                        continue; // raced with a flush; more data already available
                    }
                }
                await signal.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            foreach (FileStream s in readBuffers.Values)
            {
                s.Dispose();
            }
        }
    }

    /// <summary>
    /// Discards whole segments whose entire LSN range is below <paramref name="before"/>. Intended
    /// to be called after a checkpoint has durably captured state up to (at least) that LSN.
    /// </summary>
    /// <remarks>The segment containing <paramref name="before"/> is always retained.</remarks>
    public void TruncateBefore(Lsn before)
    {
        lock (_indexLock)
        {
            int removable = 0;
            for (int i = 0; i < _segments.Count - 1; i++) // never remove the last (active) segment
            {
                LogSegment nextSeg = _segments[i + 1];
                if (nextSeg.BaseLsn <= before)
                {
                    removable++;
                }
                else
                {
                    break;
                }
            }

            for (int i = 0; i < removable; i++)
            {
                LogSegment seg = _segments[0];
                _segments.RemoveAt(0);
                try { File.Delete(seg.Path); } catch (FileNotFoundException) { /* already gone */ }
            }

            _firstAvailableLsn = _segments[0].BaseLsn.Value;
        }
    }

    // ----- Recovery -----

    private void Recover(CancellationToken cancellationToken)
    {
        var files = Directory.EnumerateFiles(_directory, "*" + LogSegment.FileExtension)
            .Select(Path.GetFileName)
            .Where(n => n is not null && LogSegment.TryParseBaseLsn(n!, out _))
            .Select(n =>
            {
                LogSegment.TryParseBaseLsn(n!, out Lsn baseLsn);
                return (baseLsn, path: Path.Combine(_directory, n!));
            })
            .OrderBy(t => t.baseLsn.Value)
            .ToList();

        ulong expectedLsn = files.Count > 0 ? files[0].baseLsn.Value : Lsn.First.Value;
        _firstAvailableLsn = expectedLsn;

        foreach ((Lsn baseLsn, string path) in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var segment = new LogSegment(baseLsn, path);
            (bool truncated, expectedLsn) = ScanSegment(segment, expectedLsn);
            _segments.Add(segment);

            if (truncated)
            {
                break; // everything after a torn/corrupt boundary is discarded
            }
        }

        // Any segment file we did not recover lies beyond the valid tail (it followed a
        // torn/corrupt boundary). Delete it so future appends resume from a clean sequence.
        var kept = _segments.Select(s => s.Path).ToHashSet(StringComparer.Ordinal);
        foreach ((Lsn _, string path) in files)
        {
            if (!kept.Contains(path))
            {
                File.Delete(path);
            }
        }

        _nextWriteLsn = expectedLsn;
        _durableNextLsn = expectedLsn;

        // Establish (or create) the active segment to append into.
        if (_segments.Count == 0)
        {
            CreateNewActiveSegment(Lsn.First);
        }
        else
        {
            _activeSegment = _segments[^1];
            _writeStream = new FileStream(
                _activeSegment.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            _writePosition = _activeSegment.SizeBytes;
            _writeStream.Seek(_writePosition, SeekOrigin.Begin);
        }
    }

    /// <summary>
    /// Scans a segment file, populating its position index. Returns whether the file was truncated
    /// due to a torn/corrupt tail (meaning the log ends here) and the next expected LSN after it.
    /// </summary>
    private (bool Truncated, ulong NextExpectedLsn) ScanSegment(LogSegment segment, ulong expectedLsn)
    {
        byte[] bytes = File.ReadAllBytes(segment.Path);

        long validLength = 0;
        var span = bytes.AsSpan();
        int offset = 0;
        bool truncated = false;

        while (offset < span.Length)
        {
            DecodeStatus status = LogRecordCodec.TryRead(span[offset..], out LogRecord? record, out int consumed);
            if (status != DecodeStatus.Ok)
            {
                // Torn or corrupt tail: everything from here on is discarded.
                truncated = true;
                break;
            }

            if (record!.Lsn.Value != expectedLsn)
            {
                // LSN discontinuity: treat as the end of the trustworthy log.
                truncated = true;
                break;
            }

            segment.RecordAppended(record.Lsn, offset, consumed);
            expectedLsn++;
            offset += consumed;
            validLength = offset;
        }

        if (truncated && validLength != bytes.Length)
        {
            // Physically truncate the torn/corrupt tail so future appends resume from a clean boundary.
            using var fs = new FileStream(
                segment.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            fs.SetLength(validLength);
            fs.Flush(flushToDisk: true);
        }

        return (truncated, expectedLsn);
    }

    // ----- Writer loop (single consumer) -----

    private async Task WriterLoopAsync()
    {
        ChannelReader<PendingAppend> reader = _appendChannel.Reader;
        var batch = new List<PendingAppend>();
        var written = new List<(LogSegment Segment, Lsn Lsn, long Position, int Framed)>();
        var newSegments = new List<LogSegment>();

        try
        {
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                batch.Clear();
                written.Clear();
                newSegments.Clear();

                while (reader.TryRead(out PendingAppend? p))
                {
                    if (!p.IsCancelled)
                    {
                        batch.Add(p);
                    }
                }
                if (batch.Count == 0)
                {
                    continue;
                }

                try
                {
                    foreach (PendingAppend p in batch)
                    {
                        int framed = LogRecordCodec.FramedSize(p.Payload.Length);

                        // Roll to a new segment if this record would overflow the active one.
                        if (_writePosition > 0 &&
                            _writePosition + framed > _options.MaxSegmentBytes)
                        {
                            _writeStream.Flush(flushToDisk: _options.FlushToDisk);
                            _writeStream.Dispose();
                            CreateNewActiveSegment(new Lsn(_nextWriteLsn));
                            newSegments.Add(_activeSegment);
                        }

                        var lsn = new Lsn(_nextWriteLsn);
                        long position = _writePosition;
                        p.SetAssigned(lsn);

                        (byte[] buf, int len) = LogRecordCodec.Rent(
                            lsn, p.Type, p.TimestampUnixMs, p.Payload);
                        try
                        {
                            _writeStream.Write(buf.AsSpan(0, len));
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(buf);
                        }

                        _writePosition += framed;
                        _nextWriteLsn++;
                        written.Add((_activeSegment, lsn, position, framed));
                    }

                    _writeStream.Flush(flushToDisk: _options.FlushToDisk);

                    // Publish: make the batch visible to readers atomically.
                    lock (_indexLock)
                    {
                        foreach (LogSegment seg in newSegments)
                        {
                            _segments.Add(seg);
                        }
                        foreach ((LogSegment seg, Lsn lsn, long pos, int framed) in written)
                        {
                            seg.RecordAppended(lsn, pos, framed);
                        }
                        _durableNextLsn = _nextWriteLsn;
                    }

                    SignalTail();

                    // Project each record (in LSN order) into derived state, then acknowledge it.
                    // Acknowledging only after projection means a committer's AppendAsync returns with
                    // its write already visible to readers (read-your-writes after commit).
                    foreach (PendingAppend p in batch)
                    {
                        if (_onRecordDurable is not null)
                        {
                            var record = new LogRecord(p.AssignedLsn, p.Type, p.TimestampUnixMs, p.Payload);
                            _onRecordDurable(record);
                        }
                        p.TrySetResult();
                    }
                }
                catch (Exception ex)
                {
                    FaultWriter(ex, batch);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            FaultWriter(ex, batch);
        }

        // Drain any stragglers after shutdown/fault so no awaiter hangs.
        while (reader.TryRead(out PendingAppend? p))
        {
            p.TrySetException(_faultedWith ?? new ObjectDisposedException(nameof(DurableLog)));
        }
    }

    private void FaultWriter(Exception ex, List<PendingAppend> batch)
    {
        _faultedWith ??= ex;
        foreach (PendingAppend p in batch)
        {
            p.TrySetException(ex);
        }
    }

    private void CreateNewActiveSegment(Lsn baseLsn)
    {
        string path = Path.Combine(_directory, LogSegment.FileNameFor(baseLsn));
        _activeSegment = new LogSegment(baseLsn, path);
        _writeStream = new FileStream(
            path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
        _writePosition = 0;

        if (baseLsn == Lsn.First && _segments.Count == 0)
        {
            // Fresh log: the very first segment is immediately part of the (empty) index.
            lock (_indexLock)
            {
                _segments.Add(_activeSegment);
            }
        }
    }

    // ----- Reader helpers -----

    private void Locate(Lsn lsn, out string path, out long position)
    {
        // Caller holds _indexLock. Linear scan is fine: the segment count is tiny.
        for (int i = _segments.Count - 1; i >= 0; i--)
        {
            LogSegment seg = _segments[i];
            if (seg.BaseLsn <= lsn && seg.TryGetPosition(lsn, out position))
            {
                path = seg.Path;
                return;
            }
        }
        throw new InvalidOperationException($"No durable record found for LSN {lsn}.");
    }

    private static FileStream GetReadStream(Dictionary<string, FileStream> cache, string path)
    {
        if (!cache.TryGetValue(path, out FileStream? fs))
        {
            fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            cache[path] = fs;
        }
        return fs;
    }

    private static LogRecord ReadRecordAt(FileStream stream, long position)
    {
        stream.Seek(position, SeekOrigin.Begin);

        Span<byte> prefix = stackalloc byte[LogRecordCodec.LengthPrefixSize];
        ReadExactly(stream, prefix);
        uint bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(prefix);

        int framed = LogRecordCodec.LengthPrefixSize + (int)bodyLength;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(framed);
        try
        {
            prefix.CopyTo(buffer);
            ReadExactly(stream, buffer.AsSpan(LogRecordCodec.LengthPrefixSize, (int)bodyLength));

            DecodeStatus status = LogRecordCodec.TryRead(
                buffer.AsSpan(0, framed), out LogRecord? record, out _);
            if (status != DecodeStatus.Ok)
            {
                throw new InvalidDataException(
                    $"Durable record at position {position} failed to decode ({status}).");
            }
            return record!;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void ReadExactly(FileStream stream, Span<byte> destination)
    {
        int read = 0;
        while (read < destination.Length)
        {
            int n = stream.Read(destination[read..]);
            if (n == 0)
            {
                throw new EndOfStreamException("Unexpected end of segment file while reading a record.");
            }
            read += n;
        }
    }

    // ----- Tail signal -----

    private Task CurrentTailSignal()
    {
        lock (_tailLock)
        {
            return _tailSignal.Task;
        }
    }

    private void SignalTail()
    {
        lock (_tailLock)
        {
            _tailSignal.TrySetResult();
            _tailSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    /// <summary>Flushes and closes the log, completing any in-flight appends or faulting them.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _appendChannel.Writer.TryComplete();
        _shutdownCts.Cancel();

        try
        {
            await _writerTask.ConfigureAwait(false);
        }
        catch
        {
            // Writer faults are surfaced to append callers; swallow here.
        }

        // Release the final tail waiter so followers unblock.
        SignalTail();

        _writeStream?.Dispose();
        _shutdownCts.Dispose();
    }

    /// <summary>A queued append awaiting the writer loop.</summary>
    private sealed class PendingAppend
    {
        private readonly TaskCompletionSource<Lsn> _tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PendingAppend(LogRecordType type, long timestampUnixMs, byte[] payload)
        {
            Type = type;
            TimestampUnixMs = timestampUnixMs;
            Payload = payload;
        }

        public LogRecordType Type { get; }
        public long TimestampUnixMs { get; }
        public byte[] Payload { get; }

        public bool IsCancelled => _tcs.Task.IsCompleted;
        public Task<Lsn> Completion => _tcs.Task;

        // The writer records the assigned LSN when the record is framed; see WriterLoopAsync.
        public Lsn AssignedLsn { get; private set; }

        public void TrySetResult() => _tcs.TrySetResult(AssignedLsn);

        public void SetAssigned(Lsn lsn) => AssignedLsn = lsn;
        public void TrySetException(Exception ex) => _tcs.TrySetException(ex);
        public void TrySetCanceled() => _tcs.TrySetCanceled();
    }
}
