namespace SqlDuality.Core.Logging;

/// <summary>
/// Tunables for a <see cref="DurableLog"/>. Defaults are chosen to be safe and reasonable for an
/// embedded workload; durability (<see cref="FlushToDisk"/>) is on by default.
/// </summary>
public sealed record LogOptions
{
    /// <summary>
    /// Roll to a new segment file once the active segment reaches (or would exceed) this many bytes.
    /// Must be large enough to hold at least one maximally sized record. Default: 64 MiB.
    /// </summary>
    public long MaxSegmentBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// When <c>true</c> (default), each group-commit batch issues a real <c>fsync</c>
    /// (<see cref="FileStream.Flush(bool)"/> with <c>flushToDisk: true</c>) before acknowledging
    /// appends, giving crash-durability at power loss. Set to <c>false</c> only for tests or
    /// throwaway data where durability is not required.
    /// </summary>
    public bool FlushToDisk { get; init; } = true;

    /// <summary>
    /// Hard cap on a single record's payload. Guards against absurd allocations when decoding a
    /// corrupt length prefix that happened to pass the minimum-size check. Default: 256 MiB.
    /// </summary>
    public int MaxPayloadBytes { get; init; } = 256 * 1024 * 1024;
}
