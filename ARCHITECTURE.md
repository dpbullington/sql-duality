# SqlDuality Architecture

> Phase 1 (Foundation). This document explains the model the whole engine is built around and how
> the current pieces fit together. It is intentionally honest about what is and isn't implemented yet.

## The core idea: the log is the source of truth

SqlDuality inverts the usual embedded-database layering. Instead of a B-tree file that a write-ahead
log merely *protects*, the **durable log is the database**, and everything you query is a *derived
projection* of it.

```
          append (group commit + fsync)
  writers ───────────────────────────────▶  ┌───────────────────────┐
                                             │      Durable Log      │  ← source of truth
                                             │  segmented, CRC'd,    │
                                             │  ordered by LSN       │
                                             └───────────┬───────────┘
                                                         │ replay / subscribe (by LSN)
                        ┌────────────────────────────────┼────────────────────────────────┐
                        ▼                                 ▼                                 ▼
               ┌─────────────────┐              ┌──────────────────┐             ┌──────────────────┐
               │  KV materialized │              │  CDC change feed │             │  custom stream   │
               │  state (MVCC)    │              │ (Changes.Watch)  │             │  processors      │
               └─────────────────┘              └──────────────────┘             └──────────────────┘
```

Consequences of this choice:

- **Recovery is replay.** On open, state is rebuilt from the latest checkpoint plus the log tail.
- **CDC is free and exact.** A change feed is just a decode of log records; consumers resume from any LSN.
- **One mental model.** SQL plans (future) and user-authored processors are both operator graphs over
  the same source — log records keyed by offset (LSN).

## The LSN is the universal cursor

A **Log Sequence Number** (`Lsn`, a monotonic `ulong` starting at 1) is the single currency the engine
trades in:

- the identity and read address of a record,
- the anchor of an MVCC read snapshot,
- the checkpoint boundary,
- the CDC / replication resume offset.

`Lsn.None` (0) is the "before the beginning" sentinel.

## Durable Log (`SqlDuality.Core.Logging`)

### Record frame

Each record is self-describing and checksum-protected (`LogRecordCodec`):

```
uint32  BodyLength        // bytes following this prefix (header + payload + crc)
── CRC-covered body ──
byte    FormatVersion     // = 1, versioned for forward evolution
byte    RecordType        // Transaction | Checkpoint | Noop | ...
uint64  Lsn
int64   TimestampUnixMs
uint32  PayloadLength
byte[]  Payload           // opaque to the log; higher layers interpret it
──
uint32  Crc32C            // CRC-32C (Castagnoli) over the body
```

The payload is opaque on purpose: the log layer never deserializes it, so new record meanings don't
touch the durability code.

### Segments

The log is a set of files named by their first LSN (`00000000000000000001.log`). The active segment
rolls when it would exceed `LogOptions.MaxSegmentBytes`. An in-memory `LSN → (segment, position)`
index gives O(1) random reads.

### Group commit

Appends do not touch the file directly. They enqueue onto a channel; a single **writer loop** drains
all currently queued appends, frames and writes them, issues **one `fsync` per batch**, and only then
publishes the batch (advances the durable `NextLsn` and signals followers). Because visibility follows
the flush, **readers never observe non-durable data**.

### Crash safety / recovery

On open, each segment is scanned sequentially. The first record that is **truncated** (a short tail —
a torn write) or **fails CRC** (corruption) marks the end of the valid log; the segment is physically
truncated to the last good boundary and any later segment files are deleted. LSN contiguity is
verified during the scan. This is validated by `TornWriteRecoveryTests` (tail truncation, trailing
garbage, bit-flips).

## Materialized KV state (`SqlDuality.Core.Storage`)

`KeyValueStore` is a materialized projection of the log:

- A transaction commits as **one** `Transaction` log record carrying a serialized `MutationBatch`
  (`MutationBatchCodec`). One record ⇒ atomic commit, for free, via the single-record CRC.
- The in-memory index maps each key to an **immutable version chain** (`ValueVersion`), newest first.
  A read supplies a snapshot LSN and walks to the newest version `≤` snapshot → **lock-free MVCC reads**.
- Deletes are tombstone versions, so they are visible to snapshots and CDC.
- **The memtable is maintained by the log writer, not by committers.** `DurableLog` invokes a
  projection callback (`KeyValueStore.ApplyDurableRecord`) for each record in strict LSN order as it
  becomes durable. Because a single thread applies in LSN order, version chains are always ordered
  newest-on-top with no per-key apply lock — the "state is derived from the log by a stream
  processor" thesis made literal.

### Checkpoints make the log truncatable, and reclaim memory

`CheckpointAsync` writes a **key-sorted** on-disk snapshot of all live values as of the current LSN
(`CheckpointFile`, atomic temp-file + rename), then calls `DurableLog.TruncateBefore`. Recovery loads
the checkpoint and replays only the log tail above it. This is the Phase-1 form of "flush to sorted
on-disk segments."

**Garbage collection (Phase 2).** A checkpoint also bounds in-memory growth. `SnapshotTracker` records
the read snapshots held by in-flight transactions; the **watermark** is the oldest of these (or the
current LSN if none are active). At checkpoint time the store:

- prunes the conflict registry (`_lastCommit`) of entries at or below the watermark — they can never
  cause a conflict again, since every current and future snapshot is at least the watermark; and
- garbage-collects each key's version chain down to the watermark, keeping every version newer than
  it plus the single newest version at or below it (what a reader at the watermark would see). A key
  that collapses to a lone tombstone is removed entirely.

GC replaces each chain head with a **CAS** (`TryUpdate`/`TryRemove` against the observed head), so a
version prepended by a concurrent commit is never clobbered — a raced key is simply skipped and
collected at the next checkpoint.

## Transactions & isolation (`SqlDuality.Core.Transactions`)

- `Transaction.Begin` captures a read snapshot LSN (and registers it with the `SnapshotTracker` so GC
  retains versions it may read). Reads are snapshot-isolated and see the transaction's own buffered
  writes (read-your-writes).
- Commit performs **first-committer-wins** conflict detection over the write set: if any mutated key
  changed after the read snapshot, commit throws `WriteConflictException`.

### Concurrent writers (Phase 2)

Phase 1 held one gate across the whole commit — including the `fsync` — so group commit never
actually batched: one transaction flushed at a time. Phase 2 removes that bottleneck with an
**optimistic-concurrency oracle**:

1. **Oracle critical section (no I/O).** Under a short lock, the committer validates its write set
   against the conflict registry (`_lastCommit`: key → latest commit LSN), enqueues the log append,
   and claims the matching commit LSN. Because the enqueue happens under the lock, the LSN the log
   assigns equals the committer's claimed `++_commitCounter` (asserted after the fact; the invariant
   is that *every* log append flows through the commit path).
2. **Durability outside the lock.** The committer then awaits the append — and this is exactly where
   many concurrent commits coalesce into a **single group-commit `fsync`**.
3. **Apply by the log writer**, in LSN order, as above.

The result: writers run concurrently and batch their flushes; readers remain entirely lock-free;
isolation stays snapshot isolation with first-committer-wins. Blind single-key writes
(`BinaryCollection.PutAsync`/`DeleteAsync`) pass `Lsn.Max` as the read snapshot to opt out of
conflict detection (unconditional last-writer-wins).

## Stream processing (`SqlDuality.StreamProcessing`)

One small contract:

```csharp
public interface IStreamOperator<TIn, TOut>
{
    IAsyncEnumerable<TOut> ProcessAsync(IAsyncEnumerable<TIn> source, CancellationToken ct);
}
```

Operators are pull-based and composable (`StreamOperators.Map/Filter/Create/Then`). `LogStream`
adapts a `DurableLog` into the `IAsyncEnumerable<LogRecord>` source, in one-shot or continuous
(follow-the-tail) mode. `StreamExecutionEngine` wires a source to an operator. SQL operators will
implement this same interface, so the SQL engine and user code share the runtime.

## Public API (`SqlDuality.Api`)

`Database` is the façade: collection access (`BinaryCollection`, strongly-typed `Collection<T>` with a
pluggable `IValueSerializer<T>` so the API stays AOT-clean), ACID `BeginTransaction`, resumable CDC
(`Changes.WatchAsync`), and `RunProcessor` for custom stream components. `QueryAsync` exists but throws
`NotSupportedException` in Phase 1 — the SQL engine is a later deliverable.

Keys are namespaced into the shared store by collection via a length-prefixed encoding
(`CollectionKey`), which also lets CDC recover which collection a raw key belonged to.

## Status by phase

- **Phase 1 (done):** durable log, materialized MVCC key-value store, snapshot-isolated
  transactions, CDC, stream-operator runtime, thin public API.
- **Phase 2 (done):** concurrent-writer MVCC via the optimistic oracle (group-commit batching, no
  single-writer gate); checkpoint-time version GC and conflict-registry pruning bounded by the
  snapshot watermark; `KeyValueStoreStats` observability counters.

### Still deliberately omitted (Phase 3+)

SQL parser/optimizer and planner, ordered range/prefix scans, secondary + vector indexes as
log-driven materialized views, multi-process access, encryption, a full event-sourcing framework, and
distributed replication. See the project goals for the full roadmap.

A couple of known Phase-2 rough edges: a CDC follower reading from an offset that a concurrent
checkpoint truncates away will fault (truncation does not coordinate with active subscriptions yet),
and the conflict registry / version chains are only reclaimed at checkpoint (no background GC).
