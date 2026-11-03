# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

SqlDuality is a pure-managed embedded database engine for .NET 10 whose organizing principle is
**"the durable log is the source of truth; all queryable state is a derived projection of it."**
Internalize that before changing anything — it is what distinguishes this design from a conventional
B-tree + WAL database, and most architectural decisions follow from it. Read `ARCHITECTURE.md` first;
it is the single best orientation. The project is in **Phase 1 (Foundation)**.

## Commands

```bash
dotnet build                      # whole solution; warnings are errors
dotnet test                       # all suites (~25 tests; a few exercise fsync + concurrency, so ~15-20s)

# one suite / one test
dotnet test --filter "FullyQualifiedName~Concurrency"
dotnet test --filter "FullyQualifiedName~KeyValueStoreTests.Write_conflict_is_detected"

dotnet build -c Release           # also what surfaces trim/AOT analyzer warnings on the libraries
```

The solution file is `SqlDuality.slnx` (XML solution format) — `dotnet` commands with no explicit
project pick it up automatically.

## Non-negotiable constraints (enforced by the build)

These come from `Directory.Build.props` and are intentional — do not weaken them to make a change compile:

- **Pure managed, AOT/trim friendly.** No P/Invoke, no native dependencies. Library projects set
  `IsAotCompatible` and enable the trim + AOT analyzers; a trim warning fails the build. This is why
  the API uses a pluggable `IValueSerializer<T>` instead of reflection-based JSON — keep new public
  surface AOT-clean the same way.
- **Zero external packages in the core.** `SqlDuality.Core` deliberately has no NuGet dependencies
  (e.g. CRC-32C is hand-rolled in `Crc32C` rather than using `System.IO.Hashing`). Don't add package
  references to the library projects without a strong reason.
- **Warnings are errors**, nullable enabled, XML docs generated. `CS1591` (missing doc on trivial
  members like operators) is the one exemption via `NoWarn`.

## Durability correctness is the top priority

If a change touches the log, storage, or recovery, correctness under crash/power-loss outranks
performance and elegance. The invariants that recovery depends on:

- **One committed transaction = one `Transaction` log record** (a serialized `MutationBatch`).
  Atomicity relies on the single-record CRC frame — do not split a transaction across records in
  Phase 1, or you reintroduce the need for commit markers.
- **Visibility follows the fsync.** The writer loop publishes a batch (advances the durable
  `NextLsn`, signals followers) only *after* the flush, so readers never see non-durable data. Keep
  it that way.
- **Recovery stops at the first bad record.** A truncated tail or CRC failure ends the valid log and
  the segment is physically truncated. If you change the frame format, bump `FormatVersion` and keep
  the "unknown version ⇒ treat as end of log" behavior.
- After changing anything here, run the `CrashRecovery` suite — it simulates torn writes, trailing
  garbage, and bit-flips by corrupting segment files directly.

## How the layers fit (the big picture)

Dependency direction: `Api` → (`Core`, `StreamProcessing`) → `Core`. Nothing depends on `Api`.

- **`Core/Logging`** — `DurableLog` is the engine's foundation: segmented append-only files named by
  base LSN, group-committed through a single writer loop, read by many lock-free readers via an
  in-memory `LSN → (segment, position)` index. `LogRecordCodec` owns the on-disk frame; the payload
  is **opaque** to this layer (higher layers interpret it by `LogRecordType`). The `Lsn` (monotonic
  `ulong`, starts at 1) is the universal cursor: record address, MVCC snapshot anchor, checkpoint
  boundary, and CDC offset.
- **`Core/Storage`** — `KeyValueStore` is a materialized view of the log. Each key holds an immutable
  `ValueVersion` chain (newest first); reads walk to the newest version `≤` a snapshot LSN
  (lock-free MVCC). `CheckpointFile` is the sorted on-disk snapshot that lets the log truncate;
  recovery = load checkpoint + replay the log tail above it.
- **`Core/Transactions`** — `Transaction` buffers writes, gives snapshot isolation + read-your-writes,
  and commits with first-committer-wins conflict detection (`WriteConflictException`). **Phase 1
  serializes the whole commit critical section under one gate** (conflict-check → append → apply), so
  concurrent *writers* are serialized while *readers* are never blocked. Lock-free concurrent commit
  is a Phase 2 goal — don't assume it exists.
- **`StreamProcessing`** — one contract, `IStreamOperator<TIn,TOut>.ProcessAsync(source, ct)`, pull-
  based and composable (`Map`/`Filter`/`Then`). `LogStream` turns a `DurableLog` into the source;
  one-shot vs. continuous (follow-the-tail) is an `ExecutionMode`. SQL operators (future) implement
  this same interface — keep it that simple.
- **`Api`** — `Database` façade. Collections namespace their keys into the shared store via
  length-prefixed `CollectionKey` (which is also how CDC recovers a key's collection). `Changes`
  decodes log records into resumable `ChangeEvent`s. `QueryAsync` intentionally throws
  `NotSupportedException` — the SQL engine is a later phase; don't stub it out silently.

## Conventions

- All binary formats are little-endian and CRC-protected; new on-disk formats should carry a version
  byte and round-trip through a dedicated `*Codec` with explicit `TryRead`/`Write`, mirroring
  `LogRecordCodec` and `MutationBatchCodec`.
- Async-first public APIs; `IAsyncEnumerable<T>` for streams. Use `Span`/`Memory`/`ArrayPool` on hot
  paths as the log writer/reader do.
- Tests use xUnit and the `TempDir` helper for filesystem isolation; organize new tests under the
  matching `tests/SqlDuality.Tests/<Area>` folder.
