# SqlDuality

A pure-managed, embedded database engine for modern .NET, built around a Kafka-style **durable log as
the source of truth**. All committed state is an ordered, checksummed, crash-safe log; tables,
collections, indexes, change feeds, and query results are projections derived from it.

No native dependencies, no P/Invoke — AOT- and trimming-friendly managed C#.

> **Status: Phase 1 (Foundation).** The durable log, an MVCC key-value store, snapshot-isolated
> transactions, change-data-capture, a stream-operator runtime, and a thin public API are implemented
> and tested. The SQL engine, vector search, and concurrent-writer MVCC are later phases. See
> [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Why

Keep what makes SQLite great — embedded, in-process, zero-config, single-directory, strong ACID,
careful crash recovery — and modernize it: MVCC snapshot reads that never block, a hybrid
relational + document model, native vector search, built-in CDC/event streaming, and a query engine
expressed as stream operators over the durable log. Async-first throughout.

## Quick start

```csharp
using SqlDuality;

await using var db = await Database.OpenAsync("./data");

// Strongly-typed document collection (bring your own serializer — source-gen friendly).
var users = db.GetCollection("users", new MyJsonSerializer<User>());
await users.PutAsync("u1", new User("Ada"));
User? ada = await users.GetAsync("u1");

// Atomic, snapshot-isolated transaction across collections.
using (var txn = db.BeginTransaction())
{
    accounts.Put(txn, "a1", "100");
    audit.Put(txn, "a1", "created");
    await txn.CommitAsync();   // throws WriteConflictException if a key changed under us
}

// Change data capture — ordered, exact, resumable from any LSN.
await foreach (ChangeEvent change in db.Changes.WatchAsync(follow: true))
    Console.WriteLine($"{change.Lsn} {change.Kind} {change.Collection}");

// Run a custom stream processor over the change feed.
var proc = StreamOperators.Filter<ChangeEvent>(e => e.Kind == ChangeKind.Upsert)
    .Then(StreamOperators.Map<ChangeEvent, string>(e => e.Collection));
await foreach (var name in db.RunProcessor(proc, Lsn.First, ExecutionMode.OneShot)) { }
```

## Layout

| Project | Responsibility |
| --- | --- |
| `src/SqlDuality.Core` | Durable log, MVCC key-value storage, checkpoints, transactions |
| `src/SqlDuality.StreamProcessing` | `IStreamOperator` contract, operators, execution engine |
| `src/SqlDuality.Api` | `Database` façade, collections, CDC (`Changes`) |
| `tests/SqlDuality.Tests` | Log, CrashRecovery, Concurrency, Storage, Api suites |

## Build & test

```bash
dotnet build          # whole solution (warnings-as-errors; trim/AOT analyzers on for libraries)
dotnet test           # all suites

# a single suite or test
dotnet test --filter "FullyQualifiedName~CrashRecovery"
dotnet test --filter "FullyQualifiedName~DurableLogTests.Concurrent_appends_get_unique_contiguous_lsns"
```

Requires the .NET 10 SDK (targets `net10.0`).
