# Examples

Runnable, self-contained programs showing how to use `core` as a library — one
per backend. Each is `package main`; run it directly from the repo root.

| Example | Backend | Run | Shows |
|---|---|---|---|
| [map/](map/) | `Open` → `*Map` | `go run ./examples/map` | The simplest single-process table: Put/Get/GetInto/Update/Delete/ForEach/Digest/Sync. Start here. |
| [sharded/](sharded/) | `OpenSharded` → `*Sharded` | `go run ./examples/sharded` | Concurrent writers from many goroutines + the bulk ingest path (Reserve + PutRecords). |
| [shared/](shared/) | `OpenShared` → `*Shared` | `go run ./examples/shared` | One file shared across **processes**. Self-execs a separate reader process alongside the live writer. |
| [sharedsharded/](sharedsharded/) | `OpenSharedSharded` → `*Sharded` | `go run ./examples/sharedsharded` | Cross-process **and** sharded: parallel bulk load, read back from a separate process. |
| [partition/](partition/) | `Partitioner` + `IngestManifest` | `go run ./examples/partition` | Staged, larger-than-RAM build: bucket into part files, write a self-describing manifest, then ingest in group order without thrashing the disk. |
| [prune/](prune/) | `Prune` / `PruneBefore` / `PruneFlag` / `Shrink` | `go run ./examples/prune` | Maintenance: sequentially sweep a store to age out stale entries (by timestamp, by flag, or a custom predicate), reuse tombstones under churn, and reclaim space with Shrink. |

Each example writes to a temp directory and cleans up after itself. The two
cross-process examples re-exec the same binary with `-role=reader` so you see two
real PIDs against the same mmap-backed file.

For **incremental updates** (the `core/patch` package — coalesced upsert/delete
change sets), the two worked examples are the CLI (`cmd/core patch-build` /
`patch-apply`, batch produce→ship→apply) and the example server (`-patch`, live
WAL-backed accumulation); see [§21 of the user guide](../docs/userguide.md#21-patches-incremental-updates).

For the full API reference and concurrency model, see the
[user guide](../docs/userguide.md).
