# Core — Executive Summary

## What it is

**Core** is a high-performance, memory-mapped hash table for Go, purpose-built for
fixed-size key/value pairs, read-heavy workloads, and datasets that exceed available
RAM. It deliberately trades the generality of a database (variable-size records,
queries, transactions, indices) for raw speed, predictable memory layout, and the
ability to scale a single store beyond physical memory.

The central design idea: **let the operating system be the storage engine.** Each
store is a single sparse, memory-mapped file. The OS pages slot data in and out on
demand, while a compact control array (~1 byte per slot) stays resident — so a typical
point lookup faults in just one 4 KB page. A 16 GiB store costs ~0 bytes on disk until
it's filled.

## How it works

- **Swiss-table open addressing** with SWAR control-byte scanning (no SIMD, no cgo, no
  external dependencies).
- **Lock-free reads** — concurrent readers never block and never contend, even while a
  writer is active. Values ≤ 8 bytes use atomic stores; larger values use per-slot
  seqlocks.
- **Fixed geometry** — key and value lengths are set at creation (keys are opaque
  8-byte-aligned blobs, commonly xxHash64 → 8 bytes or SHA-256 → 32 bytes). No GC
  pressure, no pointer chasing, compact layout.
- **Content digest** — an order-independent hash of the full key/value set, so copies
  and replicas can be verified for divergence.

## Three deployment shapes (one `Store` interface)

| Backend     | Use when |
|-------------|----------|
| **Map**     | Single process — fastest, simplest |
| **Shared**  | Multiple processes on one host share a file (single writer, many readers, coherent via generation counter + flock) |
| **Sharded** | High write concurrency or huge stores — N independent shards enable parallel writers and bounded, non-blocking resizes |

## Performance

On a 10-core machine with data in page cache: **~6.6 ns/op** parallel lookups,
**~19 ns** lock-free updates, and **~21 M entries/sec** bulk ingest from packed binary.
Lookups scale near-linearly with cores because the sharded layout keeps each core
cache-isolated.

## Tooling

- **Library staged-build API** — `Partitioner`, `PlanLayout`, and `IngestParts`/
  `IngestManifest` let a Go program build a store **larger than RAM** without page-cache
  thrashing, from self-describing part files (a geometry manifest travels with the parts,
  so the build needs no parameters repeated and bounds resident memory deterministically).
- **Incremental patches & deltas** — a `patch` package builds, ships, and applies a
  coalesced change set (upserts + deletes) as one **idempotent, digest-verified**
  unit, plus a WAL-backed `Log` for durable live accumulation (append, coalesce,
  flush, replay on restart) — so a store can be updated in place instead of rebuilt.
  `ExportParts` + `patch.Diff` derive a delta by diffing two snapshots part-by-part
  (memory bounded by one part), for **incremental replication** that ships only the
  changes.
- **`core` CLI** — an end-to-end operator toolkit wrapping that API: `plan` (auto-size
  shards/reserve for the machine), `partition` + `ingest-parts` (out-of-RAM bulk load),
  `lookup` (with `-codex` bit-field decoding and `-in/-out` CSV batch lookup),
  `pack`/`unpack` (CRC-verified `tar.gz` bundles for distribution),
  `patch-build`/`patch-apply` (incremental updates), `digest`, `bench`, and `header`.
- **HTTP server** — a minimal JSON/binary front end: point lookups, streaming
  ingest/export with per-shard parallel transfer, digest verification over the wire,
  and an optional WAL-backed `-patch` write-buffer (coalesced upserts/deletes).

## Primary use cases

1. **Larger-than-RAM read-heavy stores** — threat intelligence (IP/domain reputation),
   GeoIP, DNS data: billions of entries where read throughput dominates.
2. **Real-time KV lookup** — rate limiting, feature flags, scoring/recommendation
   lookups at sub-20 ns latency.
3. **Content-addressable storage** — SHA-256 keys mapping to metadata, with
   digest-verified replication across regions.
4. **Bulk distribution & archival** — pre-size, load, `pack`, ship, `unpack`, and verify
   — built-in integrity at every step.
5. **Hot cache in front of slower storage** — a fast local lookup layer over
   RocksDB/MySQL/object storage.
6. **Embedded Go library** — zero-dependency, lock-free in-process map for caching,
   telemetry aggregation, or session/membership sets.

## What it is *not*

Not a general-purpose cache (no eviction/LRU — it's a durable mapping), not a database
(no transactions, MVCC, secondary indices, or query language), and not a variable-schema
store. It is a focused, production-grade **building block**: extremely fast reads,
predictable footprint, cross-process sharing, and OS-backed scale beyond RAM.
