# core

A custom, mmap-backed **open-addressing hash table** for Go — fixed-size
keys/values, **lock-free reads**, real-time updates, and a footprint that can
exceed RAM. No cgo.

```go
import "github.com/netstar-labs/core"
```

- **mmap, larger than RAM** — one `MAP_SHARED` file per table; the OS pages it,
  `MADV_RANDOM` keeps a lookup to one slot fault.
- **Swiss-style** 1-byte control tags scanned 8-wide with SWAR; the control array
  stays resident even when the slots don't.
- **Lock-free reads.** Value updates are lock-free for ≤8-byte values (atomic
  word) and seqlock-protected for larger ones. Inserts/deletes/resizes are
  single-writer.
- **Sharded** (concurrent writers, bounded resizes) and **cross-process** (shared
  readers + one writer per shard) variants.
- **Integrity** — header checksum + an order-independent content `Digest()` for
  verifying transfers and comparing replicas.
- **Maintenance** — `Prune` sweeps a store top-to-bottom (sequential, no
  thrashing) to age out stale entries by a timestamp or flag packed in the value;
  tombstones are reused under churn, and `Shrink` reclaims space after a mass
  age-out.
- **Patches & deltas** — a [`patch`](patch/) package builds, ships, and applies
  coalesced change sets (upserts + deletes) as one idempotent unit, plus a
  WAL-backed `Log` for durable live accumulation. `ExportParts` + `patch.Diff`
  derive a delta by diffing two store snapshots part-by-part — incremental
  replication that ships only what changed.

## Quick start

```go
m, err := core.Open("kv.core", 8, 8, 1_000_000) // 8-byte keys & values, ~1M cap
if err != nil {
	log.Fatal(err)
}
defer m.Close()

key := binary.BigEndian.AppendUint64(nil, 42)
val := binary.LittleEndian.AppendUint64(nil, 12345)
m.Put(key, val)

got := make([]byte, 8)
if m.GetInto(key, got) { // lock-free, zero-alloc
	fmt.Println(binary.LittleEndian.Uint64(got))
}
m.Sync() // make durable on demand
```

## Backends

| Constructor | Use when |
|---|---|
| `Open` → `*Map` | Single process, single table. Simplest and fastest. |
| `OpenShared` → `*Shared` | Multiple **processes** share one file. |
| `OpenSharded` → `*Sharded` | Single process; concurrent writers, resizes bounded to 1/N. |
| `OpenSharedSharded` → `*Sharded` | Cross-process **and** sharded. |

Keys/values are opaque fixed-size byte slices; `keyLen` and `valLen` must each be
a nonzero multiple of 8.

## Bulk loading

```go
sh, _ := core.OpenSharded("kv", 32, 8, 16, capPerShard, core.WithNoSync())
_ = sh.Reserve(50_000_000)   // one resize per shard, not log₂(n)
sh.PutRecords(packedRecords) // parallel, zero-alloc binary ingest
_ = sh.Sync()
```

When the store is **larger than RAM**, build it in destination order so writes
sweep each shard front-to-back instead of thrashing the page cache: bucket the
input with `core.NewPartitioner`, write a self-describing manifest, then
`core.IngestManifest` (or the `partition`→`ingest-parts` CLI). See
[examples/partition/](examples/partition/) and [§8 of the guide](docs/userguide.md).

## Performance (10 cores, in page cache)

| Operation | |
|---|---|
| `GetInto`, 1 goroutine | ~21 ns/op |
| `GetInto`, parallel | ~6.6 ns/op aggregate |
| `Update` (lock-free) | ~19 ns/op |
| Bulk ingest (binary, 16 shards) | ~21 M entries/s |

## Documentation

- **[Introduction](docs/introduction.md)** — what core is and is not, in a page.
- **[USERGUIDE.md](docs/userguide.md)** — full guide: API reference, concurrency
  model, durability, sharding, on-disk format, tuning, and the 350M-entry sizing
  recipe.
- **[ARCHITECTURE.md](docs/architecture.md)** — diagrams of the pipelines and flows:
  read/write paths, bulk & out-of-RAM build, transport, patches/deltas, and
  cross-process coherence.
- Package overview: `go doc github.com/netstar-labs/core`.
- Runnable **[examples/](examples/)** — one self-contained program per backend
  (`map`, `sharded`, `shared`, `sharedsharded`) plus `partition` (larger-than-RAM
  staged build) and `prune` (aging out stale entries); start with
  `go run ./examples/map`.
- Runnable CLI under `cmd/core/` (`plan`, `ingest`, `bench`, `partition`, `pack`,
  `push-parts`, `patch-build`/`patch-apply`, `export-parts`/`delta`, …) and an HTTP
  front end under `server/` (streaming ingest/export, digest verification,
  `-reserve`/`-nosync` bulk loads, optional `-patch` write-buffer).

## Status / caveats

Single source tree, `go test ./...` and `-race` clean. Deletes use tombstones (no
rebalance; reclaimed on resize); `Digest` is an on-demand O(n) scan; the
cross-process writer lock is unix-only. See the user guide for the full list.

## License

Apache-2.0 — see [LICENSE](LICENSE). Copyright NetSTAR Global, Inc. The
[internal/mmap](internal/mmap/) package is partly derived from `golang.org/x/exp/mmap`
and retains its BSD-3-Clause license ([internal/mmap/LICENSE](internal/mmap/LICENSE));
[THIRD-PARTY-NOTICES](THIRD-PARTY-NOTICES) reproduces that notice for binary distributions.
