# `core` — User Guide

A custom, mmap-backed **open-addressing hash table** for Go, tuned for
fixed-size keys/values, read-heavy workloads, and real-time updates at a scale
**larger than RAM**. Zero cgo.

```
import "github.com/netstar-labs/core"
```

- Single contiguous memory-mapped file (`MAP_SHARED`) per table — the OS pages it
  in and out, so a table can exceed RAM. `MADV_RANDOM` keeps a point lookup to a
  single slot fault.
- Swiss-table-style 1-byte control tags scanned 8-wide with SWAR; the control
  array stays resident even when the slots don't.
- **Lock-free reads.** Value updates of existing keys are lock-free for ≤8-byte
  values (atomic word) and seqlock-protected for larger ones. Structural changes
  (insert new key / delete / resize) are single-writer.
- Optional **sharding** (concurrent writers, bounded resizes) and **cross-process**
  operation (shared readers + a single writer per shard).

---

## Table of contents

1. [Concepts](#1-concepts)
2. [Quick start](#2-quick-start)
3. [Choosing a backend](#3-choosing-a-backend)
4. [Key and value sizing](#4-key-and-value-sizing)
5. [Opening and closing](#5-opening-and-closing)
6. [Reads](#6-reads)
7. [Writes](#7-writes)
8. [Bulk loading](#8-bulk-loading)
9. [Iteration and integrity](#9-iteration-and-integrity)
10. [Durability and `Sync`](#10-durability-and-sync)
11. [Concurrency model](#11-concurrency-model)
12. [Cross-process operation](#12-cross-process-operation)
13. [Sharding](#13-sharding)
14. [Performance tuning](#14-performance-tuning)
15. [On-disk format](#15-on-disk-format)
16. [API reference](#16-api-reference)
17. [Errors](#17-errors)
18. [Limitations and caveats](#18-limitations-and-caveats)
19. [Sizing for 350M entries](#19-sizing-for-350m-entries)
20. [The `core` CLI and server](#20-the-core-cli-and-server)
21. [Patches: incremental updates](#21-patches-incremental-updates)

---

## 1. Concepts

**Open addressing, Swiss-style.** Entries live in a flat slot array. In front of
it sits a dense array of 1-byte *control tags* (`empty=0x00`, `deleted=0x01`,
`full=0x80|H2` where `H2` is 7 bits of the key hash). Lookups scan control bytes
8 at a time with SWAR (no SIMD intrinsics needed). The control array is ~`capacity`
bytes — small enough to stay resident in RAM even when the much larger slot array
is paged from disk, so a lookup typically faults in **one** slot page.

**mmap, larger than RAM.** Each table is one file mapped `MAP_SHARED`. Sparse
files mean a large initial capacity costs no disk up front; the OS faults pages
in on access and writes them back under memory pressure. `MADV_RANDOM` on the
slot region disables read-ahead so a random lookup doesn't pull neighbor pages.

**Three backends, one interface.** `Map` (single process), `Shared` (cross
process), and `Sharded` (N independent shards over either). `Map` and `Shared`
satisfy the `Store` interface; `Sharded` fans keys across shards.

---

## 2. Quick start

```go
package main

import (
	"encoding/binary"
	"fmt"

	"github.com/netstar-labs/core"
)

func main() {
	// 8-byte keys, 8-byte values, room for ~1M entries to start.
	m, err := core.Open("kv.core", 8, 8, 1_000_000)
	if err != nil {
		panic(err)
	}
	defer m.Close()

	key := make([]byte, 8)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	binary.LittleEndian.PutUint64(val, 12345)

	inserted, err := m.Put(key, val) // inserted == true (new key)
	_ = err
	fmt.Println("inserted:", inserted)

	got := make([]byte, 8)
	if m.GetInto(key, got) { // lock-free, zero-alloc
		fmt.Println("value:", binary.LittleEndian.Uint64(got))
	}

	_ = m.Sync() // make durable on demand
}
```

---

## 3. Choosing a backend

| Backend | Constructor | Use when |
|---|---|---|
| `Map` | `Open` | Single process, single logical table. Simplest, fastest. |
| `Shared` | `OpenShared` | Multiple **processes** share one table (e.g. a worker cluster on one host). |
| `Sharded` (Map shards) | `OpenSharded` | Single process, want **concurrent writers** and **bounded resizes** (1/N of data per resize). |
| `Sharded` (Shared shards) | `OpenSharedSharded` | Both: cross-process **and** sharded — the full stack. |

Rule of thumb: start with `Map`; move to `OpenSharded(shards ≈ CPU cores)` when
write concurrency or resize pauses matter; use the `Shared` variants only when
multiple processes must open the same store.

Reads cost the same on every backend (lock-free, zero-alloc); the choice is
really about writes. `Sharded` writes ~3× faster than `Map` under concurrency,
while the cross-process `Shared` variants pay a 5–16× write penalty for
`MAP_SHARED` visibility. See §14 for the head-to-head numbers.

---

## 4. Key and value sizing

- `keyLen` and `valLen` are **fixed for the life of the file**. `keyLen` must be a
  **nonzero multiple of 8** (e.g. 8, 16, 24, 32); `valLen` must be a **multiple of 8**
  and may be **0** (e.g. 8 or 16 for values, or 0 for a key-only set). Otherwise
  `Open*` returns `ErrBadSize`.
- Reopening a file with different sizes than it was created with returns
  `ErrBadSize`.
- **`valLen == 0` makes a key-only *set* store** — it records membership only, with
  no value bytes per slot. `Put(key, nil)` adds a member (returns `true` the first
  time, `false` on re-add), `Contains` tests membership, and `Delete` removes it.
  `Get`/`GetInto` still work but yield an empty value, so `Contains` is the natural
  read. This is the most compact way to store a large deduplicated key set.
- **Value size decides the update path:**
  - `valLen ≤ 8` → value updates are a single atomic word store: **lock-free**,
    safe for concurrent updaters of the same key (last writer wins).
  - `valLen > 8` → value updates use a per-slot **seqlock**: reads stay lock-free,
    but writers are serialized (a writer lock is taken). Correct, just not
    lock-free on the write side.

**Keys are opaque fixed-size bytes — only the geometry matters.** The library
stores and compares keys as raw `keyLen`-byte slices; it hashes them with xxHash64
*internally* only to choose a slot, which is independent of how you filled the
bytes. So *how* you turn an application identifier into a `keyLen`-byte key is
entirely your choice: a cryptographic digest (SHA-256, BLAKE2b, SHA-3…), a
non-cryptographic hash (xxHash64, FNV…), a fixed-width encoding of an integer, or
the raw identifier itself **zero-padded** when it already fits in `keyLen` bytes
(no hash at all). The only rules are that every key you `Put`/`Get` is exactly
`keyLen` bytes and that you derive it the **same way on write and read**. Values
are likewise opaque fixed-size bytes; you choose the encoding.

The `cmd/core` CLI and the example server pick a hash purely for convenience — to
fold arbitrary-length text items down to a fixed key: **SHA-256** at `keyLen=32`,
**XXH3-128** at `keyLen=16`, **xxHash64** at `keyLen=8`. That choice lives entirely in their `makeKey`, not in
the library; your own program can derive keys however it likes — see *Choosing your
own key hash* in §20.

---

## 5. Opening and closing

```go
m,  _ := core.Open(path, keyLen, valLen, initCap, opts...)              // *Map
s,  _ := core.OpenShared(path, keyLen, valLen, initCap, opts...)        // *Shared
sh, _ := core.OpenSharded(path, keyLen, valLen, shards, capPerShard, opts...)        // *Sharded (Map shards)
sh, _ := core.OpenSharedSharded(path, keyLen, valLen, shards, capPerShard, opts...)  // *Sharded (Shared shards)
```

- `initCap` / `capPerShard` is the **initial** slot capacity (rounded up to a
  power of two). A new file is created sparse, so a big number is cheap. The table
  grows automatically beyond it.
- `shards` must be a **power of two**. Files are created at `"<path>.000"`,
  `"<path>.001"`, … (and `"<path>.NNN.meta"` for the cross-process variant).
- Always `defer x.Close()`. `Close` releases all mappings; it does **not** `Sync`
  for `Map`/`Sharded` (call `Sync` first if you need durability) — note the
  `example/server` wrapper does `Sync` then `Close`.

### Options

```go
core.WithNoSync() // skip the msync that makes a resize durable before swap-in
```

`WithNoSync` trades crash-durability of resizes for speed — ideal for bulk
loading a rebuildable table. Explicit `Sync()` still works regardless.

---

## 6. Reads

All reads are **lock-free** and never block writers.

```go
val, ok := m.Get(key)        // returns a fresh copy (1 allocation)
ok := m.GetInto(key, dst)    // zero-alloc; dst must have len == valLen
ok := m.Contains(key)        // existence only
```

`GetInto` is the hot path: it writes the value into your buffer with no
allocation and retries internally across a concurrent resize so it always
reflects the latest table.

---

## 7. Writes

```go
inserted, err := m.Put(key, val) // insert or update; inserted==true if new key
found := m.Update(key, val)      // update existing only; false if absent
existed := m.Delete(key)         // tombstone; false if absent
```

- **`Put`** inserts a new key or updates an existing one. It takes the writer lock
  and may trigger a resize. Returns whether a new entry was created.
- **`Update`** only changes the value of an existing key (never inserts). For
  `valLen ≤ 8` it is fully lock-free; for `valLen > 8` it takes the writer lock
  (seqlock). This is the "real-time update" fast path.
- **`Delete`** marks the slot as a tombstone. No rebalancing or merging is done;
  tombstones are reclaimed lazily when a resize rehashes the table.

The table **grows automatically** at 7/8 load factor (it doubles), and rehashes
in place to shed tombstones when they accumulate. A resize builds a new file,
copies live entries, `msync`s (unless `WithNoSync`), atomically renames it into
place, and swaps the active mapping — readers never block or fault on a stale
mapping (old mappings are retired, not unmapped, until `Close`).

On `*Sharded`, **`DeleteBatch(keys)`** and **`DeleteRecords(buf)`** are the bulk
delete counterparts to `PutBatch`/`PutRecords`: keys are bucketed by shard and
removed concurrently, each shard taking its writer lock once. `DeleteRecords`
reads a packed buffer of `keyLen`-byte keys (no values). For a coalesced set of
inserts/updates **and** deletes applied as one unit — built offline, shipped, or
accumulated live — see the [`patch` package](#21-patches-incremental-updates) (§21).

---

## 8. Bulk loading

For loading many entries, avoid per-key locking. Two tools:

**`Reserve(n)`** — pre-size up front so the load does **one** resize instead of
`~log₂(n)`:

```go
_ = m.Reserve(50_000_000)
```

**`Batch`** (on `Map`/`Shared`) — take the writer lock once for the whole load:

```go
err := m.Batch(func(put func(key, val []byte) (bool, error)) error {
	for i := range keys {
		if _, e := put(keys[i], vals[i]); e != nil {
			return e
		}
	}
	return nil
})
```

**Sharded bulk paths** — bucket by shard and insert into all shards concurrently:

```go
// Slice-of-slices form (e.g. parsed records):
inserted, err := sh.PutBatch(keys, vals)

// Packed-binary form (fastest): buf is N records of [keyLen | valLen] bytes,
// keys/values sliced directly out of buf with no per-record allocation.
inserted, err := sh.PutRecords(buf)
```

`PutRecords` is the fast ingest path: it hashes and buckets records in parallel
(one worker per CPU) and inserts into each shard concurrently. Process very large
inputs in chunks (e.g. a few hundred K records per call) to bound memory. Combine
with `Reserve` and `WithNoSync` for maximum throughput.

> **Measured:** on a 10-core machine, 32-byte keys / 8-byte values, 16 shards:
> ~21 M entries/s via `PutRecords` + `Reserve` + `WithNoSync`. (Use shards ≈ core
> count — more shards than cores *regresses*.)

### Loading a store larger than RAM

The ~21 M/s figure assumes the table is RAM-resident. A record's slot is chosen by
its hash, so inserts scatter writes across the **whole** slots region. Once that
region exceeds RAM, every insert risks a page fault and dirty-page writeback, and
throughput collapses to random-write IOPS — often **10–100×** slower.

The fix is to insert in **destination order** so writes sweep each shard's slots
array front-to-back instead of randomly. The placement key is the group index
`g = (hash >> 7) & mask`, where `mask = perShardCapacity/8 - 1` (see §15). Sorting
or radix-partitioning the input by `g` confines the live write set to a small
sliding window per shard. This is purely a build-time optimization — final layout
and lookups are identical regardless of insertion order.

The library exposes this radix-partition build directly, and the `core` CLI is a
thin wrapper over it. The geometry that must stay in lockstep (per-shard capacity,
the group `mask`, the part split) lives in the package — callers never re-derive
it:

```go
// 1) bucket records by the high bits of g, hashing with the store's own hash.
p, _ := core.NewPartitioner(keyLen, valLen, shards, reserve, parts)
part := p.PartKey(key)          // → 0..parts-1; write the record to file <out>.p<part>
//   p.Part(hash) is the zero-copy variant when the hash is already in hand.

// 2) make the parts self-describing, then build the store straight from them.
core.WriteManifest(core.ManifestPath(out), core.PartsManifest{
    KeyLen: keyLen, ValLen: valLen, Shards: shards, Reserve: reserve, Parts: parts,
    Records: nrec, Counts: counts,
})
m, st, _ := core.IngestManifest(core.ManifestPath(out), "kv.core", 1024, /*flushPerPart=*/true, core.WithNoSync())
```

`core.PlanLayout(keyLen, valLen, shards, reserve)` returns the same `Layout`
(per-shard capacity, total capacity, on-disk bytes) the store ends up with, so a
sizing step never has to mirror the table's rounding. See the
[partition example](../examples/partition/) for the full flow.

The CLI wraps the same calls:

```
# 1) bucket records by the high bits of g into <out>.pNNN + an <out>.manifest.json
go run ./cmd/core partition -data data.bin -out part -shards 8 -reserve 360000000 -parts 16
#    add -text:on if the source is "key<space>uint64" lines rather than packed records

# 2) load the partitions in order into one store (one Reserve, parallel per shard)
go run ./cmd/core ingest-parts -parts part -store kv.core
```

**Self-describing parts.** `partition` writes a `<out>.manifest.json` sidecar
recording the geometry the parts were bucketed for (`keyLen`, `valLen`, `shards`,
`reserve`, `parts`, per-part counts). `ingest-parts` (and `core.IngestManifest`)
read it, so you don't repeat — or risk mismatching — `-shards`/`-reserve` on the
build, and a parts directory can be shipped and ingested later or on another
machine with nothing but the manifest. Raw parts with no manifest still work, but
then `-shards`/`-reserve` must be given and **must match** the partition step:
they set the per-shard capacity, hence the `mask` used to bucket. Pick `-reserve`
slightly above the real entry count so capacity is locked before `g` is computed (a
mid-load resize would change `mask`). Choose `-parts` so one partition's
destination (`≈ storeBytes / parts` per shard, ×shards resident at once) fits
comfortably in RAM. The partition pass is sequential I/O (SSD-friendly);
`ingest-parts` then runs without thrashing.

**Bounding resident memory.** By default the load `Sync`s after each part
(`-flush:on` / `flushPerPart=true`), which msyncs only that part's dirty window and
clears it, so dirty pages never accumulate past one window — a deterministic bound
rather than relying on the kernel's background writeback. Pass `-flush:off` for a
fully in-RAM load, where the single final `Sync` is enough and per-part syncs only
cost throughput.

**`-shards` and `-parts` are independent** — they answer different questions, so it
is normal for them to differ (e.g. 8 shards, 4 parts):

- `-shards` comes from **CPU cores** (concurrent writers + bounded resize cost) and
  fixes the final store layout.
- `-parts` comes from **store size ÷ RAM budget** (how many ordered load passes keep
  the resident write window in RAM) and is purely a build-time choice.

So `-parts` may be smaller than, equal to, or larger than `-shards`; the only
constraint is `-parts ≤ groups-per-shard` (`= perShardCapacity/8`), which
`NewPartitioner`/`partition` enforces. Mechanically the shard is selected by the
high bits of the hash while the part is the high bits of the per-shard group index
`g`, so the two slice different bit ranges and never need to align. `core plan`
(§19) picks both for you from the machine and the dataset.

---

## 9. Iteration and integrity

**`ForEach`** iterates live entries over a point-in-time snapshot:

```go
sh.ForEach(func(key, val []byte) bool {
	// key and val are valid ONLY during this call — copy to retain.
	// return false to stop early.
	return true
})
```

`key` aliases the mmap and `val` aliases a reused buffer (read consistently via
the atomic/seqlock path). Entries inserted after iteration starts, or a resize,
may not be reflected.

**`Prune`** is the maintenance counterpart: a single **sequential sweep** that
tombstones every entry a predicate selects. It is the way to age out stale data
in bulk — far cheaper than iterating and calling `Delete` per key.

```go
removed := sh.Prune(func(key, val []byte) bool {
	// return true to age this entry out (tombstone it).
	return false
})
```

Why it is the right tool for "scan a shard top to bottom and drop the dead rows":

- **Sequential, not random.** The sweep walks each shard's control and slot
  regions strictly forward and advises the kernel `MADV_SEQUENTIAL` for the
  duration (restoring `MADV_RANDOM` after), so a store larger than RAM **streams
  in** instead of faulting one page at a time — no disk thrashing. Point lookups
  stay tuned for random access the rest of the time.
- **One lock for the whole pass.** It takes the shard's writer lock once (for
  `*Shared`, the cross-process flock too) rather than per key, and reads stay
  lock-free throughout. `Sharded.Prune` sweeps one shard at a time, so only
  1/N of the keyspace blocks writers at any moment.
- **No resize mid-sweep.** Tombstoning never inserts, so the mapping can't grow
  or move under the scan. `key`/`val` alias the mmap and are valid only for the
  duration of the callback — **copy** to retain.

**Common predicates.** Most aging-out keys off a value packed in a fixed word, so
two helpers build the predicate for you (both read a little-endian `uint64` at a
byte offset in the value):

```go
// Age out everything older than a retention window. Store a day count since the
// Unix epoch (time.Now().Unix()/86400) in the value, and the cutoff is plain
// subtraction. off is the byte offset of that word in the value.
todayDays := uint64(time.Now().Unix() / 86400)
removed := core.PruneBefore(sh, 0, todayDays-90) // drop anything older than 90 days

// Age out by a bit flag in the value (mask = the bits to test).
const fExpired = uint64(1) << 3
removed = core.PruneFlag(sh, 0, fExpired)
```

**Choosing the date encoding.** `PruneBefore` compares the value word as an
unsigned `uint64` — it only needs *bigger == newer*, so any monotonic encoding
works and storage/predicate are identical regardless of which you pick:

- **Days since the Unix epoch** — `time.Now().Unix() / 86400`, truncated to whole
  UTC days (~20,600 in mid-2026, fits a `uint16`) — is a **linear counter**, so a
  retention window is just subtraction: "older than 90 days" is `todayDays - 90`.
  This is the recommended choice whenever the cutoff is a *window* rather than a
  fixed calendar date. (Truncation buckets to UTC midnight, losing sub-day
  resolution — fine for age-out; integer division floors toward zero, so only
  pre-1970 timestamps would be off by one, which you won't have.)
- **`yyyymmdd` as an int** (e.g. `20260619`) also sorts as a date and is
  human-readable in a hex/decimal dump, but it is **not linear** — it has gaps
  (`20251231 → 20260101` jumps by 8870) and illegal values, so you cannot do
  date arithmetic on it; use it only for fixed calendar cutoffs.

A flag test is exactly a mask (`val & mask != 0`) — nothing more is needed to
*decide*; the tombstone is what mutates state. (To instead flip a bit and keep
the row, that is an `Update`, not a `Prune`.) For anything else — a timestamp
that is not a single word, a composite rule — pass your own predicate to `Prune`
and decode the value yourself.

**Why the callback gets the key.** For a pure timestamp/flag age-out you ignore
`key` (the helpers do). It is in the signature for the cases where aging here has
to coordinate with something else, all keyed by the same key:

- **Cascade to a secondary store/index.** When you drop a row here, also delete
  it from a reverse index, a different store, or an on-disk blob named by the key.
- **Per-key retention policy.** The key encodes a namespace/type prefix and
  different classes expire on different rules — or some keys never expire
  (a protected/whitelist set).
- **Tier before delete.** Copy `key|val` to cold storage first, then return
  `true` to tombstone — you need the key to name the archived record.
- **Audit / metrics.** Emit "aged out `<key>`" to a log or counter.

> Retaining a key/val outside the callback (appending to a slice, async logging,
> writing elsewhere) requires a **copy** — `append([]byte(nil), key...)` — since
> they alias the mmap. Reading them inline (compare, hash, synchronous write) is
> fine.

**Reclaiming the space (usually free).** A tombstone is a deletion marker, not
dead weight: a later `Put` of a new key drops straight into a tombstoned slot,
and the table auto-rehashes **at the same capacity** (no doubling, no file growth)
if tombstones ever reach the load threshold. So in a *steady-state churn* set —
roughly flat population, entries aging out at about the rate new ones arrive —
`Prune` needs no follow-up: capacity never grows and tombstones are recycled.

**`Shrink`** is for the other case — a set that has gone *permanently* sparse
(a mass age-out with no refill) where you want the file to give space back:

```go
err := sh.Shrink() // rehash each shard down to fit its live count
```

It rehashes down to the smallest capacity that holds the current live count at
the load factor (a no-op if already that small), reclaiming disk and page cache.
Reach for it deliberately after a big one-way drop in population — not as part of
routine pruning.

**`Digest`** is an order-independent content digest — a cheap **integrity /
equality proof**:

```go
d := sh.Digest() // uint64; equal digests ⇒ equal key→value sets
```

It is the sum (mod 2⁶⁴) of a per-entry hash over all live entries, so it is
independent of insert order and of how shards/slots are laid out. Uses:

- **Verify a transfer / backup**: compare source and destination digests.
- **Compare replicas or shards** in O(1) after computing each side once.
- **Detect silent drift**: any inserted/updated/deleted value changes it.

`Digest` is computed **on demand by scanning** (O(n)); it adds no cost to the
read/update hot path. `Sharded.Digest()` is the sum of per-shard digests, so you
can verify each shard independently (`sh.Shard(i).Digest()`).

**Header checksum.** Independently, every file carries an fnv64a checksum over its
immutable geometry fields (sizes, capacity, region offsets). It is validated on
open *before* any offset is trusted, so a corrupted header is reported as
`ErrCorrupt` instead of causing an out-of-bounds panic. (It does not cover the
live entries — use `Digest` for that.)

---

## 10. Durability and `Sync`

Writes land in the page cache (the `MAP_SHARED` mapping) and are flushed lazily by
the kernel. `Put`/`Update`/`Delete` do **not** fsync per operation — they're fast.

```go
err := m.Sync() // msync dirty pages to disk
```

`Sync` flushes only the pages written since the last `Sync` (a per-process dirty
bitmap at 2 MiB granularity). When the dirty set is sparse relative to the map it
`msync`s each dirty range individually; when a large fraction is dirty (the common
case for a hash table, whose writes scatter), it falls back to one whole-file
`msync`. So `Sync` is **never slower** than a plain whole-file `msync`, and is
~free when nothing changed.

- A **resize** is made durable (msync) before the new file is renamed into place,
  unless opened `WithNoSync`.
- For a rebuildable derived store, `WithNoSync` + an explicit final `Sync` is the
  fast and safe combination.

> The whole-file `msync` scan grows ~linearly with mapped size (~4 ms/GiB
> observed). The dirty-range path matters at tens-of-GB scale where that scan is
> tens-to-hundreds of ms.

---

## 11. Concurrency model

| Operation | Synchronization |
|---|---|
| `Get` / `GetInto` / `Contains` | **Lock-free** (atomic loads). |
| `Update` (`valLen ≤ 8`) | **Lock-free** atomic word store; concurrent same-key updaters are safe. |
| `Update` (`valLen > 8`) | Writer lock (seqlock); readers still lock-free. |
| `Put` / `Delete` / `Batch` / `Reserve` / `PutBatch` / `PutRecords` | Single-writer (in-process `writeMu`; cross-process flock for `Shared`). |
| `Prune` | Single-writer for the whole sweep (one lock acquisition); reads stay lock-free. `Sharded` locks one shard at a time. |
| `Shrink` / Resize | Single-writer; new mapping published atomically, old retired (readers never fault). |

- New-key publication is correct for lock-free readers: a slot's key/value are
  written, then the control byte is published with an atomic release-store, so a
  reader that observes the tag also observes the data.
- `count` and `tombstones` live in the mapped header and are maintained with
  atomics, so they stay correct even when multiple processes write the same inode.
- All operations are goroutine-safe. `Sharded` writes to **different shards**
  proceed concurrently (separate locks).

---

## 12. Cross-process operation

`OpenShared` / `OpenSharedSharded` let multiple **processes** open the same path.

- The data file is `MAP_SHARED`, so **inserts, deletes, and value updates are
  visible to every process immediately** through shared memory + atomics.
- A small `<path>.meta` file holds a **generation counter**; only a **resize**
  (which replaces the data inode) bumps it. Other processes notice on their next
  operation and remap — a one-operation staleness window, no locks on the read
  path.
- A cross-process **single-writer lock** is an advisory `flock` on the meta file.
  Many reader processes + one writer (per shard) at a time.
- There is no reader-slot table (an open-addressing table never reclaims pages out
  from under a reader the way an MVCC B-tree does); a resized-away inode stays
  alive until every process remaps.

**Caveat:** the cross-process writer lock and liveness probe are wired for
linux/darwin. On other platforms they degrade to no-ops (single-process correct;
multi-process writers not mutually excluded).

---

## 13. Sharding

`Sharded` fans keys across N independent stores by the **high bits of the hash**
(keeping the low control-tag bits and mid group-index bits full-entropy within a
shard, so per-shard distribution stays uniform). Each shard is a separate file
with its own writer lock, so:

- Writes to different shards run **concurrently**.
- A resize only rewrites **one shard** (1/N of the data) — bounding the
  stop-the-world-for-writers window.

```go
sh.Shards()        // number of shards
sh.Shard(i)        // the i-th shard as a core.Store (for per-shard work)
```

`Shard(i)` enables **per-shard export/ingest** for parallel client-side
transfers: pull each shard over its own connection, verify with
`sh.Shard(i).Digest()`, and re-ingest directly into the matching shard (no
re-routing needed, since a shard's keys all hash to that shard at the same shard
count).

---

## 14. Performance tuning

- **Use `GetInto`, not `Get`**, on the hot path (zero allocation).
- **Pick fixed sizes carefully.** `valLen ≤ 8` keeps value updates lock-free.
  Smaller records mean more entries per page and fewer faults.
- **Shards ≈ CPU cores.** More shards than cores regresses ingest (goroutine
  contention, smaller per-shard tables, more lock acquisitions).
- **`Reserve` before a bulk load** to avoid incremental resizes.
- **Use the binary path (`PutRecords`)** for ingest; it's parallel and zero-alloc.
- **`WithNoSync`** for rebuildable bulk loads; `Sync` once at the end.
- **Give the box RAM for page cache, not heap.** With non-pointer byte slices, the
  Go GC never scans the table; keep your own heap small and let the OS cache slot
  pages. Avoid container memory limits that count page cache against you.
- **Local NVMe.** Random-read latency is the ballgame once you exceed RAM.

Observed (8-byte keys/values, ~20M entries, in page cache, 10 cores):

| Operation | Throughput |
|---|---|
| `GetInto` (1 goroutine) | ~21 ns/op |
| `GetInto` (parallel) | ~6.6 ns/op aggregate |
| `Update` (lock-free) | ~19 ns/op |
| Bulk ingest (binary, 16 shards) | ~21 M entries/s |

### Backend comparison

All four backends share the same lock-free, zero-alloc read path, so the
backend choice barely affects reads — it's writes that diverge, by an order of
magnitude. Head-to-head, same machine (8 threads, 1M × 8-byte entries, in page
cache, 16 shards where applicable; `go test -bench Backend`, see
[`backendbench_test.go`](../backendbench_test.go)):

| Backend | `GetInto` parallel | `Put` parallel | Notes |
|---|---|---|---|
| `Map` | ~18 ns/op | ~370 ns/op | All writers serialize on one lock. |
| `Sharded` | ~18 ns/op | **~125 ns/op** | Shard locks let writers run in parallel — ~3× `Map`. |
| `Shared` | ~18 ns/op | ~2030 ns/op | `MAP_SHARED` makes writes cross-process-visible — a real write tax. |
| `SharedSharded` | ~18 ns/op | ~1800 ns/op | Cross-process write cost, partly recovered by shard locks. |

Takeaways: reads are effectively free across all four (every path is 0 B/op,
0 allocs/op and scales linearly with cores). For in-process write concurrency,
`Sharded` is the clear win. Reach for the `Shared` variants **only** when other
*processes* must open the same file — you pay nothing on reads but a 5–16×
penalty on writes for the cross-process visibility.

---

## 15. On-disk format

One file per table (per shard). The first page is the header; then the control
array, an optional version array (only when `valLen > 8`), then the slots. All
regions are page/word aligned.

```
[ header : 4096 bytes ]
[ control : capacity bytes ]            empty=0x00, deleted=0x01, full=0x80|H2
[ version : capacity*4 bytes ]          present only when valLen > 8 (seqlock)
[ slots   : capacity*(keyLen+valLen) ]  each slot: key | value
```

Header fields (little-endian):

| Offset | Size | Field |
|---:|---|---|
| 0 | u32 | magic `0x4854_4F41` ("HTOA") |
| 4 | u32 | version |
| 8 | u32 | keyLen |
| 12 | u32 | valLen |
| 16 | u64 | capacity (power of two) |
| 24 | u64 | count (atomic) |
| 32 | u64 | tombstones (atomic) |
| 40 | u64 | control offset |
| 48 | u64 | version-array offset (0 if none) |
| 56 | u64 | slots offset |
| 64 | u64 | slot size |
| 72 | u64 | header checksum (fnv64a over the immutable fields) |

Read this header without mapping the whole store with `core.ReadHeader(file)
(Header, error)` (or `core.Geometry` for just the key/value lengths); both validate
the magic and checksum. The `core header -store <prefix> -shards N` CLI command
renders every shard's header as a table.

Cross-process stores also have a `<path>.meta` file: magic `"HTMX"`, version, and
an atomic generation counter at offset 8.

---

## 16. API reference

### Constructors

```go
func Open(path string, keyLen, valLen, initCap int, opts ...Option) (*Map, error)
func OpenShared(path string, keyLen, valLen, initCap int, opts ...Option) (*Shared, error)
func OpenSharded(path string, keyLen, valLen, shards, capPerShard int, opts ...Option) (*Sharded, error)
func OpenSharedSharded(path string, keyLen, valLen, shards, capPerShard int, opts ...Option) (*Sharded, error)
func WithNoSync() Option
```

### `Store` (implemented by `*Map` and `*Shared`)

```go
type Store interface {
	Get(key []byte) ([]byte, bool)
	GetInto(key, dst []byte) bool
	Contains(key []byte) bool
	Put(key, val []byte) (bool, error)      // (inserted, err)
	Update(key, val []byte) bool            // found
	Delete(key []byte) bool                 // existed
	Prune(aged func(key, val []byte) bool) int // sweep-and-tombstone; returns removed
	Shrink() error                          // rehash down to fit live count
	ForEach(fn func(key, val []byte) bool)
	Digest() uint64
	Batch(fn func(put func(key, val []byte) (bool, error)) error) error
	Reserve(n int) error
	Len() int
	Sync() error
	Close() error
}
```

#### Maintenance helpers (§9)

```go
// Pruner is implemented by *Map, *Shared, and *Sharded.
type Pruner interface {
	Prune(aged func(key, val []byte) bool) int
}

// Age out by a little-endian uint64 at byte offset off in the value.
func PruneBefore(p Pruner, off int, cutoff uint64) int // val word <  cutoff
func PruneFlag(p Pruner, off int, mask uint64) int     // val word &  mask != 0
```

### `*Sharded` (not a `Store`; the top-level orchestrator)

Has the same read/write/iter/integrity/maintenance methods as `Store`
(including `Prune` and `Shrink`) **except** `Batch`, plus:

```go
func (s *Sharded) Shards() int
func (s *Sharded) Shard(i int) Store
func (s *Sharded) PutBatch(keys, vals [][]byte) (inserted int, err error)
func (s *Sharded) PutRecords(buf []byte) (int, error)
func (s *Sharded) DeleteBatch(keys [][]byte) (deleted int, err error)
func (s *Sharded) DeleteRecords(buf []byte) (deleted int, err error) // packed keyLen-byte keys
```

For a coalesced change set (inserts/updates + deletes) built, transported, and
applied as one unit, the `github.com/netstar-labs/core/patch` package layers on
these primitives — see §21.

### Staged, larger-than-RAM build (§8)

Sizing, group/part routing, and the staged ingest, owned by the package so callers
never re-derive the table's geometry:

```go
// Sizing: the geometry a store ends up with for (keyLen, valLen, shards, reserve).
func PlanLayout(keyLen, valLen, shards, reserve int) (Layout, error)

// Routing: bucket a key (or its hash) into one of `parts` part files.
func NewPartitioner(keyLen, valLen, shards, reserve, parts int) (*Partitioner, error)
func (p *Partitioner) Part(h uint64) int
func (p *Partitioner) PartKey(key []byte) int
func (p *Partitioner) Parts() int

// Self-describing parts: write/read the geometry sidecar beside <prefix>.pNNN.
func ManifestPath(prefix string) string         // "<prefix>.manifest.json"
func WriteManifest(path string, m PartsManifest) error
func ReadManifest(path string) (PartsManifest, error)
func (m PartsManifest) Partitioner() (*Partitioner, error)

// Build: load <prefix>.pNNN in group order into one pre-sized store.
// flushPerPart Syncs after each part to bound resident dirty pages.
func IngestParts(prefix, store string, keyLen, valLen, shards, reserve, capPerShard int,
	flushPerPart bool, opts ...Option) (IngestStats, error)
func IngestManifest(manifestPath, store string, capPerShard int,
	flushPerPart bool, opts ...Option) (PartsManifest, IngestStats, error)

// Export: re-bucket a live store back into <prefix>.pNNN + a manifest (inverse of
// IngestParts) — a dense, geometry-independent parts set. Same geometry across two
// exports makes them part-aligned for a delta (§21).
func ExportParts(store, prefix string, keyLen, valLen, shards, reserve, parts int,
	opts ...Option) (PartsManifest, error)
```

### Constant

```go
const PageSize = 4096
```

---

## 17. Errors

| Error | Meaning |
|---|---|
| `ErrBadSize` | `keyLen`/`valLen` invalid (not a nonzero multiple of 8) or mismatched with an existing file. |
| `ErrCorrupt` | The file header did not validate (bad magic or failed geometry checksum). |

`Put` returns `ErrBadSize` if a key/val argument length doesn't match the store's
sizes. `OpenSharded` returns an error if `shards` is not a power of two.

---

## 18. Limitations and caveats

- **Deletes don't rebalance.** Tombstones accumulate and are reclaimed only when a
  resize rehashes the table. Delete-heavy workloads stay correct but can leave the
  table sparser between resizes.
- **`Digest` is O(n)** (on-demand scan). Great for verification/fsck/replica
  compare; not for high-frequency polling. (A persisted incremental digest is a
  possible opt-in extension.)
- **Header checksum covers geometry, not data.** It catches corrupt offsets/sizes
  (prevents panics), not bit-rot in the slots. Use `Digest` to detect content
  corruption.
- **No crash-atomic torn-write detection** of the data itself; rely on `Sync`
  ordering and, for resizes, the atomic rename.
- **Cross-process writer lock is unix-only** (flock); other platforms degrade to
  single-writer-only correctness.
- **Sizes are fixed at creation** and can't change without rebuilding.
- The on-demand `Digest` and `ForEach` are **best-effort snapshots** under
  concurrent writes.

---

## 19. Sizing for 350M entries

> **Tip:** `core plan` computes all of this for a real dataset — it counts rows
> and prints the power-of-two capacity, load factor, recommended shard count,
> on-disk size, and partition sizing. It targets the current machine by default;
> pass `-cores`/`-ram` to size for a different box:
>
> ```
> go run ./cmd/core plan -data data.txt            # or -format bin, or -rows N
> go run ./cmd/core plan -data data.txt -sample 1000000     # + estimate dup rate
> go run ./cmd/core plan -rows 380000000 -cores 64 -ram 256 # target a 64-core/256 GiB box
> ```
>
> `-sample N` adds a reservoir-sampled estimate of the distinct-key rate (distinct
> key text, matching how the store dedups — keys are SHA-256-hashed, so only exact
> duplicates collapse), so you can see how far the live entry count will fall below
> the row count.
>
> `-commit <name>` materializes the recommendation as an **empty, pre-sized** sparse
> store at `<name>.000…NNN` (one `Reserve` per shard, no entries) — geometry is
> written to disk so later `ingest`/`digest`/`lookup` auto-detect it. It refuses to
> overwrite an existing store.
>
> ```
> go run ./cmd/core plan -rows 380000000 -keylen 8 -shards 8 -commit records.core
> # → empty 8/8 store, 8 shards, ~8.5 GiB apparent (sparse); then fill it:
> go run ./cmd/core ingest-bin -data records.bin -store records.core -shards 8   # geometry auto-detected
> ```

Example: 350M entries, 32-byte keys, 16-byte values.

- Per slot ≈ 48 bytes. At ~0.6 load after a resize, slots ≈ `350M / 0.6 × 48` ≈
  **28 GiB**; control array ≈ `capacity` bytes (~600 MB, stays resident).
- Create the file **sparse** with a generous `capPerShard`, or `Reserve` up front
  to avoid incremental resizes.
- Use `OpenSharded` (or `OpenSharedSharded` for a cluster) with `shards ≈ cores`
  (e.g. 64 on a many-core box).
- Bulk load with packed fixed-size records → `PutRecords` + `WithNoSync` +
  `Reserve`. Extrapolating from 21 M/s on 10 cores, a 32–64 core box lands roughly
  in the 40–80 M/s range (~5–9 s to load 350M), then lock-free reads.
- Put the files on **local NVMe**, leave RAM for page cache, and avoid cgroup
  memory limits that count the page cache.
- **If the store does not fit in RAM** (e.g. ~22–28 GiB on a 16 GiB box), the
  random-write scatter dominates and the 40–80 M/s estimate no longer holds. Load
  in destination order via the partition preprocessor (§8, *Loading a store larger
  than RAM*) to keep the write set RAM-resident.

```go
sh, _ := core.OpenSharedSharded("/data/kv", 32, 16, 64, capPerShard, core.WithNoSync())
_ = sh.Reserve(350_000_000)
// stream packed records in chunks:
//   sh.PutRecords(chunk)   // per ~256K-record chunk
_ = sh.Sync()
```

---

## 20. The `core` CLI and server

### CLI (`cmd/core`)

```
go run ./cmd/core plan   -data testdata/kv                       # recommend shards/reserve/parts (§19)
go run ./cmd/core ingest     -data testdata/kv -store kv.core -shards 8 -reserve 1000000
go run ./cmd/core ingest-bin -data testdata/kv.bin -store kv.core -shards 8
go run ./cmd/core lookup -store kv.core key000000042             # geometry + shards auto-detected
go run ./cmd/core header -store kv.core                          # dump the on-disk header as a table (shards auto-detected)
go run ./cmd/core bench                                          # self-contained: size + build + bench + clean up

# Out-of-RAM loads: partition by destination group, then ingest in order (§8).
# partition writes a manifest sidecar, so ingest-parts needs no geometry flags.
go run ./cmd/core partition    -data testdata/kv.bin -out part -shards 8 -reserve 1000000 -parts 16
go run ./cmd/core partition    -text:on -data testdata/kv -out part -shards 8 -reserve 1000000 -parts 16
go run ./cmd/core ingest-parts -parts part -store kv.core               # geometry from part.manifest.json
go run ./cmd/core ingest-parts -parts part -store kv.core -flush:off    # skip per-part Sync (in-RAM load)

# Transport: digest, pack into one tar.gz, unpack + verify (§20.1).
go run ./cmd/core digest -store kv.core                          # shards auto-detected
go run ./cmd/core pack   -glob 'part.p*' -out bundle.tar.gz -store kv.core -shards 8 -reserve 1000000 -parts 16
go run ./cmd/core unpack -in bundle.tar.gz -dir restore -store kv2.core   # rebuilds + checks digest

# Incremental update: build a patch (upserts + deletes), ship it, apply + verify (§21).
go run ./cmd/core patch-build -upserts add.txt -deletes del.txt -store kv.core -out patch.tar.gz
go run ./cmd/core patch-apply -in patch.tar.gz -store kv.core   # verifies base digest, then applies

# Delta: export a live store back to parts, diff prior vs current into a patch (§21).
go run ./cmd/core export-parts -store kv.core -out part -reserve 1000000 -parts 16
go run ./cmd/core delta old-part new-part -store old.core -out patch.tar.gz   # → patch-apply
```

`bench` is **self-contained** — it needs no dataset or pre-built store. It sizes a
synthetic test set from the machine the way `plan` does (shards from CPU cores,
entry count from a slice of RAM so the store stays in memory), builds it in a
throwaway store, runs concurrent random point lookups, prints a summary table, then
deletes the store. Override the sizing with `-n` (entries), `-shards`, `-lookups`,
or `-procs`; `-keylen`/`-vallen 0` benchmark other geometries (e.g. a key-only set).

When a value is a packed bit-record, `lookup -codex "<spec>"` decodes it into named
fields. Offsets span the full value width (`valLen*8` bits, so a 16-byte value
decodes across all 128 bits; each field is ≤ 64 bits). A spec is a comma-separated
list, either sequential `Name:width` (fields run MSB→LSB with the last at bit 0, so
unspecified high bits are reserved) or explicit `Name:offset:width` (offset from the
LSB — handy to pull a single field). For example
`lookup -store sandbox/records zxdev.com -codex "Reserved:8,Primary:16,Secondary:16,Security:16,Age:4,Risk:4"`
prints each field's bit range, decimal, and hex under the `key = value` line. To
keep a large or reused layout off the command line, pass `-codex @<path>` to load
the spec from a file (one field per line or comma-separated; blank lines and `#`
comments ignored).

For a list of keys, `lookup -in <file> -out <file.csv>` runs a **bulk CSV** lookup:
one key per line in, a CSV out (default stdout) — columns `key,value`, or `key` plus
one integer column per field when `-codex` is given. Not-found keys write an
empty-valued row (so rows stay aligned with the input) and a `bulk: …` summary goes
to stderr. Use `-in=-` for stdin.

Text keys are **hashed by geometry** into the fixed key slot — **SHA-256** for a
32-byte key, **XXH3-128** (canonical big-endian) for a 16-byte key, **xxHash64**
(little-endian) for an 8-byte key — so any input length
maps to a uniform, collision-free key (a key argument given as `2*keyLen` hex chars
is taken as the raw key instead, so exported keys round-trip); `ingest`, `lookup`,
and `partition -text` all derive keys the same way for the store's geometry.
text/`lookup` values are a little-endian uint64. **Geometry** defaults to a 32-byte key + 8-byte value
(40-byte record) and is set per command with `-keylen`/`-vallen` — e.g. an 8/8 store
(16-byte records, ~8.5 GiB for 380M):

```
go run ./cmd/core ingest-bin -data kv8.bin -store kv8.core -shards 8 -keylen 8 -vallen 8
go run ./cmd/core digest     -store kv8.core -shards 8   # geometry auto-detected
```

**`-vallen 0` builds a key-only *set* store** (see §4): slots are packed keys with
no value bytes. Load it from a **newline-delimited list of key strings** with
`ingest-keys` (each line is hashed by geometry, just like `ingest`), or from packed
key-only binary records with `ingest-bin`. `ingest` and `partition -text` read
`key&lt;space&gt;uint64` text and are rejected for a key-only store.
`lookup` reports membership as `present` / `&lt;not found&gt;` instead of a value:

```
go run ./cmd/core ingest-keys -data set.txt -store set.core -shards 8 -keylen 8   # one key per line
go run ./cmd/core ingest-bin  -data set.bin -store set.core -shards 8 -keylen 8 -vallen 0
go run ./cmd/core lookup      -store set.core -shards 8 key000000001   # → present
```

Commands that **open an existing store auto-detect** its geometry from the header
(`core.Geometry` on `<store>.000`), so `-keylen`/`-vallen` are needed only to
**create** a store (`partition`, `ingest-parts`, a fresh `ingest`/`ingest-bin`/`ingest-keys`) — e.g.
`core digest -store kv8.core -shards 8` reads 8/8 on its own. Instead of `-keylen`
you can name the hash with **`-hash`**, which overrides `-keylen` when given and which
`plan` carries into its suggested commands (see *`-hash` and `-keylen`* below).
The library still
**rejects a mismatch on open**, and the geometry must be consistent across the
`partition`→`ingest-parts`→`pack` pipeline; it travels with the data, so you rarely
restate it: `partition` writes a `<out>.manifest.json` sidecar and `ingest-parts`
reads it, while `unpack` reads it from the bundle manifest.
`lookup`, `digest`, and `header` **auto-detect** both geometry and shard count from
the store, so they need only `-store` (you may still pass `-shards` to override, but
the wrong count opens the store with that many shards, creating empty shard files
and misrouting — prefer the default). For `ingest-parts`, the manifest supplies
`-shards`, `-reserve`, and the geometry; only **raw** parts with no manifest require
those flags by hand (a fresh store can't auto-detect a geometry that doesn't exist
yet). `partition` reads packed records by default, or `-text:on`
"key&lt;space&gt;uint64" lines.

Config is parsed with the standard library's `flag` package (same as the server), so
besides `-flag value` each flag also accepts `-flag=value` or `-flag:value`. A bool is
enabled by a bare `-text` or `-text:on` and disabled by `-flush:off` (`on`/`yes`/
`true`/`1` enable, anything else disables). An unknown flag or a stray positional
argument is an error. An uppercased **environment variable** also
overrides (`SHARDS=16 core digest -store kv.core`). `core <sub> -help` (or `core <sub> help`)
prints a subcommand's flags; `core help` lists the subcommands; `core version`
prints the build stamp.

#### `-hash` and `-keylen`

Both flags matter only when **creating** a store; a command that opens an existing
store reads the width from its header and ignores them. `-hash` is a friendlier alias
for `-keylen` that also names the hash; it overrides `-keylen` when both are given.

| `-hash` value | Key width | Hash | Notes |
|---|---|---|---|
| `xxhash64`, `xx`, `64` | 8 | xxHash64, little-endian | Fastest. At 350M entries there is about a 0.3% chance of one silent collision. |
| `xxh128`, `xxhash128`, `xxh3-128`, `128`, `xxh3` | 16 | XXH3-128, canonical big-endian | Fast; negligible collision risk. Not collision-resistant against an attacker. |
| `sha256`, `sha`, `256` | 32 | SHA-256 | The default. Use it for untrusted input. |

Names are case-insensitive. `sha128` and `xxh3-64` are rejected, since they would
silently mean something other than what they say.

`-keylen` is the key width in bytes (default 32) and must be a positive multiple of 8
(`-keylen 4`, `12` or `0` fails with "invalid or mismatched key/value size").

| `-keylen` | Key derived from the text |
|---|---|
| `8`, `16`, `32` | xxHash64, XXH3-128 and SHA-256 respectively (the `-hash` table). |
| any other multiple of 8 | The SHA-256 digest, truncated or zero-padded to fit: `24` keeps the first 192 bits; above `32` the extra bytes are zero, so a `64`-byte key still carries only 32 bytes of entropy. Use these widths only when the key is yours rather than a hash of text. |

A key argument of exactly `2*keyLen` hex characters is taken as the raw key and not
hashed, at every width.

| Command | `-hash` | `-keylen` |
|---|---|---|
| `plan`, `ingest`, `ingest-bin`, `ingest-keys`, `partition`, `ingest-parts`, `pack`, `patch-build`, `export-parts`, `lookup` | yes | yes |
| `digest`, `bench` | no | yes |
| `header`, `unpack`, `patch-apply`, `delta`, `push-parts`, `notices` | no | no |

### 20.1 Packing for transport

`pack`/`unpack` bundle a store for sharing using only the standard library
(`archive/tar` + `compress/gzip`) — no external compressor. The recommended unit
is the **partition files** (packed records): they are dense, smaller than the live
store, and geometry-independent, so the recipient can rebuild at any shard count.

```
go run ./cmd/core pack   -glob 'sandbox/part.p*' -out sandbox/bundle.tar.gz \
                        -store kv.core -shards 8 -reserve 390000000 -parts 16
go run ./cmd/core unpack -in sandbox/bundle.tar.gz -dir restore -store kv2.core
```

The bundle is a single `tar.gz` of the blobs plus a `manifest.json` (written last)
recording the geometry (the same `core.PartsManifest` a `partition` sidecar carries
— `keyLen`/`valLen`, record/shard/reserve/parts counts, `prefix`), plus a per-blob
**CRC-32C** and the store's **`Digest`** (order-independent xxHash64 multiset, §9).
Two layers of verification:

- **Transport** — `unpack` recomputes each blob's CRC-32C and checks size; any
  mismatch fails the restore.
- **Content** — after rebuilding with `ingest-parts`, `unpack` recomputes `Digest`
  and compares it to the manifest. Because `Digest` is independent of shard count
  and insertion order, a bundle packed from 8 shards verifies after rebuilding into
  16 (or any other count).

Notes: gzip level is `-level 1..9` (default 6). Compression ratio is modest when
values are high-entropy (the gain comes from repeated key text). `-kind shards`
bundles the live store files instead for a same-geometry hot restore — use GNU
`tar --sparse` semantics in mind, as the slot region is large; rebuilding from
`parts` is usually smaller and preferred. Duplicate keys resolve last-write-wins,
so reproducible `Digest` across re-partitioning requires shipping the same record
stream (or deduping first) — see §8.

### HTTP server (`server/`)

A `net/http` front end with graceful shutdown (SIGINT drains then `Sync`+`Close`).

```
go run ./server -addr :8080 -store kv.core -shards 8
```

For a large bulk load, start it pre-sized and unsynced — `-reserve N` does one resize
per shard up front (no mid-load regrowth), and `-nosync` skips per-write `msync`,
relying on the graceful-shutdown `Sync` (a hard kill loses unsynced writes):

```
go run ./server -addr :8080 -store kv.core -shards 8 -reserve 350000000 -nosync
```

Pass **`-patch <dir>`** to enable the WAL-backed patch write-buffer (§21): `POST
/patch` then appends coalesced upserts/deletes to a `Log` at `<dir>`, and a flush
loop folds them into the store every `-flush` seconds (or once `-flushat` pending
changes accumulate). On restart the `Log` replays any window a prior run left
un-applied, so an acked batch survives a crash.

| Method & path | Body / params | Purpose |
|---|---|---|
| `POST /job` | `{"item","value"}` | Store one (201 create / 200 update). |
| `GET /job/{item}` | — | Look up by path (200 value / 404 miss). |
| `POST /job/bulk` | JSON array `[{"item","value"}, …]` | Buffered bulk insert. |
| `POST /job/stream` | NDJSON, or `Content-Type: application/octet-stream` packed records; optional `?shard=N` | Streamed ingest (negotiated). |
| `GET /job/export` | `Accept: application/x-ndjson` for JSON, else binary; optional `?shard=N` | Streamed dump (binary has `Content-Length`). |
| `GET /job/digest` | optional `?shard=N` | Content digest (hex) for verifying transfers. |
| `POST /patch` | NDJSON `Change` records `{"item","value","op"}` (`op:"delete"` removes) | Buffered upserts/deletes via the WAL-backed patch `Log`; folded into the store by the flush loop. Registered only with `-patch` (§21). |
| `GET /status` | — | Liveness + entry count. |

Each `item` is **hashed by geometry** into the key — **SHA-256** for a 32-byte key,
**XXH3-128** for a 16-byte key, **xxHash64** for an 8-byte key — the same derivation the CLI uses, so a store is
interchangeable between them. Geometry defaults to 32/8; `-keylen`/`-vallen` set it
for a **new** store, while an existing store's geometry is **auto-detected** from
its header (`core.Geometry`). A `GET` echoes back the `item` you queried, but the
original text is **not** stored — so JSON export emits each key as `2*keyLen`-char
hex (and re-ingesting that NDJSON decodes the hex straight back to the key). The
binary export/stream carries the raw `keyLen`-byte digests, making it the lossless
round-trip path; the `digest` proves either round-trip is faithful.

Example — parallel per-shard transfer with verification:

```bash
for i in 0 1 2 3; do
  curl -s "http://SRC/job/export?shard=$i" \
    | curl -s -XPOST --data-binary @- -H 'Content-Type: application/octet-stream' "http://DST/job/stream?shard=$i" &
done; wait
# prove the copy is faithful:
[ "$(curl -s http://SRC/job/digest)" = "$(curl -s http://DST/job/digest)" ] && echo OK
```

Example — apply partition files (§8) to a running server. `core push-parts` streams
each `part.pNNN` in order to the **whole store** (never `?shard=N`: parts are
group-index-partitioned across all shards, so the server must route by hash), then
prints the server's content digest:

```bash
go run ./cmd/core push-parts -parts part -url http://localhost:8080
```

#### Choosing your own key hash

The store never sees your item text — only the finished `keyLen`-byte key (§4). The
SHA-256/XXH3-128/xxHash64 split in the CLI and example server is just a convenient default,
and it lives in **one place**: the `makeKey` function (`server/worker.go`, mirrored
in `cmd/core/main.go`). Every write and read path funnels through it, so swapping
the hash is a single edit — the library, the wire format, and the digest are all
unaffected. You have three ways to use a different scheme in your own system:

**1. Hash client-side and send hex (no server change).** The server's `makeKey`
treats an item of exactly `2*keyLen` hex chars as the **raw key**, so a client can
derive the key with any algorithm and POST it as hex:

```bash
# key = BLAKE2b-256(item) computed by the client, sent as 64 hex chars
curl -s -XPOST localhost:8080/job -d '{"item":"<64-hex-blake2b>","value":42}'
```

**2. Swap the hash in the example server.** Edit the `case 32` branch of `makeKey`
in `server/worker.go` to use, e.g., **BLAKE2b** (`go get golang.org/x/crypto/blake2b`):

```go
import "golang.org/x/crypto/blake2b"

func makeKey(dst []byte, item string) {
	if len(item) == 2*len(dst) { // hex passthrough (unchanged)
		if tmp, err := hex.DecodeString(item); err == nil {
			copy(dst, tmp)
			return
		}
	}
	switch len(dst) {
	case 32:
		sum := blake2b.Sum256([]byte(item)) // ← was sha256.Sum256
		copy(dst, sum[:])                    // blake2b-256 is 32 bytes == keyLen
	// case 8: ... keep xxHash64, or use blake2b.Sum256(...)[:8], etc.
	}
}
```

**3. No hash at all.** If your identifiers already fit in `keyLen` bytes and are
unique within that width, store them raw (zero-padded), skipping hashing entirely:

```go
func makeKey(dst []byte, item string) {
	for i := range dst {
		dst[i] = 0
	}
	copy(dst, item) // raw bytes, zero-padded; truncates if item > keyLen
}
```

Caveat: raw or truncated keys **collide** when two items share the first `keyLen`
bytes — a hash spreads them out, which is exactly why the defaults hash. Use raw
keys only when items are guaranteed `≤ keyLen` bytes and distinct.

Whichever you pick, derive keys the **same way everywhere** that touches the store
(server `makeKey`, CLI `makeKey`, and any client that pre-hashes to hex). The
content `digest` is computed over the stored key bytes, so two stores match only if
their keys were generated identically.

---

## 21. Patches: incremental updates

A **patch** is the difference you fold into an existing store: new keys, updated
values, and removals. Because the store's insert is already an **upsert** (a
repeated key overwrites its value, §7), "insert" and "update" are one record
stream; only deletion is structurally distinct (key-only, and it tombstones). So a
patch carries two streams — **upserts** (`key|value`) and **deletes** (`key`) —
applied as one unit. The `github.com/netstar-labs/core/patch` package builds,
transports, and applies them; it is mechanism-only (no HTTP, no CLI), so the CLI
and example server are just two ways to drive it.

**Coalescing makes it safe.** A `Builder` records the *last* operation per key, so
the resulting `Snapshot` is **disjoint** — a key is in exactly one stream. That
makes `Apply` order-independent and **idempotent**: re-applying the same patch, or
one a crash left half-applied, converges to the same store.

```go
b := patch.NewBuilder(keyLen, valLen)
b.Upsert(key, val) // insert or update
b.Delete(key)      // remove
snap := b.Snapshot(baseDigest)         // coalesced, disjoint; baseDigest = target store's Digest, or ""
stats, err := patch.Apply(store, snap) // PutRecords(upserts) + DeleteRecords(deletes), in place
```

`Apply` is built on the bulk primitives `Sharded.PutRecords` and the new
`Sharded.DeleteRecords` (§7), so a patch is shard-parallel.

### Transport

`patch.WriteBundle` packs a `Snapshot` into a `tar.gz` via the `core/bundle`
layer (per-blob CRC-32C, §20.1): an upsert blob, a delete blob, and a sidecar
recording the geometry, the upsert/delete counts, and the **base digest** the
patch was built against. `patch.OpenBundle` extracts and CRC-verifies it back into
a `Snapshot`. The CLI wraps this:

```
core patch-build -upserts add.txt -deletes del.txt -store kv.core -out patch.tar.gz
core patch-apply -in patch.tar.gz -store kv.core
```

- `patch-build` reads an upsert source (`key<space>uint64` text, or packed
  `key|value` records with `-bin`) and a delete source (one key per line, or packed
  keys with `-bin`), coalesces them, and records the target store's `Digest` as the
  patch **base**.
- `patch-apply` extracts + verifies the bundle, **refuses** to apply if the store's
  current digest ≠ the patch base (a guard against applying to a diverged store —
  override with `-force=on`), then applies and reports the new digest.

### Live accumulation (the `Log`)

For a long-running process streaming changes between flushes, `patch.Log` is a
durable, coalescing accumulator: a write-ahead log of fixed-size frames plus an
in-RAM last-writer-wins index over its un-applied tail.

```go
l, _ := patch.Open(walDir, keyLen, valLen) // replays any un-applied window
l.Upsert(key, val); l.Delete(key)          // append + coalesce
l.Sync()                                    // group-commit: make appends durable
stats, _ := l.Flush(store)                  // fold the coalesced window into the store, in place
```

`Flush` seals the active WAL segment, swaps in a fresh one (so appends continue
uninterrupted — a double-buffer), makes the sealed segment durable, applies its
snapshot, `Sync`s the store, and only then drops the segment. A crash before that
removal leaves the segment to be **replayed and re-applied** (idempotently) on the
next `Open` — so the apply need not be atomic to be crash-safe. `Pending()` reports
the un-flushed change count, for size-triggered flushing.

The example server wires this behind **`-patch <dir>`**: `POST /patch` streams
`Change` records (`{"item","value","op"}`, `op:"delete"` removes) into the `Log`
and `Sync`s each request before acking; a background loop flushes every `-flush`
seconds or once `-flushat` changes accumulate; shutdown drains the buffer. Reads
see a change after the next flush — the accumulate-then-apply trade-off. See
`server/main.go` (`patchFlushLoop`) and `server/worker.go` (`StreamPatch`) for the
~20 lines of wiring.

### 21.1 Deltas: diffing two snapshots

You can also derive a patch by **diffing two store snapshots** instead of listing
the changes — incremental replication, shipping only what changed. It rests on a
property of the partition scheme: **part assignment is a pure function of the key
and the geometry** (`Part(key) = ((sum(key)>>7) & mask) >> shift`, with `mask`/`shift`
from `shards`/`reserve`/`parts`). So two stores exported with the *same* geometry
are **part-aligned** — `old.p007` and `new.p007` cover the identical slice of the
keyspace, and a key never moves between parts across snapshots.

`ExportParts` re-buckets a live store back into partition files (the inverse of
`IngestParts` — a dense, geometry-independent set, smaller than the live shard
files). `patch.Diff` then diffs each aligned part pair: a key only in new, or whose
value changed, is an upsert; a key only in old is a delete. The union of per-part
diffs is the complete delta, computed **one part at a time** (memory bounded by
1/parts of the store, not the whole store).

```go
core.ExportParts(oldStore, "old", keyLen, valLen, shards, reserve, parts) // prior snapshot → old.pNNN
core.ExportParts(newStore, "new", keyLen, valLen, shards, reserve, parts) // current snapshot → new.pNNN
snap, _ := patch.Diff("old", "new", oldDigest) // coalesced delta; base = old store's digest
patch.WriteBundle(w, snap, -1)                 // ship it; apply with the patch path
```

The CLI mirrors this; the base digest is the **old** store's, so `patch-apply`
refuses unless the target is actually at the old state:

```
core export-parts -store old.core -out old -reserve 1000000 -parts 16
core export-parts -store new.core -out new -reserve 1000000 -parts 16
core delta old new -store old.core -out patch.tar.gz   # diff → patch bundle
core patch-apply -in patch.tar.gz -store far.core      # far.core must be at old's digest
```

Both exports **must** use the same `(shards, reserve, parts, keyLen, valLen)` or the
parts won't align; `Diff` reads both manifests and refuses a mismatch. The three
pieces compose into verified incremental replication:
`export-parts(prior)` + `export-parts(current)` → `delta` → ship → `patch-apply`.
