# `core` — Architecture & Pipelines

Diagrams of the major data structures and the flows that move data through them:
the read/write paths inside one store, the bulk and out-of-RAM build pipelines,
transport (bundles), incremental updates (patches, deltas), and cross-process
coherence. Companion to the [user guide](userguide.md) (the prose reference) and
the [executive summary](executive-summary.md).

ASCII so it renders anywhere. Source pointers are given per section.

## Contents

1. [Component layers](#1-component-layers)
2. [One shard on disk](#2-one-shard-on-disk)
3. [Read path (lock-free)](#3-read-path-lock-free)
4. [Write path & resize (single writer)](#4-write-path--resize-single-writer)
5. [Sharded bulk ingest](#5-sharded-bulk-ingest)
6. [Out-of-RAM staged build](#6-out-of-ram-staged-build)
7. [Export: store → parts](#7-export-store--parts)
8. [Transport: bundles](#8-transport-bundles)
9. [Patches: incremental updates](#9-patches-incremental-updates)
10. [Integrity: digest & CRC](#10-integrity-digest--crc)
11. [Cross-process coherence](#11-cross-process-coherence)
12. [End-to-end: incremental replication](#12-end-to-end-incremental-replication)

---

## 1. Component layers

The store is three layers; an operator/transport toolkit wraps them. Everything
below `Sharded` treats keys/values as opaque fixed-size bytes.

```
                       ┌──────────────────────────────────────────────────┐
 operator / transport  │ Partitioner · ExportParts · IngestParts          │
 (build · ship · sync) │ bundle (pack/unpack) · patch (Builder/Log/Diff)  │
                       │ StoreDigest                                       │
                       └───────────────────────┬──────────────────────────┘
                                               │ public API
                       ┌───────────────────────▼──────────────────────────┐
 orchestrator          │ Sharded — routes a key to 1 of N shards by the    │
 (sharded.go)          │           high bits of sum(key); per-shard locks  │
                       └───────┬───────────────────────────────┬──────────┘
                               │ Store / hashStore interfaces   │
                ┌──────────────▼─────────┐       ┌──────────────▼───────────┐
 table backend  │ Map (single-process)   │       │ Shared (cross-process)   │
 (map.go /      │ writeMu, atomic *table │       │ + flock + generation ctr │
 shared.go)     └──────────────┬─────────┘       └──────────────┬───────────┘
                               └───────────────┬─────────────────┘
                       ┌───────────────────────▼──────────────────────────┐
 on-disk table         │ Swiss-style open addressing: control array +      │
 (table.go, one        │ slots, in a MAP_SHARED file, MADV_RANDOM          │
 mmap file per shard)  └───────────────────────────────────────────────────┘
```

`Map` and `Shared` both implement `Store` (and the hash-aware `hashStore`), so a
`Sharded` set can be built over either — single-process or cross-process — with
the same surface.

---

## 2. One shard on disk

Each shard is one sparse `MAP_SHARED` file. The control array (~1 byte/slot) stays
resident; slot pages fault in on demand, so a typical lookup costs one page fault.

```
<store>.NNN  (sparse; ~0 bytes until filled)
┌────────────────┬─────────────────────────────┬──────────────────────────────┐
│ header (page 0)│ control array               │ slots region                 │
│ magic, geometry│ 1 byte per slot:            │ capacity × (keyLen+valLen)   │
│ capacity,      │   0x00  empty               │ packed key|value records     │
│ count, tomb,   │   0x01  deleted (tombstone) │                              │
│ slots offset,  │   0x80|H2  full (H2 = low   │ (faulted on demand; the      │
│ checksum       │            7 bits of hash)  │  control array stays hot)    │
└────────────────┴─────────────────────────────┴──────────────────────────────┘
     resident           resident · SWAR-scanned        ~1 page faulted / lookup
```

`count` and `tomb` are atomic counters in the header. Source: `table.go`,
`geometry.go`. Full field table in [user guide §15](userguide.md).

---

## 3. Read path (lock-free)

No locks: a read atomically loads the shard's current `*table` and probes it. A
concurrent resize swaps in a new table; the reader keeps using the one it loaded.

```
GetInto(key) ─► h = sum(key) ──► shard = h >> (64 − shardBits)      (route, sharded.go)
                                      │
                                      ▼  shard's current *table (atomic load)
            tag = 0x80 | (h & 0x7f)         g = (h >> 7) & mask      (H2 tag · group index)
                                      │
   ┌──────────────────────────────── probe loop (linear, wraps on mask) ──────────────┐
   │ word ← 8 control bytes at group g                                                 │
   │ matchByte(word, tag) → candidate lanes → compare full key → HIT (return slot)     │
   │ matchByte(word, 0x00) ≠ 0       → MISS  (an empty ⇒ key was never inserted past)  │
   │ else g = (g + 1) & mask         → next group                                      │
   └────────────────────────────────────────────────────────────────────────────────┘
```

`matchByte` is SWAR (8 lanes per 64-bit word, no SIMD/cgo). The "stop at the first
empty" rule is what bounds probe length. Source: `table.go` `find`, `groupWord`,
`matchByte`; `sharded.go` `route`/`getIntoH`.

---

## 4. Write path & resize (single writer)

Inserts/deletes/resizes take the shard's `writeMu` (and, for `Shared`, a
cross-process flock). Value updates of ≤8-byte values are lock-free (atomic store);
larger values use a per-slot seqlock.

```
Put / Delete(key) ─► writeMu.Lock ─► t = cur.Load()
        │
        ├─ insert(key, val, h):  probe for a home
        │     ├─ key already present  → storeValue (update)            ─┐
        │     ├─ empty/deleted slot   → write key+val, publish tag     ─┤→ return
        │     └─ probe ran full       → grow, then retry               ─┘
        │
        └─ on a NEW key, keep probes short:
              count·8       > cap·7   → grow   (rehash to 2× capacity)
              (count+tomb)·8 > cap·7  → rehash (same capacity; sheds tombstones)

  resize (grow / rehash) — buildResized:
     new inode ← copy LIVE slots only  →  msync (unless WithNoSync)  →  rename in
        │
        └─► cur.Store(newTable)   ◄── ATOMIC SWAP; readers never block or fault
            old mapping → retired[]  (unmapped only at Close, so in-flight readers
                                      on the old table stay valid)
```

`Delete` writes a tombstone (`0x01`); it is reclaimed lazily by the next rehash
(which copies live slots only). Source: `map.go` `insertLockedH`/`grow`/`rehash`,
`resize.go` `buildResized`, `table.go` `insert`/`erase`.

---

## 5. Sharded bulk ingest

`PutRecords` (packed binary) and `PutBatch` ([]key,[]val) skip per-key locking:
bucket by shard in parallel, then insert one goroutine per shard.

```
packed buf:  [ key|value │ key|value │ … ]   (N records)
        │
   ── phase 1 · parallel bucket (one worker per CPU, no shared state) ──────────┐
   │   each worker over its slice:  h = sum(key);  si = route(h)                 │
   │                                local[worker][si] += {offset, h}             │
   └─────────────────────────────────────────────────────────────────────────────┘
        │
   ── phase 2 · one goroutine per shard (shards are independent) ───────────────┐
   │   shard.batchH:  writeMu once  →  insert all its records                    │
   │                  (reuses the hash computed in phase 1 — no re-hash)          │
   └─────────────────────────────────────────────────────────────────────────────┘
        ▼
   <store>.000 … <store>.NNN written concurrently

   Pair with Reserve(n) + WithNoSync  ⇒  one resize per shard, no per-key msync.
   DeleteBatch / DeleteRecords are the symmetric bulk-delete path.
```

Source: `sharded.go` `PutRecords`/`PutBatch`/`DeleteRecords`, backend `batchH`/
`deleteBatchH`.

---

## 6. Out-of-RAM staged build

When a shard's slots region exceeds RAM, inserting in hash order thrashes the page
cache. Stage it: bucket by destination group, then ingest the parts **in order** so
each shard's writes sweep front-to-back.

```
dataset (packed records, or "key<sp>uint64" text)
        │
   ┌────▼── Partitioner (geometry: shards, reserve, parts) ───────────────────────┐
   │   part = ((sum(key) >> 7) & mask) >> shift      ← high bits of the in-shard   │
   │   mask,shift derived from (shards, reserve, parts)   group index g            │
   └────┬──────────────────────────────────────────────────────────────────────────┘
        ▼
   part.p000  part.p001  …  part.pNNN     +     part.manifest.json
        │     each part = one contiguous slice of every shard's slots
        │
   ┌────▼── IngestParts / IngestManifest (geometry from the manifest) ────────────┐
   │   load parts IN ORDER → a shard's dirty window stays ~1/parts of its slots    │
   │   → random writeback becomes a forward sweep the kernel drains as you go      │
   │   flushPerPart: Sync after each part ⇒ resident dirty pages bounded to 1 part │
   └────┬──────────────────────────────────────────────────────────────────────────┘
        ▼
   <store>.000 … <store>.NNN
```

The geometry sidecar (`PartsManifest`) is the single source of `(keyLen, valLen,
shards, reserve)` that partition and ingest must agree on. Source: `partition.go`.

---

## 7. Export: store → parts

`ExportParts` is the inverse of `IngestParts`: re-bucket a live store back into a
dense, geometry-independent parts set (smaller than the live shard files).

```
live store ── ForEach (live entries) ──► for each (k, v):
                                              part = PartKey(k)
                                              │   re-routes by sum(k) — the same
                                              ▼   hash the store placed k with
                          part.p000 … part.pNNN   +   manifest (counts per part)

   feeds:  pack (transport) · push-parts (to a server) · ingest-parts (rebuild)
           delta (diff vs a prior export, §9c)
```

Part assignment is a **pure function of the key and (shards, reserve, parts)** —
the property the delta in §9c relies on. Source: `partition.go` `ExportParts`.

---

## 8. Transport: bundles

`pack`/`unpack` ship parts (or live shards) as one `tar.gz` — stdlib only — with
two independent integrity layers.

```
   parts (or live shards)                                far side
         │                                                   │
    pack ▼  archive/tar + compress/gzip                unpack ▼
   ┌─────────────────────────────────┐               ┌──────────────────────────────┐
   │ blob │ blob │ … │ manifest.json  │ ── .tar.gz ─► │ extract each blob            │
   │                                  │               │ recompute CRC-32C + size     │
   │ each blob: CRC-32C               │               │   vs manifest  → transport OK│
   │ manifest: geometry (PartsManifest│               │ IngestParts → rebuild store  │
   │   ) + store Digest (multiset)    │               │ StoreDigest == manifest      │
   └─────────────────────────────────┘               │   → content OK (any shard #) │
                                                      └──────────────────────────────┘
```

Content verification (`Digest`) is independent of shard count and order, so a
bundle packed from 8 shards verifies after rebuilding into 16. Source: `bundle/`,
`StoreDigest` in `partition.go`.

---

## 9. Patches: incremental updates

Update a store **in place** instead of rebuilding it. Insert is already an upsert,
so a patch is two streams — **upserts** (`key|value`) and **deletes** (`key`).

### 9a. Artifact (build & apply)

```
changes ─► Builder ─► (last-writer-wins per key) ─► Snapshot  (DISJOINT:
            Upsert(k,v) / Delete(k)                  a key is in exactly one stream)

   upserts: [key|value …]      deletes: [key …]
                  └────────────────┬───────────────┘
                                   ▼
          Apply(store):  PutRecords(upserts)  +  DeleteRecords(deletes)
          order-independent · idempotent (re-apply converges)
```

### 9b. Live accumulation (the WAL `Log`)

```
ingress ─► Upsert/Delete ─► append WAL frame (op│key│value) + update coalesce map
                                  │  Sync() = group-commit fsync (durable point)
   flush trigger (time / size) ─► Flush:
        ① seal active segment, swap in a FRESH one  ◄── appends continue (double buffer)
        ② fsync sealed segment
        ③ Apply(coalesced snapshot) → store ;  Sync store
        ④ drop the consumed segment(s)
   crash before ④ → next Open replays the un-applied segment → re-Apply (idempotent)
                    → converges (non-atomic apply made safe by replay)
```

### 9c. Delta (diff two snapshots)

```
   ExportParts(prior)   ─► old.pNNN ─┐
                                     ├─► Diff (per ALIGNED part pair) ─► Snapshot
   ExportParts(current) ─► new.pNNN ─┘      key only in new / value changed → upsert
                                            key only in old                 → delete
   same (shards,reserve,parts) ⇒ parts align: corresponding parts hold the same
   keyspace slice, a key never crosses parts → diff each pair independently,
   one part at a time (memory bounded by 1/parts of the store)
                                     │
                                     ▼  WriteBundle → ship → patch-apply
              base = old store's Digest ⇒ apply refuses a diverged target
```

Source: `patch/patch.go` (Builder/Snapshot/Apply), `patch/log.go` (Log),
`patch/diff.go` (Diff), `patch/bundle.go` (transport).

---

## 10. Integrity: digest & CRC

Three independent checks cover geometry, transport, and content.

```
 header checksum  — geometry validated on open (ErrCorrupt on mismatch)

 CRC-32C          — per blob in a bundle; catches TRANSPORT corruption (the wire)

 Digest           — order-independent multiset:  d = Σ entryHash(key, val)
                    • entryHash mixes xxHash(key) with xxHash(val), summed
                      → commutative ⇒ independent of slot order & tombstones
                    • Sharded.Digest = Σ per-shard digests
                      → independent of shard count
                    • verifies CONTENT: rebuilds, replicas, post-patch state
```

A patch records its **base** Digest; `patch-apply` / `delta` refuse a target whose
Digest doesn't match, so a delta can only land on the snapshot it was built from.
Source: `table.go` `entryHash`/`digest`, `sharded.go` `Digest`.

---

## 11. Cross-process coherence

`Shared` lets many processes map one store: many readers, one writer (flock). A
small shared meta file holds a generation counter so readers notice a writer's
resize and remap the data inode.

```
 process A (writer)              shared meta file (mmap)            process B (reader)
       │                         ┌──────────────────────┐                  │
  resize ─► bumpGen ────────────►│  generation counter  │◄─── gen() load ──┤ every op:
  write flock (single writer) ──►│  (atomic uint64)     │   gen != local ? │ checkRemap
                                 └──────────────────────┘   → remap inode  │ (steady state:
                                                                            │  1 atomic load)
```

Steady-state cost to a reader is a single atomic load + compare; it remaps only
when the generation actually changed. Source: `shared.go` (`metaFile`,
`checkRemap`, `localGen`).

---

## 12. End-to-end: incremental replication

How the pieces compose — build once, then ship and keep in sync.

```
 ┌─ build ──────────────────┐   ┌─ ship ──────────────────┐   ┌─ apply ──────────────────┐
 dataset → partition (§6)        pack → bundle.tar.gz (§8)      far side:
 → IngestParts → store           → transfer → unpack            • unpack → rebuild + verify
                                                                 • push-parts → live server
                                                                 • POST /patch → Log → flush

 ┌─ replicate by delta (§7 + §9c) ────────────────────────────────────────────────────────┐
 export-parts(prior) + export-parts(current) → delta → patch.tar.gz → patch-apply
 ship ONLY what changed; base = old Digest, target verified before and content after
```

Each arrow that crosses a machine boundary is covered by a CRC (transport) and a
Digest (content), so a replica is provably faithful. See [user guide §8, §20–21](userguide.md)
for the runnable commands.
