# core — a wall of numbered pigeonholes, in a building bigger than the room

*Every item has exactly one hole it can be in, you walk straight to it, and the wall never
has to fit inside the room you are standing in.*

A sorting office does not search. The pigeonholes are all the same size, each one is
numbered, and the number is worked out from the address on the item — so the clerk walks to
one hole, not along the whole wall, and nobody reads every letter to find yours. That is the
entire trick, and it is the only trick core has.

What makes it worth a library is the building. core keeps each table as one memory-mapped
file and lets the operating system be the storage engine: the OS carries forward only the
pigeonhole you reach for, while a thin strip of tags along the wall — one byte per hole,
saying *empty*, *deleted*, or *full, with a fragment of the key's hash* — stays within arm's
reach. In the usual case a lookup costs a single page fault. The file is sparse, so a table
sized for 16 GiB costs almost nothing on disk until it is filled.

## What it actually is

An open-addressing hash table for Go, backed by a memory-mapped file. It uses Swiss-table
style control bytes, scanned eight at a time with plain integer arithmetic (SWAR) — no SIMD,
no cgo. Key and value widths are fixed when the table is created, in multiples of 8 bytes (a
value width of zero makes a key-only set), so every slot is the same size and the Go garbage
collector never has a pointer to follow into it.

It comes in four shapes that share one interface: a single-process table (`Open`), a table
that several processes share (`OpenShared`: many readers, one writer), a sharded table for
concurrent writers and bounded resizes (`OpenSharded`), and both at once
(`OpenSharedSharded`). The repo's own benchmarks, on a 10-core machine with the data in page
cache, report about 6.6 ns per lookup in aggregate across goroutines, about 21 ns for a single
goroutine, and about 21 million entries per second when bulk-loading from packed binary.

## Readers never wait

Reading takes no lock. A value of eight bytes or fewer is updated with one atomic store;
wider values are guarded by a per-slot seqlock that readers check and retry. Writing —
inserting a new key, deleting, resizing — happens one writer at a time per shard, and a
resize builds the new table beside the old one and publishes it with an atomic swap, so a
reader in the middle of a lookup is never left holding a table that has vanished.

## Built to be moved, and checked

A table can be built larger than RAM by sorting its input into destination order first, so
writes sweep each shard front to back instead of thrashing the page cache. It can be packed
into a bundle, shipped, and unpacked. Changes travel as patches — coalesced upserts and
deletes applied as one idempotent unit — and two snapshots can be diffed part by part so only
what changed is sent. Underneath it all is `Digest`: an order-independent hash of a table's
entire contents, so two copies agree when they hold the same pairs, however they were built.
The `core` command and a small HTTP server wrap these steps for use outside Go code.

## The scope it keeps

core is not a database. It has no variable-size records, no queries, no transactions, no
secondary indexes, and no ordering. Sizes are fixed at creation; changing them means
rebuilding. Deletes leave tombstones that are reclaimed only when a resize rehashes the
table. Durability is explicit — `Sync` is the durable point — and the header checksum covers
the table's geometry, not its data: catching corrupt contents is `Digest`'s job, and it is an
O(n) scan. The cross-process writer lock is a unix `flock`; elsewhere a shared table is safe
for a single writer only.

It is the wall and the rule for finding a hole — not the post office, not the ledger, and
not a way to ask "which holes hold something like this?"

*Read next:* [userguide.md](userguide.md) (API, concurrency model, durability, sizing) ·
[architecture.md](architecture.md) (the read, write, build and transport paths, drawn) ·
[executive-summary.md](executive-summary.md) · [../readme.md](../readme.md)
