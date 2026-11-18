# core — dimension C: correctness + optimization

Ranked by severity. Every item here carries its reproduction, written against a **copy** of
the repo. Synthesis and verdicts: [audit-findings.md](audit-findings.md).

## C-1 · sev:high · `roundCapacity` never terminates on a negative capacity

`table.go:143` (pre-fix). `for c < uint64(n)` with `n < 0` compares against ~2^64; `c`
doubles past 2^63, wraps to 0, and the loop is unbounded.

```
roundCapacity(-1)          → no return in 2s
Open(path, 8, 8, -1)       → no return in 2s   (new file, before any I/O)
Reserve(-10)               → no return in 2s
Reserve(-1000)             → no return in 2s
Reserve(-1)                → returns          ((-1*8)/7+1 == 0)
Reserve(math.MaxInt)       → returns          (overflow lands on 0)
```

Fixed at the root, not per call site: `map.go:366`, `map.go:412`, `shared.go:460`,
`shared.go:515`, `partition.go:46` all route through it.

## C-7 · sev:high · `openTable` trusts a checksummed geometry for unsafe arithmetic

`table.go:196` (pre-fix). Magic and checksum validated; `ctrlOff`/`verOff`/`slotsOff`/
`slotSize`/`capacity` then used for slice indexing and `unsafe.Pointer`. The checksum is
unkeyed fnv64a, so a crafted file carries a valid one.

```
valid 9,216-byte store; slotsOff := 1<<40; capacity := 1<<20; checksum re-sealed
Open(...) → panic: slice bounds out of range [1099511627776:9216]
```

Panic/DoS, not memory disclosure — the bounds checks hold, and no adjacent-memory read was
demonstrated. Store files arrive over the wire via `bundle.Pack`/`Unpack` and
`patch.OpenBundle`, so this is reachable input, not just operator error.

## C-2 · sev:high · `Shrink` sizes from a counter that a crash leaves low

`map.go:412`, `shared.go:515` (pre-fix). `insert` does `publishControl` then `addCount(1)`;
a crash between them leaves a live slot uncounted forever. `Shrink` sized from that counter,
and `insertPlain` (`table.go:422`) probed with an unbounded `for {}`.

```
200 entries; storeU64(countPtr, 1); Shrink() → no return in 3s
```

## C-3 · sev:med · the resize rename is not crash-durable

`resize.go:50` (pre-fix). msync then `os.Rename`, no fsync on the parent directory.
Verified by absence across the module. Consequence is standard POSIX (*Reported*).
`Map.rehash`'s comment claimed "durably swaps the file into place".

## C-4 · sev:med · `ReserveCapPerShard` divides by zero

`partition.go:45` (pre-fix). Exported. `ReserveCapPerShard(1000, 0)` →
`integer divide by zero`.

## C-5 · sev:med · retained mappings grow without bound (PLAUSIBLE)

`map.go:431`, `shared.go:229`, `shared.go:531`. One mapping per resize, freed at `Close`
only. Worst on the read side: a `Shared` reader retains one per *writer* resize. No epoch or
refcount exists, so early release is unsafe — documented rather than fixed.

## C-6 · sev:low · `equalKey` strides by the table's key length over the caller's slice
(PLAUSIBLE)

`table.go:261`. Mechanism proven in isolation
(`equalKey(make([]byte,16), make([]byte,9))` → `index out of range [7] with length 1`);
end-to-end not reproduced — it needs a tag collision *and* an 8-byte prefix match on the same
slot, and 5,000 short-key probes did not produce one.

## Optimization

Nothing recommended. Measured baselines on the read path are already zero-allocation:
Get 57–69 ns/op serial, 6.5–8.5 ns/op parallel, 0 B/op, 0 allocs/op across all four backends.
See [benchmark-baselines.md](benchmark-baselines.md). A typed fast path in `grayResize`-style
value loading was considered and rejected for lack of measured pressure — see the
deliberate-non-changes section of the findings ledger.
