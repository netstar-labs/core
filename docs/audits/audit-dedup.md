# core — dimension B: duplication / dedup

## B-1 · CONFIRMED, applied · the reserve→capacity arithmetic, five times over

`roundCapacity((n*loadDen)/loadNum + 1)` — "scale an entry count by the 7/8 load factor, then
round to a power of two" — appeared verbatim at five sites:

| Site | Context |
|---|---|
| `map.go:366` | `Map.Reserve` |
| `map.go:412` | `Map.Shrink` |
| `shared.go:460` | `Shared.Reserve` |
| `shared.go:515` | `Shared.Shrink` |
| `partition.go:46` | `ReserveCapPerShard` (exported) |

`ReserveCapPerShard` had already noticed and documented the hazard in its own doc comment:

> the single source of truth the staged-ingest group math depends on — **correct only while
> it agrees with Sharded.Reserve + Map.Reserve**

That is a correctness invariant maintained by hand across five copies, in a module six repos
import. Staged ingest computes part boundaries from `ReserveCapPerShard` and the live store
computes its own capacity from `Map.Reserve`; a divergence misaligns parts against where keys
actually live.

**Applied:** one `capacityFor(entries int) uint64` helper in `table.go`; all five sites call
it. The five now agree by construction rather than by review.

**This is why it matters beyond tidiness** — it cross-validates C-1. The negative-capacity
hang had five entry points precisely because the arithmetic was copied five times; the helper
gives the clamp a single choke point. A finding that two dimensions surface independently is
the high-confidence kind, and this pass's B and C dimensions landed on the same lines.

## Considered and rejected

- **The four backends' `Get`/`GetInto`/`Contains`/`Update` shapes** (`map.go`, `shared.go`,
  `sharded.go`) look duplicated but are not: `Map` is lock-free over one mapping, `Shared`
  adds a generation check and a cross-process flock, `Sharded` routes by hash to N of either.
  A shared skeleton would have to be parameterised by exactly the parts that differ, and would
  put an interface dispatch on a 6.5 ns/op read path. Left alone.
- **`Map.rehash` / `Shared.resize`** both wrap `buildResized` but differ in what follows —
  `Shared` also bumps the generation counter so peers remap. The shared part is already
  factored into `buildResized`. Left alone.
