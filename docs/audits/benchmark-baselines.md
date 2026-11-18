# core — benchmark baselines

`goos: darwin · goarch: arm64 · cpu: Apple M2 Pro` (10 cores), Go 1.26,
`-benchmem -benchtime=200ms -count=3`, audit-fix branch. **Verified** output.

Ranges are reported rather than averages, per the template. The deviation from the template's
`-count=10` is deliberate: 24 benchmarks × 10 runs is a long wall-clock for a laptop, and the
3-run range is already wide enough to show which numbers are noisy.

## Reads — the hot path

| Benchmark | ns/op (range) | B/op | allocs/op |
|---|---|---|---|
| `Get_Map` | 61.53 – 62.66 | 0 | 0 |
| `Get_Sharded` | 60.70 – 69.21 | 0 | 0 |
| `Get_Shared` | 56.17 – 57.57 | 0 | 0 |
| `Get_SharedSharded` | 58.28 – 60.83 | 0 | 0 |
| `GetParallel_Map` | 6.476 – 6.932 | 0 | 0 |
| `GetParallel_Sharded` | 6.585 – 7.021 | 0 | 0 |
| `GetParallel_Shared` | 6.754 – 7.014 | 0 | 0 |
| `GetParallel_SharedSharded` | 6.577 – 8.467 | 0 | 0 |

**Zero allocations on every read path, every backend.** That is the property to guard: a
regression here is a design regression, not a tuning one.

Serial-to-parallel is ~9× on 10 cores, which is what lock-free reads over a shared mapping
should give.

`GetParallel_SharedSharded`'s 6.58 → 8.47 spread is ~13% — above the template's 10% flag.
**Inferred**, not established: scheduler noise on a busy laptop rather than a property of the
code, since the other three parallel variants sit inside 7% on the same run. Settling it needs
a quiet machine and `-count=10`.

## What these numbers do *not* measure

Stated because the omission changes how the numbers should be read: every benchmark here runs
against a table that fits in RAM, so they measure the SWAR control probe, not the page-fault
path the mmap design exists for. A larger-than-RAM working set is where `MADV_RANDOM`, the
control-array residency, and the dirty-chunk `Sync` strategy actually pay off, and nothing on
this machine represents it. Treat these as a **regression guard for the in-RAM probe**, not as
a characterisation of the store.

## Regression policy

- Any read benchmark gaining a non-zero `allocs/op` is a **failure**, not a tolerance.
- ±15% ns/op run-to-run on a laptop is noise; compare on the same machine and same `-count`.
- The 24-benchmark suite (`Get*`, `Put*`, `Update*`, `Sum*`, `SharedPut*`, `SharedBatch`) is
  the full set; this table covers the read and hash subsets. Write-path numbers are dominated
  by page faults and msync and are not stable enough on a laptop to record as baselines —
  recorded as a gap in [untestable-without-x.md](untestable-without-x.md) rather than as a
  number nobody should trust.

## Write path and the fault path (issue #26)

Measured with `bench/largerthanram` — a nested module, so it never enters the root
module graph. Uncapped local run, 2026-07, M2 Pro / 16 GiB:

```
store: 58,720,256 entries × 16-byte records, 2.13 GiB on disk
load         11m28.286s   11.721 µs/op    85,314 ops/s
sync              953ms   (whole dirty set)
random             53ms      262 ns/op  3,803,342 ops/s
sequential        390ms        6 ns/op
resident       2.13 GiB   ← the entire store
```

**Read those two read numbers as in-cache, not fault-path.** `resident` equals the
store size, so every lookup hit page cache — which is exactly what the harness itself
warns about in its output when the store fits in available memory. On a 16 GiB host,
a store big enough to fault would have to be ~20 GiB, which is why the capped runner
exists (`build/bigbench`, cgroup on Linux / Apple `container` VM on darwin).

**The write-path number is real and is the more interesting one.** 11.7 µs/op sustained
over 58.7M inserts is ~1,800× the in-cache read cost, and `Reserve` was called up front
so this is not a resize cascade — it is dirty-page writeback and fault-in on a store
already past L3. A `Sync` of the whole 2.13 GiB dirty set costs 953 ms.

Regression policy for these: they are wall-clock on a laptop and will move with disk
and cache state. Treat a change of less than 2× as noise; what matters is the shape —
load in the µs/op decade, `Sync` under a second per GiB, and `resident` never far below
the store size on an uncapped run (if it is, the run was constrained and the read
numbers mean something different).
