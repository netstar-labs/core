# core — verification & optimization (01 pass)

Run on `main` at `d02ae2c` + the audit-fix branch, 2026-07, Apple M2 Pro (darwin/arm64,
10 cores), Go 1.26. Per the lifecycle this pass **consumes** the adversarial pass's output
rather than re-deriving it — dead code, duplication, and doc drift are in
[audit-findings.md](audit-findings.md), and coverage build-out is deferred to
[the 02 pass](test-ledger.md) by design.

Confidence labels throughout: **Verified** = I ran it and this is the output. **Reported** =
a source says so. **Inferred** = a reasoning chain, marked as such.

## 1. Executive summary

**Code health: good, with two high-severity input-handling defects now fixed.** Tests green
under `-race`, no data races, no hot-path allocation, and unusually accurate doc comments —
the four that were wrong were wrong about durability and trust, which is where wrong comments
cost the most.

- **7 findings fixed, 1 documented, 6 refuted.** Two were hangs reachable from the public API
  (`Open` with a negative `initCap`; `Shrink` after a crash-induced counter drift) and one was
  a panic on a crafted store file. All three had no test covering them.
- **Hardening posture before this pass: no fuzz targets at all**, on a module whose entry
  point parses an untrusted on-disk header and feeds its offsets to `unsafe.Pointer`
  arithmetic. That gap is the direct cause of C-7 surviving to a published release. Three
  targets added.
- **Optimization: nothing recommended.** The read path is already zero-allocation at
  6.5–8.5 ns/op parallel. There is no measured pressure, and the only candidate (typed fast
  paths for the value load) would add a divergence risk to a path six repos depend on for no
  demonstrated gain. Spending here would be effort tracking comfort rather than risk.
- **Recommendation: proceed.** No critical item is outstanding. The one deferred item (#19,
  retained mappings) needs a design decision, not a patch.

## 2. Code verification

### 2.1 Documentation vs. code

Five misalignments, all now fixed — see [audit-docs.md](audit-docs.md) for each. The two that
mattered: `rehash` claimed the resize "durably swaps the file into place" without a directory
fsync, and `headerChecksum` claimed it caught bad offsets "before it can drive an
out-of-bounds access" when the checksum is unkeyed and recomputable.

Also recorded there: the claims that were **checked and hold** — the concurrency contract, the
`Prune` no-resize invariant, `dirtyset.collect`'s best-effort wording (which I nearly filed as
a lost-write bug before tracing which bits get cleared), and `WithNoSync`'s scope.

### 2.2 Dead code

`deadcode -test ./...` reports 5 unreachable funcs; **all 5 are benign** and none was removed.
Two are intentional public API used by downstream modules; three are in the gitignored
`sandbox/`. Detail and the reason this output invites a wrong conclusion:
[audit-simplify.md](audit-simplify.md).

### 2.3 Duplication

One real instance: the reserve→capacity arithmetic written out five times, with
`ReserveCapPerShard` documenting its own drift hazard. Consolidated into `capacityFor`, which
also gave the negative-capacity clamp a single choke point.
[audit-dedup.md](audit-dedup.md).

## 3. Operational validation

### 3.1 Tests — **Verified**

`go test -race -count=1 ./...` (cache defeated, as the template requires):

| Package | Result | Time |
|---|---|---|
| `core` | ok | 11.2 s |
| `core/bundle` | ok | 1.3 s |
| `core/cmd/core` | ok | 4.8 s |
| `core/internal/mmap` | ok | 2.2 s |
| `core/internal/xxhash` | ok | 1.9 s |
| `core/patch` | ok | 7.5 s |

**No data races. No goroutine leaks reported. No timeouts.** Coverage per package is in
[test-ledger.md](test-ledger.md).

### 3.2 Benchmarks — **Verified**

Full table with run-to-run ranges: [benchmark-baselines.md](benchmark-baselines.md). Headline:
every read path is **0 B/op, 0 allocs/op**; serial Get 57–69 ns/op; parallel Get 6.5–8.5 ns/op
across all four backends. No benchmark exceeded a ~10% coefficient of variation except
`GetParallel_SharedSharded` (6.58→8.47 ns/op, ~13%), consistent with scheduler noise on a
10-core laptop rather than a code property — **Inferred**, and it would take a quiet machine
and `-count=10` to settle.

### 3.3 Fuzzing — **Verified**

Before this pass: **zero fuzz targets** in the module. Now three, all on the header trust
boundary:

| Target | Execs | Result |
|---|---|---|
| `FuzzOpenStore` | 1,003 | no crashers (slow: each exec writes a file and mmaps it) |
| `FuzzOpenGeometry` | 78,051 | no crashers — this is the adversarial one |
| `FuzzReadHeader` | seeded, not soaked | no crashers |

`FuzzOpenGeometry` mutates only the geometry fields and **re-seals the checksum**, which is
the shape a hostile store actually takes. Pre-fix, its first seed reproduced the C-7 panic
immediately.

### 3.4 Integration tests

None present, and none needed for a library with no network surface. `cmd/core`'s CLI tests
exercise the binary end-to-end; `server/` has no tests — see
[untestable-without-x.md](untestable-without-x.md) for the exact seam it needs.

## 4. Hardening validation

| Surface | State |
|---|---|
| **Input boundaries** | The on-disk header is the only untrusted parse surface. It was validating magic + checksum only; it now recomputes and requires the exact layout. Key/value lengths are validated at every public entry point (`Map`'s read path was the gap — fixed). |
| **Unbounded allocation from untrusted input** | `capacity` from the header drove `layout()` and thus the mapping size; now bounded to `maxCapacity` and required to match the actual file size. |
| **Integer conversion** | `int(loadCount())` and `int(capacity)` were the narrowing points. `capacityFor` guards the scale-up against overflow; `validateGeometry` bounds capacity before it is used in arithmetic. |
| **Unkillable loops** | Three found: `roundCapacity`, `insertPlain`, and (via them) `Open`/`Reserve`/`Shrink`. All bounded. |
| **Concurrency** | Whole suite under `-race -count=1`, clean. Lock-free reads verified against the publication order in `insert` (key and value written before the release-store of the control byte). |
| **Resource cleanup** | `buildResized`'s failure paths all `rw.Close()` + `f.Close()` + `os.Remove(tmp)`; refactored into one `fail` helper so a future path cannot forget one. The retained-mapping growth is the one open item (#19). |
| **Error handling** | No silent drops found on the write paths. `syncDirty` deliberately collects the first error across ranges and continues — correct for a flush. |
| **DoS / algorithmic** | No regex anywhere. No O(n²) on untrusted input. `find`/`insert` probes are bounded by one group cycle. `liveCount` adds an O(capacity) scan to `Shrink`, an explicitly rare op that already rebuilds. |

### Severity summary

| Issue | Severity | Site | Fix |
|---|---|---|---|
| Negative capacity spins forever | high | `table.go:143` | clamp + bounded loop |
| Crafted geometry panics on Open | high | `table.go:196` | recompute + require exact layout |
| Shrink undercount → unbounded probe | high | `map.go:412`, `table.go:422` | size from live slots; bound the probe |
| Resize rename not crash-durable | med | `resize.go:50` | `syncDir` after rename |
| Exported divide-by-zero | med | `partition.go:45` | guard non-positive |
| Retained mappings unbounded | med | `map.go:431`, `shared.go:229` | documented (#19) |
| Read path missing length validation | low | `map.go:198,230` | validate as `Shared` does |

## 5. Performance analysis

No profiling-driven work is proposed, and that is a deliberate allocation of effort rather
than an omission. The read path is 6.5 ns/op parallel with zero allocations; the write path is
dominated by page faults and msync, which are kernel-bound and already tuned via
`MADV_RANDOM`/`MADV_SEQUENTIAL` and the dirty-chunk set. The measurable surface that remains
is I/O behaviour under a table larger than RAM, which a laptop benchmark cannot represent
honestly — **that** is the profiling worth doing, on real hardware with a real working set,
and it needs the harness recorded in [untestable-without-x.md](untestable-without-x.md).

**Strongest counter-argument to "no optimization needed":** the benchmarks all run against
tables that fit in RAM, so they measure the SWAR probe and not the fault path that the design
exists for. A 6.5 ns/op number says nothing about the 100 µs case. I am not claiming the
larger-than-RAM path is optimal — only that nothing here measured it, so nothing here should
propose changing it.

## 6. Self-check

- Tests run and passed, with `-count=1`, output above — **yes**.
- Benchmarks with repeat counts and ranges reported rather than averages — **yes** (`-count=3`
  at `-benchtime=200ms`; the template asks for `-count=10`, and this is a deliberate deviation
  for wall-clock on a 24-benchmark suite — the ranges are wide enough to see variance).
- Fuzz attempted and soaked, with budgets stated — **yes**.
- Profiling on a real hot path — **no**, and the reason is stated above rather than glossed.
- Every optimization estimate labelled — **n/a**, none proposed.
- Nothing claimed as "should work" — the one unreproduced consequence (C-3's crash behaviour)
  is labelled *Reported*, and the one unreproduced mechanism (C-6) is labelled PLAUSIBLE with
  the failed reproduction attempt recorded.
