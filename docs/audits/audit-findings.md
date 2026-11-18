# core — audit findings (adversarial pass)

Torture-chamber pass over `main` at `d02ae2c`, 2026-07. Four dimensions (A simpler
pathways · B duplication · C correctness + optimization · D doc-vs-code drift), then an
adversarial verification pass that tried to **refute** every candidate before any of it was
believed.

**Method deviation, stated plainly:** the template sanctions running this "as a multi-agent
workflow (parallel auditors → parallel verifiers) **or inline in one loop**"; this pass was
run inline, single-context. The substance the method depends on was kept — refute-first
posture, and an empirical reproduction for every C-dimension claim, written **against a
copy** of the repo, never the source. Six candidates died there. Read the REFUTED section
before trusting the CONFIRMED one: it is the evidence that the verification actually ran.

**Baseline** (before fan-out): `go vet`, `staticcheck`, `gofmt -l` all clean. `deadcode
-test ./...` reported 5 unreachable funcs — all benign, see A-1. Non-test source: 8,828
lines over 49 files.

**Scorecard**

| Verdict | Count |
|---|---|
| CONFIRMED, fixed this pass | 6 |
| PLAUSIBLE, documented not fixed | 1 |
| REFUTED | 6 |

Tier: core is a shared library imported by several downstream services, which makes
every one of these a shared-blast-radius change.

---

## CONFIRMED — fixed

### C-1 · sev:high — `roundCapacity` spins forever on a negative capacity  → #15

`roundCapacity(n int)` doubled from 8 `for c < uint64(n)`. `uint64(n)` on a negative int is
~2^64, so `c` walked past 2^63, the shift wrapped it to **0**, and `0 < huge` held forever —
an unkillable spin with no allocation and no syscall to show for it.

Six call sites feed it, and the arithmetic that reaches it — `(reserve*loadDen)/loadNum + 1`
— legitimately goes negative:

| Entry point | Input | Result |
|---|---|---|
| `Open` / `OpenShared` | negative `initCap` | hang, before any I/O |
| `Map.Reserve` / `Shared.Reserve` | `n ≤ -10` | hang |
| `ReserveCapPerShard` | negative reserve | hang |
| `Map.Shrink` / `Shared.Shrink` | a live count narrowing negative | hang |

**Reproduced:** `roundCapacity(-1)`, `Open(path,8,8,-1)`, `Reserve(-10)`, `Reserve(-1000)` —
none returned within 2 s.

**Sub-claims refuted by measurement, not assumed:** `Reserve(-1)` and
`Reserve(math.MaxInt)` are *safe*; the arithmetic lands exactly on 0 in both. Had I reported
the whole input space as hanging, two of those would have been wrong.

**Fix:** clamp to `[minCapacity, maxCapacity]` with a bounded loop, so it cannot step onto
2^64. Guarded by `TestRoundCapacityClampsAndTerminates`, `TestOpenNegativeInitCap`,
`TestReserveNonPositiveIsNoop`.

### C-2 · sev:high — `Shrink` sizes the rebuild from a counter that drifts low  → #16

`Shrink` sized the destination from `t.loadCount()`. `insert` publishes the control byte
**before** `addCount(1)`, so a crash in that window leaves a live slot uncounted and the
counter permanently low. A low count sizes the rebuild too small for what `forEachLive` then
hands it — and `insertPlain` probed with an unbounded `for {}`, so it span forever rather
than failing.

**Reproduced:** 200 live slots with the header counter forced to 1, then `Shrink()` — no
return within 3 s.

**Fix, two independent halves.** *Cause:* size from `liveCount()`, an O(capacity) control
scan on a path that is already an O(capacity) rebuild and documented as rare. *Symptom:*
bound `insertPlain` to one probe cycle and return `false`; `buildResized` turns that into an
`ErrCorrupt`-wrapped error naming the undersized target. Guarded by
`TestShrinkSurvivesUndercountedHeader` (which also asserts all 200 entries survive) and
`TestInsertPlainReportsFull`.

### C-3 · sev:med — the resize rename is not crash-durable  → #18

`buildResized` msyncs the rebuilt file, then `os.Rename`s it over the live path. msync makes
the file's *contents* durable and says nothing about the **directory entry** naming it, so a
crash after a successful rename can still come back to the pre-resize inode.

Verified by absence: `grep` over the module finds **no directory fsync anywhere** — every
`Sync()` is an msync or a shard fan-out. The consequence is standard POSIX semantics
(*Reported*, not reproduced — demonstrating it needs power loss).

Doc drift travelled with it: `Map.rehash` said it "**durably** swaps the file into place".

**Cross-validation:** a sibling append-only log fixed this exact class — core's own history carries
"dir-fsync on segment create" for the queue package that has since moved out. The resize
path never got the same treatment.

**Fix:** build-tagged `syncDir` (real on linux/darwin, no-op where a directory handle cannot
be fsynced), called after the rename and gated on the same `doSync` as the msync.

### C-4 · sev:med — `ReserveCapPerShard` divides by zero  → #17

`per := reserve/shards + 1` with no guard, in an **exported** function. `shards == 0`
panics; a negative reserve hangs via C-1. Both reproduced. Fixed by guarding
`shards <= 0 || reserve <= 0` → minimum capacity; a sizing hint is not a place to panic.

### C-7 · sev:high — `openTable` trusts a checksummed geometry  → #21

`openTable` validated magic and checksum, then trusted the header's own `ctrlOff` /
`verOff` / `slotsOff` / `slotSize` / `capacity` for slice indexing and `unsafe.Pointer`
arithmetic. The checksum is **unkeyed fnv64a** — integrity, not authentication — and a store
file *is* untrusted input: `bundle.Pack`/`Unpack` and `patch.OpenBundle` exist to ship
stores between machines.

**Reproduced:** a valid 9,216-byte store with `slotsOff = 1<<40`, `capacity = 1<<20`, and
the checksum re-sealed → `slice bounds out of range [1099511627776:9216]` during `Open`.

**Bounded claim:** Go's bounds checks make this a **panic/DoS**, not memory disclosure. No
read of adjacent memory was demonstrated and none is claimed.

`headerChecksum`'s own comment asserted the opposite — "caught at Open **before it can drive
an out-of-bounds access**" — true for accidental corruption, false against a crafted file.

**Fix:** `validateGeometry` recomputes `layout(keyLen, valLen, capacity)`, a pure function,
and requires the stored offsets to match **exactly**, plus capacity a power of two in range
and the implied file size within the mapping. Exact equality beats a range test when the
right answer is computable. Guarded by `TestOpenRejectsCraftedGeometry` (7 crafted shapes,
each re-sealed) and three new fuzz targets.

### C-6 / D-1 / D-2 / B-1 · sev:low — cleanup sweep  → #20

- **C-6 · PLAUSIBLE mechanism.** `equalKey` compares in 8-byte strides bounded by the
  *table's* key length while indexing the **caller's** slice, so a wrong-length key reads
  past its end. `Map.Contains`/`GetInto` did not validate key length; `Shared`'s read path
  already did, and `Put`/`Update`/`Delete` all do. Isolated proof:
  `equalKey(make([]byte,16), make([]byte,9))` → `index out of range [7] with length 1`.
  End-to-end it needs a control-tag collision **and** a matching first 8 bytes on the same
  slot; 5,000 short-key probes did **not** trigger it, and that is recorded as
  not-reproduced rather than dressed up. Fixed anyway — one comparison, and it makes `Map`
  consistent with `Shared`.
- **D-2 · CONFIRMED.** `GetInto`'s "len must be valLen" precondition failed two different
  ways: a short `dst` **panicked** for `valLen == 8` (`PutUint64`) and **silently truncated**
  for `valLen > 8` (`copy`). Both now report `false`; an oversized `dst` still works, so a
  caller can keep reusing one buffer.
- **D-1 · CONFIRMED, doc-only.** `Map.Update` returns `false` for a wrong-length key or
  value — indistinguishable from "absent" — while `Put` returns `ErrBadSize`. The bool is the
  house convention (`Contains`/`Delete` do the same) and `Update` has no error return, so the
  code stands and the doc now says so.
- **B-1 · CONFIRMED, cross-validates C-1.** The reserve→capacity arithmetic was written out
  at five sites, and `ReserveCapPerShard` documented itself as "correct only while it agrees
  with Sharded.Reserve + Map.Reserve" — a self-declared drift hazard. One `capacityFor`
  helper removes the duplication *and* gives C-1 a single choke point. A finding two
  dimensions surface independently is the high-confidence kind.

---

## PLAUSIBLE — documented, not fixed

### C-5 · sev:med — superseded mappings are retained for the process lifetime  → #19

Every resize keeps the old mapping alive; `Map.rehash` and `Shared.checkRemap`/`resize`
append to `retired`, released only at `Close`. There is no epoch or refcount, so freeing
earlier is genuinely unsafe — a lock-free reader may still be inside the old table.

The sharp edge is on the **read** side: a `Shared` consumer that only reads adopts one
mapping per *writer* resize and retains every superseded one, so its address space grows in
proportion to another process's resize count with no action of its own.

A real fix is an epoch/refcount reclaim scheme — invasive, and easy to get wrong on the
lock-free read path. This pass **documents the limitation** (package doc: "Known limitation:
retained mappings") rather than pretending to solve it. Reopen #19 when a workload needs the
reclaim.

---

## REFUTED — kept, with the reason

A killed finding is a result. Six candidates died here; four of them would have been
plausible-sounding bug reports.

| Candidate | Why it died |
|---|---|
| `equalKey` assumes `keyLen % 8 == 0` and would panic otherwise | `Open` enforces `keyLen > 0 && keyLen%8 == 0`. The assumption is guaranteed, not hoped for. |
| `loadValue`'s `PutUint64(dst, …)` panics for `valLen` 1–7 | `valLen` is 0 or a multiple of 8, so it is 0 (early return), 8 (exactly right), or > 8 (seqlock path). Unreachable. |
| Misaligned 64-bit atomics at `valOff` on non-x86 | `keyLen` and `valLen` are both multiples of 8 and `slotsOff` is page-aligned, so every value offset is 8-aligned. |
| `dirtyset.collect` clears bits and loses writes racing with `Sync` | Only bits *inside* a collected run are cleared, and those pages are in the range being msynced; a chunk marked outside any run keeps its bit and defers to the next Sync — exactly what the comment claims. |
| A 9.4 MB compiled binary and a stray package are committed in `core/sandbox/` | `sandbox/` is in `.gitignore`; `git ls-files sandbox/` is empty. Nothing there is in the repo. **Nearly filed as a repo-hygiene issue before checking.** |
| `Reserve(math.MaxInt)` overflows and hangs | Measured: `(MaxInt*8)/7 + 1` lands exactly on 0, so it returns. |

## A — simpler pathways

**A-1 · no action.** `deadcode -test ./...` reports 5 unreachable funcs. All benign:
`hash.go`'s `Sum`/`SumString` are intentional public API — downstream
services call `core.SumString`, and `deadcode` cannot see
across module boundaries. The other three are in the gitignored `sandbox/`. Nothing to
remove; the tool output is expected for a library.

No other simplification was worth its risk on a module six repos import. The SWAR probe,
the seqlock, and the dirty-set are each doing something specific and are commented to
match.

## Deliberate non-changes

- **Typed fast paths for the value load.** Not attempted: the read path is already
  zero-alloc at 57–69 ns/op serial and 6.5–8.5 ns/op parallel, and there is no measured
  pressure to justify touching it.
- **`ReadHeader` stays lenient.** It parses and returns fields without indexing anything, so
  a crafted-but-checksummed header is harmless there, and it is the diagnostic tool a caller
  uses *before* deciding to open. `openTable` is where strictness belongs.
- **`Update` keeps its bool return.** Changing it to return an error is a breaking change to
  a module six repos import, to distinguish a case the house convention already covers.
