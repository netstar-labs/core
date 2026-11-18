# core — defects found and fixed (02 pass)

Every entry: root cause in production code → the fix → the regression test. **No production
code was weakened to make a test pass**, and no defect was papered over in a test.

| # | Defect | Root cause | Production fix | Regression test |
|---|---|---|---|---|
| 1 | `Open(path,k,v,negativeInitCap)` hangs forever | `roundCapacity` compared against `uint64(n)`; a negative int is ~2^64, so the doubling loop wrapped to 0 and never terminated | clamp to `[minCapacity, maxCapacity]`, bound the loop | `TestRoundCapacityClampsAndTerminates`, `TestOpenNegativeInitCap` |
| 2 | `Reserve(-10)` / `Reserve(-1000)` hang | same | same, via `capacityFor` | `TestReserveNonPositiveIsNoop` |
| 3 | `ReserveCapPerShard(_, 0)` panics; negative reserve hangs | `reserve/shards` unguarded in an exported function | guard non-positive → minimum capacity | `TestReserveCapPerShardGuards` |
| 4 | `Shrink` hangs after a crash-induced counter drift | sized from the header counter, which `insert` bumps *after* publishing a slot; `insertPlain` probed unbounded | size from `liveCount()`; bound `insertPlain` and surface a full target as an error | `TestShrinkSurvivesUndercountedHeader`, `TestInsertPlainReportsFull` |
| 5 | A crafted store file panics out-of-bounds on `Open` | `openTable` trusted checksummed offsets for slice indexing and `unsafe.Pointer`; the checksum is unkeyed | `validateGeometry` recomputes `layout()` and requires an exact match | `TestOpenRejectsCraftedGeometry` (7 shapes) + `FuzzOpenGeometry` |
| 6 | The resize swap is not crash-durable | `os.Rename` with no fsync on the parent directory | build-tagged `syncDir`, gated on `doSync` | `TestSyncDirAndResizeStillWork` |
| 7 | `GetInto` with a short `dst` panics (valLen 8) or truncates (valLen > 8) | the documented precondition was unchecked, and the two size classes took different code paths | report `false`; explicitly allow an oversized `dst` | `TestGetIntoDstLength` |
| 8 | `Map`'s read path accepted a wrong-length key | `equalKey` strides by the *table's* key length while indexing the *caller's* slice; `Shared` validated, `Map` did not | validate in `containsH` / `getIntoH` | `TestReadPathRejectsWrongKeyLength` |
| 9 | **(2026-10-21 pass)** A resize whose rename succeeds but whose directory-fsync fails lost the resize entirely: the new table was discarded and the handle kept writing to the old table's now-unlinked inode. Writes after that point were silently lost. Found by independent research reading the source, not by this library's own audit history — see `core-rs`'s `docs/design/recon/R2-lessons-and-tests.md` defect D2. Reachable via both `Map.rehash` and `Shared.resize`; a skeptic review of the first fix (`Map` only) found `Shared` was still broken and additionally now leaking the discarded table's mmap, a regression of the fix itself | `buildResized` now returns the valid new table alongside the error in this specific case; `Map.rehash` and `Shared.resize` both publish it whenever it's non-nil, regardless of the error | `TestRehashSurvivesDirSyncFailure`, `TestSharedResizeSurvivesDirSyncFailure` |
| 10 | **(2026-10-21 pass)** `Put`/insert returned an error for a key that had, in fact, been successfully inserted, whenever a follow-up load-factor-triggered maintenance resize failed. Found the same way as #9 (R2 defect R5); reachable via both `Map.insertLockedH` and `Shared.insertLocked` — a skeptic review of the first fix (`Map` only) found `Shared` was still broken, and that silently discarding the error left a permanently-doomed resize (e.g. a corrupt on-disk state, or a full disk) retrying on every single insert forever with zero visibility | the maintenance call's error no longer affects the insert's own return value in either type; it is instead recorded in a new `MaintenanceErr()` accessor (`Map.MaintenanceErr`, `Shared.MaintenanceErr`) so a caller who cares can detect a resize that is never going to succeed, without the insert path paying for it | `TestRehashSurvivesDirSyncFailure`, `TestSharedResizeSurvivesDirSyncFailure` (both assert `MaintenanceErr() != nil` after the injected failure) |
| 11 | **(2026-10-21 pass)** `dirtyset.collect()` clears a chunk's dirty bit as part of returning it as a range to flush; if the subsequent `msync`/`SyncRange` then failed, the bit stayed cleared, so the *next* `Sync()` saw `dirty == 0` and reported success without the data ever having actually reached disk. Found the same way as #9 (R2 defect D4) | on a flush failure, `syncDirty` re-marks the affected range(s) dirty via `markOff` before returning the error, so a later `Sync()` retries them | `TestSyncDirtyRemarksOnFlushFailure` |

**Process note on #9/#10**: the first pass of each fix touched only `Map`; an adversarial skeptic review (a separate agent, instructed to try to refute the fix rather than confirm it) found the parallel `Shared` type was unpatched for both, found that the #9 fix as first written introduced a new mmap leak in `Shared.resize`, and separately found that `insertLockedH`'s/`insertLocked`'s *other* error-return site (the "table is physically full, must grow before inserting at all" branch — not touched by the first pass) could now spuriously drop a key after a resize that had, in fact, just succeeded. All three follow-on issues were fixed (`TestFullBranchRetriesAfterPublishedGrow` covers the last one, with the honest caveat that it does not reliably force the literal "full" branch itself — see the test's own comment) and the full suite plus `-race` re-run clean. No defect shipped past the skeptic's first pass.

**Not fixed in this pass, found by the same research** (`core-rs`'s R2, section A, IDs M1–M8 minus what's covered above, D1/D3/D5–D9, R3/R4/R6/R7, V4/V6, I5, P1/P2): the lock-free `Update`-vs-resize race (M1), `Update`-vs-`Delete` slot reuse (M2), and non-atomic key/value reads under concurrent slot reuse (M3) are real and reproducible-or-plausible per R2, but each needs an actual concurrency-control redesign of the hot path — not a contained patch — on a library already running in production; rushing one in this pass would trade a found, documented risk for an unreviewed new one. D1 (kernel-level durability proof) needs a Linux fault-injection host this machine doesn't have. D3 (no crash-consistency story for ordinary `Put`) and V6 (unseeded hash, flooding risk) are policy decisions — journaling vs. a documented weaker guarantee; seeding vs. byte-for-byte format compatibility — that belong with whoever owns this library's roadmap, not a unilateral audit fix. The rest are lower-severity and listed in R2 with their own design rules already captured for whoever picks them up next.

## Not a defect, after investigation

Recorded because each looked like one and would have cost a commit:

- **`equalKey`'s 8-byte stride** — `Open` enforces `keyLen % 8 == 0`.
- **`PutUint64` into a sub-8-byte value buffer** — `valLen` is 0 or a multiple of 8.
- **Misaligned 64-bit atomics at the value offset** — both lengths are multiples of 8 and
  `slotsOff` is page-aligned.
- **`dirtyset.collect` losing writes that race `Sync`** — only bits inside a collected run are
  cleared, and those pages are inside the range being msynced.
- **A 9.4 MB binary "committed" in `sandbox/`** — `sandbox/` is gitignored; nothing there is in
  the repo. This one was one command away from becoming a bogus issue.
- **`Reserve(math.MaxInt)`** — measured; the arithmetic lands exactly on 0 and it returns.

## Fuzzing

Three targets added where the module had none. `FuzzOpenGeometry` (78,051 execs) is the
adversarial one — it mutates geometry fields and re-seals the checksum. Pre-fix it reproduced
defect 5 on its first seed. No crashers post-fix, and no crasher corpus to commit.
