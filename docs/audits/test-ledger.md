# core — coverage ledger (02 pass, phase 0)

`go test -cover ./...`, 2026-07, on the audit-fix branch. **Verified** numbers.

## Per-package baseline

| Package | Coverage | Note |
|---|---|---|
| `core` (root) | **77.8%** | the table, three backends, partitioning, prune/resize |
| `core/patch` | **77.4%** | diff / bundle / log |
| `core/bundle` | **75.5%** | pack / unpack / manifest |
| `core/internal/xxhash` | **100%** | |
| `core/internal/mmap` | **48.0%** | platform-split; only the host's build tag is measured |
| `core/cmd/core` | **0.0%** | tests exist (`cli_test.go`) but exercise the built binary as a subprocess, so no statements are attributed — the number is a measurement artefact, not an absence of tests |
| `core/server` | **no test files** | the real gap, see below |
| `core/examples/*` | **no test files** | runnable documentation; `go build ./...` covers them |
| `core/sandbox` | — | gitignored local scratch, not part of the repo |

## Classification by surface

| Surface | Kind | Status |
|---|---|---|
| `table` probe/insert/erase/prune | unit | covered (`map_test.go`, `delete_test.go`, `prune_test.go`, `set_test.go`) |
| Geometry & header parse | unit + **fuzz** | covered; fuzz added this pass |
| `Map` lifecycle, resize, shrink | unit | covered, incl. the undercount regression |
| `Shared` cross-process generation/flock | unit | covered (`shared_test.go`) — single-process simulation of two openers |
| `Sharded` routing, batch, records | unit | covered (`scale_test.go`, `export_cap_test.go`) |
| `patch` diff/apply/log | unit | covered |
| `bundle` pack/unpack | unit | covered |
| `internal/mmap` | unit | partial — one platform per run |
| `cmd/core` CLI | harness (subprocess) | covered behaviourally, 0% by statement |
| `server/` HTTP routes + worker | **uncovered** | needs a seam, see [untestable-without-x.md](untestable-without-x.md) |
| Larger-than-RAM fault behaviour | **untestable here** | needs real hardware + working set |

## Added this pass

- **3 fuzz targets** where there were none: `FuzzOpenStore`, `FuzzReadHeader`,
  `FuzzOpenGeometry`. The header is the module's only untrusted parse surface and it feeds
  `unsafe.Pointer` arithmetic; its being unfuzzed is why C-7 reached a published release.
- **11 regression tests** in `audit_fixes_test.go`, each the reproduction that confirmed a
  finding — including 7 crafted-geometry shapes, each with the checksum re-sealed so only the
  new validation can reject them.

## Gaps not closed in this pass

Each is emitted as a `type:test` issue per house policy, so it lands on the tracker rather
than only here:

1. **`server/` has no tests at all** (399 + 255 + 160 lines). It binds a socket in `main` with
   no `Serve(listener)` split, so nothing can drive it hermetically.
2. **`cmd/core` reports 0% by statement.** Behaviourally covered, but a coverage gate would
   read it as untested. Either accept it with a note in CI or restructure the CLI into a
   testable `run(args, io)` core.
3. **`internal/mmap` at 48%** is a floor, not a real number: the other platform's file is never
   compiled on a given host. Cross-platform CI would raise it honestly.
4. **No fuzzing on `patch`/`bundle` wire formats.** `OpenBundle` and `Unpack` read
   attacker-supplyable archives — the same trust-boundary argument as the header, and the same
   reason to fuzz them.
5. **`Shared`'s cross-process paths are tested in one process.** A real two-process test needs
   the subprocess harness `cmd/core`'s tests already demonstrate.

## Follow-up pass (2026-07) — the gaps above, closed

| Was | Now |
|---|---|
| `server/` 0%, no test files | **76.2%** — routes, worker, streams, exports, digest, patch buffer, flush loop, drain |
| `bundle` unfuzzed | 3 targets, **79.6%** coverage; found and fixed a truncated-archive acceptance |
| `patch` unfuzzed | 4 targets incl. WAL append/replay |
| `Shared` tested in one process | 6 two-process tests via a `TestMain` re-exec harness |
| resize durability unasserted | ordering proven per resize; fault injection scripted for the kernel half |
| no fault-path measurement | `bench/largerthanram` + `build/bigbench`; write-path baselines recorded |
| third-party notice not shipped | embedded in the binary (`core notices`), emitted into the delivery, drift-guarded in CI and at build |

Ten fuzz targets total across the module now (3 root, 3 bundle, 4 patch), from three at
the start of this work.
