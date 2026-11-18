# core — harness inventory (02 pass, phase 1)

What exists to drive the code hermetically, and what each can and cannot simulate. All
stdlib-only; the module has no third-party dependency in the test tree and none was added.

## Existing

| Harness | Where | Simulates | Cannot simulate |
|---|---|---|---|
| **Temp-dir store fixtures** | `t.TempDir()` throughout | a real mmap'd store on a real filesystem, at any geometry | a filesystem that fails mid-write; a table larger than RAM |
| **Two-opener `Shared`** | `shared_test.go` | generation bump + remap between two `Shared` handles | two real processes — both openers are in one address space, so the flock is uncontended |
| **CLI subprocess driver** | `cmd/core/cli_test.go` | the built binary end-to-end, real argv, real files | statement coverage (attributed to the subprocess, hence 0.0%) |
| **Scale fixtures** | `scale_test.go`, `export_cap_test.go` | many-shard routing, capacity/part alignment | disk pressure, partial writes |
| **Crafted-header builder** | `audit_fixes_test.go` (added) | any hostile geometry with a re-sealed checksum — 7 shapes | nothing; this is complete for the header surface |
| **Fuzz seed corpus** | `fuzz_test.go` (added) | real stores at two geometries, plus a pre-sealed hostile one, as mutation seeds | corpora from the wild — no captured store files exist to seed from |

## Added this pass

- **`seedStore(t, keyLen, valLen, n)`** — writes a real store and returns its bytes. Fuzzing a
  binary header from random bytes never gets past the magic check; every target seeds from a
  genuine file so mutation starts inside the parser.
- **The re-seal helper pattern** — `headerChecksum(raw)` after mutating geometry. This is what
  makes the crafted-file tests meaningful: without re-sealing, the checksum catches everything
  and the geometry validation is never exercised.
- **`completes(t, what, fn)`** — runs a call with a deadline and fails if it does not return.
  Three of this pass's defects were infinite loops, which a normal test cannot express: it just
  hangs until the whole binary is killed, with no attribution.

## The single highest-leverage harness still missing

**A `Serve(listener)` split in `server/`.** `server/main.go` binds its socket inside the
startup path, so there is no way to hand it a `net.Listener` from `httptest` and drive the
routes. That one seam would take `server/` from *no test files* (814 lines across
`main.go`/`routes.go`/`worker.go`) to unit-testable in the same pass — routes, the worker
loop, and graceful shutdown all become reachable. Everything else on the gap list is smaller
than this.

Second: **a real two-process `Shared` harness**, reusing the subprocess technique
`cmd/core/cli_test.go` already proves works. The cross-process flock and generation counter are
the part of `Shared` most likely to be subtly wrong, and they are currently only exercised
within one address space.

## Built by the follow-up pass (2026-07)

| Harness | Where | Simulates | Cannot simulate |
|---|---|---|---|
| **Two-process re-exec** | `shared_process_test.go` (`TestMain` routes `-core.role`) | a real second process against one store: writer/reader/grower/lock-holder roles, two page tables, two genuine flocks | more than two processes; a peer on another host |
| **Resize ordering trace** | `resize.go` (`resizeTrace`), `resize_durability_test.go` | the exact durability sequence per resize — `msync → rename → syncdir` — and that `WithNoSync` skips both flushes | that a directory fsync survives power loss (kernel contract; see below) |
| **Fault injection** | `build/faultinject` | dropping un-fsynced metadata on dm-flakey, i.e. power loss between rename and dir-fsync | anything on darwin — it refuses with an explanation rather than pretending |
| **Fault-path benchmark** | `bench/largerthanram/` (nested module) | load / Sync / random / sequential at any store size, reporting the resident set so a "capped" run can be shown to be capped | a cap on darwin without a VM |
| **Capped runner** | `build/bigbench` | memory capped below the store — cgroup v2 scope on Linux, Apple `container` VM on darwin; refuses a cap ≥ store size | — |

Two notes on why these are shaped the way they are:

**The two-process harness measures something a single process cannot.** A process can
always re-acquire its own advisory flock, so an in-process test of `Shared`'s writer lock
proves nothing about exclusion. The child takes the lock and holds it for 3 s; the parent's
`Put` is timed against that and must block. It measured **3.0007 s**.

**The ordering trace exists because the failure it guards is unproducible in-process.**
Asserting `msync → rename → syncdir` closes the half core controls; that a directory fsync
makes the rename durable is POSIX, and remains labelled *Reported* until `build/faultinject`
is run on a Linux host.
