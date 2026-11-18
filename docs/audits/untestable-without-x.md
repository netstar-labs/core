# core — untestable without X

> **Status, 2026-07:** items 1, 5, and 7 are **closed** (#23, #25, #24); items 2, 3, and 4
> now have their harnesses built and are gated only on a host that can run them —
> `build/bigbench` for the memory cap and `build/faultinject` for the fault injection.
> Item 8 is closed by the `notices` subcommand's tests. What follows records the blocker
> and the artifact for each, and what changed.

"Hard to test" is not an answer. Each entry names the **exact blocker** and the **exact
artifact** that would remove it. Each is emitted as a `type:test` issue so it lands on the
tracker.

## 1. `server/` — 814 lines, no tests

**Blocker.** `server/main.go` binds its listener inside the startup path and exposes no
address, so nothing can drive the routes or the worker loop.

**Artifact required.** A `Serve(l net.Listener)` split — construct the mux and the worker, then
accept on a caller-supplied listener. `httptest.NewServer` then drives `routes.go` directly,
and the worker becomes constructible without a socket at all.

**Highest leverage on this list.** One seam unlocks routes, the worker loop, and shutdown.

## 2. Larger-than-RAM behaviour — the design's whole point, unmeasured

**Blocker.** Every benchmark and test runs against a table that fits in page cache, so they
exercise the SWAR probe and never the fault path. `MADV_RANDOM`, control-array residency, and
the dirty-chunk `Sync` strategy all exist for the regime nothing here reaches.

**Artifact required.** A machine with a known-small RAM ceiling (or a cgroup memory limit) plus
a store sized several times that, and a fixed working-set generator. A laptop cannot produce an
honest number; a container with `memory.max` can.

**Consequence today.** [benchmark-baselines.md](benchmark-baselines.md) is a regression guard
for the in-RAM probe and is labelled as such — not a characterisation of the store.

## 3. Write-path benchmark baselines

**Blocker.** `Put`/`Batch` timings are dominated by page faults and msync, which on a laptop
vary with whatever else is running. Recording them as baselines would invite chasing noise.

**Artifact required.** The same isolated machine as #2, plus a fixed filesystem state per run
(fresh volume, or `fallocate`d file and dropped caches).

## 4. Crash-durability of the resize swap

**Blocker.** C-3's fix (`syncDir` after the rename) cannot be *proven* by a normal test — the
failure mode is a power loss between the rename and the directory flush. The test in place
asserts `syncDir` succeeds and the resize still works, which is necessary and not sufficient.

**Artifact required.** A fault-injection layer: a filesystem that can drop un-fsynced metadata
on command (dm-flakey / a `nbd` device with write-ordering control, or a VM snapshot taken
between the two syscalls). Until then the fix rests on POSIX semantics, and that is labelled
*Reported* rather than *Verified* in the ledger.

## 5. Two-process `Shared` semantics

**Blocker.** `shared_test.go` opens two `Shared` handles in one process, so the advisory flock
is uncontended and the generation counter is exercised without real concurrent mappings.

**Artifact required.** A subprocess harness — the technique `cmd/core/cli_test.go` already uses
— re-execing the test binary in a `-role=writer` / `-role=reader` mode against one store.

## 6. `internal/mmap` cross-platform coverage

**Blocker.** 48% is a floor: `mmap_unix.go`, `mmap_windows.go`, and `mmap_other.go` are
mutually exclusive build tags, so any single run compiles one of them.

**Artifact required.** CI on linux + darwin + windows runners. The number is only comparable
per-platform, and a single-platform gate should say so rather than showing 48% as a deficiency.

## 7. `patch` / `bundle` wire-format fuzzing

**Blocker.** None, actually — this is unbuilt rather than unbuildable. `OpenBundle` and
`Unpack` read archives that arrive from elsewhere, which is the same trust-boundary argument
that made the header worth fuzzing.

**Artifact required.** Nothing new: seed from a real `Pack` output the way `seedStore` seeds the
header targets. Listed here so it is not mistaken for covered.

## 8. `cmd/core` statement coverage

**Blocker.** Tests drive the built binary as a subprocess, so statements are attributed to the
child and the package reports 0.0%.

**Artifact required.** Either a `run(args []string, stdin io.Reader, stdout, stderr io.Writer) int`
core the tests can call in-process, or an accepted CI exemption with this note attached — the
behaviour *is* covered, and a naive coverage gate would misread it.
