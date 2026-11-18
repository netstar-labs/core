# core — dimension A: simpler pathways

Thin by design. This is an mmap-backed open-addressing table where the complexity is
load-bearing — the SWAR control scan, the seqlock for >8-byte values, the dirty-chunk set —
and each piece is commented to match what it does. Simplifying any of it trades measured
performance or cross-process correctness for shorter code, on a module six repos import.

## A-1 · no action · `deadcode` reports 5 unreachable funcs, all benign

```
hash.go:13:6              unreachable func: Sum
hash.go:17:6              unreachable func: SumString
sandbox/codex.go:47:17    unreachable func: Codex.Encoder
sandbox/codex.go:86:6     unreachable func: atoi
sandbox/codex.go:90:17    unreachable func: Codex.Decoder
```

- **`Sum` / `SumString` are intentional public API**, documented as "exposed so callers can
  pre-compute digests, partition keys, or shard data the same way Core does" — and they are
  used by downstream modules. `deadcode` cannot see across module boundaries, so this
  output is expected for a library and is not a finding.
- **The three `sandbox/` entries are not in the repo.** `sandbox/` is gitignored
  (`.gitignore:7`); `git ls-files sandbox/` is empty. They are local scratch that the
  working-tree tools see and consumers never do.

Recording this explicitly because the raw tool output invites exactly one wrong conclusion —
"delete five dead functions" — that would have broken two downstream repos.

## Considered and rejected

- **Collapsing `advise_unix.go` / `advise_other.go`** (and now `syncdir_*.go`) into one file
  with runtime checks: the build-tag split is the idiomatic way to keep `golang.org/x/sys/unix`
  off non-unix builds. Kept, and the new `syncDir` follows the same pattern rather than
  inventing a second one.
- **`equalKey`'s hand-rolled 8-byte comparison** vs `bytes.Equal`: the stride version exists
  because keys are a fixed multiple of 8 and this sits inside the probe loop. Left alone; a
  length guard was added at the callers instead (C-6).
- **`dirtyset`'s bitmap** vs simply msyncing the whole file: the comment explains the regime
  it exists for (a table far larger than RAM, where a whole-file msync scans billions of clean
  PTEs) and `syncDirty` already falls back to whole-file when ≥12.5% is dirty. The complexity
  is justified and bounded.
