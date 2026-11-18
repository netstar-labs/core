# `core` CLI

The end-to-end command-line tool for a [core](../../readme.md) store: analyze a
dataset, bulk-ingest it into an mmap-backed sharded store, look keys up, benchmark,
pack a store for transport, and build/apply incremental patches (including deltas
diffed from two snapshots). One subcommand runs per process.

Run it with `go run ./cmd/core <sub> [flags]`, or build a `core` binary
(`go build ./cmd/core`) and run `core <sub> [flags]`. See the
[USERGUIDE](../../docs/userguide.md) for the store internals and the
[example server](../../server/README.md) for the HTTP front end.

## Quick start

```bash
go run ./cmd/core bench                                              # self-contained benchmark (no setup)
go run ./cmd/core plan   -data data.txt                             # size a load for this machine
go run ./cmd/core ingest -data data.txt -store kv.core -shards 8 -reserve 1000000
go run ./cmd/core lookup -store kv.core key000000042                # geometry + shards auto-detected
go run ./cmd/core digest -store kv.core                             # content digest (shards auto-detected)
```

## Subcommands

`core <sub> -help` lists a subcommand's flags; `core help` lists the subcommands.

| Command | Purpose |
|---------|---------|
| `plan` | Analyze a dataset (or `-rows N`) and recommend shards/reserve/parts for the machine (`-cores`/`-ram` to target another box). `-commit <name>` materializes an **empty pre-sized** store. |
| `ingest` | Bulk-load a text `key<space>uint64` dataset into a sharded store. |
| `ingest-bin` | Bulk-load packed fixed-size binary records (zero-parse, fastest). |
| `ingest-keys` | Bulk-load a newline-delimited list of key strings into a **key-only set** store (`valLen=0`), hashing each line by geometry. The text-list counterpart to `ingest-bin`'s key-only records. |
| `lookup` | Print the value (or `present`/`<not found>` for a set) for each key given as a positional arg. Geometry **and** shard count are auto-detected, so `lookup -store <prefix> <key>` needs no other flags. `-codex "<spec>"` decodes each value into named bit fields (see below); `-in <file>`/`-out <file>` run a **bulk CSV** lookup (see below). |
| `bench` | **Self-contained** lookup benchmark: sizes a synthetic test set from the machine, builds a throwaway store, runs concurrent random lookups, prints a summary table, and cleans up. |
| `digest` | Print a store's order-independent content digest, for verifying a copy. |
| `header` | Print the on-disk header in table form: geometry, per-shard capacity/count/tombstones/load, region offsets, and checksums. |
| `partition` | Bucket a dataset by destination group for an out-of-RAM load (load the parts in order). Writes a `<out>.manifest.json` so the parts are self-describing. |
| `ingest-parts` | Load `partition` files in group order into one pre-sized store. Reads the manifest, so geometry flags are optional; `-flush:off` skips the per-part `Sync`. |
| `pack` | Bundle parts (or live shards) into one `tar.gz` with a per-blob CRC-32C + store `Digest` manifest. |
| `unpack` | Extract a bundle, optionally rebuild the store and verify its `Digest`. |
| `patch-build` | Build a transportable patch bundle (coalesced upserts + deletes) against a store, recording its `Digest` as the patch base. |
| `patch-apply` | Apply a patch bundle to a store, verifying the base `Digest` first (`-force=on` to override). |
| `export-parts` | Re-bucket a live store back into partition files (`<prefix>.pNNN` + manifest) — the inverse of `ingest-parts`. |
| `delta` | Diff a prior partition set against a current one into a patch bundle — ship only what changed. |
| `push-parts` | Stream partition files in order to a running [server](../../server/README.md). |
| `version` | Print the build stamp. |

## Keys and geometry

Text keys are hashed into the fixed `keyLen`-byte key by geometry — **SHA-256** at
`keyLen=32`, **XXH3-128** at `keyLen=16`, **xxHash64** at `keyLen=8` — the same derivation the
[server](../../server/README.md) uses, so a store is interchangeable between them. A
key argument of exactly `2*keyLen` hex chars is taken as the **raw key** instead.
Values are a little-endian `uint64`.

- **Geometry** defaults to a 32-byte key + 8-byte value (40-byte record). `-keylen`/
  `-vallen` set it when **creating** a store; commands that open an existing store
  **auto-detect** it from the header (`core.Geometry` on `<store>.000`), so the flags
  are needed only for `ingest`/`ingest-bin`/`ingest-keys`/`partition`/`ingest-parts` on a new store.
- **`-hash`** is a friendlier alias for `-keylen` (see the tables below); it overrides
  `-keylen` when given (on `plan` it also flows into the suggested commands).
- `-vallen 0` builds a key-only **set** store (membership only): use `ingest-keys`
  for a newline-delimited list of key strings (hashed by geometry) or `ingest-bin`
  for packed key-only records (`ingest`'s `key<space>uint64` text is rejected), and
  `lookup` reports `present`/`<not found>`.
- **`lookup`, `digest`, `header`, and `pack` auto-detect** both geometry and shard
  count from the store, so they need only `-store` (`pack` records them in the bundle
  manifest for `unpack`; without `-store`, `pack` needs an explicit `-shards`).
  (You can still pass `-shards` to
  override, but the wrong count opens the store with that many shards — creating
  empty shard files and misrouting — so prefer the default.) For the
  `partition`→`ingest-parts` pipeline, `partition` records `-shards`, the per-shard
  capacity, and the geometry in a `<out>.manifest.json` sidecar that `ingest-parts`
  reads back and pre-sizes to (so the load never resizes), so the build needs no
  matching flags. Only **raw** parts with no manifest require `-shards`/`-reserve`
  (and, at a non-default geometry, `-keylen`/`-vallen`) to match the partition step
  by hand.

### `-hash` and `-keylen`

Both flags matter only when **creating** a store; a command that opens an existing
store reads the width from its header and ignores them.

| `-hash` value | Key width | Hash | Notes |
|---|---|---|---|
| `xxhash64`, `xx`, `64` | 8 | xxHash64, little-endian | Fastest. At 350M entries there is about a 0.3% chance of one silent collision. |
| `xxh128`, `xxhash128`, `xxh3-128`, `128`, `xxh3` | 16 | XXH3-128, canonical big-endian | Fast; negligible collision risk. Not collision-resistant against an attacker. |
| `sha256`, `sha`, `256` | 32 | SHA-256 | The default. Use it for untrusted input. |

Names are case-insensitive. `sha128` and `xxh3-64` are rejected, since they would
silently mean something other than what they say.

`-keylen` is the key width in bytes (default 32) and must be a positive multiple of 8
(`-keylen 4`, `12` or `0` fails with "invalid or mismatched key/value size").

| `-keylen` | Key derived from the text |
|---|---|
| `8`, `16`, `32` | The hashes in the table above. |
| any other multiple of 8 | The SHA-256 digest, truncated or zero-padded to fit: `24` keeps the first 192 bits; above `32` the extra bytes are zero, so a `64`-byte key still carries only 32 bytes of entropy. Use these widths only when the key is yours rather than a hash of text. |

A key argument of exactly `2*keyLen` hex characters is taken as the raw key and not
hashed, at every width.

| Command | `-hash` | `-keylen` |
|---|---|---|
| `plan`, `ingest`, `ingest-bin`, `ingest-keys`, `partition`, `ingest-parts`, `pack`, `patch-build`, `export-parts`, `lookup` | yes | yes |
| `digest`, `bench` | no | yes |
| `header`, `unpack`, `patch-apply`, `delta`, `push-parts`, `notices` | no | no |

## Decoding packed values (`-codex`)

A value is often a packed bit-record (8 bytes = 64 bits, 16 bytes = 128 bits, …).
`lookup -codex "<spec>"` masks/shifts each value apart and prints the named fields.
Offsets may span the **full value width** (`valLen*8` bits), so 16-byte values decode
across all 128 bits; a single field is still ≤ 64 bits (it must fit a `uint64`). A
value wider than 8 bytes is itself printed as big-endian hex rather than a `uint64`.
A spec is a comma-separated list of fields in one of two forms (pick one per spec):

- **Sequential** `Name:width` — fields listed **MSB→LSB**, the **last** one at bit 0;
  unspecified high bits are reserved/ignored.
- **Explicit** `Name:offset:width` — `offset` is the bit position from the LSB; use it
  to pull a single field ("individual mask") or a few.

```
core lookup -store sandbox/records zxdev.com \
  -codex "Reserved:8,Primary:16,Secondary:16,Security:16,Age:4,Risk:4"
zxdev.com = 21996829625286690
  field      bits   value  hex
  Reserved   56-63  0      0x0
  Primary    40-55  20006  0x4e26
  ...
  Risk       0-3    2      0x2

core lookup -store sandbox/records zxdev.com -codex "Age:4:4"   # one field, bits 4-7
```

**Spec from a file.** Instead of an inline spec, pass `-codex @<path>` to load it
from disk — handy for a large or shared layout you don't want to re-quote on the
command line. Fields may be one per line or comma-separated, blank lines and `#`
comments are ignored:

```
# fields.codex — packed codex record (MSB→LSB)
Reserved:8
Primary:16
Secondary:16
Security:16
Age:4
Risk:4
```

```
core lookup -store sandbox/records zxdev.com -codex @fields.codex
```

## Bulk CSV lookup (`-in` / `-out`)

`lookup -in <file>` reads a list of keys (one per line; blank lines skipped) and
writes a **CSV** to `-out <file>` (default stdout). Without `-codex` the columns are
`key,value`; with `-codex` they are `key` plus one integer column per field. A
not-found key writes an empty-valued row so rows stay aligned 1:1 with the input, and
a `bulk: N keys, F found, M missed` summary is printed to **stderr** (so a stdout CSV
is never corrupted). For stdin, use the inline form `-in=-` (or `-in /dev/stdin`).

```
core lookup -store sandbox/records -in keys.txt -out out.csv \
  -codex "Primary:16,Secondary:16,Security:16,Age:4,Risk:4"
# out.csv:
#   key,Primary,Secondary,Security,Age,Risk
#   zxdev.com,20006,0,0,2,2
#   bogus.invalid,,,,,
```

## Conventions

Flags are parsed with the standard library's `flag` package: besides `-flag value`,
each accepts `-flag=value` or `-flag:value`, and an uppercased env-var override (e.g.
`SHARDS=16 core digest -store kv.core`; the variable wins over the flag). A bool is
enabled by a bare `-text` or by `-text:on`, and disabled by `-flush:off`
(`on`/`yes`/`true`/`1` enable, anything else disables). An unknown flag or a stray
positional argument is an error. Common flags: `-store` (path prefix `<prefix>.000…NNN`), `-shards`,
`-data`, `-reserve`, `-cap`, `-keylen`, `-vallen`.

## Out-of-RAM and transport

When the store is larger than RAM, ingest in destination order to avoid page-cache
thrashing — partition by group index, then load the partitions in order. The
partition step writes a manifest sidecar, so the build needs no geometry flags and
bounds resident memory by `Sync`ing after each part (`-flush:off` to skip it for an
in-RAM load):

```bash
go run ./cmd/core partition    -data data.bin -out part -shards 8 -reserve 360000000 -parts 16
go run ./cmd/core ingest-parts -parts part    -store kv.core   # geometry from part.manifest.json
```

This is a thin wrapper over the library API (`core.Partitioner`, `core.WriteManifest`,
`core.IngestManifest`) — see the [partition example](../../examples/partition/) to
build a store larger than RAM directly from Go.

Bundle a store for sharing and verify on the far side:

```bash
go run ./cmd/core pack   -glob 'part.p*' -out bundle.tar.gz -store kv.core -shards 8 -reserve 360000000 -parts 16
go run ./cmd/core unpack -in bundle.tar.gz -dir restore -store kv2.core   # rebuilds + checks digest
```

## Incremental updates (patches)

To fold changes into an existing store instead of rebuilding it, `patch-build`
coalesces an upsert source and a delete source into one transportable bundle, and
`patch-apply` applies it after verifying the store hasn't diverged from the base it
was built against (since the store's insert is an upsert, "inserts" and "updates"
share the `-upserts` stream; only deletions are separate):

```bash
# add.txt: "key<space>uint64" lines (upserts);  del.txt: one key per line (deletes)
go run ./cmd/core patch-build -upserts add.txt -deletes del.txt -store kv.core -out patch.tar.gz
go run ./cmd/core patch-apply -in patch.tar.gz -store kv.core   # verifies base digest, applies, prints new digest
```

Sources may be packed binary (already-hashed keys) with `-bin`. This wraps the
`core/patch` library (`patch.Builder` → `patch.WriteBundle` / `patch.OpenBundle` →
`patch.Apply`); the same package's WAL-backed `patch.Log` powers the example
server's live `-patch` write-buffer. See [§21 of the guide](../../docs/userguide.md).

### Deltas — diff two snapshots

Instead of listing changes by hand, derive a patch by **diffing two store
snapshots** (incremental replication — ship only what changed). `export-parts`
re-buckets a live store back into partition files (the inverse of `ingest-parts`),
and `delta` diffs a prior parts set against a current one. Both exports must be
bucketed by the same per-shard capacity (+ `-parts`/`-shards`/geometry) so the parts
align — part assignment is a deterministic function of the key and that geometry;
`delta` checks the manifests and refuses a mismatch.

Rather than remembering a reserve, export the prior snapshot (the "golden master")
however you like, then align the current side to it with **`-like`** — it reuses the
master manifest's `capPerShard` + `parts`, so the two diff correctly with no shared
reserve to track:

```bash
go run ./cmd/core export-parts -store master.core  -out old             # records capPerShard
go run ./cmd/core export-parts -store current.core -out new -like old   # aligns to old
go run ./cmd/core delta old new -store master.core -out patch.tar.gz    # base = master's digest
go run ./cmd/core patch-apply -in patch.tar.gz -store far.core          # far.core must be at master's digest
```

(The classic form — a shared `-reserve 1000000 -parts 16` on both exports — still
works and is equivalent.) For a **ship-whole** rebuild (no delta), omit `-reserve`
entirely: `export-parts` then derives the mask from the store's actual per-shard
capacity and records it, so the rebuild needs no reserve and never resizes — but
such a set is for rebuild, not for diffing against a differently-sized one.

`export-parts` output (dense, geometry-independent) is also what `pack` and
`push-parts` consume. The diff runs one aligned part pair at a time, so memory is
bounded by 1/parts of the store. See [§21.1 of the guide](../../docs/userguide.md).

## Files

| File | Contents |
|------|----------|
| [main.go](main.go) | Dispatch, `ingest`, `ingest-bin`, `ingest-keys`, `lookup`, `bench`, `makeKey`, geometry/helpers. |
| [plan.go](plan.go) | `plan`: sizing math, machine detection, duplicate sampling. |
| [prep.go](prep.go) | `partition` / `ingest-parts` for out-of-RAM loads. |
| [pack.go](pack.go) | `digest`, `pack`, `unpack` (tar.gz bundles with a verified manifest). |
| [patch.go](patch.go) | `patch-build` / `patch-apply` (coalesced upsert/delete bundles over `core/patch`). |
| [delta.go](delta.go) | `export-parts` (store → parts) / `delta` (diff two parts sets → patch bundle). |
| [header.go](header.go) | `header`: render a store's on-disk header as a table. |
| [codex.go](codex.go) | `-codex` bit-field decoder (parse spec, decode, render). |
| [bulk.go](bulk.go) | `lookup -in/-out` bulk CSV mode. |
| [push.go](push.go) | `push-parts`: stream parts to a running server. |
