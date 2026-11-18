# Example server

A minimal `net/http` front end for a [core](../readme.md) store. It maps a JSON
`Job` (`{"item","value"}`) onto the key/value store: `POST` to store, `GET` to look
up, plus batch, streamed ingest/export, a content digest for verifying transfers,
and an optional WAL-backed patch write-buffer (`-patch`). Reads are lock-free and
writes are sharded, so the handlers need no locking of their own. Graceful shutdown
drains in-flight requests, then `Sync`s and closes the store on `SIGINT`/`SIGTERM`.

It is meant as a **working model to adapt**, not a finished product — see
[Choosing your own key hash](../docs/userguide.md) (§20) for how to swap the key
derivation, and the [USERGUIDE](../docs/userguide.md) for the store internals.

## Run

```bash
go run ./server -addr :8080 -store kv.core -shards 8
```

```bash
curl -s -XPOST localhost:8080/job -d '{"item":"alice","value":42}'
curl -s        localhost:8080/job/alice
curl -s -XPOST localhost:8080/job/bulk -d '[{"item":"a","value":1},{"item":"b","value":2}]'
```

For a large pre-sized bulk load, start the server reserved and unsynced, then push
partition files with `core push-parts` (a hard kill before shutdown loses unsynced
writes):

```bash
go run ./server -addr :8080 -store kv.core -shards 8 -reserve 350000000 -nosync
go run ./cmd/core push-parts -parts part -url http://localhost:8080
```

## Flags

Parsed with the standard library's `flag` package: each flag also takes
`-flag=value` (or `-flag:value`), a bare `-flag` for bools, and an uppercased env-var override
(e.g. `SHARDS=16`; the variable wins over the flag). A bare store name (no `/`) is
placed under `/var/core/` on linux and `_dev/var/core/` elsewhere. `-log on|off`
turns log timestamps on or off (off by default on linux). `-shards`, `-keylen`, and `-vallen` apply only when **creating**
a store; for an **existing** store the shard count is read from its `<store>.NNN`
files and the geometry from its header, so they are ignored (opening with a wrong
shard count would create empty shards and misroute keys). The effective geometry and
shard count are logged at startup (`store ready: …`).

| Flag | Default | Purpose |
|------|---------|---------|
| `-addr` (alias `-a`) | `localhost:1455` | listen address (required) |
| `-store` | `kv.core` | store path prefix (`<prefix>.000…NNN`) |
| `-shards` | `8` | number of shards for a **new** store (existing: read from disk) |
| `-capacity` (alias `-cap`) | `1024` | initial capacity per shard |
| `-reserve` | `0` | pre-size for N entries (avoids mid-load resizes) |
| `-keylen` | `32` | key length in bytes for a new store; `32`→SHA-256, `16`→XXH3-128, `8`→xxHash64 |
| `-vallen` | `8` | value length in bytes for a new store |
| `-nosync` | `false` | skip per-write `msync`; rely on `Sync` at shutdown |
| `-patch` | `""` | enable the WAL-backed patch write-buffer at this dir (empty = off); feed it via `POST /patch` |
| `-flush` | `5` | flush the patch buffer every N seconds (0 = only on size threshold + shutdown) |
| `-flushat` | `1000000` | flush the patch buffer once it reaches N pending changes |

## Endpoints

| Method & path | Body / params | Purpose |
|---|---|---|
| `POST /job` | `{"item","value"}` | Store one (201 create / 200 update). |
| `GET /job/{item}` | — | Look up by path (200 value / 404 miss). |
| `POST /job/bulk` | JSON array `[{"item","value"}, …]` | Buffered sharded bulk insert. |
| `POST /job/stream` | NDJSON, or `Content-Type: application/octet-stream` packed records; optional `?shard=N` | Streamed ingest (format negotiated on `Content-Type`). |
| `GET /job/export` | `Accept: application/x-ndjson` for JSON, else binary; optional `?shard=N` | Streamed dump (binary declares `Content-Length`). |
| `GET /job/digest` | optional `?shard=N` | Order-independent content digest (hex) for verifying a copy. |
| `POST /patch` | NDJSON `Change` records `{"item","value","op"}` (`op:"delete"` removes) | Buffered upserts/deletes (registered only with `-patch`; see below). |
| `GET /status` | — | Liveness + entry count. |

`?shard=N` targets a single shard for both `stream` and `export`, so clients can
transfer all shards in parallel over separate connections and verify each with
`digest`.

## Patch write-buffer (`-patch`)

With `-patch <dir>`, the server gains deletion and the accumulate-then-apply
pattern via the `core/patch` `Log`. `POST /patch` streams `Change` records into a
WAL-backed, coalescing buffer (last-writer-wins per key, so it carries inserts,
updates, **and** deletes), `Sync`ing each request before acking. A background loop
folds the buffer into the store every `-flush` seconds or once `-flushat` changes
accumulate, and shutdown drains it. On restart the `Log` replays any window a prior
run left un-applied — so an acked batch survives even a hard kill — and because the
apply is idempotent, a crash mid-flush is recovered by re-applying.

```bash
go run ./server -addr :8080 -store kv.core -keylen 8 -vallen 8 -patch ./wal -flush 5
# stream upserts and a delete:
printf '{"item":"a","value":1}\n{"item":"b","op":"delete"}\n' \
  | curl -s -XPOST --data-binary @- localhost:8080/patch
```

Changes become visible to reads after the next flush — the buffering trade-off.
The wiring is ~20 lines: `patchFlushLoop` in [main.go](main.go) and `StreamPatch`
in [worker.go](worker.go); see [§21 of the guide](../docs/userguide.md). The direct
`/job` write paths are unaffected and remain immediate.

## Keys

Each `item` is hashed into the fixed `keyLen`-byte key by geometry — **SHA-256** at
`keyLen=32`, **XXH3-128** at `keyLen=16`, **xxHash64** at `keyLen=8` — the same derivation `cmd/core` uses, so a
store is interchangeable between the CLI and this server. An `item` of exactly
`2*keyLen` hex chars is taken as a **raw pre-hashed key** instead, so JSON export
(which emits keys as hex, since the original text is not recoverable from the hash)
re-ingests losslessly. The binary export/stream carries raw key bytes and is the
lossless round-trip path.

The server has no `-hash` flag; the hash follows from `-keylen`, which applies only
when **creating** a store (an existing store's width is read from its header) and must
be a positive multiple of 8. The CLI's `-hash` names map to these widths: `xxhash64` →
8, `xxh128` → 16, `sha256` → 32.

| `-keylen` | Key derived from the text |
|---|---|
| `8`, `16`, `32` | xxHash64, XXH3-128 and SHA-256 respectively (the `-hash` table). |
| any other multiple of 8 | The SHA-256 digest, truncated or zero-padded to fit: `24` keeps the first 192 bits; above `32` the extra bytes are zero, so a `64`-byte key still carries only 32 bytes of entropy. Use these widths only when the key is yours rather than a hash of text. |

The key derivation is the only hashing the server does and
lives in one function, `makeKey` in [worker.go](worker.go); the library itself
treats keys as opaque fixed-size bytes.

## Files

| File | Contents |
|------|----------|
| [main.go](main.go) | Flags, store open, HTTP server, graceful shutdown, patch buffer + `patchFlushLoop`. |
| [routes.go](routes.go) | Route table and HTTP handlers (incl. `POST /patch`). |
| [worker.go](worker.go) | `Worker`: geometry, `makeKey`, Job↔store translation, `StreamPatch`. |
| [job.go](job.go) | `Job` / `Jobz` / `Change` wire structs. |
