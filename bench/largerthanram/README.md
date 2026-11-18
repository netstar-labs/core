# largerthanram — the fault-path harness

A nested module (own `go.mod`), so it never enters the root module graph and
`go test ./...` never compiles it. Run it explicitly.

Every benchmark in the root package runs against a table that fits in page cache, so
they measure the SWAR control probe and **never the page-fault path the mmap design
exists for**. `MADV_RANDOM`, control-array residency, and the dirty-chunk `Sync`
strategy are all tuned for a regime nothing else here reaches — a 6.5 ns/op lookup
says nothing about the 100 µs one.

## Running it

```sh
# straight run, uncapped — proves the harness works, measures the write path
go run . -size 1GiB -reads 200000

# the real thing: cap memory well below the store so lookups must fault
../../build/bigbench -size 8GiB -memory 1GiB
```

`build/bigbench` is the capped runner: a cgroup on Linux, an Apple `container` VM on
darwin. Uncapped numbers from this harness are a **write-path baseline only** — they
are not fault-path numbers and the harness says so in its output.

## What it reports

| Phase | Measures |
|---|---|
| `load` | sequential-ish insert throughput while the store grows (resizes included) |
| `sync` | `Sync()` cost over the dirty set the load produced |
| `random` | uniform random lookups — the number that changes utterly once the store exceeds RAM |
| `sequential` | a forward `ForEach` sweep, which `MADV_SEQUENTIAL` is meant to help |

It prints `resident=` from the OS so a reader can tell whether the run was actually
memory-constrained, rather than trusting the flags.
