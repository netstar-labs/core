# core — dimension D: doc-vs-code drift

Every claim below was checked against the code path it describes, not read for plausibility.
core's doc comments are unusually thorough, which cuts both ways: the ones that were wrong
were wrong *specifically*, and a reader had every reason to believe them.

## D-3 · CONFIRMED, fixed · "durably swaps the file into place" — it did not

`map.go:424` (`Map.rehash`) and `resize.go:11` (`buildResized`) both claimed durability. The
code msyncs the rebuilt file's contents and then `os.Rename`s it, with no fsync on the parent
directory — so the *contents* were durable and the *swap* was not. Fixed in code (`syncDir`)
rather than by softening the comment, since the claim is the one you want to be true. See C-3.

## D-4 · CONFIRMED, fixed · the header checksum was described as an out-of-bounds guard

`table.go:63`:

> so a corrupted capacity or region offset **is caught at Open before it can drive an
> out-of-bounds access**

True for accidental corruption. False against a crafted file — fnv64a is unkeyed, so the
checksum is recomputable by anyone who can write the store, and the offsets it "protects" then
flow into `unsafe.Pointer` arithmetic. The comment now states what the checksum actually
guarantees and points at `validateGeometry` for what does the guarding. See C-7.

## D-2 · CONFIRMED, fixed · `GetInto`'s precondition failed two different ways

`map.go:192` said "dst (len must be valLen)" without saying what happens otherwise — and the
two size classes diverged: `valLen == 8` **panicked** (`PutUint64` on a short slice) while
`valLen > 8` **silently truncated** (`copy`). Now both report `false`, an oversized `dst` is
explicitly allowed so a caller can reuse one buffer, and the doc says all of it.

## D-1 · CONFIRMED, fixed by documenting · `Update`'s false is overloaded

`map.go:237` said `Update` "reports whether it was found". It also returns `false` for a
wrong-length key or value, which is not the same thing — and `Put`, given the same bad input,
returns `ErrBadSize`. The bool is deliberate (it matches `Contains`/`Delete`, and `Update` has
no error return), so the doc now says "false means *not updated*, not strictly *not found*"
instead of the code changing under six consumers.

## D-5 · CONFIRMED, fixed · undocumented resource growth

Nothing told a reader that a resize retains the old mapping until `Close`, or that a `Shared`
reader accumulates one per *writer* resize. That is the kind of limitation an operator
discovers from an RSS graph. Added to the package doc as "Known limitation: retained
mappings", with the practical guidance (reopen a long-lived reader). See C-5.

## Checked and accurate

Worth recording, because the value of this dimension is partly in what it *clears*:

- **The concurrency contract** in `table.go:26-32` and the package doc — lock-free reads,
  atomics for the control word and ≤8-byte values, seqlock above that, single-writer structural
  mutation — matches the code in `loadValue`/`storeValue`/`publishControl`/`insert`.
- **`Map.Prune`'s** claim that it "never inserts, so it cannot resize" and that the mapping
  therefore stays valid for the whole sweep: correct, `prune` only publishes tombstones.
- **`dirtyset.collect`'s** "writes concurrent with collect are best-effort: one that lands
  during the scan may be deferred to the next Sync" — precisely right, and I nearly filed it as
  a lost-write bug before tracing which bits actually get cleared.
- **`table.PHash`-style geometry comments** (`table.go:19-24` file layout, the `hasVersion`
  rule at `valLen > 8`): match `layout()` exactly, which is what let `validateGeometry`
  recompute rather than range-check.
- **`WithNoSync`'s** "Explicit Sync() still works and is honored regardless of this option":
  correct — `doSync` only gates the resize-time msync (and now the directory fsync).
