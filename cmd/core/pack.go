// Pack / unpack a sharded store or its partition files into a single compressed
// bundle for transport, using only the Go standard library (archive/tar +
// compress/gzip). Per-blob CRC-32C guards transport integrity; the store's
// order-independent Digest (xxHash64 multiset) verifies logical content after
// rebuild — independent of compression, blob order, or shard count.
//
//	go run ./cmd/core digest -store kv.core -shards 8
//	go run ./cmd/core pack   -glob 'sandbox/part.p*' -out sandbox/bundle.tar.gz -store kv.core -shards 8 -reserve 390000000 -parts 16
//	go run ./cmd/core unpack -in sandbox/bundle.tar.gz -dir restore -store kv2.core
//
// Default kind is "parts" (packed records; smallest, geometry-independent,
// rebuilt with ingest-parts). Use -kind shards to bundle the live store files
// for a same-geometry hot restore.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/bundle"
)

// ---- digest: print a store's content digest ----

type digestCfg struct {
	Store  string `default:"kv.core" help:"store path prefix"`
	Shards int    `help:"shard count (0 = auto-detect from <store>.NNN files)"`
	Keylen int    `default:"32" help:"key length in bytes (auto-detected from the store)"`
	Vallen int    `default:"8" help:"value length in bytes (auto-detected from the store)"`
}

func runDigest(c *digestCfg) {
	// Geometry and shard count both come from the existing store.
	setGeometryFromStore(c.Store, c.Keylen, c.Vallen)
	shards := c.Shards
	if shards <= 0 {
		if shards = detectShards(c.Store); shards == 0 {
			fatal(fmt.Errorf("no store found at %s.000", c.Store))
		}
	}

	dig, n, err := core.StoreDigest(c.Store, keyLen, valLen, shards)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("digest: %s  (%d entries, %d shards)\n", dig, n, shards)
}

// ---- pack: bundle files into one tar.gz with a manifest ----

type packCfg struct {
	Glob    string `default:"part.p*" help:"glob (or plain prefix) of files to bundle; a * is appended if missing"`
	Out     string `default:"bundle.tar.gz" help:"output bundle path"`
	Kind    string `default:"parts" help:"parts | shards"`
	Store   string `default:"kv.core" help:"store prefix to read Digest from (recommended)"`
	Shards  int    `help:"shard count for Digest+manifest (0 = auto-detect from <store>.NNN; needs -store)"`
	Reserve int    `help:"reserve count to record for rebuild (parts)"`
	Parts   int    `help:"partition count to record (informational)"`
	Level   int    `default:"-1" help:"gzip level 1..9 (1=fast, 9=best; -1=default)"`
	Keylen  int    `default:"32" help:"key length in bytes (recorded in the manifest)"`
	Vallen  int    `default:"8" help:"value length in bytes (recorded in the manifest)"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runPack(c *packCfg) {
	// Geometry and shard count come from an existing store's header/files when -store
	// is given (consistent with the other commands); -keylen/-vallen/-hash/-shards are
	// the fallback when packing parts without a store to read.
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)
	shards := c.Shards
	if shards <= 0 {
		if c.Store != "" {
			shards = detectShards(c.Store)
		}
		if shards == 0 {
			fatal(fmt.Errorf("-shards is required when -store is not set (cannot auto-detect)"))
		}
	}

	// Accept a plain prefix: append a wildcard when the glob has none (part.p -> part.p*).
	glob := c.Glob
	if !strings.ContainsAny(glob, "*?[") {
		glob += "*"
	}
	files, err := filepath.Glob(glob)
	if err != nil {
		fatal(err)
	}
	if len(files) == 0 {
		fatal(fmt.Errorf("no files match %q", glob))
	}

	meta := bundle.Meta{
		PartsManifest: core.PartsManifest{
			KeyLen:     keyLen,
			ValLen:     valLen,
			RecordSize: recordSize,
			Shards:     shards,
			Parts:      c.Parts,
		},
		Kind:      c.Kind,
		GzipLevel: c.Level,
	}
	// Record the per-shard capacity that sizes the rebuild's mask. With -reserve,
	// convert it to the capacity it implies; otherwise (ship-whole) derive the
	// authoritative capacity from the store (max across shards) so the bundle rebuilds
	// growth-free with no remembered reserve. The latter requires -store.
	if c.Reserve > 0 {
		meta.CapPerShard = core.ReserveCapPerShard(c.Reserve, shards)
	} else if c.Kind == "parts" && c.Store != "" {
		max, min, e := core.StoreCapPerShard(c.Store, shards)
		if e != nil {
			fatal(e)
		}
		if max != min {
			fmt.Fprintf(os.Stderr, "pack: warning: shards diverged (capacity %d…%d); recording the max, so cold shards over-allocate on rebuild\n", min, max)
		}
		meta.CapPerShard = max
	}
	if c.Store != "" {
		dig, n, err := core.StoreDigest(c.Store, keyLen, valLen, shards)
		if err != nil {
			fatal(fmt.Errorf("reading store digest: %w", err))
		}
		meta.Digest = dig
		fmt.Printf("store digest: %s  (%d entries)\n", meta.Digest, n)
	}

	if err := os.MkdirAll(filepath.Dir(c.Out), 0o755); err != nil {
		fatal(err)
	}
	f, err := os.Create(c.Out)
	if err != nil {
		fatal(err)
	}
	man, err := bundle.Pack(f, files, meta) // streams blobs + manifest; geometry/digest from meta
	if err != nil {
		f.Close()
		fatal(err)
	}
	if err := f.Close(); err != nil {
		fatal(err)
	}

	var rawTotal int64
	for _, b := range man.Blobs {
		rawTotal += b.Bytes
	}
	outSize := int64(0)
	if st, err := os.Stat(c.Out); err == nil {
		outSize = st.Size()
	}
	ratio := 0.0
	if outSize > 0 {
		ratio = float64(rawTotal) / float64(outSize)
	}
	fmt.Printf("pack: %d blobs, %d records, raw %s → bundle %s (%.2fx) %s\n",
		len(man.Blobs), man.Records, humanBytes(rawTotal), humanBytes(outSize), ratio, c.Out)
}

// ---- unpack: extract, verify CRCs, optionally rebuild and verify Digest ----

type unpackCfg struct {
	In      string `default:"bundle.tar.gz" help:"input bundle path"`
	Dir     string `default:"restore" help:"directory to extract blobs into"`
	Store   string `help:"if set: rebuild here (parts) and verify Digest"`
	Shards  int    `help:"override shard count (0 = use manifest)"`
	Reserve int    `help:"override reserve count (0 = use the manifest's capacity)"`
}

func runUnpack(c *unpackCfg) {
	f, err := os.Open(c.In)
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	man, err := bundle.Unpack(f, c.Dir) // extracts blobs + verifies each size/CRC against the manifest
	if err != nil {
		fatal(err)
	}
	fmt.Printf("unpack: %d blobs OK (kind=%s, records=%d) → %s\n", len(man.Blobs), man.Kind, man.Records, c.Dir)

	if c.Store == "" {
		fmt.Printf("extracted %s.* into %s; rebuild with: ingest-parts -parts %s -store <out> (geometry read from the manifest)\n",
			man.Prefix, c.Dir, filepath.Join(c.Dir, man.Prefix))
		return
	}

	if man.Kind != "parts" {
		fatal(fmt.Errorf("-store rebuild is only supported for kind=parts; for kind=shards point OpenSharded at %s",
			filepath.Join(c.Dir, man.Prefix)))
	}
	nsh := man.Shards
	if c.Shards > 0 {
		nsh = c.Shards
	}
	// Size the rebuild's mask: an explicit -reserve overrides (the delta-aligned
	// case); otherwise use the bundle's authoritative capacity (CapPerShard, else what
	// a legacy Reserve implies), which pre-sizes each shard — a hot shard too — for a
	// growth-free rebuild.
	var capPerShard uint64
	if c.Reserve > 0 {
		capPerShard = core.ReserveCapPerShard(c.Reserve, nsh)
	} else if capPerShard = man.EffectiveCapPerShard(); capPerShard == 0 {
		fatal(fmt.Errorf("no capacity: bundle manifest carries neither capPerShard nor reserve; pass -reserve"))
	}
	// Rebuild with the geometry the bundle was packed with (no -keylen/-vallen
	// needed: pack recorded it in the manifest).
	if man.KeyLen > 0 {
		setGeometry(man.KeyLen, man.ValLen)
	}
	prefix := filepath.Join(c.Dir, man.Prefix)
	total, inserted, dur, err := ingestParts(prefix, c.Store, nsh, capPerShard, true) // flush per part: rebuild may exceed RAM
	if err != nil {
		fatal(err)
	}
	fmt.Printf("rebuilt: %d records, %d entries, %d shards in %v\n", total, inserted, nsh, dur.Round(1e6))

	if man.Digest == "" {
		fmt.Printf("manifest carried no digest — content not verified (transport CRCs passed)\n")
		return
	}
	got, n, err := core.StoreDigest(c.Store, keyLen, valLen, nsh)
	if err != nil {
		fatal(err)
	}
	if got != man.Digest {
		fatal(fmt.Errorf("DIGEST MISMATCH: rebuilt %s (%d entries) != manifest %s", got, n, man.Digest))
	}
	fmt.Printf("digest OK: %s (%d entries) — content verified\n", got, n)
}
