// Locality-preserving preprocessor for large bulk loads.
//
// The store places a record by hash, so on a box where the table is larger than
// RAM, insertions scatter writes across the whole slots region and the page
// cache thrashes. This pass reorders a packed-binary dataset so that records are
// grouped by their destination group index g = (hash>>7) & mask — the same g the
// table computes (see table.go: g = (h>>7) & t.mask). Ingesting the partitions in
// order then confines each shard's writes to a 1/parts slice of its slots region
// at a time, which fits in RAM, turning random writeback into a forward sweep.
//
//	go run ./cmd/core partition    -data kv.bin -out part -shards 8 -reserve 360000000 -parts 16
//	go run ./cmd/core ingest-parts -parts part   -store kv.core -shards 8 -reserve 360000000
//
// -shards and -reserve MUST be identical for both steps: they determine the
// per-shard capacity, hence the group mask used to bucket records.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/internal/mmap"
	"github.com/netstar-labs/core/internal/xxhash"
)

// ---- partition: bucket records by destination group range ----

type partitionCfg struct {
	Data    string `env:"order,require" help:"input dataset"`
	Out     string `default:"part" help:"output partition prefix (writes <prefix>.pNNN)"`
	Shards  int    `default:"8" help:"shard count (MUST match ingest-parts)"`
	Reserve int    `help:"reserve count used at ingest (MUST match; sizes the group mask)"`
	Parts   int    `default:"16" help:"number of partitions (power of two)"`
	Text    bool   `help:"input is text \"key<space>uint64\" lines (default: packed records)"`
	Keylen  int    `default:"32" help:"key length in bytes (MUST match ingest-parts)"`
	Vallen  int    `default:"8" help:"value length in bytes (MUST match ingest-parts)"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runPartition(c *partitionCfg) {
	setGeometry(resolveKeylen(c.Hash, c.Keylen), c.Vallen)

	if c.Reserve <= 0 {
		fatal(fmt.Errorf("-reserve is required and must equal the ingest -reserve"))
	}
	if c.Parts < 1 || c.Parts&(c.Parts-1) != 0 {
		fatal(fmt.Errorf("-parts must be a power of two, got %d", c.Parts))
	}
	if c.Text && valLen == 0 {
		fatal(fmt.Errorf("partition -text reads key<sp>uint64; a key-only set uses packed key-only records (drop -text)"))
	}

	// The library owns the group/partition geometry (per-shard capacity, the
	// g=(h>>7)&mask the table places by, the partition split), so the bucketing
	// here cannot drift out of lockstep with where ingest will store each key.
	capPerShard := core.ReserveCapPerShard(c.Reserve, c.Shards)
	part, err := core.NewPartitioner(capPerShard, c.Parts)
	if err != nil {
		fatal(err)
	}

	if err := os.MkdirAll(filepath.Dir(c.Out), 0o755); err != nil {
		fatal(err)
	}
	ws := make([]*bufio.Writer, c.Parts)
	fps := make([]*os.File, c.Parts)
	counts := make([]int, c.Parts)
	for p := 0; p < c.Parts; p++ {
		fp, err := os.Create(fmt.Sprintf("%s.p%03d", c.Out, p))
		if err != nil {
			fatal(err)
		}
		fps[p] = fp
		ws[p] = bufio.NewWriterSize(fp, 1<<20)
	}

	nrec := 0
	start := time.Now()
	if c.Text {
		// Text "key<space>uint64" lines: derive the key (geometry-aware: SHA-256 for
		// a 32-byte key, xxHash64 for an 8-byte key) exactly as cmdIngest does, so
		// xxhash.Sum routes on the same key bytes the store will store.
		f, err := os.Open(c.Data)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		k := make([]byte, keyLen)
		vb := make([]byte, valLen)
		for sc.Scan() {
			ks, vs, ok := splitKV(sc.Bytes())
			if !ok {
				continue
			}
			v, err := strconv.ParseUint(vs, 10, 64)
			if err != nil {
				continue
			}
			makeKey(k, ks)
			p := part.PartKey(k)
			binary.LittleEndian.PutUint64(vb, v)
			if _, err := ws[p].Write(k); err != nil {
				fatal(err)
			}
			if _, err := ws[p].Write(vb); err != nil {
				fatal(err)
			}
			counts[p]++
			nrec++
		}
		if err := sc.Err(); err != nil {
			fatal(err)
		}
	} else {
		// Packed 40B records: hash the key field in place, zero-copy.
		ra, err := mmap.Open(c.Data)
		if err != nil {
			fatal(err)
		}
		defer ra.Close()
		buf := ra.Bytes()
		if len(buf)%recordSize != 0 {
			fatal(fmt.Errorf("file size %d is not a multiple of record size %d", len(buf), recordSize))
		}
		for off := 0; off < len(buf); off += recordSize {
			p := part.Part(xxhash.Sum(buf[off : off+keyLen]))
			if _, err := ws[p].Write(buf[off : off+recordSize]); err != nil {
				fatal(err)
			}
			counts[p]++
			nrec++
		}
	}
	for p := 0; p < c.Parts; p++ {
		if err := ws[p].Flush(); err != nil {
			fatal(err)
		}
		if err := fps[p].Close(); err != nil {
			fatal(err)
		}
	}
	dur := time.Since(start)

	fmt.Printf("partition: %d records → %d parts in %v (%.2f M/s)\n",
		nrec, c.Parts, dur.Round(time.Millisecond), float64(nrec)/dur.Seconds()/1e6)
	for p := 0; p < c.Parts; p++ {
		fmt.Printf("  %s.p%03d: %d records\n", c.Out, p, counts[p])
	}

	// Write a manifest sidecar so the parts are self-describing: ingest-parts (or
	// core.IngestManifest) can build the store straight from them without
	// re-specifying -shards/-reserve/-keylen/-vallen or re-running this step.
	manPath := core.ManifestPath(c.Out)
	if err := core.WriteManifest(manPath, core.PartsManifest{
		KeyLen: keyLen, ValLen: valLen, RecordSize: recordSize,
		Shards: c.Shards, CapPerShard: capPerShard, Parts: c.Parts,
		Records: nrec, Prefix: filepath.Base(c.Out), Counts: counts,
	}); err != nil {
		fatal(err)
	}
	fmt.Printf("manifest: %s\n", manPath)
}

// ---- ingest-parts: load partition files in g-order into one store ----

type ingestPartsCfg struct {
	Parts   string `default:"part" help:"partition prefix (reads <prefix>.pNNN in order)"`
	Store   string `default:"kv.core" help:"output store path prefix"`
	Shards  int    `default:"8" help:"shard count (MUST match partition; ignored when a manifest is present)"`
	Reserve int    `help:"reserve count (MUST match partition; ignored when a manifest is present)"`
	Keylen  int    `default:"32" help:"key length in bytes (MUST match partition)"`
	Vallen  int    `default:"8" help:"value length in bytes (MUST match partition)"`
	Flush   bool   `default:"on" help:"Sync after each part to bound resident dirty pages (out-of-RAM safe); -flush:off for max speed when the store fits in RAM"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runIngestParts(c *ingestPartsCfg) {
	// If partition left a manifest beside the parts, it is authoritative for the
	// geometry — the whole point of the sidecar is that you needn't re-specify (or
	// risk mismatching) -shards/-reserve/-keylen/-vallen. Raw parts with no
	// manifest fall back to the flags, as before.
	var capPerShard uint64
	if m, err := core.ReadManifest(core.ManifestPath(c.Parts)); err == nil {
		c.Keylen, c.Vallen, c.Shards = m.KeyLen, m.ValLen, m.Shards
		// The manifest's effective capacity (CapPerShard, else what a legacy Reserve
		// implies) pre-sizes the load so it never resizes — a hot shard included.
		capPerShard = m.EffectiveCapPerShard()
		fmt.Printf("manifest: %s (keyLen=%d valLen=%d shards=%d capPerShard=%d parts=%d)\n",
			core.ManifestPath(c.Parts), m.KeyLen, m.ValLen, m.Shards, capPerShard, m.Parts)
	} else if c.Reserve > 0 {
		capPerShard = core.ReserveCapPerShard(c.Reserve, c.Shards)
	} else {
		fatal(fmt.Errorf("-reserve is required and must match the partition -reserve (no manifest at %s)", core.ManifestPath(c.Parts)))
	}

	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)

	total, inserted, dur, err := ingestParts(c.Parts, c.Store, c.Shards, capPerShard, c.Flush)
	if err != nil {
		fatal(err)
	}
	if total == 0 {
		fatal(fmt.Errorf("no partition files found at %s.pNNN", c.Parts))
	}
	fmt.Printf("ingest-parts: %d records, %d new entries, %d shards in %v (%.2f M/s)\n",
		total, inserted, c.Shards, dur.Round(time.Millisecond), float64(total)/dur.Seconds()/1e6)
	fmt.Printf("store on disk: %s\n", storeSize(c.Store, c.Shards))
}

// ingestParts loads <prefix>.pNNN packed-record files in order into one store via
// the library's staged loader, timing the load for the CLI report. Shared by the
// ingest-parts and unpack commands. Geometry comes from the package globals set by
// the caller (setGeometry / setGeometryFromStore). flush Syncs after each part to
// bound resident dirty pages for an out-of-RAM load.
func ingestParts(prefix, store string, shards int, capPerShard uint64, flush bool) (total, inserted int, dur time.Duration, err error) {
	start := time.Now()
	st, err := core.IngestParts(prefix, store, keyLen, valLen, shards, capPerShard, flush, core.WithNoSync())
	return st.Total, st.Inserted, time.Since(start), err
}
