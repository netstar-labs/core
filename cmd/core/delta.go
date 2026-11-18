// "export-parts" and "delta" subcommands.
//
// export-parts re-buckets a live store back into partition files (the inverse of
// ingest-parts) — a dense, geometry-independent, self-describing set that pack,
// push-parts, or ingest-parts consume:
//
//	core export-parts -store kv.core -out part -reserve 1000000 -parts 16  # delta-alignable
//	core export-parts -store kv.core -out part -parts 16                   # auto (ship-whole)
//
// With -reserve omitted the mask is auto-derived from the store's actual per-shard
// capacity and recorded in the manifest (capPerShard), so a rebuild re-ingests
// without a resize and no reserve has to be remembered — for ship-whole/rebuild.
//
// A part-wise delta needs both sides bucketed by the same mask. Rather than
// remembering a reserve, export the golden master once (any way), then align the
// other side to it with -like — it reuses the master manifest's capPerShard+parts:
//
//	core export-parts -store master.core -out old              # auto; records capPerShard
//	core export-parts -store current.core -out new -like old   # aligns to old
//
// delta diffs a prior-state parts set against a current one and writes the change
// set as a patch bundle (over core/patch) — ship only what changed, then
// patch-apply it on the far side:
//
//	core delta old new -store master.core -out patch.tar.gz
//	core patch-apply -in patch.tar.gz -store far.core   # verifies it is at the old state
//
// Part assignment is a pure function of the key and (shards, capPerShard, parts),
// so two stores exported with the SAME effective geometry are part-aligned and
// diff correctly; delta rejects a misaligned pair rather than emit a wrong patch.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/patch"
)

// ---- export-parts ----

type exportPartsCfg struct {
	Store   string `default:"kv.core" help:"source store to export"`
	Out     string `default:"part" help:"output partition prefix (writes <prefix>.pNNN + manifest)"`
	Shards  int    `help:"shard count (0 = auto-detect from <store>.NNN)"`
	Reserve int    `help:"reserve count that sizes the group mask (0 = auto-derive from the store, ship-whole; set explicitly to align a delta — reuse the same value on both sides)"`
	Like    string `help:"align to an existing parts prefix: reuse its manifest's capPerShard+parts so this export diffs against it (the golden-master delta path; overrides -reserve/-parts)"`
	Parts   int    `default:"16" help:"number of partitions (power of two)"`
	Keylen  int    `default:"32" help:"key length in bytes (auto-detected from -store)"`
	Vallen  int    `default:"8" help:"value length in bytes (auto-detected from -store)"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runExportParts(c *exportPartsCfg) {
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)
	shards := c.Shards
	if shards <= 0 {
		if shards = detectShards(c.Store); shards == 0 {
			fatal(fmt.Errorf("no store found at %s.000", c.Store))
		}
	}
	if c.Parts < 1 || c.Parts&(c.Parts-1) != 0 {
		fatal(fmt.Errorf("-parts must be a power of two, got %d", c.Parts))
	}
	if err := os.MkdirAll(filepath.Dir(c.Out), 0o755); err != nil {
		fatal(err)
	}
	var m core.PartsManifest
	var err error
	switch {
	case c.Like != "":
		// Golden-master delta path: bucket this export exactly like an existing parts
		// set (its capPerShard + parts), so the two diff correctly — no need to know or
		// reuse the master's reserve. The master manifest is the alignment authority.
		lm, e := core.ReadManifest(core.ManifestPath(c.Like))
		if e != nil {
			fatal(fmt.Errorf("reading -like manifest %s: %w", core.ManifestPath(c.Like), e))
		}
		if lm.KeyLen != keyLen || lm.ValLen != valLen || lm.Shards != shards {
			fatal(fmt.Errorf("-like %s geometry (keyLen=%d valLen=%d shards=%d) differs from this store (keyLen=%d valLen=%d shards=%d); cannot align",
				c.Like, lm.KeyLen, lm.ValLen, lm.Shards, keyLen, valLen, shards))
		}
		capPS := lm.EffectiveCapPerShard()
		if capPS == 0 {
			fatal(fmt.Errorf("-like %s manifest carries neither capPerShard nor reserve; cannot derive a mask", c.Like))
		}
		fmt.Fprintf(os.Stderr, "export-parts: aligning to %s (capPerShard=%d parts=%d)\n", c.Like, capPS, lm.Parts)
		m, err = core.ExportParts(c.Store, c.Out, keyLen, valLen, shards, capPS, lm.Parts)
	case c.Reserve > 0:
		// Explicit reserve: the delta-alignable path — both sides must reuse this value.
		m, err = core.ExportParts(c.Store, c.Out, keyLen, valLen, shards, core.ReserveCapPerShard(c.Reserve, shards), c.Parts)
	default:
		// Auto-derive the mask from the store's actual per-shard capacity (max across
		// shards). Ship-whole/rebuild only — not for a delta, which needs a shared reserve.
		max, min, e := core.StoreCapPerShard(c.Store, shards)
		if e != nil {
			fatal(e)
		}
		if max != min {
			fmt.Fprintf(os.Stderr, "export-parts: warning: shards diverged (capacity %d…%d); bucketing by the max, so cold shards over-allocate to the hot shard's size on rebuild\n", min, max)
		}
		fmt.Fprintf(os.Stderr, "export-parts: auto-derived capPerShard=%d (ship-whole; pass -reserve to align a delta)\n", max)
		m, err = core.ExportParts(c.Store, c.Out, keyLen, valLen, shards, max, c.Parts)
	}
	if err != nil {
		fatal(err)
	}
	fmt.Printf("export-parts: %d records → %d parts (%s.p000…) + %s\n",
		m.Records, m.Parts, c.Out, filepath.Base(core.ManifestPath(c.Out)))
}

// ---- delta ----

type deltaCfg struct {
	Old    string `env:"order,require" help:"prior-state partition prefix (<prefix>.pNNN + manifest)"`
	New    string `env:"order,require" help:"current-state partition prefix"`
	Out    string `default:"patch.tar.gz" help:"output patch bundle path"`
	Store  string `help:"OLD store prefix: record its Digest as the patch base (recommended)"`
	Shards int    `help:"shard count for the base digest (0 = auto-detect; needs -store)"`
	Level  int    `default:"-1" help:"gzip level 1..9 (1=fast, 9=best; -1=default)"`
}

func runDelta(c *deltaCfg) {
	// The base digest is the OLD store's, so patch-apply refuses unless the target
	// is at the old state. Geometry comes from the old parts manifest.
	baseDigest := ""
	if c.Store != "" {
		om, err := core.ReadManifest(core.ManifestPath(c.Old))
		if err != nil {
			fatal(fmt.Errorf("reading old parts manifest for base digest: %w", err))
		}
		shards := c.Shards
		if shards <= 0 {
			if shards = detectShards(c.Store); shards == 0 {
				shards = om.Shards
			}
		}
		d, n, err := core.StoreDigest(c.Store, om.KeyLen, om.ValLen, shards)
		if err != nil {
			fatal(fmt.Errorf("reading old store base digest: %w", err))
		}
		baseDigest = d
		fmt.Printf("base store digest: %s  (%d entries)\n", d, n)
	}

	snap, err := patch.Diff(c.Old, c.New, baseDigest)
	if err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(c.Out), 0o755); err != nil {
		fatal(err)
	}
	f, err := os.Create(c.Out)
	if err != nil {
		fatal(err)
	}
	man, err := patch.WriteBundle(f, snap, c.Level)
	if err != nil {
		f.Close()
		fatal(err)
	}
	if err := f.Close(); err != nil {
		fatal(err)
	}
	fmt.Printf("delta: %d upserts, %d deletes → %s\n", man.Upserts, man.Deletes, c.Out)
}
