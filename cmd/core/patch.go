// "patch-build" and "patch-apply" subcommands: produce and apply a transportable
// patch — a coalesced set of upserts (key|value) and deletes (key-only) — against
// an existing store, via the core/patch library over core/bundle.
//
//	core patch-build -upserts add.txt -deletes del.txt -store kv.core -out patch.tar.gz
//	core patch-apply -in patch.tar.gz -store kv.core
//
// build records the target store's content digest as the patch's base, so apply
// can refuse a patch built against a different store (override with -force). The
// store's insert is already an upsert, so "inserts" and "updates" share the
// -upserts stream; only deletions are a separate, key-only stream.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/internal/mmap"
	"github.com/netstar-labs/core/patch"
)

// ---- patch-build ----

type patchBuildCfg struct {
	Upserts string `help:"upsert source: 'key<sp>uint64' text lines (or packed key|value records with -bin)"`
	Deletes string `help:"delete source: one key per line (or packed keyLen-byte keys with -bin)"`
	Store   string `default:"kv.core" help:"target store: read geometry + record its digest as the patch base"`
	Shards  int    `help:"shard count for the base digest (0 = auto-detect from <store>.NNN)"`
	Out     string `default:"patch.tar.gz" help:"output patch bundle path"`
	Bin     bool   `help:"sources are packed binary (already-hashed keys), not text"`
	Level   int    `default:"-1" help:"gzip level 1..9 (1=fast, 9=best; -1=default)"`
	Keylen  int    `default:"32" help:"key length in bytes (auto-detected from -store)"`
	Vallen  int    `default:"8" help:"value length in bytes (auto-detected from -store)"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runPatchBuild(c *patchBuildCfg) {
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)
	b := patch.NewBuilder(keyLen, valLen)

	var rawUp, rawDel int
	if c.Upserts != "" {
		var err error
		if c.Bin {
			rawUp, err = loadUpsertsBin(b, c.Upserts)
		} else {
			rawUp, err = loadUpsertsText(b, c.Upserts)
		}
		if err != nil {
			fatal(err)
		}
	}
	if c.Deletes != "" {
		var err error
		if c.Bin {
			rawDel, err = loadDeletesBin(b, c.Deletes)
		} else {
			rawDel, err = loadDeletesText(b, c.Deletes)
		}
		if err != nil {
			fatal(err)
		}
	}
	if b.Len() == 0 {
		fatal(fmt.Errorf("no changes: set -upserts and/or -deletes"))
	}

	// Record the target store's digest as the patch base so apply can verify it.
	baseDigest := ""
	if c.Store != "" {
		shards := c.Shards
		if shards <= 0 {
			shards = detectShards(c.Store)
		}
		if shards > 0 {
			d, n, err := core.StoreDigest(c.Store, keyLen, valLen, shards)
			if err != nil {
				fatal(fmt.Errorf("reading store base digest: %w", err))
			}
			baseDigest = d
			fmt.Printf("base store digest: %s  (%d entries)\n", d, n)
		}
	}

	snap := b.Snapshot(baseDigest)
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
	fmt.Printf("patch build: %d upserts, %d deletes (coalesced from %d changes) → %s\n",
		man.Upserts, man.Deletes, rawUp+rawDel, c.Out)
}

// ---- patch-apply ----

type patchApplyCfg struct {
	In     string `default:"patch.tar.gz" help:"input patch bundle path"`
	Store  string `default:"kv.core" help:"store to apply the patch to"`
	Shards int    `help:"shard count (0 = auto-detect from <store>.NNN)"`
	Dir    string `help:"scratch dir to extract into (default: a temp dir, removed after)"`
	Cap    int    `default:"1024" help:"initial capacity per shard when opening"`
	Force  bool   `help:"apply even if the patch base digest does not match the store"`
}

func runPatchApply(c *patchApplyCfg) {
	f, err := os.Open(c.In)
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	dir := c.Dir
	if dir == "" {
		d, err := os.MkdirTemp("", "core-patch-apply-")
		if err != nil {
			fatal(err)
		}
		dir = d
		defer os.RemoveAll(d)
	}
	snap, man, err := patch.OpenBundle(f, dir) // extracts + CRC-verifies the blobs
	if err != nil {
		fatal(err)
	}

	// The store header is authoritative for geometry; the patch must match it.
	setGeometryFromStore(c.Store, man.KeyLen, man.ValLen)
	if man.KeyLen != keyLen || man.ValLen != valLen {
		fatal(fmt.Errorf("patch geometry keyLen=%d valLen=%d does not match store keyLen=%d valLen=%d",
			man.KeyLen, man.ValLen, keyLen, valLen))
	}
	shards := c.Shards
	if shards <= 0 {
		if shards = detectShards(c.Store); shards == 0 {
			fatal(fmt.Errorf("no store found at %s.000", c.Store))
		}
	}

	sh, err := core.OpenSharded(c.Store, keyLen, valLen, shards, c.Cap)
	if err != nil {
		fatal(err)
	}
	defer sh.Close()

	if man.BaseDigest != "" {
		cur := fmt.Sprintf("%016x", sh.Digest())
		switch {
		case cur == man.BaseDigest:
			fmt.Printf("base digest OK: %s\n", cur)
		case c.Force:
			fmt.Printf("WARNING: base digest mismatch (store %s != patch %s); applying anyway\n", cur, man.BaseDigest)
		default:
			fatal(fmt.Errorf("base digest mismatch: store %s != patch base %s (use -force to override)", cur, man.BaseDigest))
		}
	}

	st, err := patch.Apply(sh, snap)
	if err != nil {
		fatal(err)
	}
	if err := sh.Sync(); err != nil {
		fatal(err)
	}
	fmt.Printf("patch apply: %d upserts (%d new), %d deletes (%d removed)\n",
		st.Upserts, st.Inserted, st.Deletes, st.Removed)
	fmt.Printf("store now %d entries, digest %016x\n", sh.Len(), sh.Digest())
}

// ---- source loaders ----

// loadUpsertsText reads "key<sp>uint64" lines, hashing the key text into the key
// slot (the same makeKey the store uses) and storing the value little-endian.
func loadUpsertsText(b *patch.Builder, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	k := make([]byte, keyLen)
	val := make([]byte, valLen)
	n := 0
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
		binary.LittleEndian.PutUint64(val, v)
		if err := b.Upsert(k, val); err != nil {
			return n, err
		}
		n++
	}
	return n, sc.Err()
}

// loadDeletesText reads one key per line, hashing each into the key slot.
func loadDeletesText(b *patch.Builder, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	k := make([]byte, keyLen)
	n := 0
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		makeKey(k, line)
		if err := b.Delete(k); err != nil {
			return n, err
		}
		n++
	}
	return n, sc.Err()
}

// loadUpsertsBin reads packed key|value records (already-hashed keys).
func loadUpsertsBin(b *patch.Builder, path string) (int, error) {
	ra, err := mmap.Open(path)
	if err != nil {
		return 0, err
	}
	defer ra.Close()
	buf := ra.Bytes()
	if recordSize == 0 || len(buf)%recordSize != 0 {
		return 0, fmt.Errorf("%s size %d is not a multiple of record size %d", path, len(buf), recordSize)
	}
	n := 0
	for off := 0; off < len(buf); off += recordSize {
		if err := b.Upsert(buf[off:off+keyLen], buf[off+keyLen:off+recordSize]); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// loadDeletesBin reads packed keyLen-byte keys (already-hashed).
func loadDeletesBin(b *patch.Builder, path string) (int, error) {
	ra, err := mmap.Open(path)
	if err != nil {
		return 0, err
	}
	defer ra.Close()
	buf := ra.Bytes()
	if keyLen == 0 || len(buf)%keyLen != 0 {
		return 0, fmt.Errorf("%s size %d is not a multiple of key length %d", path, len(buf), keyLen)
	}
	n := 0
	for off := 0; off < len(buf); off += keyLen {
		if err := b.Delete(buf[off : off+keyLen]); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
