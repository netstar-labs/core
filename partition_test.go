package core

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// makeKeys returns n deterministic distinct keys of keyLen bytes.
func makeKeys(n, keyLen int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		k := make([]byte, keyLen)
		binary.LittleEndian.PutUint64(k, uint64(i)*0x9e3779b97f4a7c15+1)
		binary.LittleEndian.PutUint64(k[keyLen-8:], uint64(i)*0xff51afd7ed558ccd+7)
		keys[i] = k
	}
	return keys
}

// TestLayoutMatchesStore asserts the predicted per-shard capacity equals what a
// real store ends up with after Reserve — the invariant the partition math rides
// on. If Sharded.Reserve / Map.Reserve rounding ever changes, this fails loudly
// instead of silently mis-bucketing a staged load.
func TestLayoutMatchesStore(t *testing.T) {
	cases := []struct{ keyLen, valLen, shards, reserve int }{
		{8, 8, 1, 1000}, {8, 8, 4, 1_000_000}, {32, 8, 8, 5_000_000},
		{16, 0, 2, 250_000}, {8, 16, 16, 9_000_000}, {32, 8, 1, 3},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("k%d_v%d_s%d_r%d", c.keyLen, c.valLen, c.shards, c.reserve), func(t *testing.T) {
			l, err := PlanLayout(c.keyLen, c.valLen, c.shards, c.reserve)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "store")
			sh, err := OpenSharded(path, c.keyLen, c.valLen, c.shards, 8, WithNoSync())
			if err != nil {
				t.Fatal(err)
			}
			if err := sh.Reserve(c.reserve); err != nil {
				t.Fatal(err)
			}
			if err := sh.Sync(); err != nil {
				t.Fatal(err)
			}
			sh.Close()

			for i := 0; i < c.shards; i++ {
				h, err := ReadHeader(fmt.Sprintf("%s.%03d", path, i))
				if err != nil {
					t.Fatal(err)
				}
				if h.Capacity != l.CapPerShard {
					t.Fatalf("shard %d capacity %d != predicted CapPerShard %d", i, h.Capacity, l.CapPerShard)
				}
			}
		})
	}
}

// TestPartitionerMatchesPlacement asserts the Partitioner buckets each key by the
// exact group bits the store uses to place it: recomputing the partition from the
// store's actual on-disk capacity must agree with Partitioner.PartKey for every
// key, and a staged ingest (part by part) must reproduce every key.
func TestPartitionerMatchesPlacement(t *testing.T) {
	const keyLen, valLen, shards, reserve, parts = 8, 8, 4, 200_000, 16

	p, err := NewPartitioner(ReserveCapPerShard(reserve, shards), parts)
	if err != nil {
		t.Fatal(err)
	}
	if p.Parts() != parts {
		t.Fatalf("Parts()=%d want %d", p.Parts(), parts)
	}

	keys := makeKeys(50_000, keyLen)

	// Bucket the keys with the Partitioner.
	buckets := make([][][]byte, parts)
	for _, k := range keys {
		pi := p.PartKey(k)
		if pi < 0 || pi >= parts {
			t.Fatalf("PartKey out of range: %d", pi)
		}
		buckets[pi] = append(buckets[pi], k)
	}

	// Ingest part by part into a store with the SAME geometry + reserve.
	path := filepath.Join(t.TempDir(), "store")
	sh, err := OpenSharded(path, keyLen, valLen, shards, 8, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	if err := sh.Reserve(reserve); err != nil {
		t.Fatal(err)
	}
	val := make([]byte, valLen)
	for _, bucket := range buckets {
		for _, k := range bucket {
			if _, err := sh.Put(k, val); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got, want := sh.Len(), len(keys); got != want {
		t.Fatalf("store Len()=%d want %d (lost keys in staged ingest)", got, want)
	}
	for _, k := range keys {
		if !sh.Contains(k) {
			t.Fatalf("key missing after staged ingest")
		}
	}

	// Lock-step: recompute the partition from the store's REAL capacity and the
	// table's own g = (h>>7)&mask, and require it to equal Partitioner.Part.
	l, _ := PlanLayout(keyLen, valLen, shards, reserve)
	groups := l.CapPerShard >> 3
	mask := groups - 1
	gBits, partBits := trailingZeros(groups), trailingZeros(uint64(parts))
	shift := gBits - partBits
	for _, k := range keys {
		h := sum(k)
		wantPart := int(((h >> 7) & mask) >> shift)
		if got := p.Part(h); got != wantPart {
			t.Fatalf("Part(%#x)=%d want %d (mask=%#x shift=%d)", h, got, wantPart, mask, shift)
		}
	}
}

func trailingZeros(x uint64) uint {
	var n uint
	for x > 1 {
		x >>= 1
		n++
	}
	return n
}

// TestIngestManifest writes real part files + a manifest with the Partitioner,
// then rebuilds the store straight from the manifest (no geometry passed) and
// checks it equals a direct ingest of the same keys.
func TestIngestManifest(t *testing.T) {
	const keyLen, valLen, shards, reserve, parts = 8, 8, 4, 100_000, 8
	dir := t.TempDir()
	prefix := filepath.Join(dir, "part")

	p, err := NewPartitioner(ReserveCapPerShard(reserve, shards), parts)
	if err != nil {
		t.Fatal(err)
	}
	keys := makeKeys(30_000, keyLen)
	val := make([]byte, valLen)

	// Bucket and write packed records to <prefix>.pNNN.
	files := make([]*os.File, parts)
	counts := make([]int, parts)
	for i := range files {
		f, err := os.Create(fmt.Sprintf("%s.p%03d", prefix, i))
		if err != nil {
			t.Fatal(err)
		}
		files[i] = f
	}
	for _, k := range keys {
		pi := p.PartKey(k)
		if _, err := files[pi].Write(k); err != nil {
			t.Fatal(err)
		}
		if _, err := files[pi].Write(val); err != nil {
			t.Fatal(err)
		}
		counts[pi]++
	}
	for _, f := range files {
		f.Close()
	}

	// Self-describing manifest.
	manPath := ManifestPath(prefix)
	if err := WriteManifest(manPath, PartsManifest{
		KeyLen: keyLen, ValLen: valLen, RecordSize: keyLen + valLen,
		Shards: shards, Reserve: reserve, Parts: parts,
		Records: len(keys), Prefix: "part", Counts: counts,
	}); err != nil {
		t.Fatal(err)
	}

	// Build from the manifest alone — no geometry args. flushPerPart exercises the
	// per-part Sync path (deterministic resident-memory bound).
	m, st, err := IngestManifest(manPath, filepath.Join(dir, "fromparts"), 8, true, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	if m.Shards != shards || m.Reserve != reserve || m.KeyLen != keyLen {
		t.Fatalf("manifest geometry not round-tripped: %+v", m)
	}
	if st.Total != len(keys) {
		t.Fatalf("ingested Total=%d want %d", st.Total, len(keys))
	}

	// Cross-check digest against a direct build of the same keys.
	want, err := OpenSharded(filepath.Join(dir, "direct"), keyLen, valLen, shards, 8, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	want.Reserve(reserve)
	for _, k := range keys {
		want.Put(k, val)
	}
	wantDigest := want.Digest()
	want.Close()

	got, err := OpenSharded(filepath.Join(dir, "fromparts"), keyLen, valLen, shards, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.Digest() != wantDigest {
		t.Fatalf("digest from manifest build %#x != direct %#x", got.Digest(), wantDigest)
	}
}

func TestNewPartitionerValidation(t *testing.T) {
	capPS := ReserveCapPerShard(1000, 4)
	if _, err := NewPartitioner(capPS, 3); err == nil {
		t.Fatal("expected error for non-power-of-two parts")
	}
	if _, err := NewPartitioner(100, 4); err == nil { // 100 is not a power of two
		t.Fatal("expected error for non-power-of-two capPerShard")
	}
	if _, err := NewPartitioner(4, 1); err == nil { // < 8 = fewer than one group
		t.Fatal("expected error for capPerShard below one group")
	}
	// parts may not exceed the per-shard group count.
	l, _ := PlanLayout(8, 8, 1, 8) // tiny reserve → few groups
	groups := int(l.CapPerShard >> 3)
	if _, err := NewPartitioner(l.CapPerShard, groups*2); err == nil {
		t.Fatalf("expected error for parts > group count (%d)", groups)
	}
}
