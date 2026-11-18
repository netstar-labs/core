package core

import (
	"path/filepath"
	"testing"
)

// buildStore fills a fresh sharded store at `store` with n entries and returns its
// content digest. The caller owns the directory.
func buildStore(t *testing.T, store string, shards, n int) uint64 {
	t.Helper()
	src, err := OpenSharded(store, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < uint64(n); i++ {
		if _, err := src.Put(k8(i), v8(i*3)); err != nil {
			t.Fatal(err)
		}
	}
	d := src.Digest()
	src.Close()
	return d
}

// TestExportPartsCapAutoRoundTrip: an auto (capacity-derived) export records the
// store's actual per-shard capacity in the manifest (Reserve 0), and a rebuild
// pre-sizes to it — so the rebuilt store has exactly that capacity (no resize)
// and the same content digest, all without anyone supplying a reserve.
func TestExportPartsCapAutoRoundTrip(t *testing.T) {
	const (
		n      = 20000
		shards = 8
		parts  = 16
	)
	dir := t.TempDir()
	store := filepath.Join(dir, "kv")
	want := buildStore(t, store, shards, n)

	max, min, err := StoreCapPerShard(store, shards)
	if err != nil {
		t.Fatal(err)
	}
	if max != min {
		t.Fatalf("uniform load should give max==min, got %d..%d", min, max)
	}

	prefix := filepath.Join(dir, "part")
	m, err := ExportParts(store, prefix, 8, 8, shards, max, parts)
	if err != nil {
		t.Fatal(err)
	}
	if m.CapPerShard != max {
		t.Fatalf("manifest CapPerShard %d, want %d", m.CapPerShard, max)
	}
	if m.Reserve != 0 {
		t.Fatalf("auto export should record Reserve 0, got %d", m.Reserve)
	}
	if m.Records != n {
		t.Fatalf("exported %d records, want %d", m.Records, n)
	}

	// Rebuild from the manifest. Pass a deliberately tiny cap arg: the manifest's
	// CapPerShard must override it, so the load never resizes.
	rebuilt := filepath.Join(dir, "kv2")
	rm, st, err := IngestManifest(ManifestPath(prefix), rebuilt, 16, true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Total != n || rm.CapPerShard != max {
		t.Fatalf("ingest stats %+v / manifest cap %d unexpected (want total %d cap %d)", st, rm.CapPerShard, n, max)
	}

	// Growth-free: every rebuilt shard has exactly the source capacity.
	rmax, rmin, err := StoreCapPerShard(rebuilt, shards)
	if err != nil {
		t.Fatal(err)
	}
	if rmax != max || rmin != max {
		t.Fatalf("rebuilt capacity %d..%d, want uniform %d (a resize happened)", rmin, rmax, max)
	}

	sh2, err := OpenSharded(rebuilt, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh2.Close()
	if sh2.Digest() != want {
		t.Fatalf("rebuilt digest %#x != original %#x", sh2.Digest(), want)
	}
}

// TestExportPartsCapSkew: when one shard has grown past the others, the auto path
// buckets by the max capacity and the rebuild levels every shard up to it — still
// growth-free, still digest-identical. This is the "hot shard" case the max rule
// is designed for.
func TestExportPartsCapSkew(t *testing.T) {
	const (
		n      = 8000
		shards = 8
		parts  = 8
	)
	dir := t.TempDir()
	store := filepath.Join(dir, "kv")

	src, err := OpenSharded(store, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < uint64(n); i++ {
		if _, err := src.Put(k8(i), v8(i*3)); err != nil {
			t.Fatal(err)
		}
	}
	// Force shard 0 to grow well past the others — a deterministic skew.
	if err := src.Shard(0).Reserve(200000); err != nil {
		t.Fatal(err)
	}
	want := src.Digest()
	src.Close()

	max, min, err := StoreCapPerShard(store, shards)
	if err != nil {
		t.Fatal(err)
	}
	if max <= min {
		t.Fatalf("expected a skewed store (max > min), got %d..%d", min, max)
	}

	prefix := filepath.Join(dir, "part")
	if _, err := ExportParts(store, prefix, 8, 8, shards, max, parts); err != nil {
		t.Fatal(err)
	}

	rebuilt := filepath.Join(dir, "kv2")
	if _, _, err := IngestManifest(ManifestPath(prefix), rebuilt, 16, true); err != nil {
		t.Fatal(err)
	}
	rmax, rmin, err := StoreCapPerShard(rebuilt, shards)
	if err != nil {
		t.Fatal(err)
	}
	if rmax != max || rmin != max {
		t.Fatalf("rebuilt should level all shards to %d, got %d..%d", max, rmin, rmax)
	}

	sh2, err := OpenSharded(rebuilt, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh2.Close()
	if sh2.Digest() != want {
		t.Fatalf("rebuilt digest %#x != original %#x", sh2.Digest(), want)
	}
}

// TestReserveCapPerShardMatchesPlanLayout: ReserveCapPerShard is the reserve→capacity
// bridge the rest of the API sizes from, so it must agree with the capacity a real
// PlanLayout(reserve) reports — otherwise a reserve-sized export and a reserve-sized
// store would bucket by different masks.
func TestReserveCapPerShardMatchesPlanLayout(t *testing.T) {
	for _, reserve := range []int{8, 1000, 30000, 1 << 20} {
		const shards = 8
		l, err := PlanLayout(8, 8, shards, reserve)
		if err != nil {
			t.Fatal(err)
		}
		if got := ReserveCapPerShard(reserve, shards); got != l.CapPerShard {
			t.Fatalf("reserve %d: ReserveCapPerShard=%d != PlanLayout CapPerShard=%d", reserve, got, l.CapPerShard)
		}
	}
}
