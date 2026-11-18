package core

import (
	"path/filepath"
	"testing"
)

// TestExportPartsRoundTrip: build a store, export it to partition packs, rebuild a
// fresh store from those packs, and confirm the content digest is unchanged.
func TestExportPartsRoundTrip(t *testing.T) {
	const (
		n               = 20000
		shards, reserve = 8, 30000
		parts           = 16
	)
	dir := t.TempDir()
	store := filepath.Join(dir, "kv")
	src, err := OpenSharded(store, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < n; i++ {
		if _, err := src.Put(k8(i), v8(i*3)); err != nil {
			t.Fatal(err)
		}
	}
	want := src.Digest()
	src.Close()

	prefix := filepath.Join(dir, "part")
	m, err := ExportParts(store, prefix, 8, 8, shards, ReserveCapPerShard(reserve, shards), parts)
	if err != nil {
		t.Fatal(err)
	}
	if m.Records != n {
		t.Fatalf("exported %d records, want %d", m.Records, n)
	}
	sum := 0
	for _, c := range m.Counts {
		sum += c
	}
	if sum != n {
		t.Fatalf("per-part counts sum to %d, want %d", sum, n)
	}

	// Rebuild from the manifest into a fresh store; digest must match the original.
	rebuilt := filepath.Join(dir, "kv2")
	rm, st, err := IngestManifest(ManifestPath(prefix), rebuilt, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Total != n || rm.Parts != parts {
		t.Fatalf("ingest stats %+v / manifest parts %d unexpected", st, rm.Parts)
	}
	sh2, err := OpenSharded(rebuilt, 8, 8, shards, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sh2.Close()
	if sh2.Digest() != want {
		t.Fatalf("rebuilt digest %#x != original %#x", sh2.Digest(), want)
	}
}
