package patch

import (
	"path/filepath"
	"testing"

	"github.com/netstar-labs/core"
)

// buildExport builds a store via `mutate`, exports it to partition packs at
// prefix, and returns the store's digest. Same (shards, reserve, parts) across
// calls keeps the exports part-aligned.
func buildExport(t *testing.T, dir, name string, shards, reserve, parts int, mutate func(*core.Sharded)) (prefix string, digest uint64) {
	t.Helper()
	store := filepath.Join(dir, name)
	sh, err := core.OpenSharded(store, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	mutate(sh)
	digest = sh.Digest()
	sh.Close()
	prefix = filepath.Join(dir, name+"-part")
	if _, err := core.ExportParts(store, prefix, 8, 8, shards, core.ReserveCapPerShard(reserve, shards), parts); err != nil {
		t.Fatal(err)
	}
	return prefix, digest
}

// TestDiffApply: diff old→new parts, apply the delta to the old store, and confirm
// it now equals the new store (the end-to-end incremental-replication path).
func TestDiffApply(t *testing.T) {
	const (
		n               = 12000
		shards, reserve = 8, 20000
		parts           = 16
	)
	dir := t.TempDir()

	oldStore := filepath.Join(dir, "old")
	sh, err := core.OpenSharded(oldStore, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < n; i++ {
		sh.Put(k8(i), v8(i))
	}
	oldDigest := sh.Digest()
	sh.Close()
	oldParts := filepath.Join(dir, "old-part")
	if _, err := core.ExportParts(oldStore, oldParts, 8, 8, shards, core.ReserveCapPerShard(reserve, shards), parts); err != nil {
		t.Fatal(err)
	}

	// New snapshot: update evens, delete every 5th, insert 500 new keys.
	newParts, newDigest := buildExport(t, dir, "new", shards, reserve, parts, func(s *core.Sharded) {
		for i := uint64(0); i < n; i++ {
			if i%5 == 0 {
				continue // deleted relative to old
			}
			v := i
			if i%2 == 0 {
				v = i * 100 // updated
			}
			s.Put(k8(i), v8(v))
		}
		for i := uint64(n); i < n+500; i++ {
			s.Put(k8(i), v8(i)) // inserted
		}
	})

	snap, err := Diff(oldParts, newParts, fmtDigest(oldDigest))
	if err != nil {
		t.Fatal(err)
	}
	if snap.BaseDigest != fmtDigest(oldDigest) {
		t.Errorf("base digest = %q", snap.BaseDigest)
	}
	if snap.NumUpserts() == 0 || snap.NumDeletes() == 0 {
		t.Fatalf("expected upserts and deletes, got %d/%d", snap.NumUpserts(), snap.NumDeletes())
	}

	// Apply the delta to the OLD store; it must become the NEW store.
	target, err := core.OpenSharded(oldStore, 8, 8, shards, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if _, err := Apply(target, snap); err != nil {
		t.Fatal(err)
	}
	if target.Digest() != newDigest {
		t.Fatalf("after delta, digest %#x != new %#x", target.Digest(), newDigest)
	}
}

func fmtDigest(d uint64) string {
	const hexd = "0123456789abcdef"
	var b [16]byte
	for i := 15; i >= 0; i-- {
		b[i] = hexd[d&0xf]
		d >>= 4
	}
	return string(b[:])
}

// TestDiffGeometryMismatch: parts exported with different geometry can't align.
func TestDiffGeometryMismatch(t *testing.T) {
	dir := t.TempDir()
	mut := func(s *core.Sharded) {
		for i := uint64(0); i < 1000; i++ {
			s.Put(k8(i), v8(i))
		}
	}
	a, _ := buildExport(t, dir, "a", 8, 20000, 16, mut)
	b, _ := buildExport(t, dir, "b", 8, 20000, 8, mut) // different parts count
	if _, err := Diff(a, b, ""); err == nil {
		t.Fatal("Diff accepted mismatched parts geometry")
	}
}

// buildExportCap builds a store via mutate and exports it by an explicit per-shard
// capacity (the auto/golden-master path: manifest records capPerShard, reserve 0).
func buildExportCap(t *testing.T, dir, name string, shards int, capPS uint64, parts int, mutate func(*core.Sharded)) string {
	t.Helper()
	store := filepath.Join(dir, name)
	sh, err := core.OpenSharded(store, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	mutate(sh)
	sh.Close()
	prefix := filepath.Join(dir, name+"-part")
	if _, err := core.ExportParts(store, prefix, 8, 8, shards, capPS, parts); err != nil {
		t.Fatal(err)
	}
	return prefix
}

// TestDiffAlignsAcrossEncodings: a reserve-encoded export and a capPerShard-encoded
// export that resolve to the SAME effective capacity are part-aligned and diff
// correctly — the alignment key is the mask the geometry implies, not how it was
// recorded. This is what makes the golden-master -like path (capPerShard) diff
// against a legacy reserve-encoded set.
func TestDiffAlignsAcrossEncodings(t *testing.T) {
	const (
		shards, reserve, parts = 8, 20000, 16
	)
	dir := t.TempDir()
	mut := func(s *core.Sharded) {
		for i := uint64(0); i < 3000; i++ {
			s.Put(k8(i), v8(i))
		}
	}
	// Reserve-encoded old side.
	oldParts := filepath.Join(dir, "old-part")
	oldStore := filepath.Join(dir, "old")
	sh, err := core.OpenSharded(oldStore, 8, 8, shards, 16)
	if err != nil {
		t.Fatal(err)
	}
	mut(sh)
	sh.Close()
	if _, err := core.ExportParts(oldStore, oldParts, 8, 8, shards, core.ReserveCapPerShard(reserve, shards), parts); err != nil {
		t.Fatal(err)
	}
	om, err := core.ReadManifest(core.ManifestPath(oldParts))
	if err != nil {
		t.Fatal(err)
	}
	// New side bucketed by the SAME effective capacity, but recorded as capPerShard.
	newParts := buildExportCap(t, dir, "new", shards, om.EffectiveCapPerShard(), parts, mut)
	if _, err := Diff(oldParts, newParts, ""); err != nil {
		t.Fatalf("Diff rejected aligned sets across encodings: %v", err)
	}
}

// TestDiffRejectsDifferentCapacity: two auto exports at different capacities both
// record reserve 0, so aligning on Reserve would wrongly accept them. Diff must
// reject the pair on the effective capacity instead.
func TestDiffRejectsDifferentCapacity(t *testing.T) {
	const (
		shards, parts = 8, 8
	)
	dir := t.TempDir()
	mut := func(s *core.Sharded) {
		for i := uint64(0); i < 2000; i++ {
			s.Put(k8(i), v8(i))
		}
	}
	a := buildExportCap(t, dir, "a", shards, 4096, parts, mut)
	b := buildExportCap(t, dir, "b", shards, 8192, parts, mut) // same reserve(0), different mask
	if _, err := Diff(a, b, ""); err == nil {
		t.Fatal("Diff accepted two auto sets with different capacity (both reserve 0)")
	}
}
