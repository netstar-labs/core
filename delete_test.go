package core

import (
	"path/filepath"
	"testing"
)

// buildSharded opens an n-shard store and inserts keys k8(0..n).
func buildSharded(t *testing.T, keys int) *Sharded {
	t.Helper()
	sh, err := OpenSharded(filepath.Join(t.TempDir(), "s"), 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < uint64(keys); i++ {
		if _, err := sh.Put(k8(i), v8(i)); err != nil {
			t.Fatal(err)
		}
	}
	return sh
}

func TestDeleteBatch(t *testing.T) {
	const n = 20000
	sh := buildSharded(t, n)
	defer sh.Close()

	// Delete every third key, plus some never-inserted keys (must be no-ops).
	var keys [][]byte
	wantDeleted := 0
	for i := uint64(0); i < n; i += 3 {
		keys = append(keys, k8(i))
		wantDeleted++
	}
	for i := uint64(n); i < n+50; i++ { // absent keys
		keys = append(keys, k8(i))
	}

	deleted, err := sh.DeleteBatch(keys)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != wantDeleted {
		t.Fatalf("deleted=%d want %d (absent keys must not count)", deleted, wantDeleted)
	}
	if sh.Len() != n-wantDeleted {
		t.Fatalf("len=%d want %d", sh.Len(), n-wantDeleted)
	}
	// Deleted keys gone; survivors present.
	for i := uint64(0); i < n; i++ {
		_, ok := sh.Get(k8(i))
		if want := i%3 != 0; ok != want {
			t.Fatalf("key %d present=%v want %v", i, ok, want)
		}
	}

	// Re-deleting an already-tombstoned key is a no-op (idempotent).
	again, err := sh.DeleteBatch(keys)
	if err != nil {
		t.Fatal(err)
	}
	if again != 0 {
		t.Fatalf("second delete removed %d, want 0", again)
	}
}

func TestDeleteRecords(t *testing.T) {
	const n = 12000
	sh := buildSharded(t, n)
	defer sh.Close()

	// Pack keyLen-byte keys (no value) for the even keys.
	var buf []byte
	wantDeleted := 0
	for i := uint64(0); i < n; i += 2 {
		buf = append(buf, k8(i)...)
		wantDeleted++
	}

	deleted, err := sh.DeleteRecords(buf)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != wantDeleted {
		t.Fatalf("deleted=%d want %d", deleted, wantDeleted)
	}
	if sh.Len() != n-wantDeleted {
		t.Fatalf("len=%d want %d", sh.Len(), n-wantDeleted)
	}
	for i := uint64(0); i < n; i++ {
		_, ok := sh.Get(k8(i))
		if want := i%2 != 0; ok != want {
			t.Fatalf("key %d present=%v want %v", i, ok, want)
		}
	}

	// A buffer whose length is not a multiple of keyLen is rejected.
	if _, err := sh.DeleteRecords([]byte{1, 2, 3}); err != ErrBadSize {
		t.Fatalf("short buffer err=%v want ErrBadSize", err)
	}
}

// TestDeleteDigestEquivalence proves tombstones are invisible to the content
// digest: a store with {0..n} minus the odds equals a store built with only the
// evens — independent of the delete path having left tombstones behind.
func TestDeleteDigestEquivalence(t *testing.T) {
	const n = 8000
	full := buildSharded(t, n)
	defer full.Close()

	var odds [][]byte
	for i := uint64(1); i < n; i += 2 {
		odds = append(odds, k8(i))
	}
	if _, err := full.DeleteBatch(odds); err != nil {
		t.Fatal(err)
	}

	// Reference store: insert only the evens directly.
	ref, err := OpenSharded(filepath.Join(t.TempDir(), "ref"), 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	for i := uint64(0); i < n; i += 2 {
		if _, err := ref.Put(k8(i), v8(i)); err != nil {
			t.Fatal(err)
		}
	}

	if full.Digest() != ref.Digest() {
		t.Fatalf("digest after delete %#x != reference %#x", full.Digest(), ref.Digest())
	}
}
