package core

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDigestOrderIndependentAndSensitive checks the digest is independent of
// insert order but changes on any content change.
func TestDigestOrderIndependentAndSensitive(t *testing.T) {
	a, _ := Open(filepath.Join(t.TempDir(), "a.core"), 8, 8, 16)
	defer a.Close()
	b, _ := Open(filepath.Join(t.TempDir(), "b.core"), 8, 8, 16)
	defer b.Close()

	const n = 1000
	for i := uint64(0); i < n; i++ {
		a.Put(k8(i), v8(i*3))
	}
	for i := uint64(n); i > 0; i-- { // reverse order
		b.Put(k8(i-1), v8((i-1)*3))
	}
	if a.Digest() != b.Digest() {
		t.Fatalf("order-independent digest mismatch: %016x vs %016x", a.Digest(), b.Digest())
	}

	d := a.Digest()
	a.Put(k8(42), v8(999999)) // value update
	if a.Digest() == d {
		t.Fatal("digest unchanged after a value update")
	}
	a.Put(k8(42), v8(42*3)) // revert
	if a.Digest() != d {
		t.Fatal("digest not restored after reverting the value")
	}
	a.Delete(k8(7))
	if a.Digest() == d {
		t.Fatal("digest unchanged after a delete")
	}
	a.Put(k8(7), v8(7*3)) // reinsert
	if a.Digest() != d {
		t.Fatal("digest not restored after reinsert")
	}
}

// TestDigestSurvivesReopenAndResize confirms the digest is stable across many
// resizes and a close/reopen (it is a pure function of content).
func TestDigestSurvivesReopenAndResize(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.core")
	m, _ := Open(p, 8, 8, 16) // tiny ⇒ many resizes
	const n = 5000
	for i := uint64(0); i < n; i++ {
		m.Put(k8(i), v8(i))
	}
	d := m.Digest()
	m.Close()

	m2, err := Open(p, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if m2.Digest() != d {
		t.Fatalf("digest changed across reopen: %016x vs %016x", d, m2.Digest())
	}
}

// TestShardedDigest checks the whole-store digest equals the sum of shard
// digests, validating the per-shard verification path.
func TestShardedDigest(t *testing.T) {
	sh, _ := OpenSharded(filepath.Join(t.TempDir(), "s"), 8, 8, 8, 16)
	defer sh.Close()
	for i := uint64(0); i < 4000; i++ {
		sh.Put(k8(i), v8(i))
	}
	var sum uint64
	for i := 0; i < sh.Shards(); i++ {
		sum += sh.Shard(i).Digest()
	}
	if sum != sh.Digest() {
		t.Fatalf("whole digest %016x != sum of shard digests %016x", sh.Digest(), sum)
	}
	if sh.Digest() == 0 {
		t.Fatal("digest is zero for a non-empty store")
	}
}

// TestHeaderChecksumDetectsGeometryCorruption corrupts an immutable header field
// and confirms Open rejects it.
func TestHeaderChecksumDetectsGeometryCorruption(t *testing.T) {
	p := filepath.Join(t.TempDir(), "h.core")
	m, _ := Open(p, 8, 8, 16)
	m.Put(k8(1), v8(1))
	m.Close()

	flipByte(t, p, hCapacity) // corrupt the capacity field (covered by checksum)
	if _, err := Open(p, 8, 8, 16); err != ErrCorrupt {
		t.Fatalf("expected ErrCorrupt on geometry corruption, got %v", err)
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}
