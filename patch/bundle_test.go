package patch

import (
	"bytes"
	"testing"
)

func TestBundleRoundTrip(t *testing.T) {
	b := NewBuilder(8, 8)
	for i := uint64(0); i < 500; i++ {
		b.Upsert(k8(i), v8(i*7))
	}
	for i := uint64(0); i < 100; i++ {
		b.Delete(k8(10000 + i))
	}
	snap := b.Snapshot("0123456789abcdef")

	var buf bytes.Buffer
	man, err := WriteBundle(&buf, snap, -1)
	if err != nil {
		t.Fatal(err)
	}
	if man.Upserts != 500 || man.Deletes != 100 {
		t.Fatalf("manifest counts %+v", man)
	}

	got, gm, err := OpenBundle(bytes.NewReader(buf.Bytes()), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if gm.BaseDigest != "0123456789abcdef" {
		t.Errorf("baseDigest=%q", gm.BaseDigest)
	}
	if got.NumUpserts() != snap.NumUpserts() || got.NumDeletes() != snap.NumDeletes() {
		t.Fatalf("counts differ: got %d/%d want %d/%d",
			got.NumUpserts(), got.NumDeletes(), snap.NumUpserts(), snap.NumDeletes())
	}
	if !bytes.Equal(got.Upserts, snap.Upserts) || !bytes.Equal(got.Deletes, snap.Deletes) {
		t.Fatal("blob bytes did not round-trip")
	}
}

// TestBundleDetectsCorruption confirms the enclosing bundle's CRC catches a
// tampered blob byte.
func TestBundleDetectsCorruption(t *testing.T) {
	b := NewBuilder(8, 8)
	for i := uint64(0); i < 200; i++ {
		b.Upsert(k8(i), v8(i))
	}
	var buf bytes.Buffer
	if _, err := WriteBundle(&buf, b.Snapshot(""), -1); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	for i := len(raw) / 3; i < len(raw)/3+32 && i < len(raw); i++ {
		raw[i] ^= 0xff // corrupt a span mid-stream so detection is deterministic
	}
	if _, _, err := OpenBundle(bytes.NewReader(raw), t.TempDir()); err == nil {
		t.Error("OpenBundle accepted a corrupted bundle")
	}
}
