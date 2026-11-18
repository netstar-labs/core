package patch

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/netstar-labs/core"
)

func k8(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

func v8(u uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, u)
	return b
}

func openStore(t *testing.T, name string) *core.Sharded {
	t.Helper()
	sh, err := core.OpenSharded(filepath.Join(t.TempDir(), name), 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	return sh
}

// TestBuilderCoalesce checks last-writer-wins and disjointness.
func TestBuilderCoalesce(t *testing.T) {
	b := NewBuilder(8, 8)
	// upsert then delete → delete wins
	b.Upsert(k8(1), v8(10))
	b.Delete(k8(1))
	// delete then upsert → upsert wins
	b.Delete(k8(2))
	b.Upsert(k8(2), v8(20))
	// repeated upsert → last value
	b.Upsert(k8(3), v8(30))
	b.Upsert(k8(3), v8(31))

	if b.Len() != 3 {
		t.Fatalf("Len=%d want 3", b.Len())
	}
	s := b.Snapshot("")
	if s.NumUpserts() != 2 { // keys 2 and 3
		t.Fatalf("upserts=%d want 2", s.NumUpserts())
	}
	if s.NumDeletes() != 1 { // key 1
		t.Fatalf("deletes=%d want 1", s.NumDeletes())
	}
	// Disjoint: no key appears in both streams.
	del := map[string]bool{}
	for i := 0; i < len(s.Deletes); i += 8 {
		del[string(s.Deletes[i:i+8])] = true
	}
	for i := 0; i < len(s.Upserts); i += 16 {
		if del[string(s.Upserts[i:i+8])] {
			t.Fatal("key appears in both upsert and delete streams")
		}
	}
	// Key 3's surviving value is the last one written (31).
	for i := 0; i < len(s.Upserts); i += 16 {
		if string(s.Upserts[i:i+8]) == string(k8(3)) {
			if got := binary.LittleEndian.Uint64(s.Upserts[i+8 : i+16]); got != 31 {
				t.Fatalf("key 3 value=%d want 31", got)
			}
		}
	}
}

// TestApplyMatchesReference applies a mixed patch (new inserts, updates, deletes)
// and checks the store equals one built directly to the intended final state.
func TestApplyMatchesReference(t *testing.T) {
	const n = 6000
	dst := openStore(t, "dst")
	defer dst.Close()
	for i := uint64(0); i < n; i++ {
		dst.Put(k8(i), v8(i)) // base: key i -> i
	}
	base := dst.Digest()

	b := NewBuilder(8, 8)
	for i := uint64(0); i < n; i += 2 {
		b.Upsert(k8(i), v8(i*100)) // update evens to i*100
	}
	for i := uint64(1); i < n; i += 6 {
		b.Delete(k8(i)) // delete some odds
	}
	for i := uint64(n); i < n+1000; i++ {
		b.Upsert(k8(i), v8(i)) // insert new keys
	}
	snap := b.Snapshot("")

	st, err := Apply(dst, snap)
	if err != nil {
		t.Fatal(err)
	}
	if st.Upserts != snap.NumUpserts() || st.Deletes != snap.NumDeletes() {
		t.Fatalf("stats %+v inconsistent with snapshot", st)
	}

	// Reference: build the intended final state directly.
	ref := openStore(t, "ref")
	defer ref.Close()
	deleted := map[uint64]bool{}
	for i := uint64(1); i < n; i += 6 {
		deleted[i] = true
	}
	for i := uint64(0); i < n+1000; i++ {
		if deleted[i] {
			continue
		}
		switch {
		case i >= n:
			ref.Put(k8(i), v8(i))
		case i%2 == 0:
			ref.Put(k8(i), v8(i*100))
		default:
			ref.Put(k8(i), v8(i))
		}
	}
	if dst.Digest() != ref.Digest() {
		t.Fatalf("applied digest %#x != reference %#x", dst.Digest(), ref.Digest())
	}

	// Idempotent: applying the same snapshot again does not change the store.
	if _, err := Apply(dst, snap); err != nil {
		t.Fatal(err)
	}
	if dst.Digest() != ref.Digest() {
		t.Fatalf("second apply changed digest to %#x", dst.Digest())
	}
	_ = base
}
