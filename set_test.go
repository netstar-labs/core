package core

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestKeyOnlySet exercises a valLen=0 (set) Map: add/contains/get/delete,
// re-add is a no-op, iteration yields empty values, and many inserts force
// resizes that preserve membership.
func TestKeyOnlySet(t *testing.T) {
	m, _ := openTmp(t, 8, 0, 16) // valLen=0 ⇒ set; tiny cap ⇒ several grows
	defer m.Close()

	const n = 5000
	for i := uint64(0); i < n; i++ {
		created, err := m.Put(k8(i), nil)
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if !created {
			t.Fatalf("add %d not reported new", i)
		}
	}
	if m.Len() != n {
		t.Fatalf("len=%d want %d", m.Len(), n)
	}

	// re-adding an existing member is a no-op (not newly created)
	if created, _ := m.Put(k8(7), nil); created {
		t.Fatal("re-add reported new")
	}

	for i := uint64(0); i < n; i++ {
		if !m.Contains(k8(i)) {
			t.Fatalf("missing member %d", i)
		}
		val, ok := m.Get(k8(i))
		if !ok || len(val) != 0 {
			t.Fatalf("get member %d: ok=%v len=%d", i, ok, len(val))
		}
		if !m.GetInto(k8(i), nil) {
			t.Fatalf("GetInto member %d reported absent", i)
		}
	}
	if m.Contains(k8(n + 1)) {
		t.Fatal("non-member reported present")
	}

	if !m.Delete(k8(0)) || m.Contains(k8(0)) {
		t.Fatal("delete failed")
	}

	seen := 0
	m.ForEach(func(key, val []byte) bool {
		if len(key) != 8 || len(val) != 0 {
			t.Fatalf("foreach key=%d val=%d (want 8/0)", len(key), len(val))
		}
		seen++
		return true
	})
	if seen != n-1 { // one deleted
		t.Fatalf("foreach saw %d want %d", seen, n-1)
	}
}

// TestKeyOnlySetSharded builds a sharded set from key-only packed records and
// confirms the digest matches one built via Put(nil) over the same keys.
func TestKeyOnlySetSharded(t *testing.T) {
	dir := t.TempDir()
	sh, err := OpenSharded(filepath.Join(dir, "s"), 8, 0, 8, 1024)
	if err != nil {
		t.Fatalf("OpenSharded: %v", err)
	}
	defer sh.Close()

	const n = 4000
	buf := make([]byte, 0, n*8) // key-only records: 8 bytes each, no value
	for i := uint64(0); i < n; i++ {
		buf = append(buf, k8(i)...)
	}
	ins, err := sh.PutRecords(buf)
	if err != nil {
		t.Fatalf("PutRecords: %v", err)
	}
	if ins != n {
		t.Fatalf("inserted %d want %d", ins, n)
	}
	for i := uint64(0); i < n; i++ {
		if !sh.Contains(k8(i)) {
			t.Fatalf("missing %d", i)
		}
	}

	sh2, err := OpenSharded(filepath.Join(dir, "s2"), 8, 0, 8, 1024)
	if err != nil {
		t.Fatalf("OpenSharded2: %v", err)
	}
	defer sh2.Close()
	for i := uint64(0); i < n; i++ {
		if _, err := sh2.Put(k8(i), nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if d1, d2 := sh.Digest(), sh2.Digest(); d1 != d2 {
		t.Fatalf("digest mismatch: %016x vs %016x", d1, d2)
	}
}

// TestKeyOnlyBadSize confirms valLen 0 is accepted while a non-multiple-of-8 or
// negative valLen (and a zero keyLen) is still rejected.
func TestKeyOnlyBadSize(t *testing.T) {
	dir := t.TempDir()
	if m, err := Open(filepath.Join(dir, "ok"), 8, 0, 16); err != nil {
		t.Fatalf("valLen=0 rejected: %v", err)
	} else {
		m.Close()
	}
	for _, c := range []struct{ kl, vl int }{{8, 4}, {8, -8}, {0, 0}, {4, 0}} {
		if _, err := Open(filepath.Join(dir, "bad"), c.kl, c.vl, 16); !errors.Is(err, ErrBadSize) {
			t.Fatalf("keyLen=%d valLen=%d: err=%v want ErrBadSize", c.kl, c.vl, err)
		}
	}
}
