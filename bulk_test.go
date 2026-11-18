package core

import (
	"encoding/binary"
	"path/filepath"
	"testing"
)

// TestBulkLoadNoSync exercises Reserve + Batch + WithNoSync: one up-front resize,
// the writer lock taken once, no msync.
func TestBulkLoadNoSync(t *testing.T) {
	p := filepath.Join(t.TempDir(), "b.core")
	m, err := Open(p, 8, 8, 16, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	const n = 50000
	if err := m.Reserve(n); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := m.cur.Load().capacity; got < n {
		t.Fatalf("reserve gave capacity %d < %d", got, n)
	}
	err = m.Batch(func(put func(k, v []byte) (bool, error)) error {
		for i := uint64(0); i < n; i++ {
			if _, e := put(k8(i), v8(i)); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if m.Len() != n {
		t.Fatalf("len=%d want %d", m.Len(), n)
	}
	for i := uint64(0); i < n; i++ {
		if val, ok := m.Get(k8(i)); !ok || binary.LittleEndian.Uint64(val) != i {
			t.Fatalf("key %d wrong/missing", i)
		}
	}
}

// TestShardedPutBatch checks the bucketed bulk path across shards.
func TestShardedPutBatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sb")
	sh, err := OpenSharded(p, 8, 8, 8, 16, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	const n = 40000
	if err := sh.Reserve(n); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := uint64(0); i < n; i++ {
		keys[i] = k8(i)
		vals[i] = v8(i * 5)
	}
	ins, err := sh.PutBatch(keys, vals)
	if err != nil {
		t.Fatalf("putbatch: %v", err)
	}
	if ins != n {
		t.Fatalf("inserted=%d want %d", ins, n)
	}
	for i := uint64(0); i < n; i++ {
		if val, ok := sh.Get(k8(i)); !ok || binary.LittleEndian.Uint64(val) != i*5 {
			t.Fatalf("key %d wrong/missing", i)
		}
	}
}
