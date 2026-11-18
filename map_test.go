package core

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"

	"github.com/netstar-labs/core/internal/mmap"
)

func k8(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}
func k32(u uint64) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], u)
	return b
}
func v8(u uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, u)
	return b
}
func v16(u uint64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, u)
	binary.LittleEndian.PutUint64(b[8:], ^u)
	return b
}

func openTmp(t *testing.T, kl, vl, cap int) (*Map, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "m.core")
	m, err := Open(p, kl, vl, cap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return m, p
}

// TestPutGetGrow inserts far more than the initial capacity, forcing several
// resizes, then verifies every entry.
func TestPutGetGrow(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 16) // tiny start ⇒ many grows
	defer m.Close()

	const n = 20000
	for i := uint64(0); i < n; i++ {
		ins, err := m.Put(k8(i), v8(i*7))
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if !ins {
			t.Fatalf("put %d not reported new", i)
		}
	}
	if m.Len() != n {
		t.Fatalf("len=%d want %d", m.Len(), n)
	}
	for i := uint64(0); i < n; i++ {
		val, ok := m.Get(k8(i))
		if !ok || binary.LittleEndian.Uint64(val) != i*7 {
			t.Fatalf("key %d: ok=%v val=%v", i, ok, val)
		}
	}
	if _, ok := m.Get(k8(n + 1)); ok {
		t.Fatal("found absent key")
	}
}

// TestUpdateAndPutSemantics checks insert-vs-update reporting and value changes.
func TestUpdateAndPutSemantics(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 64)
	defer m.Close()

	if ins, _ := m.Put(k8(5), v8(100)); !ins {
		t.Fatal("first put should insert")
	}
	if ins, _ := m.Put(k8(5), v8(200)); ins {
		t.Fatal("second put should update, not insert")
	}
	if val, _ := m.Get(k8(5)); binary.LittleEndian.Uint64(val) != 200 {
		t.Fatal("put-update value wrong")
	}
	if !m.Update(k8(5), v8(300)) {
		t.Fatal("update of existing should succeed")
	}
	if val, _ := m.Get(k8(5)); binary.LittleEndian.Uint64(val) != 300 {
		t.Fatal("update value wrong")
	}
	if m.Update(k8(999), v8(1)) {
		t.Fatal("update of absent key should fail")
	}
}

// TestDelete removes keys and checks tombstone handling + reinsert.
func TestDelete(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 64)
	defer m.Close()

	const n = 5000
	for i := uint64(0); i < n; i++ {
		m.Put(k8(i), v8(i))
	}
	for i := uint64(0); i < n; i += 2 {
		if !m.Delete(k8(i)) {
			t.Fatalf("delete %d failed", i)
		}
	}
	if m.Delete(k8(n + 7)) {
		t.Fatal("delete of absent key reported true")
	}
	for i := uint64(0); i < n; i++ {
		_, ok := m.Get(k8(i))
		if i%2 == 0 && ok {
			t.Fatalf("deleted key %d present", i)
		}
		if i%2 == 1 && !ok {
			t.Fatalf("surviving key %d missing", i)
		}
	}
	// Reinsert a deleted key (should reuse a tombstone).
	if ins, _ := m.Put(k8(0), v8(42)); !ins {
		t.Fatal("reinsert should be new")
	}
	if val, _ := m.Get(k8(0)); binary.LittleEndian.Uint64(val) != 42 {
		t.Fatal("reinserted value wrong")
	}
}

// TestKey32Val16 exercises 32-byte keys and 16-byte seqlock values.
func TestKey32Val16(t *testing.T) {
	m, _ := openTmp(t, 32, 16, 16)
	defer m.Close()

	const n = 8000
	for i := uint64(0); i < n; i++ {
		m.Put(k32(i), v16(i))
	}
	for i := uint64(0); i < n; i++ {
		val, ok := m.Get(k32(i))
		if !ok {
			t.Fatalf("key %d missing", i)
		}
		if binary.LittleEndian.Uint64(val) != i || binary.LittleEndian.Uint64(val[8:]) != ^i {
			t.Fatalf("key %d value corrupt", i)
		}
	}
	if !m.Update(k32(123), v16(777)) {
		t.Fatal("update32 failed")
	}
	if val, _ := m.Get(k32(123)); binary.LittleEndian.Uint64(val) != 777 {
		t.Fatal("update32 value wrong")
	}
}

// TestPersistReopen confirms data survives close/reopen via the file.
func TestPersistReopen(t *testing.T) {
	if !mmapBacked {
		t.Skip("mmap not file-backed here")
	}
	m, path := openTmp(t, 8, 16, 16)
	const n = 6000
	for i := uint64(0); i < n; i++ {
		m.Put(k8(i), v16(i))
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m2, err := Open(path, 8, 16, 16)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer m2.Close()
	if m2.Len() != n {
		t.Fatalf("len after reopen=%d want %d", m2.Len(), n)
	}
	for i := uint64(0); i < n; i++ {
		val, ok := m2.Get(k8(i))
		if !ok || binary.LittleEndian.Uint64(val) != i {
			t.Fatalf("key %d lost across reopen", i)
		}
	}
}

// TestConcurrentReadsUpdates runs many lock-free readers and concurrent
// lock-free value-updaters while a writer inserts and grows, under -race.
func TestConcurrentReadsUpdates(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 16)
	defer m.Close()

	const base = 4000
	for i := uint64(0); i < base; i++ {
		m.Put(k8(i), v8(i))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]byte, 8)
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := uint64(0); i < base; i += 17 {
					m.GetInto(k8(i), dst)
				}
			}
		}()
	}
	// Lock-free updaters (8-byte values).
	for u := 0; u < 3; u++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := uint64(0); i < base; i += 13 {
					m.Update(k8(i), v8(i*1000+seed))
				}
			}
		}(uint64(u))
	}
	// Writer: insert new keys to force growth.
	for i := uint64(base); i < base+8000; i++ {
		if _, err := m.Put(k8(i), v8(i)); err != nil {
			close(stop)
			wg.Wait()
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()

	// All original + inserted keys must be present.
	for i := uint64(0); i < base+8000; i++ {
		if _, ok := m.Get(k8(i)); !ok {
			t.Fatalf("key %d missing after concurrency", i)
		}
	}
}

var mmapBacked = mmap.Backed
