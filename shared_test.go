package core

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"
)

func TestSharedBasic(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.core")
	s, err := OpenShared(p, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 10000
	for i := uint64(0); i < n; i++ {
		s.Put(k8(i), v8(i))
	}
	if s.Len() != n {
		t.Fatalf("len=%d want %d", s.Len(), n)
	}
	for i := uint64(0); i < n; i++ {
		if val, ok := s.Get(k8(i)); !ok || binary.LittleEndian.Uint64(val) != i {
			t.Fatalf("key %d wrong/missing", i)
		}
	}
	if !s.Update(k8(5), v8(555)) {
		t.Fatal("update failed")
	}
	if val, _ := s.Get(k8(5)); binary.LittleEndian.Uint64(val) != 555 {
		t.Fatal("update value wrong")
	}
	if !s.Delete(k8(6)) || s.Contains(k8(6)) {
		t.Fatal("delete failed")
	}
}

// TestSharedTwoHandlesVisibility models two processes: writes through handle A
// are visible through handle B via the shared MAP_SHARED inode, with no resize.
func TestSharedTwoHandlesVisibility(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.core")
	a, err := OpenShared(p, 8, 8, 4096) // large initial cap ⇒ no resize
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenShared(p, 8, 8, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	const n = 1500
	for i := uint64(0); i < n; i++ {
		if _, err := a.Put(k8(i), v8(i*2)); err != nil {
			t.Fatal(err)
		}
	}
	// B sees A's inserts (same inode, shared memory).
	for i := uint64(0); i < n; i++ {
		if val, ok := b.Get(k8(i)); !ok || binary.LittleEndian.Uint64(val) != i*2 {
			t.Fatalf("handle B missing key %d", i)
		}
	}
	// A's lock-free value update is visible to B.
	a.Update(k8(10), v8(9999))
	if val, _ := b.Get(k8(10)); binary.LittleEndian.Uint64(val) != 9999 {
		t.Fatal("handle B did not see update")
	}
	// And B can write where A reads.
	b.Put(k8(100000), v8(7))
	if val, ok := a.Get(k8(100000)); !ok || binary.LittleEndian.Uint64(val) != 7 {
		t.Fatal("handle A did not see B's insert")
	}
}

// TestSharedTwoHandlesResize forces handle A to resize (new inode) and confirms
// handle B follows the generation bump and still finds every key.
func TestSharedTwoHandlesResize(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.core")
	a, err := OpenShared(p, 8, 8, 16) // tiny ⇒ many resizes
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenShared(p, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	const n = 8000
	for i := uint64(0); i < n; i++ {
		if _, err := a.Put(k8(i), v8(i)); err != nil {
			t.Fatal(err)
		}
	}
	// B must remap across A's resizes and see all keys.
	for i := uint64(0); i < n; i++ {
		if val, ok := b.Get(k8(i)); !ok || binary.LittleEndian.Uint64(val) != i {
			t.Fatalf("handle B missing key %d after resize", i)
		}
	}
	if b.Len() != n {
		t.Fatalf("handle B len=%d want %d", b.Len(), n)
	}
}

// TestSharedConcurrentTwoHandles runs A as writer (with resizes) and B as a
// lock-free reader concurrently, under -race.
func TestSharedConcurrentTwoHandles(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.core")
	a, err := OpenShared(p, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenShared(p, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	const n = 6000
	for i := uint64(0); i < 500; i++ {
		a.Put(k8(i), v8(i))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
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
				for i := uint64(0); i < 500; i += 7 {
					b.GetInto(k8(i), dst)
				}
			}
		}()
	}
	for i := uint64(500); i < n; i++ {
		if _, err := a.Put(k8(i), v8(i)); err != nil {
			close(stop)
			wg.Wait()
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()

	for i := uint64(0); i < n; i++ {
		if _, ok := b.Get(k8(i)); !ok {
			t.Fatalf("handle B missing key %d", i)
		}
	}
}
