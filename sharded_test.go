package core

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"
)

func TestShardedBasicAndDistribution(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s")
	sh, err := OpenSharded(p, 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	const n = 40000
	for i := uint64(0); i < n; i++ {
		if _, err := sh.Put(k8(i), v8(i*3)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if sh.Len() != n {
		t.Fatalf("len=%d want %d", sh.Len(), n)
	}
	for i := uint64(0); i < n; i++ {
		val, ok := sh.Get(k8(i))
		if !ok || binary.LittleEndian.Uint64(val) != i*3 {
			t.Fatalf("key %d wrong/missing", i)
		}
	}
	// Every shard should have received a meaningful share of the keys.
	per := make([]int, sh.Shards())
	for i := range sh.shards {
		per[i] = sh.shards[i].Len()
		if per[i] < n/sh.Shards()/2 {
			t.Fatalf("shard %d underfilled: %d (avg %d)", i, per[i], n/sh.Shards())
		}
	}

	// Delete half, verify.
	for i := uint64(0); i < n; i += 2 {
		if !sh.Delete(k8(i)) {
			t.Fatalf("delete %d failed", i)
		}
	}
	if sh.Len() != n/2 {
		t.Fatalf("len after delete=%d want %d", sh.Len(), n/2)
	}
}

func TestShardedConcurrentWriters(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s")
	sh, err := OpenSharded(p, 8, 8, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	const workers = 8
	const perWorker = 5000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w uint64) {
			defer wg.Done()
			for i := uint64(0); i < perWorker; i++ {
				key := w*perWorker + i
				if _, err := sh.Put(k8(key), v8(key)); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(uint64(w))
	}
	wg.Wait()

	if sh.Len() != workers*perWorker {
		t.Fatalf("len=%d want %d", sh.Len(), workers*perWorker)
	}
	for w := uint64(0); w < workers; w++ {
		for i := uint64(0); i < perWorker; i++ {
			key := w*perWorker + i
			if _, ok := sh.Get(k8(key)); !ok {
				t.Fatalf("key %d missing", key)
			}
		}
	}
}

func TestShardedSharedBacked(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ss")
	sh, err := OpenSharedSharded(p, 8, 16, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	const n = 12000
	for i := uint64(0); i < n; i++ {
		sh.Put(k8(i), v16(i))
	}
	for i := uint64(0); i < n; i++ {
		val, ok := sh.Get(k8(i))
		if !ok || binary.LittleEndian.Uint64(val) != i {
			t.Fatalf("key %d wrong/missing", i)
		}
	}
}
