// Example: the *Map backend — single process, single table.
//
// This is the simplest and fastest way to use core: one mmap-backed
// open-addressing hash table opened with Open. Run it with:
//
//	go run ./examples/map
//
// It walks the full single-process API surface: Put, Get/GetInto, Update,
// Contains, Delete, Len, ForEach, Digest, and Sync.
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/netstar-labs/core"
)

func main() {
	// Keep the table file in a temp dir; clean it up on exit. In a real program
	// this path is a durable location you reopen across restarts.
	dir, err := os.MkdirTemp("", "core-example-map")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "kv.core")

	// 8-byte keys and values, room for ~1M entries. keyLen and valLen must each
	// be a nonzero multiple of 8; the table grows automatically past initCap.
	m, err := core.Open(path, 8, 8, 1_000_000)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	// --- Insert -----------------------------------------------------------
	// Keys and values are opaque fixed-size byte slices. Here we encode uint64s.
	for i := uint64(0); i < 5; i++ {
		key := binary.BigEndian.AppendUint64(nil, i)
		val := binary.LittleEndian.AppendUint64(nil, i*100)
		inserted, err := m.Put(key, val)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Put(%d -> %d) inserted=%v\n", i, i*100, inserted)
	}
	fmt.Printf("Len = %d\n\n", m.Len())

	// --- Read -------------------------------------------------------------
	// GetInto is the lock-free, zero-alloc read path: it copies the value into
	// a caller-owned buffer instead of allocating a new slice per lookup.
	dst := make([]byte, 8)
	key := binary.BigEndian.AppendUint64(nil, 3)
	if m.GetInto(key, dst) {
		fmt.Printf("GetInto(3) = %d\n", binary.LittleEndian.Uint64(dst))
	}
	// Get is the convenience form that returns a freshly allocated slice.
	if val, ok := m.Get(key); ok {
		fmt.Printf("Get(3)     = %d\n", binary.LittleEndian.Uint64(val))
	}
	fmt.Printf("Contains(3) = %v\n\n", m.Contains(key))

	// --- Update -----------------------------------------------------------
	// Update is lock-free for <=8-byte values (a single atomic word) and only
	// touches an existing key — it returns false if the key is absent.
	m.Update(key, binary.LittleEndian.AppendUint64(nil, 999))
	if val, ok := m.Get(key); ok {
		fmt.Printf("after Update, Get(3) = %d\n", binary.LittleEndian.Uint64(val))
	}

	// --- Delete -----------------------------------------------------------
	// Deletes are tombstones (reclaimed on the next resize), not rebalances.
	fmt.Printf("Delete(3) = %v, Contains(3) = %v\n\n", m.Delete(key), m.Contains(key))

	// --- Scan -------------------------------------------------------------
	// ForEach visits every live entry; return false to stop early.
	fmt.Println("ForEach:")
	m.ForEach(func(k, v []byte) bool {
		fmt.Printf("  %d -> %d\n", binary.BigEndian.Uint64(k), binary.LittleEndian.Uint64(v))
		return true
	})

	// --- Integrity + durability ------------------------------------------
	// Digest is an order-independent content hash, handy for comparing replicas
	// or verifying a transfer. Sync flushes dirty pages to make the file durable.
	fmt.Printf("\nDigest = %#x\n", m.Digest())
	if err := m.Sync(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Synced. Done.")
}
