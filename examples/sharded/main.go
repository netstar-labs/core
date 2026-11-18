// Example: the *Sharded backend — single process, concurrent writers.
//
// OpenSharded splits the table into N independent shards, each with its own
// writer lock, so writers on different shards never block each other and any
// resize is bounded to 1/N of the data. Run it with:
//
//	go run ./examples/sharded
//
// It shows two things: concurrent Put from many goroutines, and the fast bulk
// ingest path (Reserve + PutRecords) used for loading tens of millions of rows.
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/netstar-labs/core"
)

const (
	keyLen = 8
	valLen = 8
	shards = 16
)

func main() {
	dir, err := os.MkdirTemp("", "core-example-sharded")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "kv")

	// WithNoSync defers durability to explicit Sync calls — the right choice for
	// a bulk load, where you sync once at the end instead of per write.
	sh, err := core.OpenSharded(path, keyLen, valLen, shards, 1_000_000, core.WithNoSync())
	if err != nil {
		log.Fatal(err)
	}
	defer sh.Close()

	// --- Concurrent writers ----------------------------------------------
	// Each goroutine writes a disjoint key range. Because keys hash across all
	// shards, the writers genuinely run in parallel under separate shard locks.
	const writers, perWriter = 8, 10_000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				n := uint64(w*perWriter + i)
				key := binary.BigEndian.AppendUint64(nil, n)
				val := binary.LittleEndian.AppendUint64(nil, n)
				if _, err := sh.Put(key, val); err != nil {
					log.Fatal(err)
				}
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("concurrent Put: Len = %d across %d shards\n\n", sh.Len(), sh.Shards())

	// --- Bulk ingest ------------------------------------------------------
	// Reserve grows every shard once up front (one resize per shard, not
	// log2(n) as the table fills). PutRecords then ingests a packed buffer of
	// fixed-size records — keyLen bytes of key immediately followed by valLen
	// bytes of value — hashing and inserting in parallel with zero per-record
	// allocation.
	const bulk = 200_000
	if err := sh.Reserve(sh.Len() + bulk); err != nil {
		log.Fatal(err)
	}
	rec := keyLen + valLen
	buf := make([]byte, bulk*rec)
	for i := 0; i < bulk; i++ {
		n := uint64(1_000_000 + i) // disjoint from the concurrent range above
		off := i * rec
		binary.BigEndian.PutUint64(buf[off:], n)
		binary.LittleEndian.PutUint64(buf[off+keyLen:], n*2)
	}
	inserted, err := sh.PutRecords(buf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("PutRecords: %d new entries, Len = %d\n\n", inserted, sh.Len())

	// --- Verify + persist -------------------------------------------------
	dst := make([]byte, valLen)
	probe := binary.BigEndian.AppendUint64(nil, 1_000_042)
	if sh.GetInto(probe, dst) {
		fmt.Printf("GetInto(1000042) = %d (expected 2000084)\n", binary.LittleEndian.Uint64(dst))
	}
	fmt.Printf("Digest = %#x\n", sh.Digest())

	// One Sync flushes every shard. With WithNoSync this is the only durability
	// point, so the whole load commits at once.
	if err := sh.Sync(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Synced. Done.")
}
