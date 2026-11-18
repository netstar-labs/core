// Example: maintenance with Prune — aging out stale entries.
//
// Prune is a single sequential sweep that tombstones every entry a predicate
// selects. It is the way to "scan a shard top to bottom and drop the dead rows"
// without disk thrashing: the sweep advises MADV_SEQUENTIAL so a store larger
// than RAM streams in instead of faulting one page at a time, takes the writer
// lock once for the whole pass, and never resizes mid-scan. Run it with:
//
//	go run ./examples/prune
//
// The records here pack a date and a flags word into the 16-byte value:
//
//	val[0:8]  = days since the Unix epoch (time.Now().Unix()/86400), little-endian
//	val[8:16] = flags bitset
//
// A day count is a linear, monotonic integer, so PruneBefore's unsigned compare
// gives "older than" directly AND a retention window is plain subtraction
// (today - N days) — unlike a yyyymmdd int, which sorts but has gaps you can't do
// arithmetic on. It shows the three predicate forms (PruneBefore, PruneFlag, a
// custom closure that uses the key), the steady-state churn property (tombstones
// are reused, the store does not grow), and Shrink for reclaiming space after a
// mass age-out.
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/netstar-labs/core"
)

const (
	keyLen = 8
	valLen = 16 // [0:8] day-since-epoch, [8:16] flags
	shards = 8

	dateOff  = 0
	flagsOff = 8

	secsPerDay = 86400

	flagSuspended = uint64(1) << 0
)

// record builds a value with a day count and flags packed in.
func record(day, flags uint64) []byte {
	v := make([]byte, valLen)
	binary.LittleEndian.PutUint64(v[dateOff:], day)
	binary.LittleEndian.PutUint64(v[flagsOff:], flags)
	return v
}

func key(i uint64) []byte { return binary.BigEndian.AppendUint64(nil, i) }

func main() {
	dir, err := os.MkdirTemp("", "core-example-prune")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "kv")

	sh, err := core.OpenSharded(path, keyLen, valLen, shards, 100_000)
	if err != nil {
		log.Fatal(err)
	}
	defer sh.Close()

	// today as a whole-day count since the epoch (truncating seconds → UTC days).
	today := uint64(time.Now().Unix() / secsPerDay)

	// --- Load -------------------------------------------------------------
	// 10k records spread over the last ~2 years (0..729 days ago); every 50th is
	// flagged "suspended".
	const n = 10_000
	for i := uint64(0); i < n; i++ {
		day := today - i%730 // 0..729 days before today
		var flags uint64
		if i%50 == 0 {
			flags = flagSuspended
		}
		if _, err := sh.Put(key(i), record(day, flags)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("loaded %d records (today = day %d since epoch)\n", sh.Len(), today)

	// --- PruneBefore: age out everything older than a retention window ------
	// Because the value is a linear day count, "older than 365 days" is just
	// today-365 — no date math. PruneBefore tombstones every entry whose day
	// word is < the cutoff. (off is the byte offset of that word in the value.)
	const retainDays = 365
	cutoff := today - retainDays
	aged := core.PruneBefore(sh, dateOff, cutoff)
	fmt.Printf("PruneBefore(last %d days): aged out %d, %d remain\n", retainDays, aged, sh.Len())

	// --- PruneFlag: age out by a bit flag in the value --------------------
	suspended := core.PruneFlag(sh, flagsOff, flagSuspended)
	fmt.Printf("PruneFlag(suspended): aged out %d, %d remain\n", suspended, sh.Len())

	// --- Custom predicate that uses the key -------------------------------
	// The callback gets the key for cases where aging here must coordinate with
	// something else keyed the same way — a secondary index, per-key retention,
	// tiering, or an audit log. Here: drop a reserved key range (id < 100) and
	// log what we evict (copy the key, since it aliases the mmap).
	var evicted [][]byte
	dropped := sh.Prune(func(k, v []byte) bool {
		if binary.BigEndian.Uint64(k) < 100 {
			evicted = append(evicted, append([]byte(nil), k...)) // copy to retain
			return true
		}
		return false
	})
	fmt.Printf("custom prune (id < 100): aged out %d (logged %d keys)\n", dropped, len(evicted))

	// --- Steady-state churn: tombstones are reused, the store does not grow -
	// Re-add as many fresh records (dated today) as we just removed. Tombstoned
	// slots are recycled by these Puts (and auto-shed at the load threshold), so a
	// flat-population set neither grows nor needs an explicit compaction.
	lenAfterPrune := sh.Len()
	refill := aged + suspended + dropped
	for i := uint64(0); i < uint64(refill); i++ {
		if _, err := sh.Put(key(n+i), record(today, 0)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("refilled %d: len %d -> %d (tombstones reused in place)\n",
		refill, lenAfterPrune, sh.Len())

	// --- Shrink: reclaim space after a *permanent* mass age-out ------------
	// This is the deliberate, occasional case — a set that has dropped a lot of
	// population for good and you want the files to give space back. Routine
	// pruning does NOT need this. Here we age out everything before today (leaving
	// only the fresh refills), then Shrink rehashes each shard down to fit them.
	core.PruneBefore(sh, dateOff, today)
	before := sh.Digest()
	if err := sh.Shrink(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after mass prune + Shrink: %d remain, digest stable=%v\n",
		sh.Len(), sh.Digest() == before)

	if err := sh.Sync(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Synced. Done.")
}
