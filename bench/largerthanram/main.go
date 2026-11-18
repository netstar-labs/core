// Command largerthanram measures core's page-fault path: the regime the mmap design
// exists for, and the one every in-tree benchmark misses because it runs against a
// table that fits in page cache.
//
//	go run . -size 1GiB -reads 200000            # uncapped: a write-path baseline
//	../../build/bigbench -size 8GiB -memory 1GiB  # capped: the fault path
//
// It reports the resident set it actually achieved, so a reader can tell whether the
// run was memory-constrained rather than taking the flags on trust. An uncapped run
// labels its own read numbers as in-cache and not fault-path.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/netstar-labs/core"
)

func main() {
	var (
		sizeStr = flag.String("size", "1GiB", "target store size on disk (e.g. 512MiB, 8GiB)")
		dir     = flag.String("dir", "", "directory for the store (default: a temp dir, removed after)")
		reads   = flag.Int("reads", 200_000, "random lookups to time")
		keyLen  = flag.Int("keylen", 8, "key length in bytes (multiple of 8)")
		valLen  = flag.Int("vallen", 8, "value length in bytes (multiple of 8)")
		keep    = flag.Bool("keep", false, "keep the store on disk afterwards")
		sweep   = flag.Bool("sweep", true, "time a full sequential ForEach sweep")
	)
	flag.Parse()

	size, err := parseSize(*sizeStr)
	if err != nil {
		fatal("%v", err)
	}
	// Entries the target size implies, at the 7/8 load factor the table grows on.
	perEntry := int64(*keyLen + *valLen)
	entries := int(size * 7 / 8 / perEntry)
	if entries < 1024 {
		fatal("-size %s is too small to be interesting (%d entries)", *sizeStr, entries)
	}

	work := *dir
	if work == "" {
		work, err = os.MkdirTemp("", "largerthanram-")
		if err != nil {
			fatal("temp dir: %v", err)
		}
		if !*keep {
			defer os.RemoveAll(work)
		}
	}
	path := filepath.Join(work, "kv.core")

	avail := availableMemory()
	fmt.Printf("store target %s (%d entries, %d-byte records) at %s\n",
		*sizeStr, entries, perEntry, path)
	if avail > 0 {
		fmt.Printf("memory available to this process: %s\n", human(avail))
		if size < avail {
			fmt.Printf("\n  NOTE: the store (%s) fits in available memory (%s).\n", *sizeStr, human(avail))
			fmt.Printf("  Read numbers below are IN-CACHE and are not fault-path numbers.\n")
			fmt.Printf("  Use build/bigbench -memory to cap memory below the store size.\n\n")
		} else {
			fmt.Printf("\n  store exceeds available memory by %.1fx — lookups must fault\n\n",
				float64(size)/float64(avail))
		}
	}

	// Reserve up front so the load measures inserts rather than a resize cascade;
	// the resize path has its own coverage in the root package.
	m, err := core.Open(path, *keyLen, *valLen, entries)
	if err != nil {
		fatal("open: %v", err)
	}
	defer m.Close()
	if err := m.Reserve(entries); err != nil {
		fatal("reserve: %v", err)
	}

	// ---- load ----
	key := make([]byte, *keyLen)
	val := make([]byte, *valLen)
	start := time.Now()
	for i := 0; i < entries; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.LittleEndian.PutUint64(val, uint64(i)*2654435761)
		if _, err := m.Put(key, val); err != nil {
			fatal("put %d: %v", i, err)
		}
		if i > 0 && i%5_000_000 == 0 {
			fmt.Printf("  … %d entries (%s)\n", i, time.Since(start).Round(time.Second))
		}
	}
	loadDur := time.Since(start)
	report("load", loadDur, entries)

	// ---- sync ----
	start = time.Now()
	if err := m.Sync(); err != nil {
		fatal("sync: %v", err)
	}
	syncDur := time.Since(start)
	fmt.Printf("%-12s %10s  (whole dirty set)\n", "sync", syncDur.Round(time.Millisecond))

	fileSize := onDisk(path)
	fmt.Printf("%-12s %10s  resident=%s\n", "on-disk", human(fileSize), human(residentSet()))

	// ---- random reads: the number that changes once the store exceeds RAM ----
	got := make([]byte, *valLen)
	rng := rand.New(rand.NewPCG(1, 2))
	misses := 0
	start = time.Now()
	for i := 0; i < *reads; i++ {
		binary.BigEndian.PutUint64(key, uint64(rng.IntN(entries)))
		if !m.GetInto(key, got) {
			misses++
		}
	}
	randDur := time.Since(start)
	report("random", randDur, *reads)
	if misses > 0 {
		fatal("%d of %d random lookups missed — the store is not intact", misses, *reads)
	}

	// ---- sequential sweep ----
	if *sweep {
		n := 0
		start = time.Now()
		m.ForEach(func(k, v []byte) bool { n++; return true })
		seqDur := time.Since(start)
		report("sequential", seqDur, n)
		if n != entries {
			fatal("sweep saw %d entries, want %d", n, entries)
		}
	}

	fmt.Printf("\nresident at exit: %s of a %s store\n", human(residentSet()), human(fileSize))
}

// report prints a phase's total time and its per-operation cost.
func report(name string, d time.Duration, ops int) {
	if ops == 0 {
		return
	}
	per := d / time.Duration(ops)
	fmt.Printf("%-12s %10s  %8s/op  %s\n", name, d.Round(time.Millisecond), per.Round(time.Nanosecond),
		fmt.Sprintf("%.0f ops/s", float64(ops)/d.Seconds()))
}

func parseSize(s string) (int64, error) {
	mult := int64(1)
	switch {
	case len(s) > 3 && s[len(s)-3:] == "GiB":
		mult, s = 1<<30, s[:len(s)-3]
	case len(s) > 3 && s[len(s)-3:] == "MiB":
		mult, s = 1<<20, s[:len(s)-3]
	case len(s) > 3 && s[len(s)-3:] == "KiB":
		mult, s = 1<<10, s[:len(s)-3]
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("cannot parse -size %q (want e.g. 512MiB, 8GiB)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("-size must be positive")
	}
	return n * mult, nil
}

func onDisk(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// residentSet reports the Go heap's view plus, where readable, the OS resident set.
// It is printed so a reader can see whether a "capped" run really was capped.
func residentSet() int64 {
	if rss := osResident(); rss > 0 {
		return rss
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.Sys) // heap only — mapped file pages are not counted here
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "largerthanram: "+format+"\n", args...)
	os.Exit(1)
}
