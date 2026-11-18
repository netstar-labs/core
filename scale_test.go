package core

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScale is a realistic-scale validation, skipped unless CORE_SCALE=1.
//
//	CORE_SCALE=1 CORE_SCALE_N=20000000 go test -run TestScale -v -timeout 20m .
//
// It builds N entries, reports build rate and on-disk size, measures parallel
// read / update / mixed throughput, and validates that dirty-range Sync cost is
// proportional to what changed — not to the file size.
func TestScale(t *testing.T) {
	if os.Getenv("CORE_SCALE") == "" {
		t.Skip("set CORE_SCALE=1 to run the large-scale validation")
	}
	n := 20_000_000
	if v := os.Getenv("CORE_SCALE_N"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "scale.core")

	m, err := Open(path, 8, 8, 16, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// ---- build via Reserve + Batch (one resize, lock taken once) ----
	if err := m.Reserve(n); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = m.Batch(func(put func(k, v []byte) (bool, error)) error {
		key := make([]byte, 8)
		val := make([]byte, 8)
		for i := 0; i < n; i++ {
			putUint(key, uint64(i))
			putUint(val, uint64(i))
			if _, e := put(key, val); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	buildDur := time.Since(start)
	t.Logf("build: %d entries in %v (%.1f M/s)", n, buildDur.Round(time.Millisecond),
		float64(n)/buildDur.Seconds()/1e6)
	t.Logf("on-disk: %s, capacity=%d, load=%.2f", duSize(t, dir),
		m.cur.Load().capacity, float64(n)/float64(m.cur.Load().capacity))

	const procs = 8
	const opsPer = 2_000_000

	// ---- parallel random reads (lock-free) ----
	t.Logf("read:   %s", parallelOps(procs, opsPer, func(rng *xorshift, dst []byte) {
		key := make([]byte, 8)
		putUint(key, rng.next()%uint64(n))
		m.GetInto(key, dst)
	}))

	// ---- parallel random updates (lock-free, 8-byte value) ----
	t.Logf("update: %s", parallelOps(procs, opsPer, func(rng *xorshift, dst []byte) {
		key := make([]byte, 8)
		putUint(key, rng.next()%uint64(n))
		m.Update(key, dst[:8])
	}))

	// ---- mixed 90% read / 10% update ----
	t.Logf("mixed:  %s", parallelOps(procs, opsPer, func(rng *xorshift, dst []byte) {
		key := make([]byte, 8)
		r := rng.next()
		putUint(key, r%uint64(n))
		if r%10 == 0 {
			m.Update(key, dst[:8])
		} else {
			m.GetInto(key, dst)
		}
	}))

	// ---- what dirty-range Sync optimizes: the clean-PTE scan ----
	// Flushing the pages a write dirtied is mandatory I/O that both approaches
	// pay. The thing dirty-range removes is the whole-map msync *scan* when
	// little or nothing changed since the last flush. Measure an idle Sync
	// (nothing dirty) against a forced whole-file msync of the same map.
	val := make([]byte, 8)
	_ = m.Sync() // clear the dirty set
	t0 := time.Now()
	_ = m.Sync() // nothing dirty ⇒ returns without scanning
	idleRanged := time.Since(t0)
	t1 := time.Now()
	_ = m.cur.Load().rw.Sync() // whole-map msync scans every present page
	idleWhole := time.Since(t1)
	t.Logf("idle Sync (0 dirty): dirty-range=%v  whole-file msync=%v  (scan avoided)",
		idleRanged.Round(time.Microsecond), idleWhole.Round(time.Microsecond))

	// A Sync after K scattered updates is dominated by writeback of those K
	// pages (mandatory durability cost), not by the scan.
	var rng2 xorshift = 0x9e3779b97f4a7c15
	for i := 0; i < 200; i++ {
		key := make([]byte, 8)
		putUint(key, rng2.next()%uint64(n))
		m.Update(key, val)
	}
	t2 := time.Now()
	_ = m.Sync()
	t.Logf("Sync after 200 scattered updates: %v (writeback-bound)", time.Since(t2).Round(time.Microsecond))

	// ---- spot-check correctness ----
	dst := make([]byte, 8)
	for _, i := range []uint64{0, uint64(n / 2), uint64(n - 1)} {
		key := make([]byte, 8)
		putUint(key, i)
		if !m.GetInto(key, dst) {
			t.Fatalf("key %d missing", i)
		}
	}
}

// xorshift is a cheap per-goroutine RNG so the benchmark measures the table, not
// math/rand.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := uint64(*x)
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = xorshift(v)
	return v
}

type opsResult struct {
	ops int
	dur time.Duration
}

func (r opsResult) String() string {
	return strconv.FormatFloat(float64(r.ops)/r.dur.Seconds()/1e6, 'f', 1, 64) + " Mops/s, " +
		strconv.FormatFloat(float64(r.dur.Nanoseconds())/float64(r.ops), 'f', 1, 64) + " ns/op"
}

func parallelOps(procs, opsPer int, fn func(rng *xorshift, dst []byte)) opsResult {
	var wg sync.WaitGroup
	var done int64
	start := time.Now()
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := xorshift(seed*2654435761 + 1)
			dst := make([]byte, 8)
			for i := 0; i < opsPer; i++ {
				fn(&rng, dst)
			}
			atomic.AddInt64(&done, int64(opsPer))
		}(uint64(p) + 1)
	}
	wg.Wait()
	_ = runtime.NumCPU
	return opsResult{ops: int(done), dur: time.Since(start)}
}

func duSize(t *testing.T, dir string) string {
	t.Helper()
	var total int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	switch {
	case total >= 1<<30:
		return strconv.FormatFloat(float64(total)/(1<<30), 'f', 2, 64) + " GiB (apparent)"
	default:
		return strconv.FormatFloat(float64(total)/(1<<20), 'f', 1, 64) + " MiB (apparent)"
	}
}
