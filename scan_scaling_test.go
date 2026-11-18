package core

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSyncScanScaling measures how the whole-file msync *scan* cost grows with
// map size. That scan is exactly what a dirty-range idle Sync skips, so this
// finds the map size at which the skip becomes material. Gated by CORE_SCALE.
//
//	CORE_SCALE=1 go test -run TestSyncScanScaling -v -timeout 30m .
//	CORE_SCALE=1 CORE_SCALE_SIZES=10,40,160 go test -run TestSyncScanScaling -v .
func TestSyncScanScaling(t *testing.T) {
	if os.Getenv("CORE_SCALE") == "" {
		t.Skip("set CORE_SCALE=1 to run scan-scaling measurement")
	}
	sizes := []int{5_000_000, 10_000_000, 20_000_000, 40_000_000}
	if v := os.Getenv("CORE_SCALE_SIZES"); v != "" {
		sizes = nil
		for _, s := range strings.Split(v, ",") {
			m, _ := strconv.Atoi(strings.TrimSpace(s))
			sizes = append(sizes, m*1_000_000)
		}
	}

	t.Logf("%-12s %-14s %-16s %-16s", "entries", "file", "idle dirty-range", "whole-file scan")
	for _, n := range sizes {
		dir, err := os.MkdirTemp("", "scan")
		if err != nil {
			t.Fatal(err)
		}
		path := dir + "/s.core"

		m, err := Open(path, 8, 8, 16, WithNoSync())
		if err != nil {
			t.Fatal(err)
		}
		_ = m.Reserve(n)
		_ = m.Batch(func(put func(k, v []byte) (bool, error)) error {
			key := make([]byte, 8)
			val := make([]byte, 8)
			for i := 0; i < n; i++ {
				putUint(key, uint64(i))
				putUint(val, uint64(i))
				put(key, val)
			}
			return nil
		})
		_ = m.Sync() // flush + clear dirty: now all occupied pages are present & clean

		// Idle dirty-range Sync: nothing dirty ⇒ no scan.
		t0 := time.Now()
		_ = m.Sync()
		idle := time.Since(t0)

		// Forced whole-file msync: scans every present page (flushes nothing).
		t1 := time.Now()
		_ = m.cur.Load().rw.Sync()
		whole := time.Since(t1)

		fileSize := len(m.cur.Load().data)
		t.Logf("%-12d %-14s %-16v %-16v", n, mib(fileSize),
			idle.Round(time.Microsecond), whole.Round(time.Microsecond))

		m.Close()
		os.RemoveAll(dir)
	}
}

func mib(b int) string {
	if b >= 1<<30 {
		return strconv.FormatFloat(float64(b)/(1<<30), 'f', 2, 64) + " GiB"
	}
	return strconv.FormatFloat(float64(b)/(1<<20), 'f', 0, 64) + " MiB"
}
