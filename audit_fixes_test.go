package core

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Regression tests for the audit pass. Each one is the reproduction program that
// confirmed the finding, kept as a test. The hang cases assert termination with a
// deadline: before the fix they ran until the test binary was killed.

// completes runs fn and fails if it has not returned within the deadline.
func completes(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return within 5s", what)
	}
}

// roundCapacity used to convert a negative int to ~2^64, so the doubling loop ran
// past 2^63 to zero and never terminated.
func TestRoundCapacityClampsAndTerminates(t *testing.T) {
	for _, n := range []int{math.MinInt, -1000, -10, -1, 0, 1, 7, 8} {
		nn := n
		completes(t, "roundCapacity", func() {
			if got := roundCapacity(nn); got != minCapacity {
				t.Errorf("roundCapacity(%d) = %d, want the %d floor", nn, got, minCapacity)
			}
		})
	}
	for _, c := range []struct {
		in   int
		want uint64
	}{
		{9, 16}, {16, 16}, {17, 32}, {1000, 1024}, {1 << 20, 1 << 20},
	} {
		if got := roundCapacity(c.in); got != c.want {
			t.Errorf("roundCapacity(%d) = %d, want %d", c.in, got, c.want)
		}
	}
	// A capacity past the bound saturates rather than wrapping to zero.
	completes(t, "roundCapacity(MaxInt)", func() {
		if got := roundCapacity(math.MaxInt); got != maxCapacity {
			t.Errorf("roundCapacity(MaxInt) = %d, want the %d ceiling", got, maxCapacity)
		}
	})
}

func TestCapacityForBounds(t *testing.T) {
	for _, n := range []int{math.MinInt, -1, 0} {
		if got := capacityFor(n); got != minCapacity {
			t.Errorf("capacityFor(%d) = %d, want %d", n, got, minCapacity)
		}
	}
	// 7/8 load factor: 700 entries need > 800 slots, so 1024.
	if got := capacityFor(700); got != 1024 {
		t.Errorf("capacityFor(700) = %d, want 1024", got)
	}
	// The scale-up must not wrap negative and clamp back down to the floor.
	completes(t, "capacityFor(MaxInt)", func() {
		if got := capacityFor(math.MaxInt); got != maxCapacity {
			t.Errorf("capacityFor(MaxInt) = %d, want the %d ceiling", got, maxCapacity)
		}
	})
}

// Open with a negative initCap hung in roundCapacity before reaching any I/O.
func TestOpenNegativeInitCap(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []int{-1000, -1, 0} {
		nn := n
		completes(t, "Open with a negative initCap", func() {
			m, err := Open(filepath.Join(dir, "neg.core"), 8, 8, nn)
			if err != nil {
				t.Errorf("Open(initCap=%d): %v", nn, err)
				return
			}
			defer m.Close()
			k := binary.BigEndian.AppendUint64(nil, 1)
			if _, err := m.Put(k, make([]byte, 8)); err != nil {
				t.Errorf("Put into a floor-sized store: %v", err)
			}
			if !m.Contains(k) {
				t.Error("key missing from a floor-sized store")
			}
		})
	}
}

// Reserve(-10) and Reserve(-1000) hung; Reserve(-1) happened to survive because the
// arithmetic landed exactly on zero.
func TestReserveNonPositiveIsNoop(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "r.core"), 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	before := m.cur.Load().capacity
	for _, n := range []int{math.MinInt, -1000, -10, -1, 0} {
		nn := n
		completes(t, "Reserve", func() {
			if err := m.Reserve(nn); err != nil {
				t.Errorf("Reserve(%d): %v", nn, err)
			}
		})
	}
	if got := m.cur.Load().capacity; got != before {
		t.Errorf("a non-positive Reserve resized the table: %d → %d", before, got)
	}
}

// ReserveCapPerShard divided by zero on shards == 0 and hung on a negative reserve.
func TestReserveCapPerShardGuards(t *testing.T) {
	for _, c := range []struct{ reserve, shards int }{
		{1000, 0}, {-1000, 4}, {-1, -1}, {0, 0}, {math.MinInt, 1},
	} {
		cc := c
		completes(t, "ReserveCapPerShard", func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("ReserveCapPerShard(%d,%d) panicked: %v", cc.reserve, cc.shards, r)
				}
			}()
			if got := ReserveCapPerShard(cc.reserve, cc.shards); got != minCapacity {
				t.Errorf("ReserveCapPerShard(%d,%d) = %d, want %d", cc.reserve, cc.shards, got, minCapacity)
			}
		})
	}
	// The normal path is unchanged: 4000 over 4 shards is 1001 per shard, scaled by
	// 8/7 to 1144, rounded to 2048.
	if got := ReserveCapPerShard(4000, 4); got != 2048 {
		t.Errorf("ReserveCapPerShard(4000,4) = %d, want 2048", got)
	}
}

// A header count lower than the live slots (a crash between publishControl and
// addCount leaves exactly that) made Shrink size the rebuild too small, and the
// unbounded probe in insertPlain spun forever. Shrink now sizes from the occupied
// slots, and a too-small target is an error rather than a hang.
func TestShrinkSurvivesUndercountedHeader(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "s.core"), 8, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	const n = 200
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		v := binary.LittleEndian.AppendUint64(nil, uint64(i))
		if _, err := m.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	storeU64(m.cur.Load().countPtr(), 1) // the drift

	completes(t, "Shrink with an undercounted header", func() {
		if err := m.Shrink(); err != nil {
			t.Errorf("Shrink: %v", err)
		}
	})
	// Every entry must still be there: sizing from live slots means nothing was
	// dropped, and the counter is rebuilt correctly by the copy.
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		got := make([]byte, 8)
		if !m.GetInto(k, got) {
			t.Fatalf("key %d lost across Shrink", i)
		}
		if binary.LittleEndian.Uint64(got) != uint64(i) {
			t.Fatalf("key %d has the wrong value after Shrink", i)
		}
	}
	if m.Len() != n {
		t.Errorf("Len() = %d after Shrink, want %d", m.Len(), n)
	}
}

// insertPlain reports a full table instead of probing forever.
func TestInsertPlainReportsFull(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "f.core"), 8, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	tbl := m.cur.Load()
	filled := 0
	for i := 0; filled < int(tbl.capacity); i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		if tbl.insertPlain(k, make([]byte, 8), sum(k)) {
			filled++
			continue
		}
		t.Fatalf("insertPlain reported full after only %d of %d slots", filled, tbl.capacity)
	}
	completes(t, "insertPlain on a full table", func() {
		k := binary.BigEndian.AppendUint64(nil, ^uint64(0))
		if tbl.insertPlain(k, make([]byte, 8), sum(k)) {
			t.Error("insertPlain claimed to place a key in a full table")
		}
	})
}

// A dst shorter than valLen used to panic for 8-byte values (PutUint64) and
// silently truncate for larger ones. Both now report false; an oversized dst still
// works, so a caller can reuse one buffer.
func TestGetIntoDstLength(t *testing.T) {
	for _, valLen := range []int{8, 24} {
		vl := valLen
		m, err := Open(filepath.Join(t.TempDir(), "d.core"), 8, vl, 64)
		if err != nil {
			t.Fatal(err)
		}
		k := binary.BigEndian.AppendUint64(nil, 7)
		want := make([]byte, vl)
		for i := range want {
			want[i] = byte(i + 1)
		}
		if _, err := m.Put(k, want); err != nil {
			t.Fatal(err)
		}
		for _, short := range []int{0, 1, vl - 1} {
			if m.GetInto(k, make([]byte, short)) {
				t.Errorf("valLen=%d: GetInto succeeded with a %d-byte dst", vl, short)
			}
		}
		big := make([]byte, vl+16)
		if !m.GetInto(k, big) {
			t.Errorf("valLen=%d: GetInto failed with an oversized dst", vl)
		}
		for i := range want {
			if big[i] != want[i] {
				t.Errorf("valLen=%d: oversized dst got the wrong bytes", vl)
				break
			}
		}
		m.Close()
	}
}

// equalKey strides by the table's key length while indexing the caller's slice, so
// a wrong-length key could read past its end once a control tag collided and the
// first 8 bytes matched. Map's read path now validates, as Shared's already did.
func TestReadPathRejectsWrongKeyLength(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "kl.core"), 16, 8, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k, 42)
	if _, err := m.Put(k, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1, 8, 9, 15, 17, 32} {
		bad := make([]byte, n)
		copy(bad, k)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%d-byte key panicked: %v", n, r)
				}
			}()
			if m.Contains(bad) {
				t.Errorf("Contains accepted a %d-byte key (keyLen is 16)", n)
			}
			if m.GetInto(bad, make([]byte, 8)) {
				t.Errorf("GetInto accepted a %d-byte key (keyLen is 16)", n)
			}
		}()
	}
	if !m.Contains(k) { // the correct length still works
		t.Error("Contains rejected the correct 16-byte key")
	}
}

// syncDir is what makes the resize rename durable. Assert it works on a real
// directory (and is a benign no-op where unsupported) and that a resize still
// completes with it in the path.
func TestSyncDirAndResizeStillWork(t *testing.T) {
	dir := t.TempDir()
	if err := syncDir(dir); err != nil {
		t.Errorf("syncDir(%q): %v", dir, err)
	}
	m, err := Open(filepath.Join(dir, "g.core"), 8, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	const n = 500 // forces several resizes off an 8-slot floor
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		if _, err := m.Put(k, binary.LittleEndian.AppendUint64(nil, uint64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if m.Len() != n {
		t.Fatalf("Len() = %d, want %d", m.Len(), n)
	}
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		got := make([]byte, 8)
		if !m.GetInto(k, got) || binary.LittleEndian.Uint64(got) != uint64(i) {
			t.Fatalf("key %d wrong after the resizes", i)
		}
	}
}

// A store file is untrusted input (bundle/patch ship them between machines), and the
// header checksum is unkeyed fnv64a — recomputable by anyone who can write the file.
// openTable used to trust the checksummed offsets for slice indexing and
// unsafe.Pointer arithmetic, so a crafted slotsOff panicked during Open.
func TestOpenRejectsCraftedGeometry(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.core")
	m, err := Open(good, 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	k := binary.BigEndian.AppendUint64(nil, 1)
	if _, err := m.Put(k, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}

	// Each case re-seals the checksum, so only the geometry validation can catch it.
	for _, c := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"slotsOff past the end of the file", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hSlotsOff:], 1<<40)
		}},
		{"capacity far beyond the mapping", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hCapacity:], 1<<20)
		}},
		{"capacity zero (mask would underflow)", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hCapacity:], 0)
		}},
		{"capacity not a power of two", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hCapacity:], 96)
		}},
		{"slotSize disagreeing with keyLen+valLen", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hSlotSize:], 4096)
		}},
		{"ctrlOff inside the header page", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hCtrlOff:], 8)
		}},
		{"verOff set when valLen is 8", func(b []byte) {
			binary.LittleEndian.PutUint64(b[hVerOff:], PageSize)
		}},
	} {
		raw := append([]byte(nil), base...)
		c.mutate(raw)
		binary.LittleEndian.PutUint64(raw[hChecksum:], headerChecksum(raw)) // re-seal
		path := filepath.Join(dir, "bad.core")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: panicked instead of erroring: %v", c.name, r)
				}
			}()
			bad, err := Open(path, 8, 8, 64)
			if err == nil {
				bad.Close()
				t.Errorf("%s: Open accepted a crafted geometry", c.name)
			}
		}()
	}

	// The untouched file still opens, so the validation is not simply rejecting
	// everything.
	if err := os.WriteFile(filepath.Join(dir, "ok.core"), base, 0o644); err != nil {
		t.Fatal(err)
	}
	ok, err := Open(filepath.Join(dir, "ok.core"), 8, 8, 64)
	if err != nil {
		t.Fatalf("a valid store no longer opens: %v", err)
	}
	defer ok.Close()
	if !ok.Contains(k) {
		t.Error("the valid store lost its key")
	}
}

// Defect D2 (found by independent research, docs/audits/defects-log.md): after a
// resize's rename committed, a directory-fsync failure made rehash discard the
// new table and return early without publishing it. m.cur kept pointing at the
// old table, whose inode the path no longer named — every write after that was
// silently lost. Defect R5 (same source): the maintenance grow/rehash triggered
// after a *successful* insert propagated its error as the Put's own result, so a
// caller saw an error for an insert that had, in fact, succeeded.
//
// This test forces every resize attempt to hit a directory-fsync failure (via
// syncDirHook) and checks both fixes at once, since they're the same code path:
// Put must still report success (R5), and every key — inserted before, during,
// and after the failing resizes — must remain reachable through the live table
// (D2), not silently dropped into an unlinked inode.
func TestRehashSurvivesDirSyncFailure(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(filepath.Join(dir, "d2r5.core"), 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	syncDirHookMu.Lock()
	syncDirHook = func(string) error { return errors.New("injected: directory fsync failed") }
	syncDirHookMu.Unlock()
	defer func() {
		syncDirHookMu.Lock()
		syncDirHook = nil
		syncDirHookMu.Unlock()
	}()

	// 64 * 7/8 = 56: this crosses the load-factor threshold at least once
	// (64->128), forcing at least one resize attempt while the hook is live.
	const n = 100
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		v := binary.LittleEndian.AppendUint64(nil, uint64(i))
		inserted, err := m.Put(k, v)
		if err != nil {
			t.Fatalf("Put %d: got error %v, want nil — a maintenance-resize failure "+
				"must not be reported as the insert's own failure (defect R5)", i, err)
		}
		if !inserted {
			t.Fatalf("Put %d: want inserted=true for a new key", i)
		}
	}

	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		got := make([]byte, 8)
		if !m.GetInto(k, got) {
			t.Fatalf("key %d is not reachable — the resized table was not published "+
				"despite the dir-fsync failure (defect D2)", i)
		}
		if binary.LittleEndian.Uint64(got) != uint64(i) {
			t.Fatalf("key %d: got value %d, want %d", i, binary.LittleEndian.Uint64(got), i)
		}
	}
	if m.Len() != n {
		t.Fatalf("Len() = %d, want %d", m.Len(), n)
	}
}

// Defect D4 (found by independent research, docs/audits/defects-log.md):
// dirtyset.collect clears the dirty bits for every chunk it returns before the
// caller actually flushes them. If the flush (msync/SyncRange) then fails, the
// bits stay cleared, so the next syncDirty reports "clean" and never retries —
// the data silently never reaches disk, if it ever did.
func TestSyncDirtyRemarksOnFlushFailure(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(filepath.Join(dir, "d4.core"), 8, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	for i := 0; i < 5; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		v := binary.LittleEndian.AppendUint64(nil, uint64(i))
		if _, err := m.Put(k, v); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	injected := errors.New("injected: msync failed")
	var sawFullHook, sawRangeHook bool
	syncHookMu.Lock()
	syncFullHook = func() error { sawFullHook = true; return injected }
	syncRangeHook = func(int, int) error { sawRangeHook = true; return injected }
	syncHookMu.Unlock()
	defer func() {
		syncHookMu.Lock()
		syncFullHook, syncRangeHook = nil, nil
		syncHookMu.Unlock()
	}()

	if err := m.Sync(); !errors.Is(err, injected) {
		t.Fatalf("Sync() = %v, want the injected flush error", err)
	}
	if !sawFullHook && !sawRangeHook {
		t.Fatal("neither sync hook ran — the test didn't exercise syncDirty's flush path")
	}

	// Swap the hooks for ones that succeed but record whether syncDirty tried
	// to flush anything at all. Before the fix, collect() had already cleared
	// every dirty bit on the first (failed) call, so this second Sync would
	// see dirty==0 and return early without calling either hook — reporting
	// "clean" for data that was never actually flushed. After the fix, the
	// failed ranges were re-marked, so this call must attempt a real flush.
	var flushedAgain bool
	syncHookMu.Lock()
	syncFullHook = func() error { flushedAgain = true; return nil }
	syncRangeHook = func(int, int) error { flushedAgain = true; return nil }
	syncHookMu.Unlock()

	if err := m.Sync(); err != nil {
		t.Fatalf("Sync() on the retry: %v", err)
	}
	if !flushedAgain {
		t.Fatal("the second Sync did not attempt to flush anything — the failed " +
			"flush's dirty bits were not re-marked (defect D4)")
	}
}

// Shared has its own resize/insertLocked implementing the same D2/R5 shape as
// Map's rehash/insertLockedH, but was not caught by the first pass of this
// fix — found by an adversarial skeptic review, not by me. Same test as
// TestRehashSurvivesDirSyncFailure, against Shared instead of Map.
func TestSharedResizeSurvivesDirSyncFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenShared(filepath.Join(dir, "shared_d2r5.core"), 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	syncDirHookMu.Lock()
	syncDirHook = func(string) error { return errors.New("injected: directory fsync failed") }
	syncDirHookMu.Unlock()
	defer func() {
		syncDirHookMu.Lock()
		syncDirHook = nil
		syncDirHookMu.Unlock()
	}()

	const n = 100
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		v := binary.LittleEndian.AppendUint64(nil, uint64(i))
		inserted, err := s.Put(k, v)
		if err != nil {
			t.Fatalf("Put %d: got error %v, want nil (defect R5, Shared)", i, err)
		}
		if !inserted {
			t.Fatalf("Put %d: want inserted=true for a new key", i)
		}
	}
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		got := make([]byte, 8)
		if !s.GetInto(k, got) {
			t.Fatalf("key %d is not reachable — the resized table was not published "+
				"despite the dir-fsync failure (defect D2, Shared)", i)
		}
		if binary.LittleEndian.Uint64(got) != uint64(i) {
			t.Fatalf("key %d: got value %d, want %d", i, binary.LittleEndian.Uint64(got), i)
		}
	}
	if s.MaintenanceErr() == nil {
		t.Error("MaintenanceErr() = nil, want the injected dir-fsync failure to be recorded")
	}
}

// insertLockedH's and insertLocked's "full" branches treat any grow/resize
// error as "nothing happened" — but grow/resize can return a non-nil error
// after successfully publishing a larger table (the D2 syncDir-failure case),
// found by the skeptic review of the first pass of this fix. The fix guards
// on m.cur.Load() == t to tell "table didn't change" from "table changed,
// durability just wasn't confirmed" before deciding to give up vs. retry.
//
// This test does NOT reliably force the literal "full" branch itself: with
// initCap 8, the load-factor check (loadCount*8 > capacity*7) fires at
// loadCount 8, by which point this table's own insert() has typically still
// placed the entry directly rather than reporting full, so in practice every
// resize observed here goes through the load-factor branch, not "full". I
// could not find a reliable way to force literal physical fullness together
// with an in-flight syncDir failure without a second, independent failure
// injection (reported by the skeptic review as hand-traced, not reproduced,
// for the same reason) — flagged here rather than claimed as covered. What
// this test DOES verify, which is still real and was not previously true: a
// full sequence of Puts against a tiny, constantly-resizing table, under a
// permanent dir-fsync failure, never drops a key and never spuriously fails.
func TestFullBranchRetriesAfterPublishedGrow(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(filepath.Join(dir, "full_retry.core"), 8, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	syncDirHookMu.Lock()
	syncDirHook = func(string) error { return errors.New("injected: directory fsync failed") }
	syncDirHookMu.Unlock()
	defer func() {
		syncDirHookMu.Lock()
		syncDirHook = nil
		syncDirHookMu.Unlock()
	}()

	const n = 50
	var lastErr error
	ok := 0
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		v := binary.LittleEndian.AppendUint64(nil, uint64(i))
		inserted, err := m.Put(k, v)
		if err != nil {
			lastErr = err
			continue
		}
		if !inserted {
			t.Fatalf("Put %d: want inserted=true for a new key", i)
		}
		ok++
	}
	// Every Put must succeed: the table always has somewhere to grow to (no
	// genuine ErrCorrupt/overflow is in play here, only the injected dir-sync
	// failure), so m.cur.Load() == t never holds in the full branch and the
	// loop must always continue and retry the insert.
	if ok != n {
		t.Fatalf("%d/%d Puts succeeded (last error: %v) — the full branch dropped a key "+
			"that should have been retried against the table that already grew (defect D2/full-branch)",
			ok, n, lastErr)
	}
	for i := 0; i < n; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		got := make([]byte, 8)
		if !m.GetInto(k, got) {
			t.Fatalf("key %d missing after a reported-successful Put", i)
		}
	}
}
