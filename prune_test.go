package core

import (
	"path/filepath"
	"testing"
)

// TestPruneBeforeMap ages out entries whose value timestamp is older than a
// cutoff, then proves the steady-state churn invariant: tombstoned slots are
// reused by later Puts and capacity never grows.
func TestPruneBeforeMap(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 64)
	defer m.Close()

	// Value is a monotonic day counter (e.g. days since the epoch); keys 0..n with
	// ascending dates. Any monotonic encoding works — PruneBefore only compares.
	const n = 4000
	const cutoff = uint64(20260101)
	for i := uint64(0); i < n; i++ {
		date := 20250000 + i // straddles the cutoff
		if _, err := m.Put(k8(i), v8(date)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	capBefore := m.cur.Load().capacity

	want := 0
	for i := uint64(0); i < n; i++ {
		if 20250000+i < cutoff {
			want++
		}
	}

	removed := PruneBefore(m, 0, cutoff)
	if removed != want {
		t.Fatalf("pruned %d, want %d", removed, want)
	}
	if m.Len() != n-want {
		t.Fatalf("len=%d want %d", m.Len(), n-want)
	}
	if got := int(m.cur.Load().loadTomb()); got != want {
		t.Fatalf("tombstones=%d want %d", got, want)
	}
	if m.cur.Load().capacity != capBefore {
		t.Fatalf("capacity changed during prune: %d -> %d", capBefore, m.cur.Load().capacity)
	}

	// Survivors remain; aged-out keys are gone.
	for i := uint64(0); i < n; i++ {
		_, ok := m.Get(k8(i))
		stale := 20250000+i < cutoff
		if stale && ok {
			t.Fatalf("aged key %d still present", i)
		}
		if !stale && !ok {
			t.Fatalf("surviving key %d missing", i)
		}
	}

	// Refill with as many fresh keys as we aged out. Reuse is opportunistic — a
	// new key only recycles a tombstone when its probe chain reaches one before an
	// empty slot — so tombstones fall but need not hit zero. The invariant that
	// matters for steady-state churn is that capacity never grows.
	tombBefore := int(m.cur.Load().loadTomb())
	for i := uint64(0); i < uint64(want); i++ {
		if _, err := m.Put(k8(n+i), v8(20260601)); err != nil {
			t.Fatalf("refill put: %v", err)
		}
	}
	if m.Len() != n {
		t.Fatalf("len=%d after refill, want %d", m.Len(), n)
	}
	if got := int(m.cur.Load().loadTomb()); got >= tombBefore {
		t.Fatalf("tombstones=%d after refill, want < %d (no reuse happened)", got, tombBefore)
	}
	if m.cur.Load().capacity != capBefore {
		t.Fatalf("capacity grew on refill: %d -> %d (steady-state churn must not grow)", capBefore, m.cur.Load().capacity)
	}
}

// TestPruneFlagMap ages out entries that carry a bit flag in their value.
func TestPruneFlagMap(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 64)
	defer m.Close()

	const n = 2000
	const fExpired = uint64(1) << 5
	want := 0
	for i := uint64(0); i < n; i++ {
		v := i << 8 // payload in the high bits
		if i%3 == 0 {
			v |= fExpired
			want++
		}
		m.Put(k8(i), v8(v))
	}

	removed := PruneFlag(m, 0, fExpired)
	if removed != want {
		t.Fatalf("pruned %d, want %d", removed, want)
	}
	if m.Len() != n-want {
		t.Fatalf("len=%d want %d", m.Len(), n-want)
	}
	for i := uint64(0); i < n; i++ {
		_, ok := m.Get(k8(i))
		if (i%3 == 0) == ok {
			t.Fatalf("key %d: flagged=%v present=%v", i, i%3 == 0, ok)
		}
	}
}

// TestPruneSharded checks the aggregate sweep across shards and the custom
// predicate form (returning true for any key to remove).
func TestPruneSharded(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s")
	sh, err := OpenSharded(p, 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()

	const n = 10000
	const cutoff = uint64(20260101)
	for i := uint64(0); i < n; i++ {
		sh.Put(k8(i), v8(20250000+i))
	}
	want := 0
	for i := uint64(0); i < n; i++ {
		if 20250000+i < cutoff {
			want++
		}
	}

	removed := PruneBefore(sh, 0, cutoff)
	if removed != want {
		t.Fatalf("pruned %d, want %d", removed, want)
	}
	if sh.Len() != n-want {
		t.Fatalf("len=%d want %d", sh.Len(), n-want)
	}
	// Digest is order-independent; survivors must round-trip after reopen.
	d := sh.Digest()
	sh.Close()
	sh2, err := OpenSharded(p, 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer sh2.Close()
	if sh2.Digest() != d {
		t.Fatalf("digest changed across reopen: %#x != %#x", sh2.Digest(), d)
	}
	if sh2.Len() != n-want {
		t.Fatalf("reopened len=%d want %d", sh2.Len(), n-want)
	}
}

// TestShrinkAfterMassPrune ages out most of a store, then Shrink reclaims the
// capacity while preserving the survivors and their digest.
func TestShrinkAfterMassPrune(t *testing.T) {
	m, _ := openTmp(t, 8, 8, 16) // grows as we load
	defer m.Close()

	const n = 20000
	for i := uint64(0); i < n; i++ {
		m.Put(k8(i), v8(20250000+i))
	}
	capLoaded := m.cur.Load().capacity

	// Age out all but the most recent 100 entries.
	cutoff := uint64(20250000 + n - 100)
	removed := PruneBefore(m, 0, cutoff)
	if removed != n-100 {
		t.Fatalf("pruned %d, want %d", removed, n-100)
	}
	if m.cur.Load().capacity != capLoaded {
		t.Fatalf("prune changed capacity %d -> %d", capLoaded, m.cur.Load().capacity)
	}
	dBefore := m.Digest()

	if err := m.Shrink(); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if m.cur.Load().capacity >= capLoaded {
		t.Fatalf("shrink did not reduce capacity: still %d", m.cur.Load().capacity)
	}
	if m.Len() != 100 {
		t.Fatalf("len=%d after shrink, want 100", m.Len())
	}
	if m.Digest() != dBefore {
		t.Fatalf("shrink changed digest %#x -> %#x", dBefore, m.Digest())
	}
	// Survivors still resolve.
	for i := uint64(n - 100); i < n; i++ {
		if _, ok := m.Get(k8(i)); !ok {
			t.Fatalf("survivor %d missing after shrink", i)
		}
	}
}

// TestPruneShared exercises the cross-process backend's flock'd sweep.
func TestPruneShared(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.core")
	s, err := OpenShared(p, 8, 8, 4096) // large cap ⇒ no resize during the test
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 2000
	const cutoff = uint64(20260101)
	for i := uint64(0); i < n; i++ {
		if _, err := s.Put(k8(i), v8(20250000+i)); err != nil {
			t.Fatal(err)
		}
	}
	want := 0
	for i := uint64(0); i < n; i++ {
		if 20250000+i < cutoff {
			want++
		}
	}
	if removed := PruneBefore(s, 0, cutoff); removed != want {
		t.Fatalf("pruned %d, want %d", removed, want)
	}
	if s.Len() != n-want {
		t.Fatalf("len=%d want %d", s.Len(), n-want)
	}
}
