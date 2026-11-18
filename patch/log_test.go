package patch

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/netstar-labs/core"
)

// sink is the change surface shared by *Builder and the test's *logFeed, so one
// changes closure can drive either.
type sink interface {
	Upsert(key, val []byte) error
	Delete(key []byte) error
}

// referenceDigest builds the same coalesced changes through a Builder and applies
// them to a fresh store, returning its digest — the reference the Log must match.
func referenceDigest(t *testing.T, base func(*core.Sharded), changes func(sink)) uint64 {
	t.Helper()
	ref, err := core.OpenSharded(filepath.Join(t.TempDir(), "ref"), 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	base(ref)
	b := NewBuilder(8, 8)
	changes(b)
	if _, err := Apply(ref, b.Snapshot("")); err != nil {
		t.Fatal(err)
	}
	return ref.Digest()
}

func TestLogFlushMatchesReference(t *testing.T) {
	base := func(s *core.Sharded) {
		for i := uint64(0); i < 3000; i++ {
			s.Put(k8(i), v8(i))
		}
	}
	changes := func(b sink) {
		for i := uint64(0); i < 3000; i += 2 {
			b.Upsert(k8(i), v8(i*10)) // update evens
		}
		for i := uint64(1); i < 3000; i += 4 {
			b.Delete(k8(i)) // delete some odds
		}
		for i := uint64(3000); i < 3200; i++ {
			b.Upsert(k8(i), v8(i)) // insert
		}
	}
	want := referenceDigest(t, base, changes)

	store, err := core.OpenSharded(filepath.Join(t.TempDir(), "store"), 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	base(store)

	l, err := Open(t.TempDir(), 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	changes(&logFeed{l, t})
	st, err := l.Flush(store)
	if err != nil {
		t.Fatal(err)
	}
	if st.Upserts == 0 || st.Deletes == 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
	if store.Digest() != want {
		t.Fatalf("after flush digest %#x != reference %#x", store.Digest(), want)
	}
	// Segments consumed: nothing pending after a flush.
	if c, _ := l.Pending(); c != 0 {
		t.Fatalf("pending=%d after flush, want 0", c)
	}
	l.Close()
}

// logFeed adapts a Log to the Builder-shaped changes closure (Upsert/Delete).
type logFeed struct {
	l *Log
	t *testing.T
}

func (f *logFeed) Upsert(k, v []byte) error {
	if err := f.l.Upsert(k, v); err != nil {
		f.t.Fatal(err)
	}
	return nil
}
func (f *logFeed) Delete(k []byte) error {
	if err := f.l.Delete(k); err != nil {
		f.t.Fatal(err)
	}
	return nil
}

func TestLogCoalesce(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	l.Upsert(k8(1), v8(10))
	l.Delete(k8(1))         // delete wins
	l.Delete(k8(2))         // ...
	l.Upsert(k8(2), v8(20)) // upsert wins
	if c, _ := l.Pending(); c != 2 {
		t.Fatalf("pending=%d want 2", c)
	}

	store, _ := core.OpenSharded(filepath.Join(t.TempDir(), "s"), 8, 8, 8, 16)
	defer store.Close()
	store.Put(k8(1), v8(99)) // exists, should be deleted by the patch
	if _, err := l.Flush(store); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get(k8(1)); ok {
		t.Fatal("key 1 should be deleted")
	}
	if v, ok := store.Get(k8(2)); !ok || v8FromVal(v) != 20 {
		t.Fatal("key 2 should be 20")
	}
	l.Close()
}

func v8FromVal(v []byte) uint64 {
	var u uint64
	for i := 7; i >= 0; i-- {
		u = u<<8 | uint64(v[i])
	}
	return u
}

// TestLogRecovery: append + Sync + Close without flushing, then reopen and flush.
// The WAL must replay the un-applied window so no change is lost across a restart.
func TestLogRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 500; i++ {
		l.Upsert(k8(i), v8(i))
	}
	for i := uint64(0); i < 100; i++ {
		l.Delete(k8(1000 + i)) // deletes of absent keys (harmless)
	}
	if err := l.Sync(); err != nil { // durable, but NOT flushed to a store
		t.Fatal(err)
	}
	l.Close()

	// Reopen: the un-applied window must be replayed.
	l2, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := l2.Pending(); c != 600 {
		t.Fatalf("recovered pending=%d want 600", c)
	}
	store, _ := core.OpenSharded(filepath.Join(t.TempDir(), "s"), 8, 8, 8, 16)
	defer store.Close()
	st, err := l2.Flush(store)
	if err != nil {
		t.Fatal(err)
	}
	if st.Inserted != 500 {
		t.Fatalf("inserted=%d want 500", st.Inserted)
	}
	if store.Len() != 500 {
		t.Fatalf("store len=%d want 500", store.Len())
	}
	l2.Close()

	// After the flush, a fresh Open finds nothing pending (segments consumed).
	l3, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := l3.Pending(); c != 0 {
		t.Fatalf("pending=%d after consumed flush, want 0", c)
	}
	l3.Close()
}

// TestLogTornFrame: a trailing partial frame (simulating a crash mid-write) is
// ignored on replay, and the whole frames before it survive.
func TestLogTornFrame(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 10; i++ {
		l.Upsert(k8(i), v8(i))
	}
	l.Sync()
	l.Close()

	// Append garbage shorter than a frame to the active segment.
	segs, _ := filepath.Glob(filepath.Join(dir, "wal.*"))
	if len(segs) != 1 {
		t.Fatalf("want 1 segment, got %d", len(segs))
	}
	f, err := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{1, 2, 3}) // partial frame (frame size is 17)
	f.Close()

	l2, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := l2.Pending(); c != 10 {
		t.Fatalf("pending=%d want 10 (torn frame must be ignored)", c)
	}
	l2.Close()
}

// TestLogConcurrentAppendFlush drives concurrent appends while flushes run, to
// exercise the double-buffer under the race detector. After the writers stop and
// a final flush, the store must hold exactly the distinct keys written.
func TestLogConcurrentAppendFlush(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	store, err := core.OpenSharded(filepath.Join(t.TempDir(), "s"), 8, 8, 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const writers, perWriter = 8, 2000
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := uint64(w)*perWriter + uint64(i) // disjoint key ranges
				if err := l.Upsert(k8(key), v8(key)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	// Concurrent flusher.
	var fwg sync.WaitGroup
	fwg.Add(1)
	go func() {
		defer fwg.Done()
		for {
			select {
			case <-done:
				return
			default:
				if _, err := l.Flush(store); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	wg.Wait()
	close(done)
	fwg.Wait()

	if _, err := l.Flush(store); err != nil { // final flush drains the tail
		t.Fatal(err)
	}
	if store.Len() != writers*perWriter {
		t.Fatalf("store len=%d want %d", store.Len(), writers*perWriter)
	}
	l.Close()
}
