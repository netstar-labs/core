// Package core implements a custom, mmap-backed open-addressing hash table tuned
// for fixed-size keys/values, read-heavy workloads, and real-time updates at a
// scale larger than RAM. It uses no cgo.
//
// Each table is one memory-mapped file (MAP_SHARED): the OS pages it in and out,
// so a table can exceed RAM, and MADV_RANDOM keeps a point lookup to a single
// slot fault. In front of the fixed-size key/value slots sits a dense array of
// Swiss-table-style 1-byte control tags, scanned 8 at a time with SWAR; that
// control array is small enough to stay resident even when the slots do not.
//
// Reads are lock-free. Value updates of existing keys are lock-free for values up
// to 8 bytes (a single atomic word store) and seqlock-protected for larger ones;
// readers never block. Structural changes — inserting a new key, deleting, and
// resizing — are single-writer.
//
// # Backends
//
// Three backends share one [Store] interface:
//
//   - [Map] (via [Open]) — single process, single table. Simplest and fastest.
//   - [Shared] (via [OpenShared]) — multiple processes open the same file; a
//     generation counter in a sidecar .meta file plus an advisory flock provide
//     cross-process consistency and single-writer exclusion.
//   - [Sharded] (via [OpenSharded] or [OpenSharedSharded]) — N independent shards
//     over either backend, giving concurrent writers and resizes bounded to 1/N
//     of the data.
//
// # Example
//
//	m, err := core.Open("kv.core", 8, 8, 1_000_000) // 8-byte keys & values
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer m.Close()
//
//	key := binary.BigEndian.AppendUint64(nil, 42)
//	val := binary.LittleEndian.AppendUint64(nil, 12345)
//	m.Put(key, val)
//
//	got := make([]byte, 8)
//	if m.GetInto(key, got) { // lock-free, zero-alloc
//		fmt.Println(binary.LittleEndian.Uint64(got))
//	}
//	m.Sync() // make durable on demand
//
// # Sizing and concurrency
//
// keyLen and valLen are fixed for the life of the file and must each be a nonzero
// multiple of 8. The initial capacity is rounded up to a power of two and the
// file is created sparse, so a large capacity costs no disk up front; the table
// grows automatically at a 7/8 load factor.
//
// All operations are goroutine-safe. Reads ([Map.Get], [Map.GetInto],
// [Map.Contains]) take no lock. [Map.Update] is lock-free for values up to 8
// bytes. [Map.Put], [Map.Delete], [Map.Batch], [Map.Reserve], and the [Sharded]
// bulk paths ([Sharded.PutBatch], [Sharded.PutRecords]) are single-writer.
//
// # Bulk loading and durability
//
// For high-throughput ingest, call [Map.Reserve] (or [Sharded.Reserve]) up front
// to avoid incremental resizes, open with [WithNoSync] to skip per-resize msync,
// and load via [Sharded.PutRecords] (parallel, zero-alloc) or a [Map.Batch].
// Writes land in the page cache and are flushed lazily; call [Map.Sync] to make
// them durable (it msyncs only the pages written since the last Sync).
//
// # Known limitation: retained mappings
//
// A resize publishes a new mapping and keeps the old one alive, because lock-free
// readers may still be inside it and there is no epoch or refcount to tell when the
// last one leaves. Superseded mappings are therefore released only at Close, and a
// process accumulates one per resize it performs — or, for [Shared], one per
// generation it adopts from another process's resize. A long-lived reader of a
// frequently-resized [Shared] store grows its address space and page-cache
// footprint without doing anything itself; reopen it periodically if that matters.
// Steady-state churn does not resize (tombstones are reused), so this only affects
// stores that keep growing.
//
// # Integrity
//
// Every file carries a header checksum over its immutable geometry, validated on
// open ([ErrCorrupt] on failure). [Map.Digest] returns an order-independent
// content digest: equal digests imply equal key→value sets, a cheap
// integrity/equality proof for verifying transfers or comparing replicas.
//
// See USERGUIDE.md in the repository for the full guide, on-disk format, and
// tuning notes.
package core

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/netstar-labs/core/internal/mmap"
)

var (
	// ErrBadSize means keyLen/valLen are invalid or disagree with an existing file.
	ErrBadSize = errors.New("core: invalid or mismatched key/value size")

	// ErrReadOnly is returned by every mutator on a table opened WithReadOnly.
	ErrReadOnly = errors.New("core: table is open read-only")
	// ErrCorrupt means the file header did not validate.
	ErrCorrupt = errors.New("core: bad header")
)

// loadNum/loadDen set the maximum load factor (7/8). Above it the table grows.
const (
	loadNum = 7
	loadDen = 8
)

// Map is an open hash table. The active table is swapped atomically on resize so
// readers never block or fault on a stale mapping.
type Map struct {
	path     string
	keyLen   int
	valLen   int
	cfg      config
	writeMu  sync.Mutex
	cur      atomic.Pointer[table]
	retired  []*mmap.RW // old mappings, freed at Close
	maintErr atomic.Pointer[maintErrBox]
}

// maintErrBox wraps an error so atomic.Pointer can hold it (the error
// interface itself isn't a valid atomic.Pointer type parameter).
type maintErrBox struct{ err error }

// MaintenanceErr returns the error from the most recent background
// grow/rehash triggered automatically by Put crossing the load-factor
// threshold, or nil if the most recent attempt succeeded (or none has run
// yet). Put itself never returns this error — a maintenance failure does not
// mean the insert failed (see docs/audits/defects-log.md, defect R5) — but a
// maintenance resize that cannot succeed (e.g. a corrupt on-disk state, or a
// full disk) would otherwise retry silently on every single insert forever
// with no way to detect it; poll this after a sequence of inserts if that
// matters to you.
func (m *Map) MaintenanceErr() error {
	b := m.maintErr.Load()
	if b == nil {
		return nil
	}
	return b.err
}

// Open opens or creates a table at path. keyLen must be a nonzero multiple of 8;
// valLen must be a multiple of 8 and may be 0 for a key-only (set) store. A new
// file is sized for initCap entries (rounded up to a power of two) and created
// sparse, so a large capacity costs no disk up front.
func Open(path string, keyLen, valLen, initCap int, opts ...Option) (*Map, error) {
	if keyLen <= 0 || keyLen%8 != 0 || valLen < 0 || valLen%8 != 0 {
		return nil, ErrBadSize
	}
	m := &Map{path: path, keyLen: keyLen, valLen: valLen, cfg: newConfig(opts)}

	flags := os.O_RDWR | os.O_CREATE
	if m.cfg.readOnly {
		flags = os.O_RDONLY // no O_CREATE: read-only never brings a table into being
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if m.cfg.readOnly {
		// An empty file is a table that was created but never written. Read-only
		// cannot initialise it, and mapping zero bytes would hand back a Map whose
		// every lookup misses — a working-looking store with no data in it.
		if fi.Size() == 0 {
			f.Close()
			return nil, fmt.Errorf("core: %s is empty; read-only cannot create a table", path)
		}
		rw, err := mmap.MapRO(f, int(fi.Size()))
		f.Close()
		if err != nil {
			return nil, err
		}
		t, err := openTable(rw, keyLen, valLen)
		if err != nil {
			rw.Close()
			return nil, err
		}
		m.cur.Store(t)
		return m, nil
	}

	if fi.Size() == 0 {
		capacity := roundCapacity(initCap)
		_, _, _, _, fileSize := layout(keyLen, valLen, capacity)
		if err := f.Truncate(int64(fileSize)); err != nil {
			f.Close()
			return nil, err
		}
		rw, err := mmap.MapRW(f, fileSize)
		if err != nil {
			f.Close()
			return nil, err
		}
		f.Close() // mapping keeps the inode alive
		m.cur.Store(newTable(rw, keyLen, valLen, capacity))
		return m, nil
	}

	rw, err := mmap.MapRW(f, int(fi.Size()))
	f.Close()
	if err != nil {
		return nil, err
	}
	t, err := openTable(rw, keyLen, valLen)
	if err != nil {
		rw.Close()
		return nil, err
	}
	m.cur.Store(t)
	return m, nil
}

// Close releases all mappings.
func (m *Map) Close() error {
	var err error
	if t := m.cur.Load(); t != nil {
		err = t.rw.Close()
	}
	for _, rw := range m.retired {
		if e := rw.Close(); err == nil {
			err = e
		}
	}
	return err
}

// Len returns the number of live entries.
func (m *Map) Len() int { return int(m.cur.Load().loadCount()) }

func (m *Map) hash(key []byte) uint64 { return sum(key) }

// ---- reads (lock-free) ----

// Get returns a copy of key's value. ok is false if the key is absent.
func (m *Map) Get(key []byte) (val []byte, ok bool) {
	val = make([]byte, m.valLen)
	if m.GetInto(key, val) {
		return val, true
	}
	return nil, false
}

// GetInto writes key's value into dst without allocating and reports whether the
// key was found. len(dst) must be at least valLen; a shorter dst returns false
// rather than panicking (valLen == 8) or silently storing a truncated value
// (valLen > 8), which is how the two size classes used to diverge. A longer dst is
// fine — only the first valLen bytes are written — so a caller may reuse one
// oversized buffer. It retries across a concurrent resize so the value reflects
// the latest table.
func (m *Map) GetInto(key, dst []byte) bool { return m.getIntoH(key, dst, m.hash(key)) }

// getIntoH is GetInto with a caller-supplied hash (lets Sharded hash once for
// both routing and the lookup).
func (m *Map) getIntoH(key, dst []byte, h uint64) bool {
	if len(key) != m.keyLen || len(dst) < m.valLen {
		return false
	}
	for {
		t := m.cur.Load()
		slot, ok := t.find(key, h)
		if !ok {
			// Confirm no resize raced us before reporting absence.
			if m.cur.Load() == t {
				return false
			}
			continue
		}
		t.loadValue(slot, dst)
		if m.cur.Load() == t {
			return true
		}
		// Resized during the read; redo against the current table.
	}
}

// ForEach calls fn for each live entry over a snapshot of the current table.
// key and val are valid only for the duration of the call — copy them to retain.
// Returning false stops early. Entries inserted after iteration begins, or a
// resize, may not be reflected (it is a best-effort point-in-time snapshot).
func (m *Map) ForEach(fn func(key, val []byte) bool) { m.cur.Load().forEach(fn) }

// Digest returns the order-independent content digest (see table.digest). Equal
// digests imply equal key→value sets; it is a cheap integrity/equality proof.
func (m *Map) Digest() uint64 { return m.cur.Load().digest() }

// Contains reports whether key is present. A key of the wrong length reports false.
func (m *Map) Contains(key []byte) bool { return m.containsH(key, m.hash(key)) }

func (m *Map) containsH(key []byte, h uint64) bool {
	// The length check is not just input hygiene: equalKey compares in 8-byte
	// strides bounded by the *table's* key length while indexing the caller's
	// slice, so a short key reads past its end once a control tag collides and the
	// first 8 bytes match. Shared's read path already validated; Map's did not.
	if len(key) != m.keyLen {
		return false
	}
	_, ok := m.cur.Load().find(key, h)
	return ok
}

// ---- value update (lock-free for <=8-byte values) ----

// Update sets the value of an existing key and reports whether it was found. It
// does not insert. For <=8-byte values it is lock-free; for larger values it is
// serialized with other writers (seqlock) but still never blocks readers.
//
// A key or value of the wrong length also returns false — the same convention as
// Contains and Delete, and deliberately not Put's ErrBadSize, since Update has no
// error return. False therefore means "not updated", not strictly "not found".
func (m *Map) Update(key, val []byte) bool {
	if len(key) != m.keyLen || len(val) != m.valLen {
		return false
	}
	return m.updateH(key, val, m.hash(key))
}

func (m *Map) updateH(key, val []byte, h uint64) bool {
	if m.cfg.readOnly {
		return false
	}

	if m.valLen > 8 {
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		t := m.cur.Load()
		slot, ok := t.find(key, h)
		if ok {
			t.storeValue(slot, val)
		}
		return ok
	}
	for {
		t := m.cur.Load()
		slot, ok := t.find(key, h)
		if !ok {
			if m.cur.Load() == t {
				return false
			}
			continue
		}
		t.storeValue(slot, val)
		if m.cur.Load() == t {
			return true
		}
		// Resized mid-update; reapply to the new table (idempotent).
	}
}

// ---- structural mutation (single writer) ----

// Put inserts key or updates its value. Returns true if a new entry was created.
func (m *Map) Put(key, val []byte) (inserted bool, err error) {
	if len(key) != m.keyLen || len(val) != m.valLen {
		return false, ErrBadSize
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.insertLockedH(key, val, m.hash(key))
}

// putH inserts/updates with a caller-supplied hash, taking the writer lock.
func (m *Map) putH(key, val []byte, h uint64) (bool, error) {
	if m.cfg.readOnly {
		return false, ErrReadOnly
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.insertLockedH(key, val, h)
}

// insertLockedH performs one insert/update with a caller-supplied hash. The
// caller must hold writeMu.
func (m *Map) insertLockedH(key, val []byte, h uint64) (bool, error) {
	// The single insert chokepoint: Put reaches it directly and putH (which
	// Sharded calls) reaches it too, so gating either one alone leaves the other
	// open — and an ungated insert stores through a PROT_READ mapping and takes
	// the process down with SIGBUS.
	if m.cfg.readOnly {
		return false, ErrReadOnly
	}

	for {
		t := m.cur.Load()
		slot, isNew, full := t.insert(key, val, h)
		if full {
			if err := m.grow(t); err != nil && m.cur.Load() == t {
				// grow (via rehash) can return a non-nil err even after
				// successfully publishing a larger table — the D2 case,
				// where the rename committed but the directory fsync
				// failed. m.cur.Load() == t means that did NOT happen here:
				// the table genuinely did not change, so there is nothing
				// to retry the insert against and the error is real.
				return false, err
			}
			continue
		}
		if !isNew {
			t.storeValue(slot, val) // update existing
			return false, nil
		}
		// Keep probes short: grow once the live load crosses the threshold, or
		// rehash to shed tombstones. The insert above already succeeded
		// (isNew is true, the key is in t) — a failure of this follow-up
		// maintenance is not a failure of the insert the caller asked for,
		// and must not be reported as one (docs/audits/defects-log.md,
		// defect R5). The same over-threshold condition will still hold on
		// the next insert, so a maintenance failure here is naturally
		// retried rather than silently dropped — but it would retry forever
		// with zero visibility if a caller never checked, so record it for
		// MaintenanceErr rather than discarding it outright.
		if t.loadCount()*loadDen > t.capacity*loadNum {
			m.maintErr.Store(&maintErrBox{m.grow(t)})
		} else if (t.loadCount()+t.loadTomb())*loadDen > t.capacity*loadNum {
			m.maintErr.Store(&maintErrBox{m.rehash(t, t.capacity)})
		}
		return true, nil
	}
}

// Batch acquires the writer lock once and runs fn, which inserts via the
// supplied put closure. This amortizes lock (and, for Shared, cross-process
// flock) acquisition across a bulk load. Pair with Reserve to avoid resizes.
func (m *Map) Batch(fn func(put func(key, val []byte) (bool, error)) error) error {
	if m.cfg.readOnly {
		return ErrReadOnly
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return fn(func(key, val []byte) (bool, error) {
		if len(key) != m.keyLen || len(val) != m.valLen {
			return false, ErrBadSize
		}
		return m.insertLockedH(key, val, m.hash(key))
	})
}

// batchH is Batch whose put closure takes a caller-supplied hash, so a sharded
// bulk load can reuse the hash it already computed for routing.
func (m *Map) batchH(fn func(put func(key, val []byte, h uint64) (bool, error)) error) error {
	if m.cfg.readOnly {
		return ErrReadOnly
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return fn(func(key, val []byte, h uint64) (bool, error) {
		return m.insertLockedH(key, val, h)
	})
}

// deleteBatchH acquires the writer lock once and runs fn, which erases via the
// supplied del closure (which takes a caller-supplied hash, reused from shard
// routing). The delete counterpart to batchH — amortizes the lock across a bulk
// delete. del reports whether the key existed.
func (m *Map) deleteBatchH(fn func(del func(key []byte, h uint64) bool) error) error {
	if m.cfg.readOnly {
		return ErrReadOnly
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return fn(func(key []byte, h uint64) bool {
		return m.cur.Load().erase(key, h)
	})
}

// Reserve grows the table up front so it can hold at least n entries without a
// resize, turning a bulk load's many incremental resizes into at most one. A
// non-positive n is a no-op.
func (m *Map) Reserve(n int) error {
	if m.cfg.readOnly {
		return ErrReadOnly
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	need := capacityFor(n)
	t := m.cur.Load()
	if need <= t.capacity {
		return nil
	}
	return m.rehash(t, need)
}

// Delete removes key and reports whether it existed.
func (m *Map) Delete(key []byte) bool {
	if m.cfg.readOnly {
		return false
	}

	if len(key) != m.keyLen {
		return false
	}
	h := m.hash(key)
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.cur.Load().erase(key, h)
}

// Prune sweeps every live entry in file order and tombstones each one for which
// aged returns true — a maintenance pass for aging out stale entries by a
// timestamp or flag packed in the value. It holds the writer lock for the whole
// sweep (reads stay lock-free) and never inserts, so it cannot resize. key and
// val alias the mmap and are valid only for the duration of the aged call — copy
// to retain. Tombstoned slots are reused by later Puts and shed by the next
// rehash, so a steady-state churn set neither grows nor needs compaction. See
// PruneBefore / PruneFlag for the common timestamp/flag predicates. Returns the
// number of entries removed.
func (m *Map) Prune(aged func(key, val []byte) bool) int {
	if m.cfg.readOnly {
		return 0
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.cur.Load().prune(aged)
}

// Shrink rehashes the table down to the smallest power-of-two capacity that
// holds the current live count at the load factor, reclaiming disk and page
// cache after a mass age-out. It is a no-op if the table is already that small.
// Steady-state churn does NOT need it — tombstones are reused by later Puts and
// shed by the automatic same-capacity rehash, so capacity never grows. Reach for
// Shrink only when a set has gone permanently sparse (e.g. a large Prune with no
// refill) and you want the file to give space back. Holds the writer lock; do
// not call concurrently with writers.
func (m *Map) Shrink() error {
	if m.cfg.readOnly {
		return ErrReadOnly
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	t := m.cur.Load()
	// Size from the slots that are actually occupied, not the header counter: the
	// counter is bumped after a slot is published, so a crash in that window leaves
	// it permanently low, and a low count here sizes the rebuild too small to hold
	// what it is about to be handed.
	need := capacityFor(t.liveCount())
	if need >= t.capacity {
		return nil
	}
	return m.rehash(t, need)
}

// grow doubles capacity. Must hold writeMu.
func (m *Map) grow(t *table) error { return m.rehash(t, t.capacity*2) }

// rehash builds a fresh table of newCap, copies live entries, durably swaps the
// file into place, and publishes the new table. Old readers keep using the old
// mapping until they finish; it is freed at Close. Must hold writeMu.
func (m *Map) rehash(old *table, newCap uint64) error {
	nt, err := buildResized(m.path, m.keyLen, m.valLen, old, newCap, m.cfg.doSync())
	if nt != nil {
		// buildResized returns a non-nil nt alongside a non-nil err only when
		// the rename already committed and just the directory fsync failed —
		// the new table is live and correct on disk, only unconfirmed as
		// durable. It must still be published: returning early here would
		// leave m.cur pointing at old, whose inode the path no longer names,
		// and every write after that would be silently lost
		// (docs/audits/defects-log.md, defect D2).
		m.cur.Store(nt)
		m.retired = append(m.retired, old.rw)
	}
	return err
}

// Sync flushes the active table to disk, msync-ing only the pages written since
// the last Sync.
func (m *Map) Sync() error {
	if m.cfg.readOnly {
		return nil // nothing was written, so there is nothing to flush
	}
	return m.cur.Load().syncDirty()
}
