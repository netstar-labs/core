package core

import (
	"encoding/binary"
	"hash/fnv"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/netstar-labs/core/internal/mmap"
	"github.com/netstar-labs/core/internal/xxhash"
)

// syncFullHook and syncRangeHook, when non-nil, replace the real msync inside
// syncDirty, so a test can inject a flush failure without corrupting the
// mapping the way closing or unmapping the real file underneath it would.
// Tests set them under syncHookMu; production never touches them.
var (
	syncFullHook  func() error
	syncRangeHook func(off, n int) error
	syncHookMu    sync.Mutex
)

func callSyncFull(t *table) error {
	syncHookMu.Lock()
	hook := syncFullHook
	syncHookMu.Unlock()
	if hook != nil {
		return hook()
	}
	return t.rw.Sync()
}

func callSyncRange(t *table, off, n int) error {
	syncHookMu.Lock()
	hook := syncRangeHook
	syncHookMu.Unlock()
	if hook != nil {
		return hook(off, n)
	}
	return t.rw.SyncRange(off, n)
}

// A table is one open-addressing hash map backed by a single mmap'd file. It is
// Swiss-table-style: a dense array of 1-byte control tags, scanned 8 at a time
// with SWAR, sits in front of the fixed-size key/value slots. The control array
// is small enough to stay resident even when the slots exceed RAM, so a lookup
// faults in at most one slot page.
//
// File layout (offsets in the header, all regions page/word aligned):
//
//	[ header : PageSize ]
//	[ control : capacity bytes ]            empty=0x00, deleted=0x01, full=0x80|H2
//	[ version : capacity*4 bytes ]          present only when valLen > 8 (seqlock)
//	[ slots   : capacity*(keyLen+valLen) ]  key | value
//
// Concurrency: reads are lock-free. The control word and the value word are
// read with atomics; a full slot's key/value are published by an atomic
// release-store of the control byte after the key (and, for new inserts, the
// value) are written, so a reader that observes the tag also observes the data.
// Structural mutation (insert/erase/resize) is single-writer, serialized by the
// owner's writer lock — Map.writeMu, or Shared.writeMu plus a cross-process
// flock.
const (
	PageSize = 4096

	ctrlEmpty   = 0x00
	ctrlDeleted = 0x01
	ctrlFull    = 0x80 // OR'd with the 7-bit H2 tag

	loByte = 0x0101010101010101
	hiByte = 0x8080808080808080
)

// Header field offsets within the first page.
const (
	hMagic    = 0  // u32
	hVersion  = 4  // u32
	hKeyLen   = 8  // u32
	hValLen   = 12 // u32
	hCapacity = 16 // u64
	hCount    = 24 // u64
	hTomb     = 32 // u64
	hCtrlOff  = 40 // u64
	hVerOff   = 48 // u64 (0 if no version array)
	hSlotsOff = 56 // u64
	hSlotSize = 64 // u64
	hChecksum = 72 // u64 — fnv64a over the immutable geometry fields

	magicValue   = 0x4854_4f41 // "HTOA"
	versionValue = 1
)

// headerChecksum hashes only the immutable geometry fields (everything except
// the mutable count/tombstones counters), so *accidental* corruption of a capacity
// or region offset is caught at Open. It does not cover the live entries — for
// that use Digest.
//
// It is fnv64a and unkeyed, so it is an integrity check, not authentication:
// anyone able to write the file can recompute it. openTable therefore does not
// trust a checksummed geometry either — it recomputes the layout and requires the
// stored offsets to match (see validateGeometry).
func headerChecksum(h []byte) uint64 {
	s := fnv.New64a()
	s.Write(h[0:24])  // magic, version, keyLen, valLen, capacity
	s.Write(h[40:72]) // ctrlOff, verOff, slotsOff, slotSize
	return s.Sum64()
}

// entryHash is the per-element hash of the multiset digest: a commutative sum of
// these over all live entries is order-independent, so two stores with the same
// key→value set have the same digest.
func entryHash(key, val []byte) uint64 {
	h := xxhash.Sum(key)
	h ^= xxhash.Sum(val) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	return h
}

type table struct {
	rw   *mmap.RW
	data []byte

	keyLen   int
	valLen   int
	slotSize int
	capacity uint64
	mask     uint64 // numGroups-1

	ctrlOff  int
	verOff   int // 0 when !hasVersion
	slotsOff int

	hasVersion bool // valLen > 8 ⇒ seqlock-protected values

	dirty *dirtyset // pages written since the last Sync
}

func (t *table) markOff(off, n int) { t.dirty.mark(off, n) }

// syncDirty flushes the pages written since the last Sync. When the dirty set is
// sparse relative to the map, it msyncs each contiguous run individually — the
// regime that matters for a table far larger than RAM, where one whole-file
// msync would scan billions of clean PTEs. When a large fraction is dirty (the
// common case for a hash table, whose writes scatter uniformly), a single
// whole-file msync beats many per-range syscalls, so it falls back to that. This
// makes Sync never slower than a plain whole-file msync.
func (t *table) syncDirty() error {
	runs, dirty := t.dirty.collect(len(t.data))
	if dirty == 0 {
		return nil
	}
	if dirty*8 >= t.dirty.nchunks { // ≥ ~12.5% dirty ⇒ whole-file is cheaper
		if err := callSyncFull(t); err != nil {
			// collect() already cleared every bit above; Sync covers the
			// whole file, so on failure every collected run must be
			// re-marked dirty, or a later syncDirty reports "clean" for data
			// that was never actually flushed (docs/audits/defects-log.md,
			// defect D4).
			for _, r := range runs {
				t.markOff(r.off, r.n)
			}
			return err
		}
		return nil
	}
	var err error
	for _, r := range runs {
		if e := callSyncRange(t, r.off, r.n); e != nil {
			if err == nil {
				err = e
			}
			t.markOff(r.off, r.n) // this range was not actually flushed; retry it next time
		}
	}
	return err
}

// count and tombstones live in the mapped header and are mutated with atomics,
// so they stay correct when multiple processes write the same shared inode.
// (Caching them per-process would let two writers clobber each other's total —
// the slots themselves are authoritative, but the counter would drift.)
func (t *table) countPtr() *uint64 { return t.u64(hCount) }
func (t *table) tombPtr() *uint64  { return t.u64(hTomb) }
func (t *table) loadCount() uint64 { return loadU64(t.countPtr()) }
func (t *table) loadTomb() uint64  { return loadU64(t.tombPtr()) }
func (t *table) addCount(d uint64) { addU64(t.countPtr(), d); t.markOff(hCount, 8) }
func (t *table) addTomb(d uint64)  { addU64(t.tombPtr(), d); t.markOff(hTomb, 8) }

// ---- geometry ----

func alignUp(n, a int) int { return (n + a - 1) &^ (a - 1) }

const (
	// minCapacity is one control group: the smallest table the SWAR probe works on.
	minCapacity = 8
	// maxCapacity bounds the doubling in roundCapacity. It is far past any real
	// store (2^56 slots) but keeps the loop from stepping onto 2^64, where the
	// shift wraps to zero and the loop can never terminate. A capacity this large
	// fails later at Truncate with a real error instead.
	maxCapacity = 1 << 56
)

// roundCapacity rounds n up to a power-of-two slot capacity, clamped to
// [minCapacity, maxCapacity].
//
// The clamp is load-bearing, not defensive tidiness. n arrives from
// `(reserve*loadDen)/loadNum + 1` — arithmetic that legitimately lands at or below
// zero for a small or negative reserve — and `uint64(n)` on a negative int is
// ~2^64, which sent the doubling loop past 2^63 to 0 and then spun forever. That
// hung Open with a negative initCap, Reserve(n) for n ≤ -10, ReserveCapPerShard
// with a negative reserve, and Shrink whenever a live count narrowed negative.
func roundCapacity(n int) uint64 {
	if n <= minCapacity {
		return minCapacity
	}
	c := uint64(minCapacity)
	for c < uint64(n) && c < maxCapacity {
		c <<= 1
	}
	return c
}

// capacityFor converts an expected entry count into the slot capacity that holds
// it under the 7/8 load factor. It is the one home for that arithmetic, which was
// otherwise spelled out at five call sites (Map/Shared Reserve and Shrink, and
// ReserveCapPerShard) whose agreement had to be maintained by hand.
func capacityFor(entries int) uint64 {
	if entries <= 0 {
		return minCapacity
	}
	// Guard the scale-up itself: entries*loadDen overflows int for absurd inputs,
	// and a wrapped-negative product would clamp to minCapacity — silently sizing
	// a huge reserve down to one group.
	if entries > (1<<62)/loadDen {
		return maxCapacity
	}
	return roundCapacity((entries*loadDen)/loadNum + 1)
}

// layout computes region offsets and total file size for a capacity.
func layout(keyLen, valLen int, capacity uint64) (ctrlOff, verOff, slotsOff, slotSize, fileSize int) {
	slotSize = keyLen + valLen
	ctrlOff = PageSize
	ctrlEnd := ctrlOff + int(capacity)
	if valLen > 8 {
		verOff = alignUp(ctrlEnd, 8)
		ctrlEnd = verOff + int(capacity)*4
	}
	slotsOff = alignUp(ctrlEnd, PageSize)
	fileSize = slotsOff + int(capacity)*slotSize
	return
}

// newTable initializes the header of a freshly mapped (zeroed) file and returns
// a view over it. The zeroed control array already means "all empty".
func newTable(rw *mmap.RW, keyLen, valLen int, capacity uint64) *table {
	ctrlOff, verOff, slotsOff, slotSize, _ := layout(keyLen, valLen, capacity)
	t := &table{
		rw: rw, data: rw.Bytes(),
		keyLen: keyLen, valLen: valLen, slotSize: slotSize,
		capacity: capacity, mask: capacity/8 - 1,
		ctrlOff: ctrlOff, verOff: verOff, slotsOff: slotsOff,
		hasVersion: valLen > 8,
		dirty:      newDirtyset(len(rw.Bytes())),
	}
	h := t.data
	binary.LittleEndian.PutUint32(h[hMagic:], magicValue)
	binary.LittleEndian.PutUint32(h[hVersion:], versionValue)
	binary.LittleEndian.PutUint32(h[hKeyLen:], uint32(keyLen))
	binary.LittleEndian.PutUint32(h[hValLen:], uint32(valLen))
	binary.LittleEndian.PutUint64(h[hCapacity:], capacity)
	binary.LittleEndian.PutUint64(h[hCount:], 0)
	binary.LittleEndian.PutUint64(h[hTomb:], 0)
	binary.LittleEndian.PutUint64(h[hCtrlOff:], uint64(ctrlOff))
	binary.LittleEndian.PutUint64(h[hVerOff:], uint64(verOff))
	binary.LittleEndian.PutUint64(h[hSlotsOff:], uint64(slotsOff))
	binary.LittleEndian.PutUint64(h[hSlotSize:], uint64(slotSize))
	binary.LittleEndian.PutUint64(h[hChecksum:], headerChecksum(h)) // last: seals the geometry
	t.markOff(0, PageSize)                                          // header page is dirty
	t.advise()
	return t
}

// openTable parses an existing file's header into a view.
func openTable(rw *mmap.RW, wantKey, wantVal int) (*table, error) {
	h := rw.Bytes()
	if len(h) < PageSize || binary.LittleEndian.Uint32(h[hMagic:]) != magicValue {
		return nil, ErrCorrupt
	}
	// Validate the geometry checksum before trusting any offset/size field.
	if headerChecksum(h) != binary.LittleEndian.Uint64(h[hChecksum:]) {
		return nil, ErrCorrupt
	}
	keyLen := int(binary.LittleEndian.Uint32(h[hKeyLen:]))
	valLen := int(binary.LittleEndian.Uint32(h[hValLen:]))
	if keyLen != wantKey || valLen != wantVal {
		return nil, ErrBadSize
	}
	t := &table{
		rw: rw, data: h,
		keyLen: keyLen, valLen: valLen,
		slotSize: int(binary.LittleEndian.Uint64(h[hSlotSize:])),
		capacity: binary.LittleEndian.Uint64(h[hCapacity:]),
		ctrlOff:  int(binary.LittleEndian.Uint64(h[hCtrlOff:])),
		verOff:   int(binary.LittleEndian.Uint64(h[hVerOff:])),
		slotsOff: int(binary.LittleEndian.Uint64(h[hSlotsOff:])),
	}
	if err := t.validateGeometry(len(h)); err != nil {
		return nil, err
	}
	t.mask = t.capacity/8 - 1
	t.hasVersion = valLen > 8
	t.dirty = newDirtyset(len(h))
	t.advise()
	return t, nil
}

// validateGeometry checks the header's own region offsets against the layout they
// must have, and against the size actually mapped.
//
// The checksum alone is not enough. It is unkeyed, so a crafted file can carry a
// perfectly valid checksum over an absurd geometry — and every offset in the header
// then feeds slice indexing and unsafe.Pointer arithmetic. A store file is not
// always locally produced: bundle.Pack/Unpack and patch.OpenBundle exist to ship
// stores between machines, so "the header says so" is not a trust basis. A crafted
// slotsOff of 2^40 over a 9 KiB file used to panic with an out-of-range slice
// during Open.
//
// Because layout() is a pure function of (keyLen, valLen, capacity), the check is
// exact rather than a range test: recompute and require equality.
func (t *table) validateGeometry(mapped int) error {
	if t.capacity < minCapacity || t.capacity > maxCapacity || t.capacity&(t.capacity-1) != 0 {
		return ErrCorrupt // must be a power of two ≥ 8: mask is capacity/8-1
	}
	ctrlOff, verOff, slotsOff, slotSize, fileSize := layout(t.keyLen, t.valLen, t.capacity)
	if t.ctrlOff != ctrlOff || t.verOff != verOff || t.slotsOff != slotsOff || t.slotSize != slotSize {
		return ErrCorrupt
	}
	if fileSize <= 0 || fileSize > mapped {
		return ErrCorrupt // the geometry describes more file than exists
	}
	return nil
}

func (t *table) advise() { adviseRandom(t.data[t.slotsOff:]) }

// ---- raw accessors ----

func (t *table) u64(off int) *uint64 { return (*uint64)(unsafe.Pointer(&t.data[off])) }
func (t *table) u32(off int) *uint32 { return (*uint32)(unsafe.Pointer(&t.data[off])) }

func (t *table) keyBytes(slot uint64) []byte {
	o := t.slotsOff + int(slot)*t.slotSize
	return t.data[o : o+t.keyLen]
}
func (t *table) valOff(slot uint64) int   { return t.slotsOff + int(slot)*t.slotSize + t.keyLen }
func (t *table) verOffOf(slot uint64) int { return t.verOff + int(slot)*4 }

// groupWord atomically loads the 8 control bytes of group g.
func (t *table) groupWord(g uint64) uint64 {
	return loadU64(t.u64(t.ctrlOff + int(g)*8))
}

// ---- SWAR helpers ----

// matchByte returns a mask with 0x80 set in each lane whose control byte == b.
func matchByte(group uint64, b byte) uint64 {
	x := group ^ (loByte * uint64(b))
	return (x - loByte) &^ x & hiByte
}

// emptyOrDeleted returns a mask with 0x80 set in each lane that is not full
// (full bytes have the top bit set; empty=0x00 and deleted=0x01 do not).
func emptyOrDeleted(group uint64) uint64 { return ^group & hiByte }

func lane(mask uint64) uint64 { return uint64(bits.TrailingZeros64(mask)) >> 3 }

func fullTag(h uint64) byte { return ctrlFull | byte(h&0x7f) }

func equalKey(a, b []byte) bool {
	for i := 0; i < len(a); i += 8 {
		if binary.LittleEndian.Uint64(a[i:]) != binary.LittleEndian.Uint64(b[i:]) {
			return false
		}
	}
	return true
}

// ---- lookup ----

// find returns the slot holding key, or ok=false if absent.
func (t *table) find(key []byte, h uint64) (uint64, bool) {
	tag := fullTag(h)
	g := (h >> 7) & t.mask
	for probe := uint64(0); probe <= t.mask; probe++ {
		w := t.groupWord(g)
		for m := matchByte(w, tag); m != 0; m &= m - 1 {
			slot := g*8 + lane(m)
			if equalKey(t.keyBytes(slot), key) {
				return slot, true
			}
		}
		if matchByte(w, ctrlEmpty) != 0 {
			return 0, false // an empty in the group ⇒ key was never inserted past here
		}
		g = (g + 1) & t.mask
	}
	return 0, false
}

// ---- value load/store ----

func (t *table) loadValue(slot uint64, dst []byte) {
	if t.valLen == 0 {
		return // key-only (set) store: no value to load
	}
	off := t.valOff(slot)
	if !t.hasVersion {
		binary.LittleEndian.PutUint64(dst, loadU64(t.u64(off)))
		return
	}
	vp := t.u32(t.verOffOf(slot))
	for {
		s1 := loadU32(vp)
		if s1&1 != 0 {
			continue // writer mid-update
		}
		copy(dst, t.data[off:off+t.valLen])
		if loadU32(vp) == s1 {
			return
		}
	}
}

// storeValue updates an existing slot's value. For >8-byte values it is a
// seqlock write and must be serialized with other writers (Map.writeMu).
func (t *table) storeValue(slot uint64, val []byte) {
	if t.valLen == 0 {
		return // key-only (set) store: no value to store
	}
	off := t.valOff(slot)
	if !t.hasVersion {
		storeU64(t.u64(off), binary.LittleEndian.Uint64(val))
		t.markOff(off, 8)
		return
	}
	vp := t.u32(t.verOffOf(slot))
	s := loadU32(vp)
	storeU32(vp, s+1) // odd: write in progress
	copy(t.data[off:off+t.valLen], val)
	storeU32(vp, s+2) // even: published
	t.markOff(t.verOffOf(slot), 4)
	t.markOff(off, t.valLen)
}

// ---- control publication ----

// publishControl atomically sets slot's control byte (release), making a
// previously-written key/value visible to lock-free readers. Single writer.
func (t *table) publishControl(slot uint64, ctrl byte) {
	word := t.ctrlOff + int(slot&^7)
	shift := (slot & 7) * 8
	wp := t.u64(word)
	v := loadU64(wp)
	v = (v &^ (uint64(0xff) << shift)) | (uint64(ctrl) << shift)
	storeU64(wp, v)
	t.markOff(word, 8)
}

func (t *table) writeKey(slot uint64, key []byte) {
	copy(t.keyBytes(slot), key)
	t.markOff(t.slotsOff+int(slot)*t.slotSize, t.keyLen)
}
func (t *table) writeValPlain(slot uint64, val []byte) {
	o := t.valOff(slot)
	copy(t.data[o:o+t.valLen], val)
	t.markOff(o, t.valLen)
}

// ---- insert / erase (single writer) ----

// insert places key (with val for a brand-new slot) and reports the slot,
// whether it was newly created, and whether the table is full (no slot found —
// caller must resize and retry). For an existing key the value is NOT written
// here; the caller updates it via storeValue so the atomic/seqlock path is used.
func (t *table) insert(key, val []byte, h uint64) (slot uint64, isNew, full bool) {
	tag := fullTag(h)
	g := (h >> 7) & t.mask
	var ins uint64
	haveIns := false
	insDeleted := false

	for probe := uint64(0); probe <= t.mask; probe++ {
		w := t.groupWord(g)
		for m := matchByte(w, tag); m != 0; m &= m - 1 {
			s := g*8 + lane(m)
			if equalKey(t.keyBytes(s), key) {
				return s, false, false
			}
		}
		if !haveIns {
			if ed := emptyOrDeleted(w); ed != 0 {
				s := g*8 + lane(ed)
				ins, haveIns = s, true
				insDeleted = t.data[t.ctrlOff+int(s)] == ctrlDeleted
			}
		}
		if matchByte(w, ctrlEmpty) != 0 {
			if !haveIns { // shouldn't happen, but be safe
				ins = g*8 + lane(matchByte(w, ctrlEmpty))
				insDeleted = false
			}
			t.writeKey(ins, key)
			t.writeValPlain(ins, val)  // value before control: readers see it on publish
			t.publishControl(ins, tag) // release
			t.addCount(1)
			if insDeleted {
				t.addTomb(^uint64(0)) // -1
			}
			return ins, true, false
		}
		g = (g + 1) & t.mask
	}
	return 0, false, true
}

// erase tombstones key if present.
func (t *table) erase(key []byte, h uint64) bool {
	slot, ok := t.find(key, h)
	if !ok {
		return false
	}
	t.publishControl(slot, ctrlDeleted)
	t.addCount(^uint64(0)) // -1
	t.addTomb(1)
	return true
}

// insertPlain is the non-atomic insert used while building a fresh table during
// resize (no concurrent access to the new table yet). It reports false when the
// table has no free slot.
//
// The probe is bounded by one full cycle of the group ring. It used to loop
// unbounded on the reasoning that a rebuild destination always has room — true
// only while its capacity was sized correctly, and a low header count could
// undersize it (see liveCount). An unbounded probe turned that into a silent,
// unkillable spin; a bounded one turns it into an error the caller can report.
func (t *table) insertPlain(key, val []byte, h uint64) bool {
	tag := fullTag(h)
	g := (h >> 7) & t.mask
	for probe := uint64(0); probe <= t.mask; probe++ {
		w := binary.LittleEndian.Uint64(t.data[t.ctrlOff+int(g)*8:])
		if ed := emptyOrDeleted(w); ed != 0 {
			s := g*8 + lane(ed)
			t.writeKey(s, key)
			t.writeValPlain(s, val)
			t.data[t.ctrlOff+int(s)] = tag
			t.markOff(t.ctrlOff+int(s), 1)
			t.addCount(1)
			return true
		}
		g = (g + 1) & t.mask
	}
	return false
}

// forEachLive calls fn for every occupied slot (used by resize, single-writer
// context, so it reads raw value bytes).
func (t *table) forEachLive(fn func(key, val []byte)) {
	for slot := uint64(0); slot < t.capacity; slot++ {
		if c := t.data[t.ctrlOff+int(slot)]; c&ctrlFull != 0 {
			o := t.slotsOff + int(slot)*t.slotSize
			fn(t.data[o:o+t.keyLen], t.data[o+t.keyLen:o+t.slotSize])
		}
	}
}

// liveCount counts occupied slots by scanning the control array, rather than
// trusting the header counter.
//
// The counter is authoritative in normal operation, but it is incremented *after*
// the control byte is published, so a crash in that window leaves a live slot
// uncounted and the counter low forever after. Sizing a rebuild from a low count
// produces a destination too small to hold what forEachLive then feeds it — which
// used to spin insertPlain forever. Shrink pays this O(capacity) control scan (it
// is already an O(capacity) rebuild) so its sizing cannot be wrong.
func (t *table) liveCount() int {
	n := 0
	for slot := uint64(0); slot < t.capacity; slot++ {
		if t.data[t.ctrlOff+int(slot)]&ctrlFull != 0 {
			n++
		}
	}
	return n
}

// digest returns the order-independent content digest of this table: the sum
// (mod 2^64) of entryHash over every live entry. Values are read consistently
// (atomic/seqlock); it is a best-effort snapshot under concurrent writes.
func (t *table) digest() uint64 {
	var d uint64
	val := make([]byte, t.valLen)
	for slot := uint64(0); slot < t.capacity; slot++ {
		if t.data[t.ctrlOff+int(slot)]&ctrlFull != 0 {
			t.loadValue(slot, val)
			d += entryHash(t.keyBytes(slot), val)
		}
	}
	return d
}

// forEach calls fn for each live entry. key aliases the mmap and val aliases a
// reused buffer; both are valid only for the duration of the call. Values are
// read via the same atomic/seqlock path as Get, so they are consistent under
// concurrent updates. Returning false stops iteration.
func (t *table) forEach(fn func(key, val []byte) bool) bool {
	val := make([]byte, t.valLen)
	for slot := uint64(0); slot < t.capacity; slot++ {
		if t.data[t.ctrlOff+int(slot)]&ctrlFull != 0 {
			t.loadValue(slot, val)
			if !fn(t.keyBytes(slot), val) {
				return false
			}
		}
	}
	return true
}

// prune sweeps every live slot in file order and tombstones each entry for which
// aged reports true — the maintenance primitive behind Map/Shared/Sharded.Prune.
// The caller MUST hold the writer lock: it mutates control bytes and the
// count/tomb counters. Because it never inserts it cannot trigger a resize, so
// the mapping stays valid for the whole sweep. key and val alias the mmap and
// are valid only for the duration of the aged call — copy to retain. The value
// is read raw (no seqlock) since the writer lock excludes concurrent updates.
//
// The sweep is strictly forward over the control and slot regions, so it advises
// MADV_SEQUENTIAL for the pass (readahead) and restores MADV_RANDOM after: a
// store larger than RAM streams in instead of faulting one page at a time, which
// is the point of scanning a shard top to bottom. Returns the count tombstoned.
func (t *table) prune(aged func(key, val []byte) bool) int {
	adviseSequential(t.data[t.ctrlOff:]) // control + slots are both swept forward
	defer adviseRandom(t.data[t.slotsOff:])
	removed := 0
	for slot := uint64(0); slot < t.capacity; slot++ {
		if t.data[t.ctrlOff+int(slot)]&ctrlFull == 0 {
			continue
		}
		o := t.slotsOff + int(slot)*t.slotSize
		if aged(t.data[o:o+t.keyLen], t.data[o+t.keyLen:o+t.slotSize]) {
			t.publishControl(slot, ctrlDeleted)
			t.addCount(^uint64(0)) // -1
			t.addTomb(1)
			removed++
		}
	}
	return removed
}
