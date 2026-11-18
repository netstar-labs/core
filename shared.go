package core

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/netstar-labs/core/internal/mmap"
)

// Shared is the cross-process variant of Map. Multiple processes open the same
// path; the data file is MAP_SHARED, so inserts, deletes, and value updates are
// visible to every process immediately through shared memory + atomics. Only a
// resize replaces the data inode, and that is signaled through a generation
// counter in a small shared meta file so other processes remap on their next
// operation. A cross-process advisory lock (flock on the meta file) enforces a
// single writer at a time.
//
// What is NOT pinned cross-process: there is no reader-slot table, because an
// open-addressing table never reclaims pages out from under a reader the way an
// MVCC B-tree does. A resized-away inode stays alive until every process that
// mapped it remaps; the only cost a reader pays is a one-operation staleness
// window before it notices the generation bump.
type Shared struct {
	path   string
	keyLen int
	valLen int
	cfg    config

	meta     *metaFile
	cur      atomic.Pointer[table]
	localGen atomic.Uint64

	remapMu sync.Mutex // serializes remap and protects retired
	writeMu sync.Mutex // in-process writer serialization (cross-process: flock)
	retired []*mmap.RW

	// remapErr is the last failure to adopt the writer's resized inode, cleared by
	// the next success. Guarded by remapMu. While set, this handle is stale: reads
	// answer from the inode it still maps, writes are refused (see checkRemap).
	remapErr error

	maintErr atomic.Pointer[maintErrBox]
}

// MaintenanceErr returns the error from the most recent background
// grow/resize triggered automatically by an insert crossing the load-factor
// threshold, or nil if the most recent attempt succeeded (or none has run
// yet). See Map.MaintenanceErr (defect R5): the insert's own result never
// carries this error, so a maintenance resize that cannot succeed would
// otherwise retry silently forever with no way to detect it.
func (s *Shared) MaintenanceErr() error {
	b := s.maintErr.Load()
	if b == nil {
		return nil
	}
	return b.err
}

// ---- meta file ----

const (
	metaFileSize   = PageSize
	mMagic         = 0
	mVersion       = 4
	mGen           = 8           // u64, atomic
	metaMagicVal   = 0x4854_4d58 // "HTMX"
	metaVersionVal = 1
)

type metaFile struct {
	f    *os.File
	rw   *mmap.RW
	data []byte
	ro   bool // opened WithReadOnly: lockWriter refuses, nothing is ever stored
}

func openMeta(path string, ro bool) (*metaFile, error) {
	if ro {
		return openMetaRO(path)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil {
		f.Close()
		return nil, err
	} else if fi.Size() < int64(metaFileSize) {
		if err := f.Truncate(int64(metaFileSize)); err != nil {
			f.Close()
			return nil, err
		}
	}
	rw, err := mmap.MapRW(f, metaFileSize)
	if err != nil {
		f.Close()
		return nil, err
	}
	m := &metaFile{f: f, rw: rw, data: rw.Bytes()}
	if binary.LittleEndian.Uint32(m.data[mMagic:]) != metaMagicVal {
		binary.LittleEndian.PutUint32(m.data[mMagic:], metaMagicVal)
		binary.LittleEndian.PutUint32(m.data[mVersion:], metaVersionVal)
	}
	return m, nil
}

// openMetaRO opens an EXISTING, INITIALISED meta file read-only. The writer path
// above repairs a short or unstamped file in place; a reader cannot, and must not
// pretend it can: a meta that is missing, short, or unstamped is a table no
// writer has finished creating, and the only honest answer is to refuse.
func openMetaRO(path string) (*metaFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < int64(metaFileSize) {
		f.Close()
		return nil, fmt.Errorf("core: %s is not an initialised meta file (%d bytes); read-only cannot repair it", path, fi.Size())
	}
	rw, err := mmap.MapRO(f, metaFileSize)
	if err != nil {
		f.Close()
		return nil, err
	}
	m := &metaFile{f: f, rw: rw, data: rw.Bytes(), ro: true}
	if binary.LittleEndian.Uint32(m.data[mMagic:]) != metaMagicVal {
		m.close()
		return nil, fmt.Errorf("core: %s is not stamped; read-only cannot initialise it", path)
	}
	return m, nil
}

func (m *metaFile) genPtr() *uint64 { return (*uint64)(unsafe.Pointer(&m.data[mGen])) }
func (m *metaFile) gen() uint64     { return atomic.LoadUint64(m.genPtr()) }
func (m *metaFile) bumpGen() uint64 { return atomic.AddUint64(m.genPtr(), 1) }
func (m *metaFile) initGenIfZero() {
	atomic.CompareAndSwapUint64(m.genPtr(), 0, 1)
}
func (m *metaFile) lockWriter() error {
	if m.ro {
		return ErrReadOnly // never flock an O_RDONLY handle: a reader must not exclude the writer
	}
	return flockEx(m.f)
}
func (m *metaFile) unlockWriter() error { return flockUn(m.f) }
func (m *metaFile) close() error {
	err := m.rw.Close()
	if e := m.f.Close(); err == nil {
		err = e
	}
	return err
}

// ---- open / close ----

// OpenShared opens or creates a cross-process table at path. keyLen must be a
// nonzero multiple of 8; valLen must be a multiple of 8 and may be 0 (key-only set).
func OpenShared(path string, keyLen, valLen, initCap int, opts ...Option) (*Shared, error) {
	if keyLen <= 0 || keyLen%8 != 0 || valLen < 0 || valLen%8 != 0 {
		return nil, ErrBadSize
	}
	cfg := newConfig(opts)
	meta, err := openMeta(path+".meta", cfg.readOnly)
	if err != nil {
		return nil, err
	}
	s := &Shared{path: path, keyLen: keyLen, valLen: valLen, cfg: cfg, meta: meta}

	if cfg.readOnly {
		// No creation lock (a reader must never exclude the writer), no create,
		// no initGenIfZero (a store). Whatever generation the meta holds is the
		// one this reader starts from; a writer bumping it later is a remap.
		f, err := os.Open(path)
		if err != nil {
			meta.close()
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			meta.close()
			return nil, err
		}
		if fi.Size() == 0 {
			f.Close()
			meta.close()
			return nil, fmt.Errorf("core: %s is empty; read-only cannot create a table", path)
		}
		rw, err := mmap.MapRO(f, int(fi.Size()))
		f.Close()
		if err != nil {
			meta.close()
			return nil, err
		}
		t, err := openTable(rw, keyLen, valLen)
		if err != nil {
			rw.Close()
			meta.close()
			return nil, err
		}
		s.cur.Store(t)
		s.localGen.Store(meta.gen())
		return s, nil
	}

	// Serialize first-time creation across processes.
	if err := meta.lockWriter(); err != nil {
		meta.close()
		return nil, err
	}
	defer meta.unlockWriter()

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		meta.close()
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		meta.close()
		return nil, err
	}

	var t *table
	if fi.Size() == 0 {
		capacity := roundCapacity(initCap)
		_, _, _, _, fileSize := layout(keyLen, valLen, capacity)
		if err := f.Truncate(int64(fileSize)); err != nil {
			f.Close()
			meta.close()
			return nil, err
		}
		rw, err := mmap.MapRW(f, fileSize)
		if err != nil {
			f.Close()
			meta.close()
			return nil, err
		}
		f.Close()
		t = newTable(rw, keyLen, valLen, capacity)
	} else {
		rw, err := mmap.MapRW(f, int(fi.Size()))
		f.Close()
		if err != nil {
			meta.close()
			return nil, err
		}
		t, err = openTable(rw, keyLen, valLen)
		if err != nil {
			rw.Close()
			meta.close()
			return nil, err
		}
	}
	meta.initGenIfZero()
	s.cur.Store(t)
	s.localGen.Store(meta.gen())
	return s, nil
}

func (s *Shared) Close() error {
	s.remapMu.Lock()
	defer s.remapMu.Unlock()
	var err error
	if t := s.cur.Load(); t != nil {
		err = t.rw.Close()
	}
	for _, rw := range s.retired {
		if e := rw.Close(); err == nil {
			err = e
		}
	}
	if e := s.meta.close(); err == nil {
		err = e
	}
	return err
}

func (s *Shared) Len() int {
	_ = s.checkRemap()
	return int(s.cur.Load().loadCount())
}
func (s *Shared) Sync() error {
	if s.cfg.readOnly {
		return nil
	}
	return s.cur.Load().syncDirty()
}

// checkRemap adopts the latest data inode if another process resized. Cheap in
// steady state: a single atomic load + compare, remapping only on a generation
// change.
func (s *Shared) checkRemap() error {
	if s.meta.gen() == s.localGen.Load() {
		return nil
	}
	s.remapMu.Lock()
	defer s.remapMu.Unlock()
	g := s.meta.gen()
	if g == s.localGen.Load() {
		return nil
	}
	s.remapErr = s.remapLocked(g)
	return s.remapErr
}

// RemapErr reports whether this handle is currently unable to adopt the writer's
// resized inode. Nil in steady state. Non-nil means every read is answering from
// a snapshot the writer has abandoned and every write is being refused — the
// signal a health check should surface rather than let the process look fine.
func (s *Shared) RemapErr() error {
	s.remapMu.Lock()
	defer s.remapMu.Unlock()
	return s.remapErr
}

// remapLocked adopts generation g's inode. Caller holds remapMu.
//
// The old inode is unlinked but intact and MAP_SHARED, and the writer writes only
// into the new one, so on failure the caller still holds a CONSISTENT snapshot:
// stale, never torn. That is why a read may return from it. A write may not — it
// would land in an inode nobody will ever read again, and report success.
func (s *Shared) remapLocked(g uint64) error {
	flags, mapf := os.O_RDWR, mmap.MapRW
	if s.cfg.readOnly {
		flags, mapf = os.O_RDONLY, mmap.MapRO // the resized inode is adopted with the same rights as the first
	}
	f, err := os.OpenFile(s.path, flags, 0o644)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	rw, err := mapf(f, int(fi.Size()))
	f.Close()
	if err != nil {
		return err
	}
	t, err := openTable(rw, s.keyLen, s.valLen)
	if err != nil {
		rw.Close()
		return err
	}
	old := s.cur.Swap(t)
	if old != nil {
		s.retired = append(s.retired, old.rw)
	}
	s.localGen.Store(g)
	return nil
}

// ---- reads (lock-free) ----

func (s *Shared) Get(key []byte) ([]byte, bool) {
	val := make([]byte, s.valLen)
	if s.GetInto(key, val) {
		return val, true
	}
	return nil, false
}

func (s *Shared) GetInto(key, dst []byte) bool {
	if len(key) != s.keyLen {
		return false
	}
	return s.getIntoH(key, dst, sum(key))
}

func (s *Shared) getIntoH(key, dst []byte, h uint64) bool {
	// See Map.GetInto: a dst shorter than valLen would panic for 8-byte values and
	// silently truncate for larger ones. A longer dst is fine.
	if len(key) != s.keyLen || len(dst) < s.valLen {
		return false
	}
	for {
		// A failed remap makes the generation unreachable, so without this the
		// retry below never terminates: every Get that hit it would pin its
		// goroutine forever. Answering from the snapshot is safe (remapLocked).
		remapErr := s.checkRemap()
		t := s.cur.Load()
		g := s.localGen.Load()
		slot, ok := t.find(key, h)
		if !ok {
			if s.meta.gen() == g || remapErr != nil {
				return false
			}
			continue // resized; retry against the new inode
		}
		t.loadValue(slot, dst)
		if s.meta.gen() == g || remapErr != nil {
			return true
		}
	}
}

func (s *Shared) Contains(key []byte) bool {
	if len(key) != s.keyLen {
		return false
	}
	return s.containsH(key, sum(key))
}

// ForEach calls fn for each live entry over a snapshot of the current inode.
// key and val are valid only for the duration of the call. Returning false stops
// early. See Map.ForEach for snapshot semantics.
func (s *Shared) ForEach(fn func(key, val []byte) bool) {
	_ = s.checkRemap()
	s.cur.Load().forEach(fn)
}

// Digest returns the order-independent content digest (see Map.Digest).
func (s *Shared) Digest() uint64 {
	_ = s.checkRemap()
	return s.cur.Load().digest()
}

func (s *Shared) containsH(key []byte, h uint64) bool {
	_ = s.checkRemap()
	_, ok := s.cur.Load().find(key, h)
	return ok
}

// ---- value update ----

func (s *Shared) Update(key, val []byte) bool {
	if len(key) != s.keyLen || len(val) != s.valLen {
		return false
	}
	return s.updateH(key, val, sum(key))
}

func (s *Shared) updateH(key, val []byte, h uint64) bool {
	// The <=8-byte path below is lock-free by design and never reaches
	// lockWriter, so the lock cannot gate it: without this check a read-only
	// table would storeValue straight into a PROT_READ mapping.
	if s.cfg.readOnly {
		return false
	}
	if s.valLen > 8 {
		// Seqlock writes need a single writer cross-process.
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		if s.meta.lockWriter() != nil {
			return false
		}
		defer s.meta.unlockWriter()
		if s.checkRemap() != nil {
			return false // never store into an inode the writer has abandoned
		}
		t := s.cur.Load()
		slot, ok := t.find(key, h)
		if ok {
			t.storeValue(slot, val)
		}
		return ok
	}
	for {
		if s.checkRemap() != nil {
			return false // never store into an inode the writer has abandoned; also ends the retry
		}
		t := s.cur.Load()
		g := s.localGen.Load()
		slot, ok := t.find(key, h)
		if !ok {
			if s.meta.gen() == g {
				return false
			}
			continue
		}
		t.storeValue(slot, val)
		if s.meta.gen() == g {
			return true
		}
	}
}

// ---- structural mutation (single writer, cross-process) ----

func (s *Shared) Put(key, val []byte) (bool, error) {
	if len(key) != s.keyLen || len(val) != s.valLen {
		return false, ErrBadSize
	}
	return s.putH(key, val, sum(key))
}

func (s *Shared) putH(key, val []byte, h uint64) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.meta.lockWriter(); err != nil {
		return false, err
	}
	defer s.meta.unlockWriter()
	if err := s.checkRemap(); err != nil {
		return false, err
	}
	return s.insertLocked(key, val, h)
}

// insertLocked performs one insert/update. The caller must hold writeMu, the
// cross-process flock, and have already checkRemap'd.
func (s *Shared) insertLocked(key, val []byte, h uint64) (bool, error) {
	for {
		t := s.cur.Load()
		slot, isNew, full := t.insert(key, val, h)
		if full {
			if err := s.resize(t, t.capacity*2); err != nil && s.cur.Load() == t {
				// Same reasoning as Map.insertLockedH's full branch: resize
				// (via buildResized) can return a non-nil err after already
				// publishing a larger table (defect D2's syncDir-failure
				// case). s.cur.Load() == t means that did not happen: the
				// table genuinely did not change, so the error is real.
				return false, err
			}
			continue
		}
		if !isNew {
			t.storeValue(slot, val)
			return false, nil
		}
		// Same reasoning as Map.insertLockedH: the insert above already
		// succeeded, so a follow-up maintenance resize's failure is not the
		// insert's own failure and must not be reported as one (defect R5) —
		// recorded for MaintenanceErr instead of discarded outright.
		if t.loadCount()*loadDen > t.capacity*loadNum {
			s.maintErr.Store(&maintErrBox{s.resize(t, t.capacity*2)})
		} else if (t.loadCount()+t.loadTomb())*loadDen > t.capacity*loadNum {
			s.maintErr.Store(&maintErrBox{s.resize(t, t.capacity)})
		}
		return true, nil
	}
}

// Batch acquires the in-process and cross-process writer locks once and runs fn,
// inserting via the supplied put closure. This amortizes the per-Put flock and
// remap check across a bulk load. Pair with Reserve to avoid resizes.
func (s *Shared) Batch(fn func(put func(key, val []byte) (bool, error)) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.meta.lockWriter(); err != nil {
		return err
	}
	defer s.meta.unlockWriter()
	if err := s.checkRemap(); err != nil {
		return err
	}
	return fn(func(key, val []byte) (bool, error) {
		if len(key) != s.keyLen || len(val) != s.valLen {
			return false, ErrBadSize
		}
		return s.insertLocked(key, val, sum(key))
	})
}

// batchH is Batch whose put closure takes a caller-supplied hash.
func (s *Shared) batchH(fn func(put func(key, val []byte, h uint64) (bool, error)) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.meta.lockWriter(); err != nil {
		return err
	}
	defer s.meta.unlockWriter()
	if err := s.checkRemap(); err != nil {
		return err
	}
	return fn(func(key, val []byte, h uint64) (bool, error) {
		return s.insertLocked(key, val, h)
	})
}

// deleteBatchH is batchH's delete counterpart: it takes the in-process writer
// lock and the cross-process write flock once, remaps if a peer grew the file,
// then erases each key under that single critical section. del reports whether
// the key existed.
func (s *Shared) deleteBatchH(fn func(del func(key []byte, h uint64) bool) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.meta.lockWriter(); err != nil {
		return err
	}
	defer s.meta.unlockWriter()
	if err := s.checkRemap(); err != nil {
		return err
	}
	return fn(func(key []byte, h uint64) bool {
		return s.cur.Load().erase(key, h)
	})
}

// Reserve grows the table up front to hold at least n entries without a resize.
func (s *Shared) Reserve(n int) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.meta.lockWriter(); err != nil {
		return err
	}
	defer s.meta.unlockWriter()
	if err := s.checkRemap(); err != nil {
		return err
	}
	need := capacityFor(n)
	t := s.cur.Load()
	if need <= t.capacity {
		return nil
	}
	return s.resize(t, need)
}

func (s *Shared) Delete(key []byte) bool {
	if len(key) != s.keyLen {
		return false
	}
	h := sum(key)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.meta.lockWriter() != nil {
		return false
	}
	defer s.meta.unlockWriter()
	if s.checkRemap() != nil {
		return false // a tombstone in an abandoned inode is a delete nobody sees
	}
	return s.cur.Load().erase(key, h)
}

// Prune is Map.Prune for the cross-process backend: it takes the in-process
// writer lock and the cross-process write flock once, remaps if a peer grew the
// file, then sweeps the inode under that single critical section. It never
// inserts, so no resize and no generation bump — the tombstones it writes are
// published with the same atomic release Delete uses, so peers observe them. key
// and val alias the mapped inode and are valid only for the duration of the aged
// call. Returns the number of entries removed (0 if the flock cannot be taken, or
// the resized inode cannot be adopted — see RemapErr).
func (s *Shared) Prune(aged func(key, val []byte) bool) int {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.meta.lockWriter() != nil {
		return 0
	}
	defer s.meta.unlockWriter()
	if s.checkRemap() != nil {
		return 0
	}
	return s.cur.Load().prune(aged)
}

// Shrink is Map.Shrink for the cross-process backend: it rehashes the inode down
// to fit the live count, bumping the generation so peers remap onto the smaller
// file. See Map.Shrink for when to use it. Holds both writer locks.
func (s *Shared) Shrink() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.meta.lockWriter(); err != nil {
		return err
	}
	defer s.meta.unlockWriter()
	if err := s.checkRemap(); err != nil {
		return err
	}
	t := s.cur.Load()
	// See Map.Shrink: size from occupied slots, not the header counter, which a
	// crash between publication and increment leaves permanently low.
	need := capacityFor(t.liveCount())
	if need >= t.capacity {
		return nil
	}
	return s.resize(t, need)
}

// resize replaces the data inode and bumps the generation so other processes
// remap. Must hold the writer lock.
func (s *Shared) resize(old *table, newCap uint64) error {
	nt, err := buildResized(s.path, s.keyLen, s.valLen, old, newCap, s.cfg.doSync())
	if nt != nil {
		// Same reasoning as Map.rehash: buildResized returns a non-nil nt
		// alongside a non-nil err only when the rename already committed and
		// just the directory fsync failed. nt is live and correct; discarding
		// it here would both lose the resize (old's inode is no longer named
		// by path — defect D2) and leak nt's mmap, since nothing would ever
		// close it. Publish it and bump the generation regardless of err.
		s.remapMu.Lock()
		s.cur.Store(nt)
		s.retired = append(s.retired, old.rw)
		s.remapMu.Unlock()
		g := s.meta.bumpGen()
		s.localGen.Store(g)
	}
	return err
}
