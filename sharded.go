package core

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

// Store is the common surface of a single hash table, implemented by both *Map
// (single-process) and *Shared (cross-process). Sharded is generic over it, so a
// sharded set can be built from either backend.
type Store interface {
	Get(key []byte) ([]byte, bool)
	GetInto(key, dst []byte) bool
	Contains(key []byte) bool
	Put(key, val []byte) (bool, error)
	Update(key, val []byte) bool
	Delete(key []byte) bool
	Prune(aged func(key, val []byte) bool) int
	Shrink() error
	ForEach(fn func(key, val []byte) bool)
	Digest() uint64
	Batch(fn func(put func(key, val []byte) (bool, error)) error) error
	Reserve(n int) error
	Len() int
	Sync() error
	Close() error
}

var (
	_ Store = (*Map)(nil)
	_ Store = (*Shared)(nil)
)

// hashStore is the hash-aware fast path implemented by *Map and *Shared. It lets
// Sharded hash a key exactly once and reuse that hash for both shard routing and
// the in-shard operation, instead of hashing twice (once to route, once inside
// the shard).
type hashStore interface {
	getIntoH(key, dst []byte, h uint64) bool
	containsH(key []byte, h uint64) bool
	updateH(key, val []byte, h uint64) bool
	putH(key, val []byte, h uint64) (bool, error)
	batchH(fn func(put func(key, val []byte, h uint64) (bool, error)) error) error
	deleteBatchH(fn func(del func(key []byte, h uint64) bool) error) error
}

var (
	_ hashStore = (*Map)(nil)
	_ hashStore = (*Shared)(nil)
)

// Sharded fans keys across N independent Stores by the high bits of the hash.
// Each shard has its own writer lock and its own file, so writes to different
// shards proceed concurrently and a resize only rewrites the one shard that
// overflowed — bounding the stop-the-world-for-writers window to 1/N of the data.
type Sharded struct {
	shards    []Store
	hstores   []hashStore // same objects as shards, for the single-hash fast path
	shardBits uint
	keyLen    int
	valLen    int
}

// OpenSharded opens an N-shard set of single-process Maps at "<path>.000" …
// shards must be a power of two. capPerShard is the initial capacity of each.
func OpenSharded(path string, keyLen, valLen, shards, capPerShard int, opts ...Option) (*Sharded, error) {
	return openShardedWith(path, keyLen, valLen, shards, func(p string) (Store, error) {
		return Open(p, keyLen, valLen, capPerShard, opts...)
	})
}

// OpenSharedSharded is the cross-process variant: each shard is a *Shared table,
// so multiple processes can open the same set and share readers + a single
// writer per shard.
func OpenSharedSharded(path string, keyLen, valLen, shards, capPerShard int, opts ...Option) (*Sharded, error) {
	return openShardedWith(path, keyLen, valLen, shards, func(p string) (Store, error) {
		return OpenShared(p, keyLen, valLen, capPerShard, opts...)
	})
}

func openShardedWith(path string, keyLen, valLen, shards int, open func(string) (Store, error)) (*Sharded, error) {
	if shards <= 0 || shards&(shards-1) != 0 {
		return nil, fmt.Errorf("core: shards must be a power of two, got %d", shards)
	}
	if keyLen <= 0 || keyLen%8 != 0 || valLen < 0 || valLen%8 != 0 { // valLen 0 ⇒ key-only/set
		return nil, ErrBadSize
	}
	s := &Sharded{
		shards:    make([]Store, shards),
		hstores:   make([]hashStore, shards),
		shardBits: uint(bits.TrailingZeros(uint(shards))),
		keyLen:    keyLen,
		valLen:    valLen,
	}
	for i := 0; i < shards; i++ {
		st, err := open(fmt.Sprintf("%s.%03d", path, i))
		if err != nil {
			for j := 0; j < i; j++ {
				s.shards[j].Close()
			}
			return nil, err
		}
		s.shards[i] = st
		s.hstores[i] = st.(hashStore)
	}
	return s, nil
}

// route returns the shard index for a hash. Using the high bits keeps the low 7
// bits (the control tag H2) and the mid bits (the in-shard group index)
// full-entropy within a shard, so per-shard distribution stays uniform.
func (s *Sharded) route(h uint64) int {
	if s.shardBits == 0 {
		return 0
	}
	return int(h >> (64 - s.shardBits))
}

func (s *Sharded) shardIndex(key []byte) int { return s.route(sum(key)) }

// All routed operations hash the key once and reuse it for both routing and the
// in-shard call (via the hashStore fast path).

func (s *Sharded) Get(key []byte) ([]byte, bool) {
	val := make([]byte, s.valLen)
	if s.GetInto(key, val) {
		return val, true
	}
	return nil, false
}

func (s *Sharded) GetInto(key, dst []byte) bool {
	h := sum(key)
	return s.hstores[s.route(h)].getIntoH(key, dst, h)
}

func (s *Sharded) Contains(key []byte) bool {
	h := sum(key)
	return s.hstores[s.route(h)].containsH(key, h)
}

func (s *Sharded) Update(key, val []byte) bool {
	if len(key) != s.keyLen || len(val) != s.valLen {
		return false
	}
	h := sum(key)
	return s.hstores[s.route(h)].updateH(key, val, h)
}

func (s *Sharded) Put(key, val []byte) (bool, error) {
	if len(key) != s.keyLen || len(val) != s.valLen {
		return false, ErrBadSize
	}
	h := sum(key)
	return s.hstores[s.route(h)].putH(key, val, h)
}

func (s *Sharded) Delete(key []byte) bool { return s.shards[s.shardIndex(key)].Delete(key) }

// Prune sweeps every live entry across all shards in file order and tombstones
// each one for which aged returns true — a maintenance pass for aging out stale
// entries by a timestamp or flag packed in the value. Each shard is swept under
// its own writer lock and released before the next, so reads stay lock-free and
// only the shard being swept blocks writers (1/N of the keyspace at a time). key
// and val alias the mmap and are valid only for the duration of the aged call —
// copy to retain. Tombstoned slots are reused by later Puts and shed by the next
// per-shard rehash, so a steady-state churn set neither grows nor needs an
// explicit compaction. See PruneBefore / PruneFlag for the common predicates.
// Returns the total number of entries removed.
func (s *Sharded) Prune(aged func(key, val []byte) bool) int {
	total := 0
	for _, sh := range s.shards {
		total += sh.Prune(aged)
	}
	return total
}

// Pruner is the maintenance surface shared by *Map, *Shared, and *Sharded: a
// single Prune sweep that tombstones entries a predicate selects. The PruneBefore
// and PruneFlag helpers build the two common predicates over it, so a caller need
// not decode value words by hand.
type Pruner interface {
	Prune(aged func(key, val []byte) bool) int
}

var (
	_ Pruner = (*Map)(nil)
	_ Pruner = (*Shared)(nil)
	_ Pruner = (*Sharded)(nil)
)

// PruneBefore tombstones every entry whose little-endian uint64 at byte offset
// off in the value is < cutoff — the "age out everything older than" pass. The
// word need only be a monotonic date (bigger == newer); a day count since the
// Unix epoch (time.Now().Unix()/86400) is the natural choice, since then a
// retention window is plain subtraction — cutoff = todayDays - keepDays. off must
// satisfy off+8 <= valLen. Returns the number of entries removed.
func PruneBefore(p Pruner, off int, cutoff uint64) int {
	return p.Prune(func(_, val []byte) bool {
		return binary.LittleEndian.Uint64(val[off:off+8]) < cutoff
	})
}

// PruneFlag tombstones every entry where any masked bit of the little-endian
// uint64 at byte offset off in the value is set — aging out by a flag (or set of
// flags) packed in the value. mask is OR'd bits to test (e.g. 1<<3 for one flag).
// off must satisfy off+8 <= valLen. Returns the number of entries removed.
func PruneFlag(p Pruner, off int, mask uint64) int {
	return p.Prune(func(_, val []byte) bool {
		return binary.LittleEndian.Uint64(val[off:off+8])&mask != 0
	})
}

// Shrink rehashes each shard down to fit its live count, reclaiming disk and
// page cache after a mass age-out across the store. Each shard shrinks under its
// own writer lock. See Map.Shrink for when this is worthwhile (rarely — only
// after a permanent drop in population). Returns the first shard error, if any.
func (s *Sharded) Shrink() error {
	for _, sh := range s.shards {
		if err := sh.Shrink(); err != nil {
			return err
		}
	}
	return nil
}

// Digest returns the order-independent content digest of the whole store: the
// sum (mod 2^64) of the per-shard digests. Equal digests imply equal contents.
func (s *Sharded) Digest() uint64 {
	var d uint64
	for _, sh := range s.shards {
		d += sh.Digest()
	}
	return d
}

// ForEach calls fn for each live entry across all shards. key and val are valid
// only for the duration of the call. Returning false stops iteration.
func (s *Sharded) ForEach(fn func(key, val []byte) bool) {
	for _, sh := range s.shards {
		stopped := false
		sh.ForEach(func(k, v []byte) bool {
			if fn(k, v) {
				return true
			}
			stopped = true
			return false
		})
		if stopped {
			return
		}
	}
}

// Shards returns the number of shards.
func (s *Sharded) Shards() int { return len(s.shards) }

// Shard returns the i-th shard as a Store, for per-shard export/ingest (e.g.
// parallel client-side transfers, where each shard is a separate file and a
// separate writer lock). i must be in [0, Shards()).
func (s *Sharded) Shard(i int) Store { return s.shards[i] }

// Reserve pre-sizes every shard to hold its expected share of n entries.
func (s *Sharded) Reserve(n int) error {
	per := n/len(s.shards) + 1
	for _, sh := range s.shards {
		if err := sh.Reserve(per); err != nil {
			return err
		}
	}
	return nil
}

// PutRecords bulk-inserts a packed buffer of fixed-size records — keyLen bytes
// of key immediately followed by valLen bytes of value (the store's sizes) — and
// is the fast binary ingest path: keys and values are sliced directly out of buf
// with no per-record allocation, the put closure copies them into the slots.
// Records are hashed and bucketed by shard in parallel (one worker per CPU),
// then inserted with one goroutine per shard, since shards are independent.
// len(buf) must be a multiple of the record size. Process the input in chunks
// (e.g. a few hundred K records) to bound the per-shard index buffers. Returns
// the number of new entries.
func (s *Sharded) PutRecords(buf []byte) (int, error) {
	rec := s.keyLen + s.valLen
	if rec == 0 || len(buf)%rec != 0 {
		return 0, ErrBadSize
	}
	nrec := len(buf) / rec
	if nrec == 0 {
		return 0, nil
	}
	nsh := len(s.shards)

	// Phase 1 — parallel bucketing. Each worker hashes a contiguous record range
	// into its own per-shard buckets, so there is no shared state and no locking
	// on the hot hashing loop.
	workers := runtime.GOMAXPROCS(0)
	if workers > nrec {
		workers = nrec
	}
	local := make([][][]recRef, workers) // local[worker][shard] = []recRef
	var wg1 sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo, hi := w*nrec/workers, (w+1)*nrec/workers
		wg1.Add(1)
		go func(w, lo, hi int) {
			defer wg1.Done()
			b := make([][]recRef, nsh)
			for i := lo; i < hi; i++ {
				off := i * rec
				h := sum(buf[off : off+s.keyLen])
				si := s.route(h)
				b[si] = append(b[si], recRef{off: off, h: h})
			}
			local[w] = b
		}(w, lo, hi)
	}
	wg1.Wait()

	// Phase 2 — parallel insert, one goroutine per shard, draining every worker's
	// bucket for that shard (shards are fully independent).
	var inserted int64
	errs := make([]error, nsh)
	var wg2 sync.WaitGroup
	for si := 0; si < nsh; si++ {
		if shardEmpty(local, si) {
			continue
		}
		wg2.Add(1)
		go func(si int, hs hashStore, ep *error) {
			defer wg2.Done()
			ins := 0
			*ep = hs.batchH(func(put func(k, v []byte, h uint64) (bool, error)) error {
				for w := 0; w < workers; w++ {
					for _, r := range local[w][si] {
						isNew, e := put(buf[r.off:r.off+s.keyLen], buf[r.off+s.keyLen:r.off+rec], r.h)
						if e != nil {
							return e
						}
						if isNew {
							ins++
						}
					}
				}
				return nil
			})
			atomic.AddInt64(&inserted, int64(ins))
		}(si, s.hstores[si], &errs[si])
	}
	wg2.Wait()
	for _, e := range errs {
		if e != nil {
			return int(inserted), e
		}
	}
	return int(inserted), nil
}

func shardEmpty(local [][][]recRef, si int) bool {
	for w := range local {
		if len(local[w][si]) > 0 {
			return false
		}
	}
	return true
}

// recRef is a bucketed record: its offset in the buffer and its precomputed hash.
type recRef struct {
	off int
	h   uint64
}

// PutBatch bulk-inserts keys/vals, bucketing by shard and inserting into all
// shards concurrently, each under its writer lock taken exactly once. This is
// the fast bulk-load path: combined with Reserve and WithNoSync it avoids
// per-key locking, per-key flock (for the cross-process backend), and resizes.
// Returns the number of new entries.
func (s *Sharded) PutBatch(keys, vals [][]byte) (inserted int, err error) {
	if len(keys) != len(vals) {
		return 0, ErrBadSize
	}
	// Bucket indices by shard, carrying each key's hash.
	type idxRef struct {
		i int
		h uint64
	}
	buckets := make([][]idxRef, len(s.shards))
	for i, key := range keys {
		h := sum(key)
		buckets[s.route(h)] = append(buckets[s.route(h)], idxRef{i, h})
	}
	// Insert into all shards concurrently.
	var total int64
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for si, refs := range buckets {
		if len(refs) == 0 {
			continue
		}
		wg.Add(1)
		go func(hs hashStore, refs []idxRef, ep *error) {
			defer wg.Done()
			ins := 0
			*ep = hs.batchH(func(put func(k, v []byte, h uint64) (bool, error)) error {
				for _, r := range refs {
					isNew, e := put(keys[r.i], vals[r.i], r.h)
					if e != nil {
						return e
					}
					if isNew {
						ins++
					}
				}
				return nil
			})
			atomic.AddInt64(&total, int64(ins))
		}(s.hstores[si], refs, &errs[si])
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return int(total), e
		}
	}
	return int(total), nil
}

// DeleteBatch removes keys, bucketing by shard and deleting from all shards
// concurrently, each under its writer lock taken exactly once — the delete
// counterpart to PutBatch. A key that is absent is a no-op. Returns the number of
// keys that existed and were tombstoned.
func (s *Sharded) DeleteBatch(keys [][]byte) (deleted int, err error) {
	type idxRef struct {
		i int
		h uint64
	}
	buckets := make([][]idxRef, len(s.shards))
	for i, key := range keys {
		h := sum(key)
		buckets[s.route(h)] = append(buckets[s.route(h)], idxRef{i, h})
	}
	var total int64
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for si, refs := range buckets {
		if len(refs) == 0 {
			continue
		}
		wg.Add(1)
		go func(hs hashStore, refs []idxRef, ep *error) {
			defer wg.Done()
			n := 0
			*ep = hs.deleteBatchH(func(del func(key []byte, h uint64) bool) error {
				for _, r := range refs {
					if del(keys[r.i], r.h) {
						n++
					}
				}
				return nil
			})
			atomic.AddInt64(&total, int64(n))
		}(s.hstores[si], refs, &errs[si])
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return int(total), e
		}
	}
	return int(total), nil
}

// DeleteRecords removes a packed buffer of fixed-size keys — keyLen bytes each,
// no value — and is the delete-stream counterpart to PutRecords. Keys are hashed
// and bucketed by shard in parallel (one worker per CPU), then deleted with one
// goroutine per shard. len(buf) must be a multiple of keyLen. Returns the number
// of keys tombstoned (absent keys are skipped).
func (s *Sharded) DeleteRecords(buf []byte) (int, error) {
	k := s.keyLen
	if k == 0 || len(buf)%k != 0 {
		return 0, ErrBadSize
	}
	nrec := len(buf) / k
	if nrec == 0 {
		return 0, nil
	}
	nsh := len(s.shards)

	// Phase 1 — parallel bucketing (mirrors PutRecords: no shared state, no locks).
	workers := runtime.GOMAXPROCS(0)
	if workers > nrec {
		workers = nrec
	}
	local := make([][][]recRef, workers) // local[worker][shard] = []recRef
	var wg1 sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo, hi := w*nrec/workers, (w+1)*nrec/workers
		wg1.Add(1)
		go func(w, lo, hi int) {
			defer wg1.Done()
			b := make([][]recRef, nsh)
			for i := lo; i < hi; i++ {
				off := i * k
				h := sum(buf[off : off+k])
				si := s.route(h)
				b[si] = append(b[si], recRef{off: off, h: h})
			}
			local[w] = b
		}(w, lo, hi)
	}
	wg1.Wait()

	// Phase 2 — parallel delete, one goroutine per shard.
	var deleted int64
	errs := make([]error, nsh)
	var wg2 sync.WaitGroup
	for si := 0; si < nsh; si++ {
		if shardEmpty(local, si) {
			continue
		}
		wg2.Add(1)
		go func(si int, hs hashStore, ep *error) {
			defer wg2.Done()
			n := 0
			*ep = hs.deleteBatchH(func(del func(key []byte, h uint64) bool) error {
				for w := 0; w < workers; w++ {
					for _, r := range local[w][si] {
						if del(buf[r.off:r.off+k], r.h) {
							n++
						}
					}
				}
				return nil
			})
			atomic.AddInt64(&deleted, int64(n))
		}(si, s.hstores[si], &errs[si])
	}
	wg2.Wait()
	for _, e := range errs {
		if e != nil {
			return int(deleted), e
		}
	}
	return int(deleted), nil
}

// RemapErr reports the first shard unable to adopt its writer's resized inode
// (see Shared.RemapErr), or nil. Only the cross-process backend can be in that
// state; for Map shards it is always nil.
func (s *Sharded) RemapErr() error {
	for _, st := range s.shards {
		if r, ok := st.(interface{ RemapErr() error }); ok {
			if err := r.RemapErr(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Sharded) Len() int {
	n := 0
	for _, sh := range s.shards {
		n += sh.Len()
	}
	return n
}

func (s *Sharded) Sync() error {
	var err error
	for _, sh := range s.shards {
		if e := sh.Sync(); err == nil {
			err = e
		}
	}
	return err
}

func (s *Sharded) Close() error {
	var err error
	for _, sh := range s.shards {
		if e := sh.Close(); err == nil {
			err = e
		}
	}
	return err
}
