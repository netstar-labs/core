package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/internal/xxh3"
	"github.com/netstar-labs/core/internal/xxhash"
	"github.com/netstar-labs/core/patch"
)

// Key/value geometry. Item is hashed into the fixed key slot by width — SHA-256
// for a 32-byte key, XXH3-128 for a 16-byte key, xxHash64 for an 8-byte key — and Value is a little-endian
// uint64. The values are package-level (not const) so the geometry can adopt an
// existing store's header or the -keylen/-vallen flags; they are set once by
// setGeometry before the server starts serving and only read by handlers after.
var (
	keyLen     = 32
	valLen     = 8
	recordSize = keyLen + valLen
)

// setGeometry applies the chosen key/value lengths to the package-level geometry.
func setGeometry(klen, vlen int) {
	keyLen, valLen = klen, vlen
	recordSize = keyLen + valLen
}

// setGeometryFromStore adopts an existing store's on-disk geometry when its first
// shard (<path>.000) is present — the header is authoritative — and otherwise
// falls back to the -keylen/-vallen flag values for a new store.
func setGeometryFromStore(path string, klen, vlen int) {
	if k, v, err := core.Geometry(path + ".000"); err == nil {
		setGeometry(k, v)
		return
	}
	setGeometry(klen, vlen)
}

// detectShards counts the consecutive shard files <path>.000, .001, … present on
// disk. Returns 0 when none exist (a new store). The header records geometry but
// not the shard count, so an existing store's shard count must be read from the
// files — opening it with a different count creates empty shards and misroutes
// every key.
func detectShards(path string) int {
	n := 0
	for {
		if _, err := os.Stat(fmt.Sprintf("%s.%03d", path, n)); err != nil {
			return n
		}
		n++
	}
}

// Worker owns the core store and translates Jobs to key/value operations. The
// store gives lock-free reads and concurrent (sharded) writes, so the HTTP
// handlers need no locking of their own. log is the optional WAL-backed patch
// buffer (nil unless -patch is set): POST /patch feeds it and a background loop
// folds it into the store (see main).
type Worker struct {
	store *core.Sharded
	log   *patch.Log
}

// NewWorker opens (or creates) a sharded store at path. For an existing store the
// geometry (and the key hash it selects — SHA-256 at 32 bytes, XXH3-128 at 16, xxHash64 at 8) and
// the shard count are both adopted from disk, so the -shards/-keylen/-vallen flags
// apply only when creating a new store; opening an existing one with a mismatched
// shard count would create empty shards and misroute every key. reserve > 0
// pre-sizes the store for that many entries (one resize per shard up front,
// avoiding mid-load regrowth); noSync skips per-write msync for bulk-load
// throughput, relying on the graceful-shutdown Sync in Close.
func NewWorker(path string, shards, capPerShard, reserve, klen, vlen int, noSync bool) (*Worker, error) {
	setGeometryFromStore(path, klen, vlen)
	if n := detectShards(path); n > 0 {
		shards = n // an existing store's on-disk shard count is authoritative
	}
	var opts []core.Option
	if noSync {
		opts = append(opts, core.WithNoSync())
	}
	s, err := core.OpenSharded(path, keyLen, valLen, shards, capPerShard, opts...)
	if err != nil {
		return nil, err
	}
	if reserve > 0 {
		if err := s.Reserve(reserve); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return &Worker{store: s}, nil
}

// Close flushes and releases the store.
func (w *Worker) Close() error {
	_ = w.store.Sync()
	return w.store.Close()
}

// scope is the subset of operations export/digest needs; satisfied by both
// *core.Sharded (the whole store) and a single core.Store (one shard, via
// store.Shard(i)).
type scope interface {
	ForEach(fn func(k, v []byte) bool)
	Len() int
	Digest() uint64
}

// shardPutBatch / shardPutRecords insert directly into a single shard with no
// routing — used when a request targets one shard via ?shard=N. The records must
// belong to that shard, which holds when they were exported from a store with
// the same shard count.
func shardPutBatch(sh core.Store, keys, vals [][]byte) (int, error) {
	inserted := 0
	err := sh.Batch(func(put func(k, v []byte) (bool, error)) error {
		for i := range keys {
			isNew, e := put(keys[i], vals[i])
			if e != nil {
				return e
			}
			if isNew {
				inserted++
			}
		}
		return nil
	})
	return inserted, err
}

func shardPutRecords(sh core.Store, buf []byte) (int, error) {
	inserted := 0
	err := sh.Batch(func(put func(k, v []byte) (bool, error)) error {
		for off := 0; off+recordSize <= len(buf); off += recordSize {
			isNew, e := put(buf[off:off+keyLen], buf[off+keyLen:off+recordSize])
			if e != nil {
				return e
			}
			if isNew {
				inserted++
			}
		}
		return nil
	})
	return inserted, err
}

// makeKey writes the fixed-size key for item into dst, choosing the hash by key
// width to match the store geometry: a 32-byte key takes the SHA-256 digest, a
// 16-byte key the XXH3-128 digest (canonical big-endian), an 8-byte key the
// xxHash64 digest (little-endian) — the same derivation the
// cmd/core CLI uses, so a store is interchangeable between them. An item of exactly
// 2*keyLen hex chars (e.g. a key emitted by JSON export) is decoded back into its
// raw bytes so exports re-ingest losslessly; the cost of that symmetry is that an
// item which is itself 2*keyLen hex chars is treated as a pre-hashed key. Any other
// width falls back to a SHA-256 digest truncated/zero-padded to fit.
func makeKey(dst []byte, item string) {
	if len(item) == 2*len(dst) {
		if tmp, err := hex.DecodeString(item); err == nil {
			copy(dst, tmp)
			return
		}
	}
	switch len(dst) {
	case 8:
		binary.LittleEndian.PutUint64(dst, xxhash.Sum([]byte(item)))
	case 16:
		sum := xxh3.HashString128(item).Bytes()
		copy(dst, sum[:])
	case 32:
		sum := sha256.Sum256([]byte(item))
		copy(dst, sum[:])
	default:
		for i := range dst {
			dst[i] = 0
		}
		sum := sha256.Sum256([]byte(item))
		copy(dst, sum[:]) // first min(len(dst), 32) digest bytes
	}
}

// Put stores job.Item -> job.Value and reports whether the entry was newly
// created (vs. an update of an existing item).
func (w *Worker) Put(job Job) (created bool, err error) {
	key := make([]byte, keyLen)
	makeKey(key, job.Item)
	val := make([]byte, valLen)
	binary.LittleEndian.PutUint64(val, job.Value)
	return w.store.Put(key, val)
}

// PutBatch stores many jobs in one call, using the store's sharded bulk path
// (bucketed by shard, inserted concurrently). Returns the number of new entries.
func (w *Worker) PutBatch(jobs []Jobz) (inserted int, err error) {
	keys := make([][]byte, len(jobs))
	vals := make([][]byte, len(jobs))
	for i, job := range jobs {
		k := make([]byte, keyLen)
		makeKey(k, job.Item)
		v := make([]byte, valLen)
		binary.LittleEndian.PutUint64(v, job.Value)
		keys[i], vals[i] = k, v
	}
	return w.store.PutBatch(keys, vals)
}

// StreamJSON ingests a stream of JSON Jobs (newline- or whitespace-delimited)
// without buffering the whole payload, flushing to the store every batchSize
// entries. Returns counts of decoded, stored (non-empty item), and newly
// inserted entries.
func (w *Worker) StreamJSON(r io.Reader, batchSize int, putBatch func(keys, vals [][]byte) (int, error)) (received, stored, inserted int, err error) {
	dec := json.NewDecoder(r)
	keys := make([][]byte, 0, batchSize)
	vals := make([][]byte, 0, batchSize)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		ins, e := putBatch(keys, vals)
		inserted += ins
		keys, vals = keys[:0], vals[:0]
		return e
	}
	for {
		var job Jobz
		switch e := dec.Decode(&job); {
		case e == io.EOF:
			err = flush()
			return
		case e != nil:
			err = e
			_ = flush() // persist what we parsed before the error
			return
		}
		received++
		if job.Item == "" {
			continue
		}
		stored++
		k := make([]byte, keyLen)
		makeKey(k, job.Item)
		v := make([]byte, valLen)
		binary.LittleEndian.PutUint64(v, job.Value)
		keys, vals = append(keys, k), append(vals, v)
		if len(keys) >= batchSize {
			if err = flush(); err != nil {
				return
			}
		}
	}
}

// StreamBinary ingests packed fixed-size records (keyLen-byte key followed by an
// 8-byte little-endian value) straight from r via the store's binary bulk path,
// reading chunkRecords at a time. The key is the opaque keyLen-byte digest, so
// binary ingest/export round-trips keys losslessly. Returns the record and
// new-insert counts.
func (w *Worker) StreamBinary(r io.Reader, chunkRecords int, putRecords func(buf []byte) (int, error)) (records, inserted int, err error) {
	rec := keyLen + valLen
	buf := make([]byte, chunkRecords*rec)
	carry := 0 // bytes of a partial record kept at the front of buf
	for {
		n, rerr := io.ReadFull(r, buf[carry:])
		total := carry + n
		full := total - total%rec
		if full > 0 {
			ins, perr := putRecords(buf[:full])
			if perr != nil {
				return records, inserted, perr
			}
			records += full / rec
			inserted += ins
		}
		carry = total - full
		copy(buf, buf[full:total]) // retain the partial record for the next read
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return records, inserted, rerr
		}
	}
	if carry != 0 {
		return records, inserted, fmt.Errorf("trailing %d bytes are not a whole %d-byte record", carry, rec)
	}
	return records, inserted, nil
}

// StreamPatch decodes a stream of Change records (newline/whitespace-delimited
// JSON) into the patch buffer, hashing each Item into the key slot. Upserts and
// deletes are coalesced by the Log and folded into the store by the flush loop,
// so they become visible to reads after the next flush. Returns the counts of
// records received, upserts, and deletes appended.
func (w *Worker) StreamPatch(r io.Reader) (received, upserts, deletes int, err error) {
	dec := json.NewDecoder(r)
	key := make([]byte, keyLen)
	val := make([]byte, valLen)
	for {
		var c Change
		switch e := dec.Decode(&c); {
		case e == io.EOF:
			return received, upserts, deletes, nil
		case e != nil:
			return received, upserts, deletes, e
		}
		received++
		if c.Item == "" {
			continue
		}
		makeKey(key, c.Item)
		if c.Op == "delete" {
			if err = w.log.Delete(key); err != nil {
				return received, upserts, deletes, err
			}
			deletes++
			continue
		}
		binary.LittleEndian.PutUint64(val, c.Value)
		if err = w.log.Upsert(key, val); err != nil {
			return received, upserts, deletes, err
		}
		upserts++
	}
}

// Get looks up item and returns a Job with Value/Found populated.
func (w *Worker) Get(item string) Job {
	key := make([]byte, keyLen)
	makeKey(key, item)
	val := make([]byte, valLen)
	if w.store.GetInto(key, val) {
		return Job{Item: item, Value: binary.LittleEndian.Uint64(val), Found: true}
	}
	return Job{Item: item, Found: false}
}

// ExportBinary streams entries to out as packed fixed-size records (keyLen-byte
// key + 8-byte little-endian value) — symmetric with the binary ingest path. The
// key is the raw keyLen-byte digest, so this is the lossless round-trip path. If
// limit >= 0 it writes at most limit records (so a caller that declared a
// Content-Length never overruns it); limit < 0 means unbounded. Returns the
// number of records written.
func (w *Worker) ExportBinary(out io.Writer, limit int, sc scope) (int, error) {
	bw := bufio.NewWriterSize(out, 1<<16)
	rec := make([]byte, recordSize)
	n := 0
	var werr error
	sc.ForEach(func(k, v []byte) bool {
		if limit >= 0 && n >= limit {
			return false // stop at the declared length
		}
		copy(rec[:keyLen], k)
		copy(rec[keyLen:], v)
		if _, werr = bw.Write(rec); werr != nil {
			return false
		}
		n++
		return true
	})
	if werr != nil {
		return n, werr
	}
	return n, bw.Flush()
}

// ExportJSON streams every entry to out as newline-delimited JSON Jobs, with each
// Item carrying the key's keyLen-byte digest as 2*keyLen-char hex (the original
// text is not recoverable from the hash). It re-ingests via the JSON stream path,
// which decodes a hex Item straight back into the key. Returns the number of jobs.
func (w *Worker) ExportJSON(out io.Writer, sc scope) (int, error) {
	bw := bufio.NewWriterSize(out, 1<<16)
	enc := json.NewEncoder(bw) // Encode writes a trailing newline ⇒ NDJSON
	n := 0
	var werr error
	sc.ForEach(func(k, v []byte) bool {
		job := Jobz{
			Item:  hex.EncodeToString(k), // key is an opaque keyLen-byte digest; emit hex
			Value: binary.LittleEndian.Uint64(v),
		}
		if werr = enc.Encode(&job); werr != nil {
			return false
		}
		n++
		return true
	})
	if werr != nil {
		return n, werr
	}
	return n, bw.Flush()
}

// Len reports the number of stored entries.
func (w *Worker) Len() int { return w.store.Len() }

// Shards reports the number of shards the store was opened with (read from disk
// for an existing store).
func (w *Worker) Shards() int { return w.store.Shards() }
