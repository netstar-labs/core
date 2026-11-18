// Package patch builds and applies coalesced change sets — upserts (key|value)
// and deletes (key-only) — against a core sharded store.
//
// A patch is the difference you want to fold into an existing store: new keys,
// updated values, and removals. Because the store's insert is already an upsert
// (a repeated key overwrites its value), "insert" and "update" are the same
// record stream; only deletion is structurally distinct (key-only, and it leaves
// a tombstone). So a patch carries two streams — upserts and deletes — bound by
// the geometry they target.
//
// A Builder coalesces a stream of changes so the last operation seen for a key
// wins; the resulting Snapshot is therefore disjoint (a key is in exactly one of
// the two streams). That makes Apply order-independent and idempotent: applying
// the same snapshot twice, or re-applying one that a crash left half-applied,
// converges to the same store — the property the live accumulation layer relies
// on for crash recovery.
//
// This file is the in-RAM artifact layer (Builder, Snapshot, Apply). Transport
// (partition + bundle) lives in bundle.go; durable live accumulation (WAL +
// flush) lives in log.go.
package patch

import "github.com/netstar-labs/core"

// Op is the kind of a change.
type Op uint8

const (
	OpUpsert Op = iota // insert or update: carries a value
	OpDelete           // remove: key only
)

// Builder coalesces a stream of changes into a disjoint set: the last operation
// recorded for a key wins, so a key that is upserted then deleted ends up only in
// the delete stream (and vice-versa), and repeated upserts collapse to the final
// value. It is the shared coalescing core of both the CLI patch producer and the
// live accumulation Log. A Builder is not safe for concurrent use.
type Builder struct {
	keyLen, valLen int
	m              map[string]entry
}

type entry struct {
	op  Op
	val []byte // upsert value; nil for a delete
}

// NewBuilder returns a Builder for a store of the given geometry.
func NewBuilder(keyLen, valLen int) *Builder {
	return &Builder{keyLen: keyLen, valLen: valLen, m: make(map[string]entry)}
}

// Upsert records that key should be set to val, superseding any prior change to
// key. key/val must match the builder's geometry.
func (b *Builder) Upsert(key, val []byte) error {
	if len(key) != b.keyLen || len(val) != b.valLen {
		return core.ErrBadSize
	}
	b.m[string(key)] = entry{op: OpUpsert, val: append([]byte(nil), val...)}
	return nil
}

// Delete records that key should be removed, superseding any prior change to key.
func (b *Builder) Delete(key []byte) error {
	if len(key) != b.keyLen {
		return core.ErrBadSize
	}
	b.m[string(key)] = entry{op: OpDelete}
	return nil
}

// Len reports the number of distinct keys accumulated so far.
func (b *Builder) Len() int { return len(b.m) }

// Reset clears the builder for reuse (e.g. the next accumulation window).
func (b *Builder) Reset() { b.m = make(map[string]entry) }

// Snapshot materializes the coalesced changes into packed buffers. baseDigest is
// the content digest of the store the patch targets (recorded for verification on
// apply), or "" if not computed.
func (b *Builder) Snapshot(baseDigest string) *Snapshot {
	return snapshotFromMap(b.keyLen, b.valLen, b.m, baseDigest)
}

// snapshotFromMap packs a coalesced key→entry map into a Snapshot. Shared by the
// Builder and the live Log so both produce the same disjoint upsert/delete split.
func snapshotFromMap(keyLen, valLen int, m map[string]entry, baseDigest string) *Snapshot {
	s := &Snapshot{KeyLen: keyLen, ValLen: valLen, BaseDigest: baseDigest}
	for k, e := range m {
		if e.op == OpDelete {
			s.Deletes = append(s.Deletes, k...)
			continue
		}
		s.Upserts = append(s.Upserts, k...)
		s.Upserts = append(s.Upserts, e.val...)
	}
	return s
}

// Snapshot is a coalesced, disjoint change set: packed upsert records (keyLen
// bytes of key followed by valLen bytes of value) and packed delete keys (keyLen
// bytes each). No key appears in both streams.
type Snapshot struct {
	KeyLen, ValLen int
	BaseDigest     string // digest of the target store when built, "" if unknown
	Upserts        []byte // packed key|value records
	Deletes        []byte // packed key records
}

// NumUpserts returns the count of upsert records.
func (s *Snapshot) NumUpserts() int {
	rec := s.KeyLen + s.ValLen
	if rec == 0 {
		return 0
	}
	return len(s.Upserts) / rec
}

// NumDeletes returns the count of delete keys.
func (s *Snapshot) NumDeletes() int {
	if s.KeyLen == 0 {
		return 0
	}
	return len(s.Deletes) / s.KeyLen
}

// Stats reports what Apply did.
type Stats struct {
	Upserts  int // upsert records applied
	Inserted int // of those, newly created (the rest were updates of existing keys)
	Deletes  int // delete keys applied
	Removed  int // of those, keys that existed and were tombstoned (the rest were absent)
}

// Apply folds a snapshot into dst in place. Because the snapshot is disjoint, the
// upsert and delete passes are order-independent and the operation is idempotent:
// re-applying the same snapshot (or one a crash left half-applied) converges to
// the same store. Apply does not verify BaseDigest or Sync dst — the caller owns
// that policy (see bundle.go's IngestBundle and the Log's Flush).
func Apply(dst *core.Sharded, s *Snapshot) (Stats, error) {
	st := Stats{Upserts: s.NumUpserts(), Deletes: s.NumDeletes()}
	ins, err := dst.PutRecords(s.Upserts)
	if err != nil {
		return st, err
	}
	st.Inserted = ins
	rem, err := dst.DeleteRecords(s.Deletes)
	if err != nil {
		return st, err
	}
	st.Removed = rem
	return st, nil
}
