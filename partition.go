package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strings"

	"github.com/netstar-labs/core/internal/mmap"
)

// Out-of-RAM bulk loading. On a box where a shard's slots region is larger than
// RAM, inserting in hash order scatters writes across the whole region and the
// page cache thrashes. The fix is to stage the load: first bucket records by the
// high bits of their destination group index (Partitioner), then ingest the
// buckets in order (IngestParts), so each shard's writes stay confined to a
// 1/parts slice of its slots at a time — random writeback becomes a forward sweep.
//
// The geometry that makes this work — per-shard capacity, the group mask, the
// shard route — is owned here, not re-derived by callers, so the bucketing can
// never drift out of lockstep with where the table (table.go) actually places a
// key. The invariant: a store opened with the same (keyLen, valLen, shards) and
// Reserve(reserve) has exactly Layout.CapPerShard slots per shard, hence the same
// group mask the Partitioner buckets by.
//
// Per-shard capacity is the canonical currency throughout this file: NewPartitioner,
// ExportParts, and IngestParts all take a capPerShard. A reserve count is just one
// way to name that capacity — ReserveCapPerShard converts a human "expect N entries"
// into the capacity it implies, and the live store's header records the capacity
// directly (StoreCapPerShard). For a ship-whole export, StoreCapPerShard reads the
// actual capacity (max across shards) so the parts align to where keys already live
// and a rebuild pre-sizes to that exact capacity — no remembered reserve, no resize.
// The manifest's CapPerShard, when set, is authoritative over a legacy Reserve.

// ReserveCapPerShard is the per-shard slot capacity a Sharded.Reserve(reserve) over
// `shards` shards yields: split the reserve evenly (per = reserve/shards + 1), apply
// the 7/8 grow factor, then round up to a power of two. It is the bridge from a
// human reserve count to the canonical capacity, and the single source of truth the
// staged-ingest group math depends on — correct only while it agrees with
// Sharded.Reserve + Map.Reserve.
// A non-positive shards or reserve yields the minimum capacity rather than
// panicking or hanging: shards == 0 used to divide by zero, and a negative reserve
// used to spin roundCapacity forever.
func ReserveCapPerShard(reserve, shards int) uint64 {
	if shards <= 0 || reserve <= 0 {
		return minCapacity
	}
	return capacityFor(reserve/shards + 1)
}

// Layout is the on-disk geometry a sharded store ends up with for a given
// (keyLen, valLen, shards, reserve) — the sizing a "plan" step reports and the
// numbers IngestParts and Partitioner build on. It mirrors the result of opening
// the store and calling Reserve(reserve), without touching disk.
type Layout struct {
	KeyLen, ValLen, Shards int
	Reserve                int
	CapPerShard            uint64 // slots per shard (power of two)
	TotalCapacity          uint64 // CapPerShard * Shards
	ShardBytes             int64  // page-aligned on-disk size of one shard file
	StoreBytes             int64  // ShardBytes * Shards
}

// PlanLayout computes the Layout for the given geometry and reserve without
// creating anything. shards must be a power of two; keyLen and valLen must be
// multiples of 8 (valLen 0 = key-only set); reserve must be positive.
func PlanLayout(keyLen, valLen, shards, reserve int) (Layout, error) {
	if shards <= 0 || shards&(shards-1) != 0 {
		return Layout{}, fmt.Errorf("core: shards must be a power of two, got %d", shards)
	}
	if keyLen <= 0 || keyLen%8 != 0 || valLen < 0 || valLen%8 != 0 {
		return Layout{}, ErrBadSize
	}
	if reserve <= 0 {
		return Layout{}, fmt.Errorf("core: reserve must be positive, got %d", reserve)
	}
	capPerShard := ReserveCapPerShard(reserve, shards)
	_, _, _, _, fileSize := layout(keyLen, valLen, capPerShard)
	return Layout{
		KeyLen: keyLen, ValLen: valLen, Shards: shards, Reserve: reserve,
		CapPerShard:   capPerShard,
		TotalCapacity: capPerShard * uint64(shards),
		ShardBytes:    int64(fileSize),
		StoreBytes:    int64(fileSize) * int64(shards),
	}, nil
}

// StoreCapPerShard reads the actual per-shard slot capacity from each shard file's
// header ("<store>.000".."<store>.NNN") and returns the max and min across all
// shards, without mapping the stores. The capacity field is the live group mask
// (mask = capacity/8 - 1), so the max is the single uniform CapPerShard a
// geometry-independent export must bucket by: it is ≥ every shard's own mask, so a
// store reserved to it re-ingests every part without a resize. A max != min means
// the shards have diverged (a hot shard grew past the others); the caller can warn
// that the cold shards will be over-allocated to the hot shard's size. shards must
// match the store's shard count. Returns an error if any shard file is missing or
// its header is corrupt.
func StoreCapPerShard(store string, shards int) (max, min uint64, err error) {
	if shards <= 0 || shards&(shards-1) != 0 {
		return 0, 0, fmt.Errorf("core: shards must be a power of two, got %d", shards)
	}
	for i := 0; i < shards; i++ {
		h, e := ReadHeader(fmt.Sprintf("%s.%03d", store, i))
		if e != nil {
			return 0, 0, fmt.Errorf("core: reading shard %d header: %w", i, e)
		}
		if i == 0 || h.Capacity > max {
			max = h.Capacity
		}
		if i == 0 || h.Capacity < min {
			min = h.Capacity
		}
	}
	return max, min, nil
}

// Partitioner maps a key (or its precomputed hash) to one of Parts() output
// buckets, by the high bits of the in-shard group index g = (h>>7) & mask — the
// same g the table computes in table.go. The mask comes from the per-shard
// capacity for (shards, reserve), so the bucketing matches where a store opened
// with that geometry and Reserve(reserve) will place keys.
type Partitioner struct {
	mask  uint64
	shift uint
	parts int
}

// NewPartitioner builds a Partitioner for the staged load of a store, splitting it
// into `parts` buckets. capPerShard is the store's per-shard slot capacity — the
// header's capacity field (StoreCapPerShard), or ReserveCapPerShard(reserve, shards)
// when sizing from a reserve count. Using the store's actual capacity makes the
// bucketing mask identical to the live table's mask, so the parts align to where the
// store already placed keys. capPerShard must be a power of two and at least 8 (one
// group); parts must be a power of two not exceeding CapPerShard/8. It MUST match
// the value used at ingest, since it sizes the group mask.
func NewPartitioner(capPerShard uint64, parts int) (*Partitioner, error) {
	if parts < 1 || parts&(parts-1) != 0 {
		return nil, fmt.Errorf("core: parts must be a power of two, got %d", parts)
	}
	if capPerShard < 8 || capPerShard&(capPerShard-1) != 0 {
		return nil, fmt.Errorf("core: capPerShard must be a power of two ≥ 8, got %d", capPerShard)
	}
	groups := capPerShard >> 3 // 8 slots per group; mirrors table mask = cap/8 - 1
	gBits := uint(bits.TrailingZeros64(groups))
	partBits := uint(bits.TrailingZeros64(uint64(parts)))
	if partBits > gBits {
		return nil, fmt.Errorf("core: parts %d exceeds group count %d per shard", parts, groups)
	}
	return &Partitioner{mask: groups - 1, shift: gBits - partBits, parts: parts}, nil
}

// Parts returns the number of output buckets.
func (p *Partitioner) Parts() int { return p.parts }

// Part returns the bucket index for a precomputed key hash (the zero-copy path
// for packed records whose hash is already in hand).
func (p *Partitioner) Part(h uint64) int { return int(((h >> 7) & p.mask) >> p.shift) }

// PartKey returns the bucket index for a key, hashing it with the same function
// the store routes on, so the bucketing can't diverge from the store's placement.
func (p *Partitioner) PartKey(key []byte) int { return p.Part(sum(key)) }

// ManifestSuffix is appended to a part prefix to name its manifest sidecar:
// parts "<prefix>.pNNN" are described by "<prefix>.manifest.json".
const ManifestSuffix = ".manifest.json"

// ManifestPath returns the manifest sidecar path for a part prefix.
func ManifestPath(prefix string) string { return prefix + ManifestSuffix }

// PartsManifest makes a set of "<prefix>.pNNN" files self-describing: it records
// the geometry they were bucketed for, so a build can ingest them directly
// without re-running the partition step or re-specifying dimensions. It is the
// single source of the (keyLen, valLen, shards, reserve) that partition and
// ingest MUST agree on — read it instead of passing them again by hand.
type PartsManifest struct {
	Format      string `json:"format"`                // "core/parts"
	KeyLen      int    `json:"keyLen"`                // bytes of key per record
	ValLen      int    `json:"valLen"`                // bytes of value per record (0 = key-only set)
	RecordSize  int    `json:"recordSize"`            // KeyLen + ValLen
	Shards      int    `json:"shards"`                // shard count the parts were bucketed for
	CapPerShard uint64 `json:"capPerShard,omitempty"` // per-shard slot capacity the parts were bucketed by — the canonical mask geometry; ingest pre-sizes to it for a growth-free load. New exports always set this.
	Reserve     int    `json:"reserve,omitempty"`     // legacy: reserve count older exports recorded instead of CapPerShard; read via EffectiveCapPerShard, no longer written
	Parts       int    `json:"parts"`                 // number of part files
	Records     int    `json:"records"`               // total records across all parts
	Prefix      string `json:"prefix"`                // base name of the part files (informational)
	Counts      []int  `json:"counts,omitempty"`      // per-part record counts (len == Parts), when recorded
}

// WriteManifest writes m as indented JSON to path (typically ManifestPath(prefix)).
func WriteManifest(path string, m PartsManifest) error {
	if m.Format == "" {
		m.Format = "core/parts"
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ReadManifest reads and validates a parts manifest written by WriteManifest.
func ReadManifest(path string) (PartsManifest, error) {
	var m PartsManifest
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("core: parse parts manifest %s: %w", path, err)
	}
	if m.RecordSize == 0 {
		m.RecordSize = m.KeyLen + m.ValLen
	}
	return m, nil
}

// EffectiveCapPerShard returns the per-shard slot capacity that sizes the
// manifest's group mask: CapPerShard when recorded (authoritative, the auto path),
// otherwise the capacity the Reserve implies. It is the alignment key — two parts
// sets are part-aligned (their corresponding parts hold the same keyspace slice,
// so a delta between them is valid) iff their EffectiveCapPerShard, Shards, Parts,
// and key/value geometry all match. Comparing Reserve alone is wrong: an auto
// export records Reserve 0, so two auto sets at different capacities would compare
// equal. Returns 0 when the manifest carries neither (an incomplete manifest).
func (m PartsManifest) EffectiveCapPerShard() uint64 {
	if m.CapPerShard > 0 {
		return m.CapPerShard
	}
	if m.Reserve > 0 && m.Shards > 0 {
		return ReserveCapPerShard(m.Reserve, m.Shards)
	}
	return 0
}

// Partitioner builds the Partitioner the manifest's geometry implies, bucketing by
// its effective per-shard capacity (CapPerShard when recorded, else the capacity the
// legacy Reserve implies).
func (m PartsManifest) Partitioner() (*Partitioner, error) {
	return NewPartitioner(m.EffectiveCapPerShard(), m.Parts)
}

// IngestStats reports what IngestParts loaded.
type IngestStats struct {
	Files    int // partition files read
	Total    int // records read
	Inserted int // new entries inserted (duplicates already present are not counted)
}

// IngestParts loads "<prefix>.pNNN" packed-record files (keyLen+valLen bytes per
// record) in group order into one sharded store at `store`, pre-sized to capPerShard
// so the staged forward-sweep property holds and the load never resizes. It stops at
// the first missing partition index. shards and capPerShard MUST match the values the
// partition step bucketed by, since capPerShard sizes the group mask (size it from a
// reserve via ReserveCapPerShard, or reuse a store's via StoreCapPerShard).
// capPerShard 0 falls back to a small default. With no partition files found, it
// returns a zero IngestStats and no error — the caller decides whether that is an error.
//
// flushPerPart makes the resident-memory bound deterministic instead of relying
// on the kernel's background writeback: after each part is loaded, every shard is
// Sync'd, which (via the dirty-chunk set) msyncs only that part's window and
// clears it, so the next part starts clean and dirty pages never accumulate past
// one window. Leave it off for a fully in-RAM load, where the single final Sync
// is enough and per-part syncs only cost throughput.
func IngestParts(prefix, store string, keyLen, valLen, shards int, capPerShard uint64, flushPerPart bool, opts ...Option) (IngestStats, error) {
	var st IngestStats
	if capPerShard <= 0 {
		capPerShard = 1024
	}
	sh, err := OpenSharded(store, keyLen, valLen, shards, int(capPerShard), opts...)
	if err != nil {
		return st, err
	}
	defer sh.Close()

	rec := keyLen + valLen
	const chunkRecs = 1 << 18 // bound per-shard index buffers
	for p := 0; ; p++ {
		path := fmt.Sprintf("%s.p%03d", prefix, p)
		if _, e := os.Stat(path); e != nil {
			break // no more partitions
		}
		ra, e := mmap.Open(path)
		if e != nil {
			return st, e
		}
		buf := ra.Bytes()
		if rec == 0 || len(buf)%rec != 0 {
			ra.Close()
			return st, fmt.Errorf("core: %s size %d is not a multiple of record size %d", path, len(buf), rec)
		}
		st.Files++
		st.Total += len(buf) / rec
		for off := 0; off < len(buf); off += chunkRecs * rec {
			end := off + chunkRecs*rec
			if end > len(buf) {
				end = len(buf)
			}
			ins, e := sh.PutRecords(buf[off:end])
			if e != nil {
				ra.Close()
				return st, e
			}
			st.Inserted += ins
		}
		ra.Close()
		if flushPerPart {
			if err = sh.Sync(); err != nil { // bound the dirty set to one part's window
				return st, err
			}
		}
	}
	if st.Files == 0 {
		return st, nil
	}
	if err = sh.Sync(); err != nil {
		return st, err
	}
	return st, nil
}

// IngestManifest builds a store directly from a self-describing set of parts: it
// reads the manifest at manifestPath, takes the geometry from it, and ingests the
// "<prefix>.pNNN" files sitting beside it — no need to re-run partition or re-specify
// dimensions. The parts prefix is derived from the manifest location, so the
// directory can be moved or shipped. It sizes the load to the manifest's effective
// per-shard capacity (CapPerShard, else what the legacy Reserve implies), so the
// rebuild never resizes — pre-sizing a hot shard too. The capPerShard arg is a
// fallback used only when the manifest carries no geometry. flushPerPart is forwarded
// to IngestParts (deterministic resident-memory bound).
func IngestManifest(manifestPath, store string, capPerShard uint64, flushPerPart bool, opts ...Option) (PartsManifest, IngestStats, error) {
	m, err := ReadManifest(manifestPath)
	if err != nil {
		return m, IngestStats{}, err
	}
	prefix := strings.TrimSuffix(manifestPath, ManifestSuffix)
	if c := m.EffectiveCapPerShard(); c > 0 {
		capPerShard = c
	}
	st, err := IngestParts(prefix, store, m.KeyLen, m.ValLen, m.Shards, capPerShard, flushPerPart, opts...)
	return m, st, err
}

// StoreDigest opens the store at `store` read-only and returns its order-
// independent content Digest as a 16-hex-digit string, along with the entry
// count. It is the companion to a transport bundle: record the digest before
// packing (bundle.Meta.Digest) and re-derive it after a rebuild to confirm the
// rebuilt store carries the same logical content — independent of compression,
// blob order, or shard count. shards must match the store on disk.
func StoreDigest(store string, keyLen, valLen, shards int) (digest string, n int, err error) {
	sh, err := OpenSharded(store, keyLen, valLen, shards, 1024)
	if err != nil {
		return "", 0, err
	}
	defer sh.Close()
	return fmt.Sprintf("%016x", sh.Digest()), sh.Len(), nil
}

// ExportParts re-buckets a live store back into partition files "<prefix>.pNNN"
// (plus a manifest) — the inverse of IngestParts. It opens the store read-only and
// routes every live entry through a Partitioner for (capPerShard, parts), writing
// each packed key|value record to its destination part. The result is a dense,
// self-describing, geometry-independent parts set that IngestParts, IngestManifest,
// push, or a bundle can consume — smaller and more portable than the live shard
// (slot) files.
//
// capPerShard sizes the group mask, so it (with parts) defines which part a key
// lands in; the manifest records it. Pass StoreCapPerShard(store, shards) (the max)
// to bucket by the store's actual capacity — the auto, ship-whole path, where a
// rebuild re-ingests every part without a resize. Pass ReserveCapPerShard(reserve,
// shards) to bucket by a chosen reserve instead: two stores exported with the *same*
// capPerShard and parts are part-aligned (their corresponding parts hold the same
// keyspace slice), the property a part-wise delta relies on. Returns the manifest.
func ExportParts(store, prefix string, keyLen, valLen, shards int, capPerShard uint64, parts int, opts ...Option) (PartsManifest, error) {
	p, err := NewPartitioner(capPerShard, parts)
	if err != nil {
		return PartsManifest{}, err
	}
	sh, err := OpenSharded(store, keyLen, valLen, shards, 1024, opts...)
	if err != nil {
		return PartsManifest{}, err
	}
	defer sh.Close()

	files := make([]*os.File, parts)
	bw := make([]*bufio.Writer, parts)
	for i := range files {
		f, e := os.Create(fmt.Sprintf("%s.p%03d", prefix, i))
		if e != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return PartsManifest{}, e
		}
		files[i] = f
		bw[i] = bufio.NewWriterSize(f, 1<<20)
	}

	rec := keyLen + valLen
	buf := make([]byte, rec)
	counts := make([]int, parts)
	var writeErr error
	sh.ForEach(func(k, v []byte) bool {
		pi := p.PartKey(k) // routes by the same hash the store placed with
		copy(buf, k)
		copy(buf[keyLen:], v)
		if _, e := bw[pi].Write(buf); e != nil {
			writeErr = e
			return false
		}
		counts[pi]++
		return true
	})
	for i := range bw {
		if e := bw[i].Flush(); e != nil && writeErr == nil {
			writeErr = e
		}
		if e := files[i].Close(); e != nil && writeErr == nil {
			writeErr = e
		}
	}
	if writeErr != nil {
		return PartsManifest{}, writeErr
	}

	total := 0
	for _, c := range counts {
		total += c
	}
	m := PartsManifest{
		KeyLen: keyLen, ValLen: valLen, RecordSize: rec,
		Shards: shards, CapPerShard: capPerShard, Parts: parts,
		Records: total, Prefix: filepath.Base(prefix), Counts: counts,
	}
	if err := WriteManifest(ManifestPath(prefix), m); err != nil {
		return m, err
	}
	return m, nil
}
