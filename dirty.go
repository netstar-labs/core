package core

// dirtyset tracks which coarse chunks of the mapping have been written since the
// last Sync, so Sync can msync only those ranges instead of the whole file. It
// lives in the Go heap (per process), is marked by writers, and drained by Sync.
//
// Marking is cheap: a chunk that is already dirty costs one atomic load, so the
// steady-state value-update path (which keeps hitting the same already-dirty
// slot pages) pays almost nothing. The chunk is coarse (a few MiB) to keep the
// bitmap tiny — a 16 GiB table needs ~1 KiB of bits.
const (
	dirtyChunkShift = 21 // 2 MiB chunks (also a transparent-huge-page boundary)
	dirtyChunkSize  = 1 << dirtyChunkShift
)

type dirtyset struct {
	bits    []uint64
	nchunks int
}

func newDirtyset(fileSize int) *dirtyset {
	n := (fileSize + dirtyChunkSize - 1) >> dirtyChunkShift
	if n == 0 {
		n = 1
	}
	return &dirtyset{bits: make([]uint64, (n+63)/64), nchunks: n}
}

// mark records that bytes [off, off+n) were written.
func (d *dirtyset) mark(off, n int) {
	if n <= 0 {
		return
	}
	c0 := off >> dirtyChunkShift
	c1 := (off + n - 1) >> dirtyChunkShift
	for c := c0; c <= c1; c++ {
		w := c >> 6
		if w >= len(d.bits) {
			break
		}
		mask := uint64(1) << uint(c&63)
		if loadU64(&d.bits[w])&mask == 0 {
			orU64(&d.bits[w], mask)
		}
	}
}

type chunkRange struct{ off, n int }

// collect returns the maximal runs of contiguous dirty chunks (as byte ranges
// clamped to fileSize) and the total number of dirty chunks, clearing the bits
// as it goes. Writes concurrent with collect are best-effort: one that lands
// during the scan may be deferred to the next Sync.
func (d *dirtyset) collect(fileSize int) (runs []chunkRange, dirty int) {
	c := 0
	for c < d.nchunks {
		if !d.isSet(c) {
			c++
			continue
		}
		start := c
		for c < d.nchunks && d.isSet(c) {
			c++
		}
		dirty += c - start
		off := start << dirtyChunkShift
		end := c << dirtyChunkShift
		if end > fileSize {
			end = fileSize
		}
		runs = append(runs, chunkRange{off, end - off})
		for x := start; x < c; x++ {
			andU64(&d.bits[x>>6], ^(uint64(1) << uint(x&63)))
		}
	}
	return runs, dirty
}

func (d *dirtyset) isSet(c int) bool {
	w := c >> 6
	if w >= len(d.bits) {
		return false
	}
	return loadU64(&d.bits[w])&(uint64(1)<<uint(c&63)) != 0
}
