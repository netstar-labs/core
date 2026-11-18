package core

import "sync/atomic"

// Thin wrappers over sync/atomic for word access into the mmap. On amd64/arm64
// an atomic load of an aligned word compiles to a plain MOV, so lock-free reads
// pay no penalty over an unsynchronized read while remaining race-free under the
// Go memory model.

func loadU64(p *uint64) uint64          { return atomic.LoadUint64(p) }
func storeU64(p *uint64, v uint64)      { atomic.StoreUint64(p, v) }
func addU64(p *uint64, d uint64) uint64 { return atomic.AddUint64(p, d) }

// orU64 / andU64 are CAS-loop bit operations on a word, used by the dirty-page
// bitmap. orU64 is a no-op fast path when the bits are already set, so a value
// update whose chunk is already dirty pays only an atomic load.
func orU64(p *uint64, mask uint64) {
	for {
		old := atomic.LoadUint64(p)
		if old&mask == mask || atomic.CompareAndSwapUint64(p, old, old|mask) {
			return
		}
	}
}

func andU64(p *uint64, mask uint64) {
	for {
		old := atomic.LoadUint64(p)
		nv := old & mask
		if nv == old || atomic.CompareAndSwapUint64(p, old, nv) {
			return
		}
	}
}
func loadU32(p *uint32) uint32     { return atomic.LoadUint32(p) }
func storeU32(p *uint32, v uint32) { atomic.StoreUint32(p, v) }
