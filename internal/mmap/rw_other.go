// Original netstar-labs work — not derived from golang.org/x/exp/mmap, which is
// read-only and has no writable mapping. See LICENSE in this directory.

//go:build !linux && !darwin

package mmap

import "os"

// Backed reports whether MapRW returns a file-backed mapping on this platform.
const Backed = false

// MapRW falls back to a heap buffer on platforms without a writable-mmap binding
// here (including windows): the mapping is not file-backed, so changes are not
// paged to disk. f is accepted for signature parity and left untouched.
func MapRW(_ *os.File, size int) (*RW, error) {
	return &RW{data: make([]byte, size)}, nil
}

func unmapRegion([]byte) error { return nil }

// Sync is a no-op on the heap-backed fallback (nothing is file-mapped).
func (m *RW) Sync() error { return nil }

// SyncRange is a no-op on the heap-backed fallback.
func (m *RW) SyncRange(off, n int) error { return nil }
