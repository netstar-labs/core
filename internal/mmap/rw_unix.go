// Original netstar-labs work — not derived from golang.org/x/exp/mmap, which is
// read-only and has no writable mapping. See LICENSE in this directory.

//go:build linux || darwin

package mmap

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Sync flushes dirty pages of the mapping to the backing file (msync MS_SYNC).
// It is a no-op for an empty mapping. Use it to make committed writes durable
// across power loss; ordinary process crashes are covered by the page cache.
func (m *RW) Sync() error {
	if len(m.data) == 0 {
		return nil
	}
	return unix.Msync(m.data, unix.MS_SYNC)
}

// SyncRange flushes only the bytes [off, off+n) of the mapping. off must be
// page-aligned (the kernel rounds the length up to a page). Flushing only the
// pages that were actually written avoids a full-mapping page-table walk on a
// table far larger than the dirty set.
func (m *RW) SyncRange(off, n int) error {
	if n <= 0 || len(m.data) == 0 || off < 0 || off >= len(m.data) {
		return nil
	}
	end := off + n
	if end > len(m.data) {
		end = len(m.data)
	}
	return unix.Msync(m.data[off:end], unix.MS_SYNC)
}

// Backed reports whether MapRW returns a file-backed mapping on this platform.
const Backed = true

// MapRW maps the first size bytes of f read/write and shared. f must already be
// at least size bytes long (e.g. via (*os.File).Truncate). A size of 0 yields
// an empty mapping.
func MapRW(f *os.File, size int) (*RW, error) {
	if size == 0 {
		return &RW{data: []byte{}}, nil
	}
	d, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &RW{data: d}, nil
}

func unmapRegion(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}
