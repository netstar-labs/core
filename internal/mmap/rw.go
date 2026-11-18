// Original netstar-labs work — not derived from golang.org/x/exp/mmap, which is
// read-only and has no writable mapping. See LICENSE in this directory.

package mmap

// RW is a writable, shared memory mapping over a file the caller owns.
//
// Unlike ReaderAt, RW does not open or close the underlying file: the caller
// passes an already-open and already-sized *os.File to MapRW and stays
// responsible for closing (and, for a scratch file, removing) it. (*RW).Close
// releases only the mapping.
//
// On linux/darwin the mapping is MAP_SHARED with PROT_READ|PROT_WRITE, so stores
// land in the file and the kernel may evict dirty pages under memory pressure —
// which lets callers build structures larger than RAM. On other platforms MapRW
// falls back to a heap buffer that is not file-backed; see Backed.
//
// The slice returned by Bytes is valid only until Close. Concurrent access from
// multiple goroutines follows the same rules as any shared []byte; concurrent
// Close with access is not safe.
type RW struct {
	data []byte
}

// Bytes returns the writable mapped region. Valid only until Close.
func (m *RW) Bytes() []byte { return m.data }

// Len returns the size of the mapped region in bytes.
func (m *RW) Len() int { return len(m.data) }

// Close releases the mapping. It does not touch the underlying file. Calling it
// more than once is safe.
func (m *RW) Close() error {
	if m.data == nil {
		return nil
	}
	d := m.data
	m.data = nil
	return unmapRegion(d)
}
