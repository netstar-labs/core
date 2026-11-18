//go:build linux || darwin

package mmap

import (
	"os"
	"syscall"
)

// MapRO maps the first size bytes of f read-only and shared, returning the same
// RW handle a writable mapping uses so every reader downstream is unchanged.
//
// The mapping is PROT_READ: a store through Bytes faults rather than corrupting
// the file. That is deliberate. A read-only Store gates its mutators before they
// reach memory, so the protection is the backstop for a path that forgot to —
// a crash at the offending instruction, not a silent write into a master another
// process owns.
//
// MAP_SHARED, not MAP_PRIVATE: a private mapping would copy pages on first touch
// and hide every subsequent update by the writer, which is the opposite of what a
// live reader wants.
func MapRO(f *os.File, size int) (*RW, error) {
	if size == 0 {
		return &RW{data: []byte{}}, nil
	}
	d, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &RW{data: d}, nil
}
