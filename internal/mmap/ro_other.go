//go:build !linux && !darwin

package mmap

import (
	"fmt"
	"os"
)

// MapRO on a platform without a real mapping. MapRW falls back to a heap buffer
// here (see Backed), which cannot be read-only and is not file-backed, so a
// read-only open would silently be neither shared nor protected. Refuse instead.
func MapRO(_ *os.File, _ int) (*RW, error) {
	return nil, fmt.Errorf("mmap: read-only mapping unsupported on this platform")
}
