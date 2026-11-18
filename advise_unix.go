//go:build linux || darwin

package core

import "golang.org/x/sys/unix"

// adviseRandom tells the kernel the region is accessed randomly, disabling
// readahead so a point lookup faults in one page instead of a run of them.
func adviseRandom(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = unix.Madvise(b, unix.MADV_RANDOM)
}

// adviseSequential tells the kernel the region will be read front-to-back,
// enabling aggressive readahead. A full forward sweep (e.g. prune) then streams
// in instead of faulting one page at a time, which is what MADV_RANDOM would
// otherwise force. Pair with a restoring adviseRandom once the sweep is done.
func adviseSequential(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = unix.Madvise(b, unix.MADV_SEQUENTIAL)
}
