//go:build !linux && !darwin

package core

// adviseRandom is a no-op where madvise is unavailable.
func adviseRandom([]byte) {}

// adviseSequential is a no-op where madvise is unavailable.
func adviseSequential([]byte) {}
