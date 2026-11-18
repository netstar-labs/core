//go:build linux || darwin

package core

import (
	"os"

	"golang.org/x/sys/unix"
)

// flockEx / flockUn implement the cross-process single-writer lock as an
// advisory lock on the meta file. The lock is tied to the open file
// description, so two handles in the same process (separate opens) also exclude
// each other — which is what makes in-process tests meaningful.
func flockEx(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_EX) }
func flockUn(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_UN) }
