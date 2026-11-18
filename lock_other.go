//go:build !linux && !darwin

package core

import "os"

// Without an flock binding here, the cross-process writer lock degrades to a
// no-op: a single process is still correct, but concurrent multi-process writers
// are not mutually excluded.
func flockEx(*os.File) error { return nil }
func flockUn(*os.File) error { return nil }
