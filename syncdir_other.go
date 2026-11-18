//go:build !linux && !darwin

package core

// syncDir is a no-op where a directory handle cannot be fsynced (Windows opens
// directories differently and does not support FlushFileBuffers on one). The
// rename is still atomic there; only its durability across a crash is weaker.
func syncDir(string) error { return nil }
