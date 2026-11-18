//go:build linux || darwin

package core

import "os"

// syncDir fsyncs a directory so a rename within it is durable.
//
// msync makes a file's *contents* durable; it says nothing about the directory
// entry that names it. A resize writes a temp file, msyncs it, and renames it over
// the live path — and without this the rename can be lost to a crash, bringing back
// the pre-resize inode even though the rebuild itself was flushed. Append-only logs
// fsync the directory on segment create for the same reason.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
