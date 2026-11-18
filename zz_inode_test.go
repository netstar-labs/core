package core

import (
	"os"
	"syscall"
	"testing"
)

// inode extracts the inode number from a FileInfo, so a test can prove the resize
// replaced the file rather than rewriting it in place.
func inode(t *testing.T, fi os.FileInfo) uint64 {
	t.Helper()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("inode numbers unavailable on this platform")
	}
	return uint64(st.Ino)
}
