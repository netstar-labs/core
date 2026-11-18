package core

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/netstar-labs/core/internal/mmap"
)

// resizeTrace, when non-nil, records each durability step of buildResized in the
// order it happens: "msync", "rename", "syncdir". It exists because the ordering is
// the whole correctness argument for the resize — msync the contents, swap the
// inode, then flush the directory entry — and the failure it guards against (a crash
// between the rename and the directory flush) cannot be produced in-process.
//
// Asserting the sequence is what IS testable here; that a directory fsync makes a
// rename durable is the kernel's contract, not ours, and is checked by the
// fault-injection runner in build/faultinject (see docs/audits/untestable-without-x.md).
// Tests set it under resizeTraceMu; production never touches it.
var (
	resizeTrace   []string
	resizeTraceMu sync.Mutex
)

func traceResize(step string) {
	resizeTraceMu.Lock()
	if resizeTrace != nil {
		resizeTrace = append(resizeTrace, step)
	}
	resizeTraceMu.Unlock()
}

// syncDirHook, when non-nil, replaces the real syncDir call inside
// buildResized, so a test can inject a directory-fsync failure after a real
// rename without needing a filesystem that can actually fail fsync(2). Tests
// set it under syncDirHookMu; production never touches it.
var (
	syncDirHook   func(dir string) error
	syncDirHookMu sync.Mutex
)

func callSyncDir(dir string) error {
	syncDirHookMu.Lock()
	hook := syncDirHook
	syncDirHookMu.Unlock()
	if hook != nil {
		return hook(dir)
	}
	return syncDir(dir)
}

// buildResized creates a new table of newCap, copies every live entry from old
// into it, optionally makes it durable (msync, skipped when doSync is false —
// e.g. WithNoSync), and atomically renames it over path (replacing the inode).
// It returns the new table view. The old mapping is untouched — callers keep it
// alive for in-flight readers and free it on Close. Callers must hold their
// writer lock.
func buildResized(path string, keyLen, valLen int, old *table, newCap uint64, doSync bool) (*table, error) {
	tmp := fmt.Sprintf("%s.resizing.%d", path, os.Getpid())
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	_, _, _, _, fileSize := layout(keyLen, valLen, newCap)
	if err := f.Truncate(int64(fileSize)); err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, err
	}
	rw, err := mmap.MapRW(f, fileSize)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, err
	}

	fail := func(err error) (*table, error) {
		rw.Close()
		f.Close()
		os.Remove(tmp)
		return nil, err
	}

	nt := newTable(rw, keyLen, valLen, newCap)
	overflow := false
	old.forEachLive(func(key, val []byte) {
		if !nt.insertPlain(key, val, sum(key)) {
			overflow = true
		}
	})
	if overflow {
		// The destination was sized too small for what the source actually holds —
		// the header count under-reported the live slots. Report it instead of
		// leaving entries silently behind (or, as before, spinning forever).
		return fail(fmt.Errorf("%w: rebuild target of %d slots too small for the live entries of %s",
			ErrCorrupt, newCap, path))
	}

	if doSync {
		// syncDirty flushes the written chunks, falling back to a whole-file
		// msync when most of the table is dirty — which a fresh rebuild usually is.
		if err := nt.syncDirty(); err != nil {
			return fail(err)
		}
		traceResize("msync")
	}
	if err := os.Rename(tmp, path); err != nil { // atomic replace of the data inode
		return fail(err)
	}
	traceResize("rename")
	// Rename only becomes durable once the *directory* entry is flushed. Without
	// this, a crash after a successful msync+rename can still come back to the old
	// inode — the file contents were durable, the swap was not.
	if doSync {
		if err := callSyncDir(filepath.Dir(path)); err != nil {
			// The rename already committed: path now names nt's inode, and nt
			// is a valid, correct table — only its durability confirmation
			// failed. Discarding nt here (as this used to do) and returning
			// nil would leave the caller still writing to old, whose inode
			// the path no longer names: those writes would be silently lost
			// the moment the process closed (docs/audits/defects-log.md,
			// defect D2). Return nt alongside the error so the caller can
			// still publish it; the error says durability isn't confirmed,
			// not that the resize failed.
			traceResize("syncdir-failed")
			f.Close()
			return nt, fmt.Errorf("resize committed but directory fsync failed, durability not confirmed: %w", err)
		}
		traceResize("syncdir")
	}
	f.Close() // the mapping keeps the new inode alive
	return nt, nil
}
