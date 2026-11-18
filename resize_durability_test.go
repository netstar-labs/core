package core

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// The resize swap's correctness argument is an ordering: msync the rebuilt contents,
// atomically rename it over the live path, then fsync the directory so the rename
// itself is durable. Get that order wrong and a crash can come back to the
// pre-resize inode with the new contents sitting in an unreferenced file.
//
// What is testable in-process is the ORDER. That a directory fsync makes a rename
// durable is the kernel's contract, and proving it needs power loss — see
// build/faultinject and docs/audits/untestable-without-x.md. These tests close the
// half that is ours, so the remaining claim is narrowed to POSIX rather than resting
// on "we believe we call it".

// captureResizeTrace records the durability steps of every resize performed by fn.
func captureResizeTrace(t *testing.T, fn func()) []string {
	t.Helper()
	resizeTraceMu.Lock()
	resizeTrace = []string{}
	resizeTraceMu.Unlock()
	t.Cleanup(func() {
		resizeTraceMu.Lock()
		resizeTrace = nil
		resizeTraceMu.Unlock()
	})
	fn()
	resizeTraceMu.Lock()
	got := append([]string(nil), resizeTrace...)
	resizeTraceMu.Unlock()
	return got
}

func TestResizeDurabilityOrdering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.core")
	m, err := Open(path, 8, 8, 8) // tiny, so writes force resizes
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	trace := captureResizeTrace(t, func() {
		for i := 0; i < 200; i++ {
			k := binary.BigEndian.AppendUint64(nil, uint64(i))
			if _, err := m.Put(k, binary.LittleEndian.AppendUint64(nil, uint64(i))); err != nil {
				t.Fatal(err)
			}
		}
	})

	if len(trace) == 0 {
		t.Fatal("no resize happened — the test proves nothing")
	}
	if len(trace)%3 != 0 {
		t.Fatalf("trace is not whole resizes: %v", trace)
	}
	// Every resize must be exactly msync → rename → syncdir, in that order.
	for i := 0; i < len(trace); i += 3 {
		step := trace[i : i+3]
		if step[0] != "msync" || step[1] != "rename" || step[2] != "syncdir" {
			t.Errorf("resize %d ordered %v, want [msync rename syncdir]", i/3, step)
		}
	}
	t.Logf("%d resizes, each msync → rename → syncdir", len(trace)/3)

	// The data must still be intact, so the ordering assertion is not passing over a
	// broken store.
	got := make([]byte, 8)
	for i := 0; i < 200; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		if !m.GetInto(k, got) || binary.LittleEndian.Uint64(got) != uint64(i) {
			t.Fatalf("key %d wrong after %d resizes", i, len(trace)/3)
		}
	}
}

// WithNoSync trades crash-durability for speed, so it must skip BOTH the content
// msync and the directory fsync — skipping only one would be the worst of both: the
// cost of a flush with none of the guarantee.
func TestResizeNoSyncSkipsBothFlushes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.core")
	m, err := Open(path, 8, 8, 8, WithNoSync())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	trace := captureResizeTrace(t, func() {
		for i := 0; i < 200; i++ {
			k := binary.BigEndian.AppendUint64(nil, uint64(i))
			if _, err := m.Put(k, binary.LittleEndian.AppendUint64(nil, uint64(i))); err != nil {
				t.Fatal(err)
			}
		}
	})

	if len(trace) == 0 {
		t.Fatal("no resize happened — the test proves nothing")
	}
	for _, step := range trace {
		if step != "rename" {
			t.Errorf("WithNoSync still performed %q; it must skip every flush", step)
		}
	}
	t.Logf("%d resizes under WithNoSync, rename only", len(trace))
}

// The rename replaces the data inode, so after a resize the live path must be a
// different file than before — and the temp file must not be left behind.
func TestResizeReplacesInodeAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kv.core")
	m, err := Open(path, 8, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeIno := inode(t, before)

	for i := 0; i < 200; i++ {
		k := binary.BigEndian.AppendUint64(nil, uint64(i))
		if _, err := m.Put(k, binary.LittleEndian.AppendUint64(nil, uint64(i))); err != nil {
			t.Fatal(err)
		}
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if inode(t, after) == beforeIno {
		t.Error("the resize did not replace the data inode")
	}
	if after.Size() <= before.Size() {
		t.Errorf("the store did not grow: %d → %d bytes", before.Size(), after.Size())
	}

	// No .resizing.<pid> litter may survive a successful resize.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if name := e.Name(); name != filepath.Base(path) {
			t.Errorf("resize left %q behind in the store directory", name)
		}
	}
}

// syncDir must reject a path that is not a directory rather than silently succeeding
// — a silent success there would make the durability step a no-op nobody noticed.
func TestSyncDirRejectsNonDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := syncDir(dir); err != nil {
		t.Fatalf("syncDir on a real directory: %v", err)
	}
	if err := syncDir(filepath.Join(dir, "does-not-exist")); err == nil {
		t.Error("syncDir accepted a nonexistent path")
	}
}
