package core

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// TestHelperProcess is re-executed as a separate OS process by TestCrossProcess.
// It opens the same Shared store and writes the key range [CORE_LO, CORE_HI).
// When CORE_HELPER is unset it is a no-op, so a normal `go test` run ignores it.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("CORE_HELPER") != "1" {
		return
	}
	path := os.Getenv("CORE_PATH")
	lo, _ := strconv.Atoi(os.Getenv("CORE_LO"))
	hi, _ := strconv.Atoi(os.Getenv("CORE_HI"))

	s, err := OpenShared(path, 8, 8, 16)
	if err != nil {
		t.Fatalf("child open: %v", err)
	}
	defer s.Close()
	for i := lo; i < hi; i++ {
		if _, err := s.Put(k8(uint64(i)), v8(uint64(i))); err != nil {
			t.Fatalf("child put %d: %v", i, err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("child sync: %v", err)
	}
}

// TestCrossProcess is a true multi-process integration test: a child process and
// the parent concurrently write disjoint key ranges to the same Shared store
// (with resizes on both sides), exercising the cross-process flock writer lock,
// the shared MAP_SHARED inode, and generation-driven remap. Afterwards the
// parent must see every key from both processes.
func TestCrossProcess(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("cross-process flock only wired on unix")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "mp.core")

	parent, err := OpenShared(path, 8, 8, 16) // tiny ⇒ both sides resize
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()

	const half = 6000

	child := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "-test.v")
	child.Env = append(os.Environ(),
		"CORE_HELPER=1",
		"CORE_PATH="+path,
		"CORE_LO=0",
		"CORE_HI="+strconv.Itoa(half),
	)
	var out bytes.Buffer
	child.Stdout, child.Stderr = &out, &out
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()

	// Parent writes the upper half concurrently with the child writing the lower
	// half. flock serializes the two processes' writers.
	for i := half; i < 2*half; i++ {
		if _, err := parent.Put(k8(uint64(i)), v8(uint64(i))); err != nil {
			t.Fatalf("parent put %d: %v", i, err)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("child process failed: %v\n--- child output ---\n%s", err, out.String())
	}

	// Force a remap to the latest inode, then verify the union of both ranges.
	for i := 0; i < 2*half; i++ {
		if _, ok := parent.Get(k8(uint64(i))); !ok {
			t.Fatalf("parent missing key %d after child finished\n--- child output ---\n%s", i, out.String())
		}
	}
	if parent.Len() != 2*half {
		t.Fatalf("parent len=%d want %d", parent.Len(), 2*half)
	}
}
