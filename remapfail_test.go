package core

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// growPast puts n keys through w, forcing several resizes from an initial capacity
// of 64 (grow fires at 56 live entries). Keys are "%08d", values "v%07d".
func growPast(t *testing.T, w interface {
	Put(key, val []byte) (bool, error)
}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := w.Put([]byte(fmt.Sprintf("%08d", i)), []byte(fmt.Sprintf("v%07d", i))); err != nil {
			t.Fatal(err)
		}
	}
}

func k(i int) []byte { return []byte(fmt.Sprintf("%08d", i)) }

// bounded fails the test if fn runs longer than limit. The pre-fix behaviour was an
// infinite retry, so wall time IS the assertion — the mutant hung a suite for 10 min.
func bounded(t *testing.T, limit time.Duration, what string, fn func()) {
	t.Helper()
	start := time.Now()
	fn()
	if d := time.Since(start); d > limit {
		t.Fatalf("%s took %v; a remap failure must be terminal, not a retry loop", what, d)
	}
}

// TestSharedRemapFailureReadsAreStaleNotForever: the writer resizes, the reader
// cannot open the new inode (0000), and Get must return PROMPTLY with the answer
// the reader's own snapshot holds — old keys found, new keys not — while
// RemapErr says why. Once the inode is readable again the reader recovers.
func TestSharedRemapFailureReadsAreStaleNotForever(t *testing.T) {
	skipIfRoot(t)
	path := filepath.Join(t.TempDir(), "g.core")
	w, err := OpenShared(path, 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	growPast(t, w, 1) // k(0) lives in the ORIGINAL inode the reader will map

	r, err := OpenShared(path, 8, 8, 64, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.Contains(k(0)) {
		t.Fatal("reader cannot see k(0) before growth")
	}

	growPast(t, w, 500) // several resizes; the reader has not remapped yet
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0o644) })

	bounded(t, 2*time.Second, "Get on an old key", func() {
		if got, ok := r.Get(k(0)); !ok || string(got) != "v0000000" {
			t.Errorf("Get(k0) = %q ok=%v; the snapshot holds it and must be served", got, ok)
		}
	})
	bounded(t, 2*time.Second, "Get on a key only the new inode has", func() {
		if _, ok := r.Get(k(499)); ok {
			t.Error("Get(k499) found a key that is not in the reader's snapshot")
		}
	})
	if r.RemapErr() == nil {
		t.Error("RemapErr = nil while the reader is demonstrably stale")
	}

	// Recovery: the inode becomes readable, the next operation adopts it.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, ok := r.Get(k(499)); !ok || string(got) != "v0000499" {
		t.Errorf("after recovery Get(k499) = %q ok=%v", got, ok)
	}
	if err := r.RemapErr(); err != nil {
		t.Errorf("RemapErr after recovery = %v, want nil", err)
	}
}

// TestSharedRemapFailureRefusesWrites: a second READ-WRITE handle that cannot
// adopt the resized inode must refuse every write — storing into the abandoned
// inode would be lost and reported as success. Covers both the lock-free <=8-byte
// update and the seqlock >8-byte one, plus Delete and Prune; and proves that
// once readable again, a write lands where the OTHER handle can see it.
func TestSharedRemapFailureRefusesWrites(t *testing.T) {
	skipIfRoot(t)
	for _, valLen := range []int{8, 16} {
		t.Run(fmt.Sprintf("valLen=%d", valLen), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "w.core")
			val := func(s string) []byte { b := make([]byte, valLen); copy(b, s); return b }
			w1, err := OpenShared(path, 8, valLen, 64)
			if err != nil {
				t.Fatal(err)
			}
			defer w1.Close()
			if _, err := w1.Put(k(0), val("orig")); err != nil {
				t.Fatal(err)
			}
			w2, err := OpenShared(path, 8, valLen, 64) // maps the original inode
			if err != nil {
				t.Fatal(err)
			}
			defer w2.Close()

			for i := 1; i < 500; i++ { // w1 resizes; w2 has not remapped
				if _, err := w1.Put(k(i), val("x")); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chmod(path, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(path, 0o644) })

			bounded(t, 2*time.Second, "Update", func() {
				if w2.Update(k(0), val("lost")) {
					t.Error("Update reported success into an abandoned inode")
				}
			})
			bounded(t, 2*time.Second, "Delete", func() {
				if w2.Delete(k(0)) {
					t.Error("Delete reported success into an abandoned inode")
				}
			})
			bounded(t, 2*time.Second, "Prune", func() {
				if n := w2.Prune(func(_, _ []byte) bool { return true }); n != 0 {
					t.Errorf("Prune removed %d from an abandoned inode", n)
				}
			})
			if w2.RemapErr() == nil {
				t.Error("RemapErr = nil while writes are being refused")
			}
			// Nothing leaked into the live inode.
			if got, _ := w1.Get(k(0)); string(got) != string(val("orig")) {
				t.Errorf("live inode k0 = %q, want orig", got)
			}

			// Recovery: the write lands in the live inode and the other handle sees it.
			os.Chmod(path, 0o644)
			if !w2.Update(k(0), val("new")) {
				t.Fatal("Update refused after the inode became readable")
			}
			if got, _ := w1.Get(k(0)); string(got) != string(val("new")) {
				t.Errorf("w1 sees k0 = %q after w2's post-recovery Update, want new", got)
			}
		})
	}
}

// TestShardedRemapErrSurfaces: the sharded wrapper is what a lookup server holds,
// so the degradation signal has to come out of it.
func TestShardedRemapErrSurfaces(t *testing.T) {
	skipIfRoot(t)
	base := filepath.Join(t.TempDir(), "z.core")
	w, err := OpenSharedSharded(base, 8, 8, 2, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	growPast(t, w, 1)
	r, err := OpenSharedSharded(base, 8, 8, 2, 64, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.RemapErr() != nil {
		t.Fatal("RemapErr non-nil in steady state")
	}
	growPast(t, w, 600)
	for i := 0; i < 2; i++ {
		os.Chmod(fmt.Sprintf("%s.%03d", base, i), 0)
		t.Cleanup(func() { os.Chmod(fmt.Sprintf("%s.%03d", base, i), 0o644) })
	}
	bounded(t, 2*time.Second, "sharded Get", func() { r.Get(k(599)) })
	if r.RemapErr() == nil {
		t.Error("Sharded.RemapErr = nil after a shard failed to remap")
	}
	// A Map-backed sharded set can never be in this state.
	m, err := OpenSharded(filepath.Join(t.TempDir(), "m.core"), 8, 8, 2, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.RemapErr() != nil {
		t.Error("Map-backed Sharded.RemapErr non-nil")
	}
}
