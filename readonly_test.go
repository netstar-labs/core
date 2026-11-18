package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Every read-only case runs against both single-table backends. The mode has to
// mean the same thing on each, and the two have different write seams: Map gates
// its chokepoints, Shared gates the writer lock plus the lock-free update path.
type opener func(path string, keyLen, valLen, cap int, opts ...Option) (Store, error)

var backends = []struct {
	name string
	open opener
}{
	{"Map", func(p string, k, v, c int, o ...Option) (Store, error) { return Open(p, k, v, c, o...) }},
	{"Shared", func(p string, k, v, c int, o ...Option) (Store, error) { return OpenShared(p, k, v, c, o...) }},
}

var shardedBackends = []struct {
	name string
	open func(path string, keyLen, valLen, shards, cap int, opts ...Option) (*Sharded, error)
}{
	{"Sharded", OpenSharded},
	{"SharedSharded", OpenSharedSharded},
}

// seedRO builds a one-entry table with the given backend and returns its path.
func seedRO(t *testing.T, open opener) (path string, key, val []byte) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "t.core")
	m, err := open(path, 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	key, val = []byte("abcdefgh"), []byte("12345678")
	if _, err := m.Put(key, val); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	return path, key, val
}

func skipIfRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file modes; the permission checks are meaningless")
	}
}

func TestReadOnlyReads(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			path, key, want := seedRO(t, b.open)
			m, err := b.open(path, 8, 8, 64, WithReadOnly())
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if got, ok := m.Get(key); !ok || string(got) != string(want) {
				t.Errorf("Get = %q ok=%v, want %q true", got, ok, want)
			}
			if m.Len() != 1 || !m.Contains(key) {
				t.Errorf("Len=%d Contains=%v", m.Len(), m.Contains(key))
			}
		})
	}
}

// TestReadOnlyRefusesEveryMutator: no write may reach the mapping. A store through
// PROT_READ is SIGBUS, so a missing gate is a crash here, not a wrong answer —
// which is what makes this test the one that finds them. It found two in Map
// (Sharded.Put via putH, Map.Put via insertLockedH) and one in Shared (the
// lock-free <=8-byte update that never takes the writer lock).
func TestReadOnlyRefusesEveryMutator(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			path, key, val := seedRO(t, b.open)
			m, err := b.open(path, 8, 8, 64, WithReadOnly())
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()

			if m.Update(key, []byte("87654321")) {
				t.Error("Update succeeded")
			}
			if _, err := m.Put([]byte("zzzzzzzz"), val); !errors.Is(err, ErrReadOnly) {
				t.Errorf("Put err = %v", err)
			}
			if err := m.Batch(func(put func(k, v []byte) (bool, error)) error { return nil }); !errors.Is(err, ErrReadOnly) {
				t.Errorf("Batch err = %v", err)
			}
			if err := m.Reserve(1000); !errors.Is(err, ErrReadOnly) {
				t.Errorf("Reserve err = %v", err)
			}
			if m.Delete(key) {
				t.Error("Delete succeeded")
			}
			if n := m.Prune(func(k, v []byte) bool { return true }); n != 0 {
				t.Errorf("Prune removed %d", n)
			}
			if err := m.Shrink(); !errors.Is(err, ErrReadOnly) {
				t.Errorf("Shrink err = %v", err)
			}
			if err := m.Sync(); err != nil {
				t.Errorf("Sync err = %v, want nil", err)
			}
			if got, ok := m.Get(key); !ok || string(got) != string(val) {
				t.Errorf("value changed to %q", got)
			}
		})
	}
}

// TestReadOnlyDoesNotCreate: read-only brings nothing into being — not the table,
// not the meta sidecar — and refuses a zero-length file rather than serving a
// store whose every lookup misses while looking healthy.
func TestReadOnlyDoesNotCreate(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			missing := filepath.Join(dir, "absent.core")
			if _, err := b.open(missing, 8, 8, 64, WithReadOnly()); err == nil {
				t.Error("opened a table that does not exist")
			}
			for _, p := range []string{missing, missing + ".meta"} {
				if _, err := os.Stat(p); err == nil {
					t.Errorf("read-only created %s", p)
				}
			}
			empty := filepath.Join(dir, "empty.core")
			os.WriteFile(empty, nil, 0o644)
			if _, err := b.open(empty, 8, 8, 64, WithReadOnly()); err == nil {
				t.Error("accepted a zero-length file as a table")
			}
		})
	}
}

// TestReadOnlyOpensAModeReadOnlyFile is why the mode exists: a process that
// cannot write the files must still serve from them.
func TestReadOnlyOpensAModeReadOnlyFile(t *testing.T) {
	skipIfRoot(t)
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			path, key, _ := seedRO(t, b.open)
			for _, p := range []string{path, path + ".meta"} {
				if _, err := os.Stat(p); err == nil {
					os.Chmod(p, 0o444)
					t.Cleanup(func() { os.Chmod(p, 0o644) })
				}
			}
			if m, err := b.open(path, 8, 8, 64); err == nil {
				m.Close()
				t.Error("a read-write open of 0444 files succeeded; the premise of this mode is wrong")
			}
			m, err := b.open(path, 8, 8, 64, WithReadOnly())
			if err != nil {
				t.Fatalf("read-only open of 0444 files: %v", err)
			}
			defer m.Close()
			if !m.Contains(key) {
				t.Error("Contains = false")
			}
		})
	}
}

// TestShardedReadOnly: the option must reach every shard on both sharded
// constructors — the sharded forms are the entry points a lookup front end uses.
func TestShardedReadOnly(t *testing.T) {
	for _, b := range shardedBackends {
		t.Run(b.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "z.core")
			s, err := b.open(base, 8, 8, 4, 64)
			if err != nil {
				t.Fatal(err)
			}
			key, val := []byte("abcdefgh"), []byte("12345678")
			if _, err := s.Put(key, val); err != nil {
				t.Fatal(err)
			}
			s.Close()

			ro, err := b.open(base, 8, 8, 4, 64, WithReadOnly())
			if err != nil {
				t.Fatal(err)
			}
			defer ro.Close()
			if got, ok := ro.Get(key); !ok || string(got) != string(val) {
				t.Errorf("Get = %q ok=%v", got, ok)
			}
			if _, err := ro.Put([]byte("zzzzzzzz"), val); !errors.Is(err, ErrReadOnly) {
				t.Errorf("Put err = %v, want ErrReadOnly", err)
			}
			if ro.Update(key, []byte("87654321")) {
				t.Error("Update succeeded on a read-only shard")
			}
		})
	}
}

// TestReadOnlySharedFollowsResize is the property that makes Shared the right
// backend for a live reader: when the writer grows the table and renames a new
// inode over the path, the read-only reader notices the generation bump and
// adopts the new inode — READ-ONLY. The data file is made 0444 between the
// writer's resize and the reader's remap, so a remap that opened O_RDWR would
// fail and leave the reader stale: that is the mutant this test exists to catch.
func TestReadOnlySharedFollowsResize(t *testing.T) {
	skipIfRoot(t)
	path := filepath.Join(t.TempDir(), "grow.core")
	w, err := OpenShared(path, 8, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Put([]byte("00000000"), []byte("v0000000")); err != nil {
		t.Fatal(err)
	}

	r, err := OpenShared(path, 8, 8, 64, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Len() != 1 {
		t.Fatalf("reader Len before growth = %d, want 1", r.Len())
	}

	// Capacity 64 grows at 56 live entries; 500 forces several resizes.
	const n = 500
	for i := 1; i < n; i++ {
		if _, err := w.Put([]byte(fmt.Sprintf("%08d", i)), []byte(fmt.Sprintf("v%07d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// The writer already holds its mapping; only the reader's remap is affected.
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0o644) })

	last := []byte(fmt.Sprintf("%08d", n-1))
	if got, ok := r.Get(last); !ok || string(got) != fmt.Sprintf("v%07d", n-1) {
		t.Fatalf("reader did not follow the resize: Get(last) = %q ok=%v", got, ok)
	}
	if r.Len() != n {
		t.Errorf("reader Len after growth = %d, want %d", r.Len(), n)
	}
	// The adopted inode is still read-only.
	if _, err := r.Put([]byte("zzzzzzzz"), []byte("00000000")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Put after remap err = %v, want ErrReadOnly", err)
	}
	if r.Update(last, []byte("00000000")) {
		t.Error("Update after remap succeeded")
	}
}

// TestReadOnlySharedNeedsInitialisedMeta: the writer path repairs a short or
// unstamped meta in place. A reader cannot, so every such state is a refusal —
// and refusing must not leave a meta behind.
func TestReadOnlySharedNeedsInitialisedMeta(t *testing.T) {
	dir := t.TempDir()
	// A Map-built table has no .meta at all.
	path, _, _ := seedRO(t, backends[0].open)
	if _, err := OpenShared(path, 8, 8, 64, WithReadOnly()); err == nil {
		t.Error("opened with no meta file")
	}
	if _, err := os.Stat(path + ".meta"); err == nil {
		t.Error("read-only created the meta file")
	}
	// A meta that exists but was never grown to size (writer crashed mid-create).
	short := filepath.Join(dir, "short.core")
	os.WriteFile(short, make([]byte, PageSize*4), 0o644)
	os.WriteFile(short+".meta", nil, 0o644)
	if _, err := OpenShared(short, 8, 8, 64, WithReadOnly()); err == nil {
		t.Error("accepted a zero-length meta")
	}
	// Full size but never stamped.
	os.WriteFile(short+".meta", make([]byte, metaFileSize), 0o644)
	if _, err := OpenShared(short, 8, 8, 64, WithReadOnly()); err == nil {
		t.Error("accepted an unstamped meta")
	}
}
