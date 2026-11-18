package core

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// Fuzz targets for the store's byte-parsing trust boundary: the on-disk header.
//
// A store file is not always locally produced — bundle.Pack/Unpack and
// patch.OpenBundle exist to ship stores between machines — so Open must treat the
// header as untrusted input. The invariants asserted here are the ones the 02 pass
// mandates for a parser: never panic, and never index outside the mapping.
//
// These targets are what would have caught the crafted-geometry out-of-bounds
// (validateGeometry): a checksum-valid header with a slotsOff past the end of the
// file panicked during Open.

// seedStore writes a real store and returns its bytes, the seed corpus every target
// mutates from — random bytes alone almost never reach past the magic check.
func seedStore(t testing.TB, keyLen, valLen, n int) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seed.core")
	m, err := Open(path, keyLen, valLen, 64)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		k := make([]byte, keyLen)
		binary.BigEndian.PutUint64(k, uint64(i))
		if _, err := m.Put(k, make([]byte, valLen)); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// FuzzOpenStore feeds arbitrary bytes to Open as a store file. Open must return an
// error or a store that is safe to query — never panic, and never read outside the
// mapping.
func FuzzOpenStore(f *testing.F) {
	f.Add(seedStore(f, 8, 8, 32))
	f.Add(seedStore(f, 16, 24, 8))
	f.Add([]byte{})
	f.Add(make([]byte, PageSize))

	// A checksum-valid header over a hostile geometry — the shape that used to panic.
	crafted := seedStore(f, 8, 8, 4)
	binary.LittleEndian.PutUint64(crafted[hSlotsOff:], 1<<40)
	binary.LittleEndian.PutUint64(crafted[hCapacity:], 1<<20)
	binary.LittleEndian.PutUint64(crafted[hChecksum:], headerChecksum(crafted))
	f.Add(crafted)

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.core")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Skip()
		}
		m, err := Open(path, 8, 8, 64)
		if err != nil {
			return // rejecting malformed input is the correct outcome
		}
		defer m.Close()
		// Anything Open accepts must survive being used.
		key := make([]byte, 8)
		_ = m.Contains(key)
		_ = m.GetInto(key, make([]byte, 8))
		_ = m.Len()
		_ = m.Digest()
		m.ForEach(func(k, v []byte) bool { return true })
	})
}

// FuzzReadHeader feeds arbitrary bytes to the header reader, which callers use to
// discover a store's geometry before opening it.
func FuzzReadHeader(f *testing.F) {
	f.Add(seedStore(f, 8, 8, 4))
	f.Add(make([]byte, PageSize))
	f.Add([]byte("not a store"))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "h.core")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Skip()
		}
		h, err := ReadHeader(path)
		if err != nil {
			return
		}
		// A header that validates must at least be self-consistent about its sizes,
		// or a caller feeding it to Open would be trusting nonsense.
		if h.KeyLen < 0 || h.ValLen < 0 {
			t.Fatalf("ReadHeader returned negative sizes: keyLen=%d valLen=%d", h.KeyLen, h.ValLen)
		}
		if _, _, err := Geometry(path); err != nil {
			t.Fatalf("ReadHeader accepted a file that Geometry rejected: %v", err)
		}
	})
}

// FuzzOpenGeometry mutates only the geometry fields of a valid header, keeping the
// checksum correct — the adversarial case, where every field the parser reads is
// attacker-chosen but internally consistent.
func FuzzOpenGeometry(f *testing.F) {
	base := seedStore(f, 8, 8, 16)
	f.Add(uint64(1<<40), uint64(1<<20), uint64(8), uint64(16))
	f.Add(uint64(PageSize), uint64(0), uint64(0), uint64(16))
	f.Add(uint64(PageSize), uint64(7), uint64(0), uint64(16))

	f.Fuzz(func(t *testing.T, slotsOff, capacity, verOff, slotSize uint64) {
		raw := append([]byte(nil), base...)
		binary.LittleEndian.PutUint64(raw[hSlotsOff:], slotsOff)
		binary.LittleEndian.PutUint64(raw[hCapacity:], capacity)
		binary.LittleEndian.PutUint64(raw[hVerOff:], verOff)
		binary.LittleEndian.PutUint64(raw[hSlotSize:], slotSize)
		binary.LittleEndian.PutUint64(raw[hChecksum:], headerChecksum(raw)) // re-seal
		path := filepath.Join(t.TempDir(), "g.core")
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Skip()
		}
		m, err := Open(path, 8, 8, 64)
		if err != nil {
			return
		}
		defer m.Close()
		_ = m.Contains(make([]byte, 8))
		_ = m.Len()
		m.ForEach(func(k, v []byte) bool { return true })
	})
}
