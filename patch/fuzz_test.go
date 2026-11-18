package patch

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/netstar-labs/core"
)

// The patch bundle is how a change set travels between machines, so OpenBundle is a
// trust boundary. Invariants: never panic, never hang, never allocate unboundedly
// from a declared length, and never report success on a snapshot whose records do
// not divide evenly into the geometry it claims.

func seedSnapshot(t testing.TB, keyLen, valLen, upserts, deletes int) *Snapshot {
	t.Helper()
	s := &Snapshot{KeyLen: keyLen, ValLen: valLen, BaseDigest: "cafebabe"}
	for i := 0; i < upserts; i++ {
		rec := make([]byte, keyLen+valLen)
		for j := range rec {
			rec[j] = byte(i + j)
		}
		s.Upserts = append(s.Upserts, rec...)
	}
	for i := 0; i < deletes; i++ {
		rec := make([]byte, keyLen)
		for j := range rec {
			rec[j] = byte(0x80 + i + j)
		}
		s.Deletes = append(s.Deletes, rec...)
	}
	return s
}

func seedPatchBundle(t testing.TB, keyLen, valLen, upserts, deletes int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := WriteBundle(&buf, seedSnapshot(t, keyLen, valLen, upserts, deletes), -1); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// FuzzOpenBundle throws arbitrary bytes at the patch bundle reader.
func FuzzOpenBundle(f *testing.F) {
	f.Add(seedPatchBundle(f, 8, 8, 4, 2))
	f.Add(seedPatchBundle(f, 32, 8, 1, 0))
	f.Add(seedPatchBundle(f, 8, 8, 0, 0))
	f.Add([]byte{})
	f.Add([]byte("not a patch bundle"))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		snap, man, err := OpenBundle(bytes.NewReader(data), dir)
		if err != nil {
			return // rejecting malformed input is the correct outcome
		}
		if snap == nil || man == nil {
			t.Fatal("OpenBundle returned a nil snapshot or manifest with no error")
		}
		// A snapshot that verified must be self-consistent: its record buffers have
		// to divide evenly into the geometry it declares, or every consumer that
		// walks them by stride reads a partial record off the end.
		if snap.KeyLen <= 0 {
			t.Fatalf("accepted a snapshot with keyLen %d", snap.KeyLen)
		}
		if rec := snap.KeyLen + snap.ValLen; rec > 0 && len(snap.Upserts)%rec != 0 {
			t.Fatalf("upserts (%d bytes) do not divide by the record size %d",
				len(snap.Upserts), rec)
		}
		if len(snap.Deletes)%snap.KeyLen != 0 {
			t.Fatalf("deletes (%d bytes) do not divide by keyLen %d",
				len(snap.Deletes), snap.KeyLen)
		}
		// The counts derived from the buffers must agree with the buffers.
		if got, want := snap.NumUpserts()*(snap.KeyLen+snap.ValLen), len(snap.Upserts); got != want {
			t.Fatalf("NumUpserts implies %d bytes, buffer has %d", got, want)
		}
	})
}

// FuzzWriteOpenBundleRoundTrip drives arbitrary record payloads through the writer
// and back. A change set that mutates in transit would corrupt a store silently.
func FuzzWriteOpenBundleRoundTrip(f *testing.F) {
	f.Add([]byte("0123456789abcdef"), []byte("01234567"), uint8(8), uint8(8))
	f.Add([]byte{}, []byte{}, uint8(8), uint8(8))

	f.Fuzz(func(t *testing.T, ups, dels []byte, k, v uint8) {
		keyLen, valLen := int(k), int(v)
		if keyLen <= 0 || keyLen > 64 || valLen < 0 || valLen > 64 {
			t.Skip() // the writer's contract is a positive keyLen
		}
		rec := keyLen + valLen
		// Only well-formed buffers are the writer's job; a caller handing it a
		// partial record is a caller bug, not a format question.
		if rec == 0 || len(ups)%rec != 0 || len(dels)%keyLen != 0 {
			t.Skip()
		}
		s := &Snapshot{KeyLen: keyLen, ValLen: valLen, Upserts: ups, Deletes: dels}
		var buf bytes.Buffer
		if _, err := WriteBundle(&buf, s, -1); err != nil {
			return
		}
		dir := t.TempDir()
		got, _, err := OpenBundle(bytes.NewReader(buf.Bytes()), dir)
		if err != nil {
			t.Fatalf("a bundle WriteBundle produced does not open: %v", err)
		}
		if got.KeyLen != keyLen || got.ValLen != valLen {
			t.Fatalf("geometry changed: %d/%d in, %d/%d out", keyLen, valLen, got.KeyLen, got.ValLen)
		}
		if !bytes.Equal(got.Upserts, ups) {
			t.Fatalf("upserts changed across the round-trip (%d in, %d out)", len(ups), len(got.Upserts))
		}
		if !bytes.Equal(got.Deletes, dels) {
			t.Fatalf("deletes changed across the round-trip (%d in, %d out)", len(dels), len(got.Deletes))
		}
	})
}

// FuzzApply feeds arbitrary snapshots at a real store. Apply walks the record
// buffers by stride, so a buffer that does not divide evenly is the case that would
// read off the end.
func FuzzApply(f *testing.F) {
	f.Add([]byte("0123456789abcdef"), []byte("01234567"))
	f.Add([]byte{}, []byte{})
	f.Add([]byte("short"), []byte("x"))

	f.Fuzz(func(t *testing.T, ups, dels []byte) {
		dir := t.TempDir()
		st, err := core.OpenSharded(filepath.Join(dir, "kv.core"), 2, 8, 8, 64)
		if err != nil {
			t.Skip()
		}
		defer st.Close()
		// Deliberately unaligned buffers included: Apply must reject or truncate
		// safely, never index past the end.
		s := &Snapshot{KeyLen: 8, ValLen: 8, Upserts: ups, Deletes: dels}
		_, _ = Apply(st, s)
		// Whatever it did, the store must still be usable afterwards.
		_ = st.Len()
		_ = st.Digest()
	})
}

// FuzzLogAppendReplay drives arbitrary change sequences through the WAL and asserts
// the replay path agrees with what was appended — the property the durability claim
// in server/main.go rests on.
func FuzzLogAppendReplay(f *testing.F) {
	f.Add([]byte("abcdefgh"), uint64(1), true)
	f.Add([]byte("zzzzzzzz"), uint64(0), false)

	f.Fuzz(func(t *testing.T, key []byte, val uint64, isDelete bool) {
		if len(key) != 8 {
			t.Skip() // the Log's contract is a keyLen-sized key
		}
		dir := t.TempDir()
		lg, err := Open(dir, 8, 8)
		if err != nil {
			t.Skip()
		}
		v := make([]byte, 8)
		for i := range v {
			v[i] = byte(val >> (8 * i))
		}
		if isDelete {
			err = lg.Delete(key)
		} else {
			err = lg.Upsert(key, v)
		}
		if err != nil {
			lg.Close()
			return
		}
		if err := lg.Sync(); err != nil {
			t.Fatalf("Sync after a successful append: %v", err)
		}
		pending, _ := lg.Pending()
		if pending != 1 {
			t.Fatalf("pending = %d after one append, want 1", pending)
		}
		if err := lg.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// Reopening must find the un-applied window rather than lose it.
		again, err := Open(dir, 8, 8)
		if err != nil {
			t.Fatalf("reopening a WAL we just wrote: %v", err)
		}
		defer again.Close()
		if p, _ := again.Pending(); p != 1 {
			t.Fatalf("replay found %d pending, want the 1 we appended", p)
		}
	})
}

// A patch bundle must reject a truncated tail for the same reason a store bundle
// must: it is the shipping format, and a short transfer that verifies looks complete.
func TestOpenBundleRejectsTailTruncation(t *testing.T) {
	full := seedPatchBundle(t, 8, 8, 4, 2)
	for cut := 1; cut <= 32 && cut < len(full); cut++ {
		dir := t.TempDir()
		if _, _, err := OpenBundle(bytes.NewReader(full[:len(full)-cut]), dir); err == nil {
			t.Errorf("OpenBundle accepted a bundle with its last %d byte(s) removed", cut)
		}
	}
	dir := t.TempDir()
	if _, _, err := OpenBundle(bytes.NewReader(full), dir); err != nil {
		t.Fatalf("the intact patch bundle no longer opens: %v", err)
	}
}
