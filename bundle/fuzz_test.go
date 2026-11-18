package bundle

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Unpack is a trust boundary: a bundle is the shipping format, so it arrives from
// another machine. The invariants asserted here are the ones the house 02 pass
// mandates for a parser — never panic, never hang, never allocate unboundedly from
// a length field — plus one specific to an archive: never write outside the
// destination directory.

// seedBundle packs a small real bundle and returns its bytes. Random input never
// gets past the header, so every target mutates from a genuine archive.
func seedBundle(t testing.TB, files map[string]string) []byte {
	t.Helper()
	dir := t.TempDir()
	var paths []string
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	var buf bytes.Buffer
	if _, err := Pack(&buf, paths, Meta{Kind: "shards"}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// FuzzUnpack throws arbitrary bytes at the archive reader.
func FuzzUnpack(f *testing.F) {
	f.Add(seedBundle(f, map[string]string{"a.core": "hello"}))
	f.Add(seedBundle(f, map[string]string{"a.core": "hello", "b.core": "world"}))
	f.Add(seedBundle(f, map[string]string{"empty.core": ""}))
	f.Add([]byte{})
	f.Add([]byte("not a bundle"))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		man, err := Unpack(bytes.NewReader(data), dir)
		if err != nil {
			return // rejecting malformed input is the correct outcome
		}
		if man == nil {
			t.Fatal("Unpack returned a nil manifest with no error")
		}
		// Nothing may escape the destination directory — the classic archive bug.
		// Every extracted path must resolve inside dir.
		root, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Fatalf("resolving temp dir: %v", err)
		}
		err = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			real, err := filepath.EvalSymlinks(p)
			if err != nil {
				return nil // a dangling link is not an escape by itself
			}
			if rel, err := filepath.Rel(root, real); err == nil {
				if rel == ".." || len(rel) > 2 && rel[:3] == "../" {
					t.Fatalf("Unpack wrote outside the destination: %s → %s", p, real)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking the destination: %v", err)
		}
		// A manifest that verified must describe what is on disk.
		for _, b := range man.Blobs {
			if b.Bytes < 0 || b.Records < 0 {
				t.Fatalf("manifest reports negative sizes: %+v", b)
			}
			if b.Name == "" {
				t.Fatalf("manifest blob has no name: %+v", b)
			}
		}
	})
}

// FuzzPackUnpackRoundTrip drives arbitrary file content through Pack and back out
// through Unpack. Content is ours rather than hostile, but a size or CRC mismatch
// that silently corrupted a shipped shard would be the worst kind of bug to find in
// production.
func FuzzPackUnpackRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte{}, []byte{})
	f.Add(bytes.Repeat([]byte{0}, 4096), []byte{0xff})

	f.Fuzz(func(t *testing.T, a, b []byte) {
		src := t.TempDir()
		pa := filepath.Join(src, "a.core")
		pb := filepath.Join(src, "b.core")
		if err := os.WriteFile(pa, a, 0o644); err != nil {
			t.Skip()
		}
		if err := os.WriteFile(pb, b, 0o644); err != nil {
			t.Skip()
		}
		var buf bytes.Buffer
		if _, err := Pack(&buf, []string{pa, pb}, Meta{Kind: "shards"}); err != nil {
			return // Pack refusing something is a decision, not a crash
		}
		dst := t.TempDir()
		if _, err := Unpack(bytes.NewReader(buf.Bytes()), dst); err != nil {
			t.Fatalf("a bundle Pack produced does not Unpack: %v", err)
		}
		for name, want := range map[string][]byte{"a.core": a, "b.core": b} {
			got, err := os.ReadFile(filepath.Join(dst, name))
			if err != nil {
				t.Fatalf("%s missing after round-trip: %v", name, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s changed across the round-trip: %d bytes in, %d out", name, len(want), len(got))
			}
		}
	})
}

// FuzzUnpackTruncated feeds every prefix of a valid bundle. A truncated archive must
// be an error, never a partial extraction reported as success — that is how a
// half-transferred shard would get treated as complete.
func FuzzUnpackTruncated(f *testing.F) {
	full := seedBundle(f, map[string]string{"x.core": "0123456789abcdef"})
	f.Add(uint16(0))
	f.Add(uint16(len(full) / 2))
	f.Add(uint16(len(full) - 1))

	f.Fuzz(func(t *testing.T, cut uint16) {
		n := int(cut)
		if n >= len(full) {
			return // that is the whole bundle, not a truncation
		}
		dir := t.TempDir()
		if _, err := Unpack(bytes.NewReader(full[:n]), dir); err == nil {
			t.Fatalf("Unpack accepted a bundle truncated to %d of %d bytes", n, len(full))
		}
	})
}

// TestUnpackRejectsTailTruncation is the regression for the trailer check. tar ends
// at its two zero blocks, leaving the gzip stream unread, and gzip.Reader.Close does
// not validate CRC32/ISIZE — so removing any of the final ~13 bytes (tar padding +
// gzip footer) used to verify clean: every blob's own CRC matched, because none of
// their bytes were gone. A truncated transfer read as a complete bundle.
func TestUnpackRejectsTailTruncation(t *testing.T) {
	full := seedBundle(t, map[string]string{"x.core": "0123456789abcdef"})
	for cut := 1; cut <= 64 && cut < len(full); cut++ {
		dir := t.TempDir()
		if _, err := Unpack(bytes.NewReader(full[:len(full)-cut]), dir); err == nil {
			t.Errorf("Unpack accepted a bundle with its last %d byte(s) removed", cut)
		}
	}
	// The intact bundle still unpacks, so this is not rejecting everything.
	dir := t.TempDir()
	if _, err := Unpack(bytes.NewReader(full), dir); err != nil {
		t.Fatalf("the intact bundle no longer unpacks: %v", err)
	}
}
