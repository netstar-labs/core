package bundle

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/netstar-labs/core"
)

// writeParts creates n part files "<dir>/part.pNNN", each recs records of
// recordSize bytes, filled with a per-file byte pattern. Returns their paths.
func writeParts(t *testing.T, dir string, n, recs, recordSize int) []string {
	t.Helper()
	var paths []string
	for p := 0; p < n; p++ {
		buf := make([]byte, recs*recordSize)
		for i := range buf {
			buf[i] = byte(p*7 + i) // deterministic, file-distinct content
		}
		path := filepath.Join(dir, "part.p"+string(rune('0'+p)))
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}

func TestPackUnpackRoundTrip(t *testing.T) {
	const (
		n, recs, recordSize = 3, 10, 16
		shards, reserve     = 4, 500
	)
	src := t.TempDir()
	files := writeParts(t, src, n, recs, recordSize)

	meta := Meta{
		PartsManifest: core.PartsManifest{
			KeyLen: 8, ValLen: 8, RecordSize: recordSize,
			Shards: shards, Reserve: reserve, Parts: n,
		},
		Kind:      "parts",
		Digest:    "0123456789abcdef",
		GzipLevel: -1,
	}

	var buf bytes.Buffer
	packed, err := Pack(&buf, files, meta)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if packed.Format != Format {
		t.Errorf("Format = %q, want %q", packed.Format, Format)
	}
	if packed.Prefix != "part" {
		t.Errorf("Prefix = %q, want part", packed.Prefix)
	}
	if want := n * recs; packed.Records != want {
		t.Errorf("Records = %d, want %d", packed.Records, want)
	}
	if len(packed.Blobs) != n {
		t.Fatalf("Blobs = %d, want %d", len(packed.Blobs), n)
	}
	for _, b := range packed.Blobs {
		if b.Records != recs {
			t.Errorf("blob %s Records = %d, want %d", b.Name, b.Records, recs)
		}
	}

	dst := t.TempDir()
	got, err := Unpack(bytes.NewReader(buf.Bytes()), dst)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if got.Digest != meta.Digest || got.Shards != shards || got.Reserve != reserve {
		t.Errorf("manifest geometry mismatch: %+v", got)
	}
	// Every extracted blob must be byte-identical to its source.
	for _, f := range files {
		want, _ := os.ReadFile(f)
		have, err := os.ReadFile(filepath.Join(dst, filepath.Base(f)))
		if err != nil {
			t.Fatalf("read extracted %s: %v", f, err)
		}
		if !bytes.Equal(want, have) {
			t.Errorf("blob %s round-trip mismatch", filepath.Base(f))
		}
	}
}

func TestPackValidation(t *testing.T) {
	src := t.TempDir()
	files := writeParts(t, src, 1, 10, 16)

	if _, err := Pack(&bytes.Buffer{}, files, Meta{Kind: "bogus"}); err == nil {
		t.Error("Pack accepted an invalid kind")
	}
	if _, err := Pack(&bytes.Buffer{}, nil, Meta{Kind: "parts", PartsManifest: core.PartsManifest{RecordSize: 16}}); err == nil {
		t.Error("Pack accepted an empty file list")
	}
	// recordSize=7 does not divide the 160-byte part → size check fires.
	meta := Meta{Kind: "parts", PartsManifest: core.PartsManifest{RecordSize: 7}}
	if _, err := Pack(&bytes.Buffer{}, files, meta); err == nil {
		t.Error("Pack accepted a part whose size is not a multiple of RecordSize")
	}
}

func TestUnpackDetectsCorruption(t *testing.T) {
	src := t.TempDir()
	files := writeParts(t, src, 2, 8, 16)
	meta := Meta{Kind: "parts", PartsManifest: core.PartsManifest{RecordSize: 16}}

	var buf bytes.Buffer
	if _, err := Pack(&buf, files, meta); err != nil {
		t.Fatal(err)
	}

	// Decompress, flip a byte inside a blob's data (offset 600 lands in the
	// payload, not the trailing manifest), recompress. The manifest still carries
	// the original CRC-32C, so Unpack must detect the altered blob.
	gzr, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tarBytes, err := io.ReadAll(gzr)
	if err != nil {
		t.Fatal(err)
	}
	tarBytes[600] ^= 0xff
	var corrupt bytes.Buffer
	gzw := gzip.NewWriter(&corrupt)
	if _, err := gzw.Write(tarBytes); err != nil {
		t.Fatal(err)
	}
	if err := gzw.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Unpack(bytes.NewReader(corrupt.Bytes()), t.TempDir()); err == nil {
		t.Error("Unpack accepted a corrupted bundle")
	}
}
