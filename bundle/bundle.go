// Package bundle packs a sharded store's partition files (or its live shard
// files) into a single compressed tar.gz for transport, and unpacks them back,
// using only the Go standard library (archive/tar + compress/gzip). Per-blob
// CRC-32C guards transport integrity; the store's order-independent content
// Digest (recorded in the manifest by the caller) verifies logical content after
// a rebuild — independent of compression, blob order, or shard count.
//
// Pack streams to any io.Writer (a file, an object-store object, an HTTP body),
// so a bundle can be produced for transport without a temp file. Unpack streams
// from any io.Reader and verifies every blob against the manifest as it extracts;
// rebuilding a store from the extracted parts (and verifying the Digest) is the
// caller's job, via core.IngestParts / core.IngestManifest and core.StoreDigest.
package bundle

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/netstar-labs/core"
)

// Format is the bundle format tag written into every manifest.
const Format = "core-pack/1"

// ManifestName is the in-tar name of the JSON manifest sidecar.
const ManifestName = "manifest.json"

// crcTable is the CRC-32C (Castagnoli) table used for per-blob transport CRCs.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Blob describes one file carried in a bundle, as recorded in the manifest.
type Blob struct {
	Name    string `json:"name"`
	Bytes   int64  `json:"bytes"`
	Records int    `json:"records"` // 0 for kind=shards
	CRC32C  string `json:"crc32c"`
}

// Meta is the caller-supplied geometry and tags for a bundle. The geometry is the
// embedded core.PartsManifest, so it shares one definition with the partition
// step instead of a parallel copy; its fields flatten into the manifest JSON.
type Meta struct {
	core.PartsManifest        // keyLen,valLen,recordSize,shards,reserve,parts,records,prefix,counts
	Kind               string // "parts" | "shards"
	Digest             string // hex content digest (core.StoreDigest), "" if none
	GzipLevel          int    // gzip level 1..9, or -1 for the default
}

// Manifest is the bundle's JSON sidecar. The geometry is the embedded
// core.PartsManifest; the bundle-specific Format (json:"format") shadows the
// embedded one, naming the bundle format rather than the parts format.
type Manifest struct {
	core.PartsManifest        // format,keyLen,valLen,recordSize,shards,reserve,parts,records,prefix,counts
	Format             string `json:"format"` // bundle format tag (shadows PartsManifest.Format)
	Kind               string `json:"kind"`   // "parts" | "shards"
	Digest             string `json:"digest"` // hex of the store's content Digest, "" if not computed
	GzipLevel          int    `json:"gzipLevel"`
	Blobs              []Blob `json:"blobs"`
}

// DerivePrefix recovers the common base prefix from a file path:
// "x/part.p007" -> "part" for kind=parts; "x/kv.core.003" -> "kv.core" for
// kind=shards.
func DerivePrefix(path, kind string) string {
	base := filepath.Base(path)
	if kind == "shards" {
		if i := strings.LastIndex(base, "."); i >= 0 {
			return base[:i]
		}
		return base
	}
	if i := strings.LastIndex(base, ".p"); i >= 0 {
		return base[:i]
	}
	return base
}

// Pack streams files into a tar.gz bundle written to w, then appends a
// manifest.json sidecar (written last, since its per-blob CRC-32C values are
// produced while streaming the blobs). It records meta's geometry, kind, digest
// and gzip level verbatim — it does not open the store; compute meta.Digest with
// core.StoreDigest beforehand if you want content verification on rebuild.
//
// For kind=parts each blob's size must be a multiple of meta.RecordSize, and its
// record count is recorded and summed into the manifest's Records. kind=shards
// and kind=patch carry their blobs verbatim with no record interpretation (patch
// blobs are an upsert/delete stream plus a sidecar, sized independently). files
// are sorted by name so the bundle order is deterministic. Pack does not close w.
func Pack(w io.Writer, files []string, meta Meta) (*Manifest, error) {
	switch meta.Kind {
	case "parts", "shards", "patch":
	default:
		return nil, fmt.Errorf("bundle: kind must be parts, shards, or patch, got %q", meta.Kind)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("bundle: no files to pack")
	}
	if meta.Kind == "parts" && meta.RecordSize <= 0 {
		return nil, fmt.Errorf("bundle: kind=parts needs a positive RecordSize, got %d", meta.RecordSize)
	}
	files = append([]string(nil), files...)
	sort.Strings(files)

	man := Manifest{
		PartsManifest: meta.PartsManifest,
		Format:        Format,
		Kind:          meta.Kind,
		Digest:        meta.Digest,
		GzipLevel:     meta.GzipLevel,
	}
	if man.Prefix == "" {
		man.Prefix = DerivePrefix(files[0], meta.Kind)
	}

	gzw, err := gzip.NewWriterLevel(w, meta.GzipLevel)
	if err != nil {
		return nil, err
	}
	tw := tar.NewWriter(gzw)

	for _, path := range files {
		st, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		bm := Blob{Name: filepath.Base(path), Bytes: st.Size()}
		if meta.Kind == "parts" {
			if st.Size()%int64(meta.RecordSize) != 0 {
				return nil, fmt.Errorf("bundle: %s size %d is not a multiple of record size %d", path, st.Size(), meta.RecordSize)
			}
			bm.Records = int(st.Size() / int64(meta.RecordSize))
			man.Records += bm.Records
		}
		hdr := &tar.Header{Name: bm.Name, Mode: 0o644, Size: st.Size(), ModTime: st.ModTime()}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		in, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		h := crc32.New(crcTable)
		if _, err := io.Copy(io.MultiWriter(tw, h), in); err != nil {
			in.Close()
			return nil, err
		}
		in.Close()
		bm.CRC32C = fmt.Sprintf("%08x", h.Sum32())
		man.Blobs = append(man.Blobs, bm)
	}

	// manifest last: its blob CRCs are produced while streaming the blobs above.
	mb, err := json.MarshalIndent(&man, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := tw.WriteHeader(&tar.Header{Name: ManifestName, Mode: 0o644, Size: int64(len(mb))}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(mb); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gzw.Close(); err != nil {
		return nil, err
	}
	return &man, nil
}

// Unpack reads a tar.gz bundle from r, extracting every blob into dir and
// verifying it against the manifest's size and CRC-32C. It returns the parsed
// manifest. Blob names are reduced to their base (path-traversal guard), so the
// bundle's flat layout is reproduced under dir. Unpack does not close r, and does
// not rebuild a store — do that with core.IngestParts / core.IngestManifest on
// the extracted "<Prefix>.pNNN" files (kind=parts), then core.StoreDigest to
// verify the returned manifest's Digest.
func Unpack(r io.Reader, dir string) (*Manifest, error) {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	gotCRC := map[string]string{}
	gotSize := map[string]int64{}
	var man Manifest
	haveMan := false

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name := filepath.Base(hdr.Name) // guard against path traversal
		if name == ManifestName {
			mb, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(mb, &man); err != nil {
				return nil, fmt.Errorf("bundle: parsing manifest: %w", err)
			}
			haveMan = true
			continue
		}
		dst := filepath.Join(dir, name)
		out, err := os.Create(dst)
		if err != nil {
			return nil, err
		}
		h := crc32.New(crcTable)
		n, err := io.Copy(io.MultiWriter(out, h), tr)
		out.Close()
		if err != nil {
			return nil, err
		}
		gotCRC[name] = fmt.Sprintf("%08x", h.Sum32())
		gotSize[name] = n
	}

	// Drain the gzip stream so its trailer is validated.
	//
	// tar.Next returns io.EOF at the two zero blocks that end the archive, which
	// leaves the rest of the gzip stream unread — and gzip.Reader.Close does NOT
	// check the CRC32/ISIZE trailer; only a Read that reaches io.EOF does. Without
	// this, a bundle truncated anywhere in its final ~13 bytes (tar's end padding
	// plus the gzip footer) verified clean: every blob's own CRC matched, because
	// none of their bytes were missing. For the format that ships shards to a
	// remote party, a truncated transfer reading as complete is the failure that matters.
	if _, err := io.Copy(io.Discard, gzr); err != nil {
		return nil, fmt.Errorf("bundle: truncated or corrupt archive tail: %w", err)
	}

	if !haveMan {
		return nil, fmt.Errorf("bundle: no %s in archive", ManifestName)
	}

	// Verify every manifest blob was extracted with matching size and CRC.
	var bad []string
	for _, b := range man.Blobs {
		switch {
		case gotCRC[b.Name] == "":
			bad = append(bad, fmt.Sprintf("MISSING %s", b.Name))
		case gotSize[b.Name] != b.Bytes:
			bad = append(bad, fmt.Sprintf("SIZE %s: got %d want %d", b.Name, gotSize[b.Name], b.Bytes))
		case gotCRC[b.Name] != b.CRC32C:
			bad = append(bad, fmt.Sprintf("CRC %s: got %s want %s", b.Name, gotCRC[b.Name], b.CRC32C))
		}
	}
	if len(bad) > 0 {
		return &man, fmt.Errorf("bundle: %d blob(s) failed verification: %s", len(bad), strings.Join(bad, "; "))
	}
	return &man, nil
}
