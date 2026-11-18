package patch

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/bundle"
)

// ManifestFormat tags the patch sidecar.
const ManifestFormat = "core-patch/1"

// Blob names inside a patch bundle (alongside bundle's own manifest.json).
const (
	sidecarName = "patch.json"
	upsertsName = "upserts.bin"
	deletesName = "deletes.bin"
)

// Manifest is the patch sidecar carried inside a bundle. It records the geometry,
// the digest of the store the patch was built against (for verification before
// apply), and the upsert/delete counts. The blob bytes live beside it; per-blob
// transport integrity (CRC-32C) is the enclosing bundle's job.
type Manifest struct {
	Format     string `json:"format"`               // ManifestFormat
	KeyLen     int    `json:"keyLen"`               // bytes of key per record
	ValLen     int    `json:"valLen"`               // bytes of value per upsert
	BaseDigest string `json:"baseDigest,omitempty"` // target store digest when built, "" if unknown
	Upserts    int    `json:"upserts"`              // upsert record count
	Deletes    int    `json:"deletes"`              // delete key count
}

// WriteBundle packs a snapshot into a transportable tar.gz on w: an upsert blob,
// a delete blob, and the patch sidecar, wrapped by core/bundle (which adds a
// per-blob CRC-32C and its own manifest). gzipLevel is the gzip level (-1 =
// default). It returns the patch manifest. WriteBundle does not close w.
func WriteBundle(w io.Writer, s *Snapshot, gzipLevel int) (*Manifest, error) {
	man := &Manifest{
		Format: ManifestFormat, KeyLen: s.KeyLen, ValLen: s.ValLen,
		BaseDigest: s.BaseDigest, Upserts: s.NumUpserts(), Deletes: s.NumDeletes(),
	}
	mb, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return nil, err
	}

	// bundle.Pack streams from files, so stage the three blobs in a temp dir. A
	// patch is interval-bounded, so this is cheap and the dir is always removed.
	dir, err := os.MkdirTemp("", "core-patch-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	for _, f := range []struct {
		name string
		data []byte
	}{{sidecarName, mb}, {upsertsName, s.Upserts}, {deletesName, s.Deletes}} {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, 0o644); err != nil {
			return nil, err
		}
	}

	_, err = bundle.Pack(w,
		[]string{filepath.Join(dir, sidecarName), filepath.Join(dir, upsertsName), filepath.Join(dir, deletesName)},
		bundle.Meta{
			PartsManifest: core.PartsManifest{KeyLen: s.KeyLen, ValLen: s.ValLen, RecordSize: s.KeyLen + s.ValLen, Prefix: "patch"},
			Kind:          "patch",
			Digest:        s.BaseDigest,
			GzipLevel:     gzipLevel,
		})
	if err != nil {
		return nil, err
	}
	return man, nil
}

// OpenBundle reads a patch bundle from r, extracting and CRC-verifying its blobs
// into dir (via core/bundle), and reconstructs the Snapshot and its Manifest. It
// checks that the blob sizes agree with the sidecar's declared counts. It does
// not apply the patch or verify BaseDigest — the caller owns that (see Apply).
func OpenBundle(r io.Reader, dir string) (*Snapshot, *Manifest, error) {
	if _, err := bundle.Unpack(r, dir); err != nil { // verifies every blob's size + CRC
		return nil, nil, err
	}
	mb, err := os.ReadFile(filepath.Join(dir, sidecarName))
	if err != nil {
		return nil, nil, err
	}
	var man Manifest
	if err := json.Unmarshal(mb, &man); err != nil {
		return nil, nil, fmt.Errorf("patch: parsing %s: %w", sidecarName, err)
	}
	if man.Format != ManifestFormat {
		return nil, nil, fmt.Errorf("patch: unknown patch format %q", man.Format)
	}
	up, err := os.ReadFile(filepath.Join(dir, upsertsName))
	if err != nil {
		return nil, nil, err
	}
	del, err := os.ReadFile(filepath.Join(dir, deletesName))
	if err != nil {
		return nil, nil, err
	}
	s := &Snapshot{KeyLen: man.KeyLen, ValLen: man.ValLen, BaseDigest: man.BaseDigest, Upserts: up, Deletes: del}
	if s.NumUpserts() != man.Upserts || s.NumDeletes() != man.Deletes {
		return nil, nil, fmt.Errorf("patch: blob sizes disagree with manifest (upserts %d/%d, deletes %d/%d)",
			s.NumUpserts(), man.Upserts, s.NumDeletes(), man.Deletes)
	}
	return s, &man, nil
}
