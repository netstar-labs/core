package patch

import (
	"bytes"
	"fmt"
	"os"

	"github.com/netstar-labs/core"
)

// Diff computes the change set that turns the store snapshot described by
// oldPrefix's partition files into newPrefix's, and returns it as a coalesced
// Snapshot: a key present only in new (or whose value changed) is an upsert; a key
// present only in old is a delete.
//
// It works part-by-part. Part assignment is a pure function of the key and the
// geometry (shards, reserve, parts), so two stores exported with the *same*
// geometry are part-aligned — corresponding parts hold the same slice of the
// keyspace and a key never moves between parts across snapshots. Diff therefore
// diffs each aligned pair independently; the union is the complete delta. Both
// parts sets MUST share geometry: Diff reads each manifest and refuses a mismatch.
//
// Only one part pair is resident at a time, so memory is bounded by a single part
// (1/parts of the store), not the whole store. baseDigest is recorded on the
// Snapshot — pass the OLD store's digest so a later Apply (or patch-apply) can
// verify the target is actually at the old state before applying the delta.
func Diff(oldPrefix, newPrefix, baseDigest string) (*Snapshot, error) {
	om, err := core.ReadManifest(core.ManifestPath(oldPrefix))
	if err != nil {
		return nil, fmt.Errorf("patch: old manifest: %w", err)
	}
	nm, err := core.ReadManifest(core.ManifestPath(newPrefix))
	if err != nil {
		return nil, fmt.Errorf("patch: new manifest: %w", err)
	}
	// Align on the effective per-shard capacity, not Reserve: an auto (capacity-
	// derived) export records Reserve 0, so comparing Reserve would let two auto
	// sets at different capacities — different masks — pass as aligned and produce a
	// silently wrong diff. EffectiveCapPerShard collapses both encodings (reserve
	// and capPerShard) to the one number that actually sizes the mask.
	if om.KeyLen != nm.KeyLen || om.ValLen != nm.ValLen ||
		om.Shards != nm.Shards || om.EffectiveCapPerShard() != nm.EffectiveCapPerShard() || om.Parts != nm.Parts {
		return nil, fmt.Errorf("patch: parts geometry differs — old(keyLen=%d valLen=%d shards=%d capPerShard=%d parts=%d) vs new(keyLen=%d valLen=%d shards=%d capPerShard=%d parts=%d); cannot align (export the new side with -like <old>, or a matching -reserve)",
			om.KeyLen, om.ValLen, om.Shards, om.EffectiveCapPerShard(), om.Parts,
			nm.KeyLen, nm.ValLen, nm.Shards, nm.EffectiveCapPerShard(), nm.Parts)
	}

	b := NewBuilder(om.KeyLen, om.ValLen)
	for i := 0; i < om.Parts; i++ {
		oldPath := fmt.Sprintf("%s.p%03d", oldPrefix, i)
		newPath := fmt.Sprintf("%s.p%03d", newPrefix, i)
		if err := diffPart(b, oldPath, newPath, om.KeyLen, om.ValLen); err != nil {
			return nil, err
		}
	}
	return b.Snapshot(baseDigest), nil
}

// diffPart diffs one aligned part pair into b. It indexes the old part by key
// (values reference the old buffer, valid until the next iteration — Builder.Upsert
// copies what it keeps), then streams the new part: a key that is new or whose
// value changed becomes an upsert; a key in old but not new becomes a delete.
func diffPart(b *Builder, oldPath, newPath string, keyLen, valLen int) error {
	rec := keyLen + valLen
	oldBuf, err := os.ReadFile(oldPath)
	if err != nil {
		return err
	}
	if rec == 0 || len(oldBuf)%rec != 0 {
		return fmt.Errorf("patch: %s size %d is not a multiple of record size %d", oldPath, len(oldBuf), rec)
	}
	old := make(map[string][]byte, len(oldBuf)/rec)
	for off := 0; off+rec <= len(oldBuf); off += rec {
		old[string(oldBuf[off:off+keyLen])] = oldBuf[off+keyLen : off+rec]
	}

	newBuf, err := os.ReadFile(newPath)
	if err != nil {
		return err
	}
	if len(newBuf)%rec != 0 {
		return fmt.Errorf("patch: %s size %d is not a multiple of record size %d", newPath, len(newBuf), rec)
	}
	seen := make(map[string]struct{}, len(newBuf)/rec)
	for off := 0; off+rec <= len(newBuf); off += rec {
		key := newBuf[off : off+keyLen]
		val := newBuf[off+keyLen : off+rec]
		seen[string(key)] = struct{}{}
		if ov, ok := old[string(key)]; !ok || !bytes.Equal(ov, val) {
			if err := b.Upsert(key, val); err != nil { // insert or update
				return err
			}
		}
	}
	for ks := range old {
		if _, ok := seen[ks]; !ok {
			if err := b.Delete([]byte(ks)); err != nil {
				return err
			}
		}
	}
	return nil
}
