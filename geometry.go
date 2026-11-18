package core

import (
	"encoding/binary"
	"io"
	"os"
)

// Header is the parsed first-page header of a store file, as read by ReadHeader.
// The geometry fields (Magic, Version, KeyLen, ValLen, SlotSize, and the region
// offsets) are identical across the shards of one sharded store; Count and Tomb
// are the per-shard live-entry and tombstone counters.
type Header struct {
	Magic    uint32 // magic value, spells "HTOA"
	Version  uint32 // on-disk format version
	KeyLen   int    // key length in bytes
	ValLen   int    // value length in bytes (0 = key-only set)
	Capacity uint64 // slot capacity (power of two)
	Count    uint64 // live entries
	Tomb     uint64 // tombstoned (deleted) slots
	CtrlOff  uint64 // byte offset of the control array
	VerOff   uint64 // byte offset of the version array (0 if valLen <= 8)
	SlotsOff uint64 // byte offset of the slots region
	SlotSize uint64 // bytes per slot (keyLen + valLen)
	Checksum uint64 // fnv64a over the immutable geometry fields
}

// ReadHeader reads and validates the full header of a single store file — a Map or
// Shared file, or one shard of a sharded store (e.g. "<prefix>.000") — without
// mapping the whole store. The magic and the geometry checksum are validated, so a
// corrupt or foreign file returns ErrCorrupt rather than bogus fields.
func ReadHeader(file string) (Header, error) {
	f, err := os.Open(file)
	if err != nil {
		return Header{}, err
	}
	defer f.Close()

	h := make([]byte, PageSize)
	if _, err := io.ReadFull(f, h); err != nil {
		return Header{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint32(h[hMagic:]) != magicValue {
		return Header{}, ErrCorrupt
	}
	if headerChecksum(h) != binary.LittleEndian.Uint64(h[hChecksum:]) {
		return Header{}, ErrCorrupt
	}
	return Header{
		Magic:    binary.LittleEndian.Uint32(h[hMagic:]),
		Version:  binary.LittleEndian.Uint32(h[hVersion:]),
		KeyLen:   int(binary.LittleEndian.Uint32(h[hKeyLen:])),
		ValLen:   int(binary.LittleEndian.Uint32(h[hValLen:])),
		Capacity: binary.LittleEndian.Uint64(h[hCapacity:]),
		Count:    binary.LittleEndian.Uint64(h[hCount:]),
		Tomb:     binary.LittleEndian.Uint64(h[hTomb:]),
		CtrlOff:  binary.LittleEndian.Uint64(h[hCtrlOff:]),
		VerOff:   binary.LittleEndian.Uint64(h[hVerOff:]),
		SlotsOff: binary.LittleEndian.Uint64(h[hSlotsOff:]),
		SlotSize: binary.LittleEndian.Uint64(h[hSlotSize:]),
		Checksum: binary.LittleEndian.Uint64(h[hChecksum:]),
	}, nil
}

// Geometry reads the key and value lengths recorded in an existing store file's
// header, without mapping the whole store. file is a single store file: a Map or
// Shared file, or one shard of a sharded store (e.g. "<prefix>.000"). The magic
// and the geometry checksum are validated, so a corrupt or foreign file returns
// an error rather than a bogus size.
//
// It lets a caller open a store without already knowing its geometry — read the
// lengths from the file, then pass them to Open/OpenSharded (which still validate
// the header against the values supplied). For the full header, use ReadHeader.
func Geometry(file string) (keyLen, valLen int, err error) {
	h, err := ReadHeader(file)
	if err != nil {
		return 0, 0, err
	}
	return h.KeyLen, h.ValLen, nil
}
