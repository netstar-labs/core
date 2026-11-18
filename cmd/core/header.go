// "header" subcommand: print a store's on-disk header in table form. Reads each
// shard's first-page header (geometry, capacity, live/tombstone counts, region
// offsets, checksum) via core.ReadHeader without mapping the whole store.
//
//	core header -store kv.core -shards 8

package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/netstar-labs/core"
)

type headerCfg struct {
	Store  string `default:"kv.core" help:"store path prefix"`
	Shards int    `help:"shard count (0 = auto-detect from <store>.NNN files)"`
}

func runHeader(c *headerCfg) {
	shards := c.Shards
	if shards <= 0 {
		if shards = detectShards(c.Store); shards == 0 {
			fatal(fmt.Errorf("no store found at %s.000", c.Store))
		}
	}

	// Read and validate every shard's header (ReadHeader checks magic + checksum).
	hdrs := make([]core.Header, shards)
	for i := 0; i < shards; i++ {
		path := fmt.Sprintf("%s.%03d", c.Store, i)
		h, err := core.ReadHeader(path)
		if err != nil {
			fatal(fmt.Errorf("shard %03d (%s): %w", i, path, err))
		}
		hdrs[i] = h
	}
	g := hdrs[0]

	// Structural geometry must be identical across shards; capacity/offsets may
	// differ legitimately and are shown per shard.
	uniform := true
	for _, h := range hdrs[1:] {
		if h.Magic != g.Magic || h.Version != g.Version ||
			h.KeyLen != g.KeyLen || h.ValLen != g.ValLen || h.SlotSize != g.SlotSize {
			uniform = false
			break
		}
	}

	fmt.Printf("\nheader — %s (%d shard%s)\n", c.Store, shards, plural(shards))

	// --- geometry (identical across shards) ---
	fmt.Println("\ngeometry")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  magic\t%#08x  (%q)\n", g.Magic, magicString(g.Magic))
	fmt.Fprintf(tw, "  version\t%d\n", g.Version)
	fmt.Fprintf(tw, "  key length\t%d B  (cmd/core hash: %s)\n", g.KeyLen, hashName(g.KeyLen))
	if g.ValLen == 0 {
		fmt.Fprintf(tw, "  value length\t0 B  (key-only set)\n")
	} else {
		fmt.Fprintf(tw, "  value length\t%d B\n", g.ValLen)
	}
	fmt.Fprintf(tw, "  slot size\t%d B  (key+value)\n", g.SlotSize)
	verNote := "none (valLen ≤ 8)"
	if g.ValLen > 8 {
		verNote = "present (seqlock; valLen > 8)"
	}
	fmt.Fprintf(tw, "  version array\t%s\n", verNote)
	tw.Flush()
	if !uniform {
		fmt.Println("  ! shards do not share one geometry — the row above is shard 000")
	}

	// --- per-shard counters / layout ---
	fmt.Println("\nshards")
	tw = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  shard\tcapacity\tcount\ttomb\tload\tslots off\ton-disk\tchecksum")
	var totCap, totCount, totTomb uint64
	var totBytes int64
	for i, h := range hdrs {
		fmt.Fprintf(tw, "  %03d\t%s\t%s\t%s\t%.3f\t%s\t%s\t%#016x\n",
			i, comma(int64(h.Capacity)), comma(int64(h.Count)), comma(int64(h.Tomb)),
			loadFactor(h.Count, h.Capacity), comma(int64(h.SlotsOff)),
			humanBytes(shardSize(c.Store, i)), h.Checksum)
		totCap += h.Capacity
		totCount += h.Count
		totTomb += h.Tomb
		totBytes += shardSize(c.Store, i)
	}
	if shards > 1 {
		fmt.Fprintf(tw, "  total\t%s\t%s\t%s\t%.3f\t\t%s\t\n",
			comma(int64(totCap)), comma(int64(totCount)), comma(int64(totTomb)),
			loadFactor(totCount, totCap), humanBytes(totBytes))
	}
	tw.Flush()

	fmt.Println("\nall shards: magic + geometry checksum verified by read")
	fmt.Println()
}

// hashName labels the key hash cmd/core derives at a given key width (a CLI
// convention — the width is what the header records, not the hash itself).
func hashName(keyLen int) string {
	switch keyLen {
	case 32:
		return "SHA-256"
	case 16:
		return "XXH3-128"
	case 8:
		return "xxHash64"
	default:
		return "raw/custom"
	}
}

// magicString renders the 4-byte magic in big-endian order, so 0x48544f41 reads
// as its "HTOA" mnemonic.
func magicString(m uint32) string {
	return string([]byte{byte(m >> 24), byte(m >> 16), byte(m >> 8), byte(m)})
}

func loadFactor(count, capacity uint64) float64 {
	if capacity == 0 {
		return 0
	}
	return float64(count) / float64(capacity)
}

func shardSize(prefix string, i int) int64 {
	if fi, err := os.Stat(fmt.Sprintf("%s.%03d", prefix, i)); err == nil {
		return fi.Size()
	}
	return 0
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
