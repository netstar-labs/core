// Example: staged, out-of-RAM bulk build with Partitioner + IngestParts.
//
// On a box where a store is larger than RAM, inserting in hash order scatters
// writes across the whole file and the page cache thrashes. The fix is to stage
// the load in two passes:
//
//  1. Partition — bucket the input into `parts` files by the high bits of each
//     key's destination group, so each part maps to one contiguous slice of every
//     shard's slots.
//  2. Ingest — load the parts in order. Each part only writes its own slice, so a
//     shard's dirty window stays ~1/parts of its slots at a time: random writeback
//     becomes a forward sweep that the kernel can drain as you go.
//
// A manifest sidecar makes the parts self-describing, so the build step needs no
// geometry repeated by hand — and parts produced once can be ingested later (or
// elsewhere) with nothing but the manifest. Run it with:
//
//	go run ./examples/partition
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/netstar-labs/core"
)

const (
	keyLen  = 8
	valLen  = 8
	shards  = 8
	reserve = 500_000 // the eventual entry count — sizes the store and the group mask
	parts   = 16      // staged load splits the work into 16 forward-sweep windows
	nrec    = 300_000
)

func main() {
	dir, err := os.MkdirTemp("", "core-example-partition")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	prefix := filepath.Join(dir, "part")   // parts: <prefix>.pNNN (+ manifest)
	store := filepath.Join(dir, "kv.core") // built store: <store>.NNN per shard

	// --- Pass 1: partition ------------------------------------------------
	// A Partitioner owns the geometry — per-shard capacity, the group mask the
	// table places by, the partition split — so the bucketing here lands records
	// in exactly the slice the ingest will write them to. shards+reserve here MUST
	// equal the shards+reserve the store is built with below; the manifest carries
	// them so you can't get that wrong.
	p, err := core.NewPartitioner(core.ReserveCapPerShard(reserve, shards), parts)
	if err != nil {
		log.Fatal(err)
	}

	files := make([]*os.File, parts)
	counts := make([]int, parts)
	for i := range files {
		if files[i], err = os.Create(fmt.Sprintf("%s.p%03d", prefix, i)); err != nil {
			log.Fatal(err)
		}
	}

	// Parallel partition pipeline:
	//
	//   generator (main) → genCh → K encode workers → partCh[pi] → P part writers
	//
	// Encode+route runs across K workers, each with its own rec scratch and per-part
	// accumulation buffers — p.PartKey is not shared state so no mutex needed. Each
	// part file has exactly ONE writer goroutine — files are not O_APPEND, concurrent
	// fd writes would race the shared offset — and that writer buffers through a
	// bufio.Writer, coalescing the old per-record write() into block writes. Record
	// order within a part is not preserved; ingest places by key hash, not order.
	const (
		batchSize = 8192
		chanDepth = 64
		writeBuf  = 1 << 20
	)
	recLen := keyLen + valLen

	type batch struct{ start, end int }
	genCh := make(chan batch, chanDepth)
	partCh := make([]chan []byte, parts)
	for i := range partCh {
		partCh[i] = make(chan []byte, chanDepth)
	}

	var writerWg sync.WaitGroup
	for i := range files {
		writerWg.Add(1)
		go func(i int) {
			defer writerWg.Done()
			bw := bufio.NewWriterSize(files[i], writeBuf)
			var nbytes int
			for block := range partCh[i] {
				if _, err := bw.Write(block); err != nil {
					log.Fatal(err)
				}
				nbytes += len(block)
			}
			if err := bw.Flush(); err != nil {
				log.Fatal(err)
			}
			counts[i] = nbytes / recLen
		}(i)
	}

	encWorkers := runtime.GOMAXPROCS(0)
	var encWg sync.WaitGroup
	for w := 0; w < encWorkers; w++ {
		encWg.Add(1)
		go func() {
			defer encWg.Done()
			rec := make([]byte, recLen)
			bufs := make([][]byte, parts)
			for b := range genCh {
				for n := b.start; n < b.end; n++ {
					binary.BigEndian.PutUint64(rec, uint64(n))
					pi := p.PartKey(rec[:keyLen])
					binary.LittleEndian.PutUint64(rec[keyLen:], uint64(n)*2)
					bufs[pi] = append(bufs[pi], rec...)
				}
				for pi, buf := range bufs {
					if len(buf) > 0 {
						partCh[pi] <- buf
						bufs[pi] = nil // writer owns the sent block
					}
				}
			}
		}()
	}

	for n := 0; n < nrec; n += batchSize {
		end := n + batchSize
		if end > nrec {
			end = nrec
		}
		genCh <- batch{n, end}
	}
	close(genCh)
	encWg.Wait() // encoders done → no more sends to partCh
	for _, ch := range partCh {
		close(ch)
	}
	writerWg.Wait() // all blocks flushed to disk
	for _, f := range files {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}

	// Write the manifest sidecar so the parts describe their own geometry.
	manPath := core.ManifestPath(prefix)
	if err := core.WriteManifest(manPath, core.PartsManifest{
		KeyLen: keyLen, ValLen: valLen, RecordSize: keyLen + valLen,
		Shards: shards, CapPerShard: core.ReserveCapPerShard(reserve, shards), Parts: parts,
		Records: nrec, Prefix: filepath.Base(prefix), Counts: counts,
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("partitioned %d records into %d parts (+ %s)\n", nrec, parts, filepath.Base(manPath))

	// --- Pass 2: ingest ---------------------------------------------------
	// IngestManifest reads the geometry from the manifest and loads the parts in
	// order into a freshly pre-sized store — no shards/reserve/keyLen repeated.
	// This is the step you could run later or on another machine from just the
	// parts directory. flushPerPart=true Syncs after each part, so resident dirty
	// pages stay bounded to one window even when the store is larger than RAM.
	m, st, err := core.IngestManifest(manPath, store, 1024, true, core.WithNoSync())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ingested %d records (%d new) from manifest: shards=%d capPerShard=%d\n",
		st.Total, st.Inserted, m.Shards, m.CapPerShard)

	// --- Verify -----------------------------------------------------------
	sh, err := core.OpenSharded(store, keyLen, valLen, shards, 1024)
	if err != nil {
		log.Fatal(err)
	}
	defer sh.Close()
	dst := make([]byte, valLen)
	probe := binary.BigEndian.AppendUint64(nil, 42)
	if sh.GetInto(probe, dst) {
		fmt.Printf("GetInto(42) = %d (expected 84)\n", binary.LittleEndian.Uint64(dst))
	}
	fmt.Printf("Len = %d, Digest = %#x\nDone.\n", sh.Len(), sh.Digest())
}
