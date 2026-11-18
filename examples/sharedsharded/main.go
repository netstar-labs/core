// Example: the *Sharded backend opened cross-process — sharded AND shared.
//
// OpenSharedSharded combines both axes: N independent shards (concurrent
// writers, bounded resizes) AND a MAP_SHARED file set so other processes can
// read the same data. Run it with:
//
//	go run ./examples/sharedsharded
//
// Like the shared example, this re-execs itself so the reader is a genuinely
// separate process: the parent bulk-loads across shards as the writer, then
// spawns a child reader against the same path set.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/netstar-labs/core"
)

const (
	keyLen = 8
	valLen = 8
	shards = 8
)

func main() {
	role := flag.String("role", "writer", "writer (parent) or reader (child)")
	path := flag.String("path", "", "shared sharded base path (set by the parent)")
	flag.Parse()

	if *role == "reader" {
		reader(*path)
		return
	}
	writer()
}

func writer() {
	dir, err := os.MkdirTemp("", "core-example-sharedsharded")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "kv")

	sh, err := core.OpenSharedSharded(path, keyLen, valLen, shards, 1_000_000, core.WithNoSync())
	if err != nil {
		log.Fatal(err)
	}
	defer sh.Close()

	// Bulk-load a packed record buffer across all shards in parallel.
	const n = 50_000
	if err := sh.Reserve(n); err != nil {
		log.Fatal(err)
	}
	rec := keyLen + valLen
	buf := make([]byte, n*rec)
	for i := 0; i < n; i++ {
		off := i * rec
		binary.BigEndian.PutUint64(buf[off:], uint64(i))
		binary.LittleEndian.PutUint64(buf[off+keyLen:], uint64(i*100))
	}
	if _, err := sh.PutRecords(buf); err != nil {
		log.Fatal(err)
	}
	// Sync every shard so a separate process sees a complete, durable load.
	if err := sh.Sync(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[writer pid=%d] loaded %d entries across %d shards, digest=%#x\n",
		os.Getpid(), sh.Len(), sh.Shards(), sh.Digest())

	cmd := exec.Command(os.Args[0], "-role=reader", "-path="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("reader process failed: %v", err)
	}
	fmt.Println("[writer] reader finished. Done.")
}

func reader(path string) {
	if path == "" {
		log.Fatal("reader: -path is required")
	}
	sh, err := core.OpenSharedSharded(path, keyLen, valLen, shards, 1_000_000)
	if err != nil {
		log.Fatal(err)
	}
	defer sh.Close()

	dst := make([]byte, valLen)
	for _, i := range []uint64{0, 123, 49_999} {
		key := binary.BigEndian.AppendUint64(nil, i)
		if sh.GetInto(key, dst) {
			fmt.Printf("[reader pid=%d] %d -> %d\n", os.Getpid(), i, binary.LittleEndian.Uint64(dst))
		}
	}
	fmt.Printf("[reader pid=%d] Len=%d digest=%#x\n", os.Getpid(), sh.Len(), sh.Digest())
}
