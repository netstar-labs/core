// Example: the *Shared backend — one file, multiple processes.
//
// OpenShared maps a single .core file MAP_SHARED so several processes can read
// it concurrently while one process writes. Run it with:
//
//	go run ./examples/shared
//
// To make the cross-process behaviour real (rather than two handles in one
// process) this program re-execs itself: the parent is the writer, and it spawns
// a separate child process as the reader against the same file while the writer
// is still open.
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

func main() {
	role := flag.String("role", "writer", "writer (parent) or reader (child)")
	path := flag.String("path", "", "shared .core file path (set by the parent)")
	flag.Parse()

	if *role == "reader" {
		reader(*path)
		return
	}
	writer()
}

// writer creates the file, populates it, and then — while keeping it open —
// launches a separate reader process against the same path.
func writer() {
	dir, err := os.MkdirTemp("", "core-example-shared")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "kv.core")

	m, err := core.OpenShared(path, 8, 8, 1_000_000)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	for i := uint64(0); i < 5; i++ {
		key := binary.BigEndian.AppendUint64(nil, i)
		val := binary.LittleEndian.AppendUint64(nil, i*100)
		if _, err := m.Put(key, val); err != nil {
			log.Fatal(err)
		}
	}
	// Sync so the child process is guaranteed to observe the writes.
	if err := m.Sync(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[writer pid=%d] wrote %d entries, digest=%#x\n", os.Getpid(), m.Len(), m.Digest())

	// Spawn a separate process as the reader. The writer stays open, so this
	// demonstrates shared readers concurrent with a live writer.
	cmd := exec.Command(os.Args[0], "-role=reader", "-path="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("reader process failed: %v", err)
	}
	fmt.Println("[writer] reader finished. Done.")
}

// reader runs in a separate process and opens the same file read-side.
func reader(path string) {
	if path == "" {
		log.Fatal("reader: -path is required")
	}
	m, err := core.OpenShared(path, 8, 8, 1_000_000)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	dst := make([]byte, 8)
	for i := uint64(0); i < 5; i++ {
		key := binary.BigEndian.AppendUint64(nil, i)
		if m.GetInto(key, dst) {
			fmt.Printf("[reader pid=%d] %d -> %d\n", os.Getpid(), i, binary.LittleEndian.Uint64(dst))
		}
	}
	// The digest matches the writer's: same content, independently computed.
	fmt.Printf("[reader pid=%d] Len=%d digest=%#x\n", os.Getpid(), m.Len(), m.Digest())
}
