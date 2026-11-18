package main

// Integration tests that drive the *built* core binary (and server) as a user
// would — the only layer that exercises flag/env parsing, command wiring, and the
// manifest contract end to end. These are what would have caught the push-parts
// $PATH collision (invisible to library tests). Skipped under -short.

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var (
	coreBin string // built in TestMain
	srvBin  string
)

func TestMain(m *testing.M) {
	// Building the binaries is the slow part; skip it entirely under -short, where
	// every test below skips anyway.
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "core-cli-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	coreBin = filepath.Join(dir, "core")
	srvBin = filepath.Join(dir, "coresrv")
	for _, b := range []struct{ out, pkg string }{
		{coreBin, "."},
		{srvBin, "github.com/netstar-labs/core/server"},
	} {
		if out, err := exec.Command("go", "build", "-o", b.out, b.pkg).CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", b.pkg, err, out)
			os.RemoveAll(dir)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runCore runs the core binary and fails the test on a non-zero exit.
func runCore(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(coreBin, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("core %s\n  failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

var digestRe = regexp.MustCompile(`digest:\s*([0-9a-f]+)`)

func digestOf(t *testing.T, store string) string {
	t.Helper()
	out := runCore(t, "digest", "-store", store, "-shards", "8")
	mm := digestRe.FindStringSubmatch(out)
	if mm == nil {
		t.Fatalf("no digest in output: %q", out)
	}
	return mm[1]
}

// writeDataset writes n packed 32-byte-key/8-byte-value records. val(i) supplies
// each value, so a mutated snapshot can be produced from the same keys.
func writeDataset(t *testing.T, path string, n int, val func(i int) uint64) {
	t.Helper()
	buf := make([]byte, 40*n)
	for i := 0; i < n; i++ {
		rec := buf[i*40:]
		binary.LittleEndian.PutUint64(rec, uint64(i+1)) // distinct, non-zero key
		binary.LittleEndian.PutUint64(rec[32:], val(i))
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func ingestBin(t *testing.T, data, store string) {
	t.Helper()
	runCore(t, "ingest-bin", "-data", data, "-store", store, "-shards", "8", "-keylen", "32", "-vallen", "8")
}

// TestCLIExportAutoIngest: export-parts with no -reserve auto-derives capPerShard,
// records it (and omits reserve), and ingest-parts rebuilds an identical store from
// the manifest alone.
func TestCLIExportAutoIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("builds binaries")
	}
	dir := t.TempDir()
	data := filepath.Join(dir, "d.bin")
	writeDataset(t, data, 20000, func(i int) uint64 { return uint64(i * 3) })
	src := filepath.Join(dir, "src.core")
	ingestBin(t, data, src)

	part := filepath.Join(dir, "part")
	out := runCore(t, "export-parts", "-store", src, "-out", part, "-parts", "16")
	if !strings.Contains(out, "auto-derived capPerShard=") {
		t.Fatalf("expected auto-derive notice, got: %q", out)
	}
	man, err := os.ReadFile(part + ".manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(man), `"capPerShard"`) {
		t.Fatalf("manifest missing capPerShard: %s", man)
	}
	if strings.Contains(string(man), `"reserve"`) {
		t.Fatalf("auto manifest should omit reserve: %s", man)
	}

	rebuilt := filepath.Join(dir, "rebuilt.core")
	runCore(t, "ingest-parts", "-parts", part, "-store", rebuilt)
	if a, b := digestOf(t, src), digestOf(t, rebuilt); a != b {
		t.Fatalf("rebuilt digest %s != source %s", b, a)
	}
}

// TestCLIDeltaLike: export a master, align the current side with -like, diff, and
// apply the patch to a rebuilt-from-master store — it must become the current store.
// Exercises the golden-master path without any shared reserve.
func TestCLIDeltaLike(t *testing.T) {
	if testing.Short() {
		t.Skip("builds binaries")
	}
	dir := t.TempDir()
	const n = 16000

	masterData := filepath.Join(dir, "m.bin")
	writeDataset(t, masterData, n, func(i int) uint64 { return uint64(i) })
	master := filepath.Join(dir, "master.core")
	ingestBin(t, masterData, master)

	// current: change every 4th value, append 4000 new keys.
	curData := filepath.Join(dir, "c.bin")
	writeDataset(t, curData, n+4000, func(i int) uint64 {
		if i < n && i%4 == 0 {
			return uint64(i * 7)
		}
		return uint64(i)
	})
	current := filepath.Join(dir, "current.core")
	ingestBin(t, curData, current)

	old := filepath.Join(dir, "old")
	newp := filepath.Join(dir, "new")
	runCore(t, "export-parts", "-store", master, "-out", old, "-parts", "16")
	out := runCore(t, "export-parts", "-store", current, "-out", newp, "-like", old)
	if !strings.Contains(out, "aligning to") {
		t.Fatalf("expected -like alignment notice, got: %q", out)
	}

	patch := filepath.Join(dir, "patch.tar.gz")
	runCore(t, "delta", old, newp, "-store", master, "-out", patch)

	far := filepath.Join(dir, "far.core")
	runCore(t, "ingest-parts", "-parts", old, "-store", far) // rebuild master state
	runCore(t, "patch-apply", "-in", patch, "-store", far)
	if a, b := digestOf(t, current), digestOf(t, far); a != b {
		t.Fatalf("after patch, far digest %s != current %s", b, a)
	}
}

// TestCLIPushPartsServer: stream a parts set to a running server and verify the
// server's store matches the source. Guards the push-parts URL/env regression.
func TestCLIPushPartsServer(t *testing.T) {
	if testing.Short() {
		t.Skip("builds binaries, starts a server")
	}
	dir := t.TempDir()
	data := filepath.Join(dir, "d.bin")
	writeDataset(t, data, 20000, func(i int) uint64 { return uint64(i + 1) })
	src := filepath.Join(dir, "src.core")
	ingestBin(t, data, src)
	part := filepath.Join(dir, "part")
	runCore(t, "export-parts", "-store", src, "-out", part, "-parts", "16")
	srcDigest := digestOf(t, src)

	addr := freeAddr(t)
	srvStore := filepath.Join(dir, "srv.core")
	srv := exec.Command(srvBin, "-addr", addr, "-store", srvStore,
		"-shards", "8", "-reserve", "50000", "-keylen", "32", "-vallen", "8", "-nosync")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Process.Kill(); _ = srv.Wait() }()

	base := "http://" + addr
	waitReady(t, base+"/job/digest")
	runCore(t, "push-parts", "-parts", part, "-url", base)

	// Stop the server (release the mmap), then read its store digest.
	_ = srv.Process.Kill()
	_ = srv.Wait()
	if got := digestOf(t, srvStore); got != srcDigest {
		t.Fatalf("server store digest %s != source %s", got, srcDigest)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitReady(t *testing.T, url string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server not ready at %s", url)
}
