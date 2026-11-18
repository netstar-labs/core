package core

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Two-process tests for Shared. Everything else exercises it with two handles in one
// address space, where the advisory flock is uncontended and both "processes" share
// one page table — which is precisely the setup that cannot fail the way a real
// deployment fails.
//
// The harness re-execs this test binary in a role, the technique cmd/core's CLI tests
// already use here. TestMain routes a role invocation away from the test runner, so a
// child never tries to run the suite.

var (
	roleFlag  = flag.String("core.role", "", "internal: run as a two-process test child")
	storeFlag = flag.String("core.store", "", "internal: store path for the child")
	nFlag     = flag.Int("core.n", 0, "internal: record count for the child")
	keyFlag   = flag.Int("core.keylen", 8, "internal: key length for the child")
	valFlag   = flag.Int("core.vallen", 8, "internal: value length for the child")
)

func TestMain(m *testing.M) {
	flag.Parse()
	if *roleFlag != "" {
		os.Exit(runChildRole(*roleFlag, *storeFlag, *nFlag, *keyFlag, *valFlag))
	}
	os.Exit(m.Run())
}

func childKey(i int, keyLen int) []byte {
	k := make([]byte, keyLen)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func childVal(i int, valLen int) []byte {
	v := make([]byte, valLen)
	binary.LittleEndian.PutUint64(v, uint64(i)*7919)
	return v
}

// runChildRole is the child's entry point. It prints one line of result to stdout so
// the parent can assert on it, and exits non-zero on failure.
func runChildRole(role, path string, n, keyLen, valLen int) int {
	switch role {
	case "write":
		// A second process writing the same inode. Every Put takes the cross-process
		// flock, so this contends with the parent for real.
		s, err := OpenShared(path, keyLen, valLen, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child open: %v\n", err)
			return 1
		}
		defer s.Close()
		for i := 0; i < n; i++ {
			if _, err := s.Put(childKey(i, keyLen), childVal(i, valLen)); err != nil {
				fmt.Fprintf(os.Stderr, "child put %d: %v\n", i, err)
				return 1
			}
		}
		if err := s.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "child sync: %v\n", err)
			return 1
		}
		fmt.Printf("wrote %d len %d digest %016x\n", n, s.Len(), s.Digest())
		return 0

	case "grow":
		// Force several resizes from a second process, so the parent has to adopt new
		// generations mid-read. Each resize replaces the data inode.
		s, err := OpenShared(path, keyLen, valLen, 8)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child open: %v\n", err)
			return 1
		}
		defer s.Close()
		for i := 0; i < n; i++ {
			if _, err := s.Put(childKey(i, keyLen), childVal(i, valLen)); err != nil {
				fmt.Fprintf(os.Stderr, "child put %d: %v\n", i, err)
				return 1
			}
		}
		fmt.Printf("grew to len %d digest %016x\n", s.Len(), s.Digest())
		return 0

	case "read":
		// Read back what another process wrote, adopting its generation.
		s, err := OpenShared(path, keyLen, valLen, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child open: %v\n", err)
			return 1
		}
		defer s.Close()
		missing := 0
		got := make([]byte, valLen)
		for i := 0; i < n; i++ {
			if !s.GetInto(childKey(i, keyLen), got) {
				missing++
				continue
			}
			want := childVal(i, valLen)
			for b := range want {
				if got[b] != want[b] {
					fmt.Fprintf(os.Stderr, "child value mismatch at %d\n", i)
					return 1
				}
			}
		}
		fmt.Printf("read %d missing %d len %d digest %016x\n", n, missing, s.Len(), s.Digest())
		return 0

	case "holdlock":
		// Take the writer lock and hold it, so the parent can prove the flock actually
		// excludes a second writer across processes.
		s, err := OpenShared(path, keyLen, valLen, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child open: %v\n", err)
			return 1
		}
		defer s.Close()
		if err := s.meta.lockWriter(); err != nil {
			fmt.Fprintf(os.Stderr, "child lock: %v\n", err)
			return 1
		}
		fmt.Println("locked")
		os.Stdout.Sync()
		time.Sleep(3 * time.Second) // parent measures its wait against this
		s.meta.unlockWriter()
		fmt.Println("unlocked")
		return 0
	}
	fmt.Fprintf(os.Stderr, "unknown role %q\n", role)
	return 2
}

// child re-execs this test binary in a role and returns its stdout.
func child(t *testing.T, role, path string, n, keyLen, valLen int) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0],
		"-core.role", role,
		"-core.store", path,
		"-core.n", fmt.Sprint(n),
		"-core.keylen", fmt.Sprint(keyLen),
		"-core.vallen", fmt.Sprint(valLen),
	)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// A store written entirely by another process must read back correctly here — the
// baseline cross-process case, with two real page tables and two real flocks.
func TestSharedWriteInChildReadInParent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kv.core")
	const n = 500

	out, err := child(t, "write", path, n, 8, 8)
	if err != nil {
		t.Fatalf("writer child failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, fmt.Sprintf("wrote %d len %d", n, n)) {
		t.Fatalf("writer child output: %s", out)
	}

	s, err := OpenShared(path, 8, 8, 64)
	if err != nil {
		t.Fatalf("parent open: %v", err)
	}
	defer s.Close()
	if s.Len() != n {
		t.Errorf("parent sees %d entries, want %d", s.Len(), n)
	}
	got := make([]byte, 8)
	for i := 0; i < n; i++ {
		if !s.GetInto(childKey(i, 8), got) {
			t.Fatalf("parent cannot read key %d written by the child", i)
		}
		if binary.LittleEndian.Uint64(got) != uint64(i)*7919 {
			t.Fatalf("parent read the wrong value for key %d", i)
		}
	}
}

// The parent holds a store open while a child resizes it repeatedly. The parent must
// adopt each new generation and keep reading correctly — this is checkRemap under a
// real inode swap, which a single-process test cannot produce.
func TestSharedParentAdoptsChildResizes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kv.core")

	// Start small so the child's writes force several doublings.
	s, err := OpenShared(path, 8, 8, 8)
	if err != nil {
		t.Fatalf("parent open: %v", err)
	}
	defer s.Close()
	if _, err := s.Put(childKey(0, 8), childVal(0, 8)); err != nil {
		t.Fatal(err)
	}
	capBefore := s.cur.Load().capacity

	const n = 2000
	out, err := child(t, "grow", path, n, 8, 8)
	if err != nil {
		t.Fatalf("grow child failed: %v\n%s", err, out)
	}

	// The parent has not touched the store since; a read must adopt the new inode.
	got := make([]byte, 8)
	for i := 0; i < n; i++ {
		if !s.GetInto(childKey(i, 8), got) {
			t.Fatalf("parent lost key %d across the child's resizes (out: %s)", i, out)
		}
		if binary.LittleEndian.Uint64(got) != uint64(i)*7919 {
			t.Fatalf("parent read a stale value for key %d — remap did not take", i)
		}
	}
	if s.Len() != n {
		t.Errorf("parent Len() = %d after remap, want %d", s.Len(), n)
	}
	capAfter := s.cur.Load().capacity
	if capAfter <= capBefore {
		t.Errorf("capacity did not grow across the child's resizes: %d → %d", capBefore, capAfter)
	}
	t.Logf("adopted the child's growth: capacity %d → %d, %d entries", capBefore, capAfter, s.Len())
}

// Both processes write disjoint halves concurrently. The flock must serialise the
// structural mutations so neither loses entries.
func TestSharedConcurrentWritersBothProcesses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kv.core")
	const half = 400

	s, err := OpenShared(path, 8, 8, 2048)
	if err != nil {
		t.Fatalf("parent open: %v", err)
	}
	defer s.Close()

	// Child writes keys [0,half); parent writes [half,2*half) at the same time.
	type result struct {
		out string
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := child(t, "write", path, half, 8, 8)
		done <- result{out, err}
	}()

	for i := half; i < 2*half; i++ {
		if _, err := s.Put(childKey(i, 8), childVal(i, 8)); err != nil {
			t.Fatalf("parent put %d: %v", i, err)
		}
	}
	r := <-done
	if r.err != nil {
		t.Fatalf("concurrent writer child failed: %v\n%s", r.err, r.out)
	}

	// Every key from both processes must be present.
	got := make([]byte, 8)
	for i := 0; i < 2*half; i++ {
		if !s.GetInto(childKey(i, 8), got) {
			t.Fatalf("key %d lost to a concurrent cross-process write (child: %s)", i, r.out)
		}
	}
	if s.Len() != 2*half {
		t.Errorf("Len() = %d, want %d — a concurrent writer clobbered the counter",
			s.Len(), 2*half)
	}
}

// The advisory flock must actually exclude a second writer across processes. The
// child takes the lock and holds it for 3s; the parent's Put must block until it is
// released rather than proceed in parallel.
func TestSharedWriterLockExcludesAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kv.core")

	s, err := OpenShared(path, 8, 8, 64)
	if err != nil {
		t.Fatalf("parent open: %v", err)
	}
	defer s.Close()

	cmd := exec.Command(os.Args[0], "-core.role", "holdlock", "-core.store", path,
		"-core.n", "0", "-core.keylen", "8", "-core.vallen", "8")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()

	// Wait for the child to report that it holds the lock.
	buf := make([]byte, 64)
	deadline := time.Now().Add(10 * time.Second)
	held := false
	for time.Now().Before(deadline) {
		n, _ := stdout.Read(buf)
		if n > 0 && strings.Contains(string(buf[:n]), "locked") {
			held = true
			break
		}
	}
	if !held {
		t.Fatal("child never reported holding the writer lock")
	}

	start := time.Now()
	if _, err := s.Put(childKey(1, 8), childVal(1, 8)); err != nil {
		t.Fatalf("parent put: %v", err)
	}
	waited := time.Since(start)

	// The child holds it for 3s. A Put that returns immediately means the flock is
	// not excluding across processes at all — which every single-process test would
	// have missed, since a process can always re-acquire its own flock.
	if waited < 1500*time.Millisecond {
		t.Errorf("parent's Put returned after %v while another process held the writer lock — flock is not excluding", waited)
	}
	t.Logf("parent blocked %v on the cross-process writer lock", waited)
}

// A 32-byte-key store written by a child must open and read in the parent, so the
// geometry is genuinely read from the file rather than assumed from the flags.
func TestSharedGeometryCrossesProcesses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wide.core")
	const n = 100

	out, err := child(t, "write", path, n, 32, 24)
	if err != nil {
		t.Fatalf("writer child failed: %v\n%s", err, out)
	}
	k, v, err := Geometry(path)
	if err != nil {
		t.Fatalf("Geometry: %v", err)
	}
	if k != 32 || v != 24 {
		t.Fatalf("Geometry reports %d/%d, want 32/24", k, v)
	}
	// Opening with the wrong geometry must be refused, not silently misread.
	if _, err := OpenShared(path, 8, 8, 64); err == nil {
		t.Error("OpenShared accepted a store whose geometry does not match")
	}
	s, err := OpenShared(path, k, v, 64)
	if err != nil {
		t.Fatalf("open with the file's own geometry: %v", err)
	}
	defer s.Close()
	if s.Len() != n {
		t.Errorf("Len() = %d, want %d", s.Len(), n)
	}
}

// A round trip in the other direction: the parent writes, a child reads and reports.
func TestSharedParentWriteChildRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kv.core")
	const n = 300

	s, err := OpenShared(path, 8, 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := s.Put(childKey(i, 8), childVal(i, 8)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	parentDigest := s.Digest()
	s.Close() // release the mapping before the child reads

	out, err := child(t, "read", path, n, 8, 8)
	if err != nil {
		t.Fatalf("reader child failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "missing 0") {
		t.Errorf("child could not read everything the parent wrote: %s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("digest %016x", parentDigest)) {
		t.Errorf("child computed a different digest than the parent (%016x): %s", parentDigest, out)
	}
}
