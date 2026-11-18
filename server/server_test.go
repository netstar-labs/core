package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/netstar-labs/core/patch"
)

// Tests for the HTTP front end. No refactor was needed to write these: NewWorker
// takes a path and routes() returns a plain http.Handler, so httptest drives the
// whole surface directly. The package simply had no tests.
//
// The one real hazard is that the key/value geometry lives in package-level vars
// (keyLen, valLen, recordSize) which setGeometry mutates, so a test that changes
// geometry leaks into every later test in the binary. newServer restores it.

// newServer brings up a Worker over a temp store and returns it with a live
// httptest server. Geometry is set explicitly and restored on cleanup.
func newServer(t *testing.T, shards, klen, vlen int, withPatch bool) (*Worker, *httptest.Server) {
	t.Helper()
	prevK, prevV := keyLen, valLen
	t.Cleanup(func() { setGeometry(prevK, prevV) })
	setGeometry(klen, vlen)

	dir := t.TempDir()
	w, err := NewWorker(filepath.Join(dir, "kv.core"), shards, 1024, 0, klen, vlen, false)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	if withPatch {
		lg, err := patch.Open(filepath.Join(dir, "wal"), klen, vlen)
		if err != nil {
			t.Fatalf("patch.Open: %v", err)
		}
		w.log = lg
		t.Cleanup(func() { lg.Close() })
	}

	srv := httptest.NewServer(w.routes())
	t.Cleanup(srv.Close)
	return w, srv
}

// decode reads a JSON body into v and fails on a status outside want.
func decode(t *testing.T, resp *http.Response, want int, v any) {
	t.Helper()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("status %d, want %d — body: %s", resp.StatusCode, want, body)
	}
	if v != nil {
		if err := json.Unmarshal(body, v); err != nil {
			t.Fatalf("decode %q: %v", body, err)
		}
	}
}

func post(t *testing.T, url, ctype, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, ctype, strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

// startFlushLoop runs patchFlushLoop and returns a stop func that cancels it and
// WAITS for it to return. The wait is the point: cancelling alone lets the loop sit
// inside Flush while the test's cleanup closes the store underneath it, which the
// race detector reports as mmap.RW.Close racing RW.Sync. main() needs the same wait,
// and did not have one until this test found it.
func startFlushLoop(t *testing.T, w *Worker, flushSecs, flushAt int) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); patchFlushLoop(ctx, w, flushSecs, flushAt) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("patchFlushLoop did not return within 5s of cancellation")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestJobPutGetRoundTrip(t *testing.T) {
	_, srv := newServer(t, 4, 8, 8, false)

	var created Job
	decode(t, post(t, srv.URL+"/job", "application/json", `{"item":"alice","value":42}`),
		http.StatusCreated, &created)
	if !created.Found || created.Value != 42 || created.Status != http.StatusCreated {
		t.Errorf("first POST: %+v", created)
	}

	// A second POST of the same item is an update, not a create.
	var updated Job
	decode(t, post(t, srv.URL+"/job", "application/json", `{"item":"alice","value":43}`),
		http.StatusOK, &updated)
	if updated.Status != http.StatusOK {
		t.Errorf("second POST status = %d, want 200 (update)", updated.Status)
	}

	resp, err := http.Get(srv.URL + "/job/alice")
	if err != nil {
		t.Fatal(err)
	}
	var got Job
	decode(t, resp, http.StatusOK, &got)
	if !got.Found || got.Value != 43 {
		t.Errorf("GET after update: %+v, want value 43", got)
	}

	// A miss is a 404 with Found false, not an error.
	resp, err = http.Get(srv.URL + "/job/nobody")
	if err != nil {
		t.Fatal(err)
	}
	var miss Job
	decode(t, resp, http.StatusNotFound, &miss)
	if miss.Found {
		t.Error("absent item reported Found")
	}
}

func TestJobRejectsBadInput(t *testing.T) {
	_, srv := newServer(t, 2, 8, 8, false)
	for name, body := range map[string]string{
		"not json":      `{`,
		"missing item":  `{"value":1}`,
		"empty item":    `{"item":"","value":1}`,
		"wrong type":    `{"item":123}`,
		"array not obj": `[{"item":"a"}]`,
	} {
		resp := post(t, srv.URL+"/job", "application/json", body)
		func() {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s: status %d, want 400", name, resp.StatusCode)
			}
		}()
	}
}

func TestBulk(t *testing.T) {
	_, srv := newServer(t, 4, 8, 8, false)

	var out map[string]any
	decode(t, post(t, srv.URL+"/job/bulk", "application/json",
		`[{"item":"a","value":1},{"item":"b","value":2},{"item":"","value":3},{"item":"c","value":4}]`),
		http.StatusOK, &out)

	// The entry with no item is dropped, not stored and not an error.
	if out["received"] != 4.0 || out["stored"] != 3.0 || out["inserted"] != 3.0 {
		t.Errorf("bulk counts = %v, want received 4 / stored 3 / inserted 3", out)
	}
	for _, item := range []string{"a", "b", "c"} {
		resp, err := http.Get(srv.URL + "/job/" + item)
		if err != nil {
			t.Fatal(err)
		}
		var j Job
		decode(t, resp, http.StatusOK, &j)
	}

	// An empty array and a JSON null are both no-ops rather than failures — the
	// handler slices jobs[:0], which is legal on a nil slice.
	for _, body := range []string{`[]`, `null`} {
		var o map[string]any
		decode(t, post(t, srv.URL+"/job/bulk", "application/json", body), http.StatusOK, &o)
		if o["stored"] != 0.0 {
			t.Errorf("bulk %s: stored = %v, want 0", body, o["stored"])
		}
	}
	// Malformed JSON is a 400.
	resp := post(t, srv.URL+"/job/bulk", "application/json", `[{`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("malformed bulk: status %d, want 400", resp.StatusCode)
	}
}

func TestStreamJSONAndBinary(t *testing.T) {
	w, srv := newServer(t, 4, 8, 8, false)

	// NDJSON ingest.
	var js map[string]any
	decode(t, post(t, srv.URL+"/job/stream", "application/json",
		"{\"item\":\"j1\",\"value\":1}\n{\"item\":\"j2\",\"value\":2}\n"),
		http.StatusOK, &js)
	if js["format"] != "json" || js["inserted"] != 2.0 {
		t.Errorf("json stream = %v", js)
	}

	// Binary ingest: packed keyLen+valLen records, keys already hashed.
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		key := make([]byte, keyLen)
		makeKey(key, fmt.Sprintf("b%d", i))
		val := make([]byte, valLen)
		binary.LittleEndian.PutUint64(val, uint64(100+i))
		buf.Write(key)
		buf.Write(val)
	}
	var bs map[string]any
	decode(t, post(t, srv.URL+"/job/stream", "application/octet-stream", buf.String()),
		http.StatusOK, &bs)
	if bs["format"] != "binary" || bs["records"] != 3.0 || bs["inserted"] != 3.0 {
		t.Errorf("binary stream = %v", bs)
	}
	if w.Len() != 5 {
		t.Errorf("store has %d entries, want 5", w.Len())
	}

	// A truncated binary record is a 400 that still reports what landed.
	short := buf.Bytes()[:recordSize+3]
	var trunc map[string]any
	decode(t, post(t, srv.URL+"/job/stream", "application/octet-stream", string(short)),
		http.StatusBadRequest, &trunc)
	if _, ok := trunc["error"]; !ok {
		t.Errorf("truncated stream did not report an error: %v", trunc)
	}
}

func TestStreamShardRouting(t *testing.T) {
	w, srv := newServer(t, 4, 8, 8, false)

	// ?shard=N puts the whole stream in that shard, bypassing hash routing.
	var out map[string]any
	decode(t, post(t, srv.URL+"/job/stream?shard=2", "application/json",
		"{\"item\":\"s1\",\"value\":1}\n{\"item\":\"s2\",\"value\":2}\n"), http.StatusOK, &out)
	if out["shard"] != 2.0 || out["inserted"] != 2.0 {
		t.Errorf("shard stream = %v", out)
	}
	if got := w.store.Shard(2).Len(); got != 2 {
		t.Errorf("shard 2 has %d entries, want 2", got)
	}

	// Out-of-range and non-numeric shards are 400s, naming the valid range.
	for _, q := range []string{"?shard=4", "?shard=-1", "?shard=abc"} {
		resp := post(t, srv.URL+"/job/stream"+q, "application/json", "{}")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, resp.StatusCode)
		}
		if !strings.Contains(string(body), "0..3") {
			t.Errorf("%s: error %q does not name the range", q, body)
		}
	}
}

func TestExportBinaryAndJSON(t *testing.T) {
	w, srv := newServer(t, 2, 8, 8, false)
	const n = 20
	for i := 0; i < n; i++ {
		resp := post(t, srv.URL+"/job", "application/json",
			fmt.Sprintf(`{"item":"e%d","value":%d}`, i, i))
		resp.Body.Close()
	}

	// Binary export declares an exact Content-Length and delivers exactly that.
	resp, err := http.Get(srv.URL + "/job/export")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if want := n * recordSize; len(body) != want {
		t.Errorf("binary export is %d bytes, want %d", len(body), want)
	}
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(n*recordSize) {
		t.Errorf("Content-Length = %q, want %d", got, n*recordSize)
	}

	// NDJSON export: one Job per line, no Content-Length (variable length).
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/job/export", nil)
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	nd, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q", ct)
	}
	lines := 0
	for _, l := range strings.Split(strings.TrimSpace(string(nd)), "\n") {
		if l == "" {
			continue
		}
		var j Jobz
		if err := json.Unmarshal([]byte(l), &j); err != nil {
			t.Fatalf("export line %q: %v", l, err)
		}
		lines++
	}
	if lines != n {
		t.Errorf("NDJSON export has %d lines, want %d", lines, n)
	}

	// A per-shard export sums back to the whole store.
	total := 0
	for s := 0; s < w.store.Shards(); s++ {
		resp, err := http.Get(fmt.Sprintf("%s/job/export?shard=%d", srv.URL, s))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if len(b)%recordSize != 0 {
			t.Fatalf("shard %d export is not a record multiple: %d bytes", s, len(b))
		}
		total += len(b) / recordSize
	}
	if total != n {
		t.Errorf("per-shard exports total %d records, want %d", total, n)
	}
}

// Export → re-import must be lossless and digest-identical, which is the property
// the binary format exists for.
func TestExportImportRoundTripDigest(t *testing.T) {
	src, srcSrv := newServer(t, 4, 8, 8, false)
	for i := 0; i < 50; i++ {
		resp := post(t, srcSrv.URL+"/job", "application/json",
			fmt.Sprintf(`{"item":"r%d","value":%d}`, i, i*7))
		resp.Body.Close()
	}
	resp, err := http.Get(srcSrv.URL + "/job/export")
	if err != nil {
		t.Fatal(err)
	}
	dump, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	dst, dstSrv := newServer(t, 4, 8, 8, false)
	var in map[string]any
	decode(t, post(t, dstSrv.URL+"/job/stream", "application/octet-stream", string(dump)),
		http.StatusOK, &in)
	if in["inserted"] != 50.0 {
		t.Fatalf("re-import inserted %v, want 50", in["inserted"])
	}
	if src.store.Digest() != dst.store.Digest() {
		t.Errorf("digest differs after round-trip: %016x vs %016x",
			src.store.Digest(), dst.store.Digest())
	}

	// And the digest endpoint reports the same value.
	var d1, d2 map[string]any
	r1, _ := http.Get(srcSrv.URL + "/job/digest")
	decode(t, r1, http.StatusOK, &d1)
	r2, _ := http.Get(dstSrv.URL + "/job/digest")
	decode(t, r2, http.StatusOK, &d2)
	if d1["digest"] != d2["digest"] {
		t.Errorf("digest endpoint differs: %v vs %v", d1["digest"], d2["digest"])
	}
	if d1["entries"] != 50.0 {
		t.Errorf("digest entries = %v, want 50", d1["entries"])
	}
}

func TestStatusAndPatchRouteGating(t *testing.T) {
	w, srv := newServer(t, 2, 8, 8, false)
	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	decode(t, resp, http.StatusOK, &st)
	if st["ok"] != true || st["entries"] != float64(w.Len()) {
		t.Errorf("status = %v", st)
	}

	// Without -patch the route is not registered at all.
	resp = post(t, srv.URL+"/patch", "application/json", `{"item":"a","value":1}`)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Error("POST /patch succeeded with no patch buffer configured")
	}
}

// The patch buffer is accumulate-then-apply: a POSTed change is durable in the WAL
// and counted as pending, but not visible to a read until a flush folds it in.
func TestPatchBufferAccumulatesThenApplies(t *testing.T) {
	w, srv := newServer(t, 2, 8, 8, true)

	var out map[string]any
	decode(t, post(t, srv.URL+"/patch", "application/json",
		"{\"item\":\"p1\",\"value\":11}\n{\"item\":\"p2\",\"value\":22}\n"),
		http.StatusOK, &out)
	if out["received"] != 2.0 || out["upserts"] != 2.0 || out["pending"] != 2.0 {
		t.Errorf("patch response = %v", out)
	}

	// Not yet visible — this is the documented behaviour, not a bug.
	resp, err := http.Get(srv.URL + "/job/p1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("a pending patch was visible before flush (status %d)", resp.StatusCode)
	}

	if _, err := w.log.Flush(w.store); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var j Job
	r, _ := http.Get(srv.URL + "/job/p1")
	decode(t, r, http.StatusOK, &j)
	if j.Value != 11 {
		t.Errorf("after flush p1 = %d, want 11", j.Value)
	}

	// A delete op removes an applied entry.
	decode(t, post(t, srv.URL+"/patch", "application/json",
		"{\"item\":\"p1\",\"op\":\"delete\"}\n"), http.StatusOK, nil)
	if _, err := w.log.Flush(w.store); err != nil {
		t.Fatalf("flush delete: %v", err)
	}
	r, _ = http.Get(srv.URL + "/job/p1")
	r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("deleted item still present (status %d)", r.StatusCode)
	}
}

// patchFlushLoop is drivable directly — it takes a context and the worker, so no
// server plumbing is involved.
func TestPatchFlushLoopSizeTrigger(t *testing.T) {
	w, srv := newServer(t, 2, 8, 8, true)
	decode(t, post(t, srv.URL+"/patch", "application/json",
		"{\"item\":\"f1\",\"value\":1}\n{\"item\":\"f2\",\"value\":2}\n"), http.StatusOK, nil)

	// flushSecs 0 disables the time trigger; flushAt 1 fires on the next tick.
	stop := startFlushLoop(t, w, 0, 1)

	// Wait on the STORE, not on Pending(). Flush zeroes pending when it rotates the
	// segment, which happens before the window is applied — so pending == 0 is not a
	// readiness signal. Waiting on it here produced a 404 and looked exactly like a
	// dropped change; see patch.Log.Pending's doc.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && w.Len() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if w.Len() == 0 {
		pending, _ := w.log.Pending()
		t.Fatalf("flush loop applied nothing after 5s (pending=%d)", pending)
	}
	stop() // out of Flush before the store is closed
	var j Job
	r, _ := http.Get(srv.URL + "/job/f1")
	decode(t, r, http.StatusOK, &j)
	if j.Value != 1 {
		t.Errorf("f1 = %d after loop flush, want 1", j.Value)
	}
}

// The flush loop must return on context cancellation rather than leak.
func TestPatchFlushLoopStopsOnCancel(t *testing.T) {
	w, _ := newServer(t, 2, 8, 8, true)
	stop := startFlushLoop(t, w, 1, 10)
	stop() // fails the test if the loop does not return promptly
}

// A 32-byte key store is the -keylen 32 default (SHA-256 of the item); exercise it
// so the geometry switch is covered, not just the 8-byte xxHash path.
func TestSHA256KeyGeometry(t *testing.T) {
	w, srv := newServer(t, 2, 32, 8, false)
	if keyLen != 32 || recordSize != 40 {
		t.Fatalf("geometry not applied: keyLen=%d recordSize=%d", keyLen, recordSize)
	}
	decode(t, post(t, srv.URL+"/job", "application/json", `{"item":"sha","value":9}`),
		http.StatusCreated, nil)
	var j Job
	r, _ := http.Get(srv.URL + "/job/sha")
	decode(t, r, http.StatusOK, &j)
	if j.Value != 9 {
		t.Errorf("value = %d, want 9", j.Value)
	}
	if w.Len() != 1 {
		t.Errorf("entries = %d, want 1", w.Len())
	}
}

// An item that is exactly a hex-encoded key is used verbatim rather than hashed —
// which is how an exported binary key round-trips back through the JSON API.
func TestHexItemIsUsedVerbatim(t *testing.T) {
	_, srv := newServer(t, 2, 8, 8, false)
	key := make([]byte, 8)
	makeKey(key, "some-item")
	hexKey := fmt.Sprintf("%x", key)

	decode(t, post(t, srv.URL+"/job", "application/json",
		fmt.Sprintf(`{"item":%q,"value":77}`, hexKey)), http.StatusCreated, nil)

	// The hashed form and the hex form must be the same slot.
	var j Job
	r, _ := http.Get(srv.URL + "/job/some-item")
	decode(t, r, http.StatusOK, &j)
	if j.Value != 77 {
		t.Errorf("hex item did not alias the hashed key: got %d, want 77", j.Value)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	_, srv := newServer(t, 2, 8, 8, false)
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/job/x", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// ServeMux has no DELETE pattern registered, so it 405s with an Allow header.
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("DELETE /job/x: status %d, want 405", resp.StatusCode)
	}
}

// The binary+?shard=N path (shardPutRecords) is the parallel-transfer ingest: a
// client pulls shard K's binary export and pushes it straight back into shard K
// without re-routing. Exercised separately from the JSON shard path.
func TestStreamBinaryIntoOneShard(t *testing.T) {
	w, srv := newServer(t, 4, 8, 8, false)
	if got := w.Shards(); got != 4 {
		t.Fatalf("Shards() = %d, want 4", got)
	}

	var buf bytes.Buffer
	for i := 0; i < 5; i++ {
		key := make([]byte, keyLen)
		makeKey(key, fmt.Sprintf("direct%d", i))
		val := make([]byte, valLen)
		binary.LittleEndian.PutUint64(val, uint64(i))
		buf.Write(key)
		buf.Write(val)
	}
	var out map[string]any
	decode(t, post(t, srv.URL+"/job/stream?shard=1", "application/octet-stream", buf.String()),
		http.StatusOK, &out)
	if out["format"] != "binary" || out["records"] != 5.0 || out["shard"] != 1.0 {
		t.Fatalf("binary shard stream = %v", out)
	}
	// All five landed in shard 1, regardless of where their hash would route them.
	if got := w.store.Shard(1).Len(); got != 5 {
		t.Errorf("shard 1 has %d entries, want 5", got)
	}
	if w.Len() != 5 {
		t.Errorf("store has %d entries, want 5", w.Len())
	}
}

// drain is the shutdown sequence main() runs after the server stops accepting: it
// must fold the patch buffer in and close the WAL, leaving nothing un-applied.
func TestDrainAppliesAndClosesTheLog(t *testing.T) {
	w, srv := newServer(t, 2, 8, 8, true)
	decode(t, post(t, srv.URL+"/patch", "application/json",
		"{\"item\":\"d1\",\"value\":5}\n{\"item\":\"d2\",\"value\":6}\n"), http.StatusOK, nil)
	if w.Len() != 0 {
		t.Fatalf("patch applied before drain: %d entries", w.Len())
	}

	if err := drain(w); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if w.Len() != 2 {
		t.Errorf("after drain: %d entries, want 2", w.Len())
	}
	var j Job
	r, _ := http.Get(srv.URL + "/job/d1")
	decode(t, r, http.StatusOK, &j)
	if j.Value != 5 {
		t.Errorf("d1 = %d after drain, want 5", j.Value)
	}
	// A second drain on a closed log must not panic; it reports rather than crashes.
	_ = drain(w)
}

// drain must be a no-op (not a nil-pointer panic) when no patch buffer is configured.
func TestDrainWithoutPatchBuffer(t *testing.T) {
	w, _ := newServer(t, 2, 8, 8, false)
	if err := drain(w); err != nil {
		t.Errorf("drain with no log: %v", err)
	}
}
