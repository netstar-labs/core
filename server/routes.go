package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// routes wires the HTTP endpoints to the Worker. Each pattern carries its method
// so the wildcard read (GET /job/{item}) does not conflict with the literal
// sub-routes under /job/ (Go 1.22 ServeMux rejects ambiguous method/path pairs).
//
//	GET  /job/{item}                              -> returns the value for item
//	POST /job        body: {"item":"k","value":1} -> stores k -> 1
//	POST /job/bulk   body: [{"item","value"}, …]  -> sharded bulk insert
//	POST /job/stream NDJSON or octet-stream        -> streamed ingest (?shard=N)
//	GET  /job/export (Accept negotiates) (?shard=N)-> streamed dump
//	GET  /job/digest (?shard=N)                    -> content digest
//	POST /patch      NDJSON Change records          -> buffered upsert/delete (-patch only)
//	GET  /status                                   -> liveness
func (w *Worker) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /job/{item}", w.handleJob)
	mux.HandleFunc("POST /job", w.handleJob)
	mux.HandleFunc("POST /job/bulk", w.handleBulk)
	mux.HandleFunc("POST /job/stream", w.handleStream)
	mux.HandleFunc("GET /job/export", w.handleExport)
	mux.HandleFunc("GET /job/digest", w.handleDigest)
	if w.log != nil { // patch write-buffer enabled via -patch
		mux.HandleFunc("POST /patch", w.handlePatch)
	}
	mux.HandleFunc("GET /status", func(rw http.ResponseWriter, _ *http.Request) {
		writeJSON(rw, http.StatusOK, map[string]any{"ok": true, "entries": w.Len()})
	})
	return mux
}

// handlePatch streams Change records into the WAL-backed patch buffer (see the
// -patch flag). Upserts and deletes are coalesced in the Log and folded into the
// store by the background flush loop, so they become visible to reads after the
// next flush — the accumulate-then-apply pattern. The response reports the
// appended counts and the current pending (un-flushed) change count.
//
//	POST /patch  body (NDJSON): {"item":"a","value":1}\n{"item":"b","op":"delete"}
func (w *Worker) handlePatch(rw http.ResponseWriter, r *http.Request) {
	received, upserts, deletes, err := w.StreamPatch(r.Body)
	if err == nil { // group-commit: make this request's batch durable before acking
		err = w.log.Sync()
	}
	pending, _ := w.log.Pending()
	resp := map[string]any{
		"received": received, "upserts": upserts, "deletes": deletes, "pending": pending,
	}
	status := http.StatusOK
	if err != nil { // partial: report what was appended plus the error
		resp["error"] = err.Error()
		status = http.StatusBadRequest
	}
	writeJSON(rw, status, resp)
}

// parseShard reads an optional ?shard=N. ok=false means "whole store"; an
// out-of-range value is an error.
func (w *Worker) parseShard(r *http.Request) (idx int, ok bool, err error) {
	v := r.URL.Query().Get("shard")
	if len(v) == 0 {
		return -1, false, nil
	}
	n, e := strconv.Atoi(v)
	if e != nil || n < 0 || n >= w.store.Shards() {
		return 0, false, fmt.Errorf("shard must be 0..%d", w.store.Shards()-1)
	}
	return n, true, nil
}

// handleStream ingests a stream of entries without buffering the whole payload,
// negotiating the wire format on Content-Type:
//
//	application/octet-stream → packed fixed records (fast binary path)
//	anything else (json)     → newline/whitespace-delimited JSON Jobs
//
// An optional ?shard=N routes the whole stream straight into one shard (no
// re-routing) — the symmetric ingest for ?shard=N export, for parallel transfers.
func (w *Worker) handleStream(rw http.ResponseWriter, r *http.Request) {
	shard, hasShard, serr := w.parseShard(r)
	if serr != nil {
		http.Error(rw, serr.Error(), http.StatusBadRequest)
		return
	}

	// Pick the insert target: a single shard (direct) or the whole store (routed).
	putBatch := w.store.PutBatch
	putRecords := w.store.PutRecords
	if hasShard {
		sh := w.store.Shard(shard)
		putBatch = func(k, v [][]byte) (int, error) { return shardPutBatch(sh, k, v) }
		putRecords = func(buf []byte) (int, error) { return shardPutRecords(sh, buf) }
	}

	var resp map[string]any
	var err error
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/octet-stream") {
		records, inserted, e := w.StreamBinary(r.Body, 1<<16, putRecords)
		resp = map[string]any{"format": "binary", "records": records, "inserted": inserted}
		err = e
	} else {
		received, stored, inserted, e := w.StreamJSON(r.Body, 100_000, putBatch)
		resp = map[string]any{"format": "json", "received": received, "stored": stored, "inserted": inserted}
		err = e
	}
	if hasShard {
		resp["shard"] = shard
	}
	resp["entries"] = w.Len()
	status := http.StatusOK
	if err != nil { // partial ingest: report what landed plus the error
		resp["error"] = err.Error()
		status = http.StatusBadRequest
	}
	writeJSON(rw, status, resp)
}

// handleExport streams the whole store out, negotiating on Accept:
//
//	Accept: application/*json* → newline-delimited JSON Jobs (variable length)
//	otherwise                  → packed fixed records (default)
//
// The binary export declares a Content-Length of entries×recordSize, snapshotted
// up front, and caps the body to exactly that many records. A client can then
// detect truncation (dropped connection, or a concurrent delete shrinking the
// store) as a short read. The NDJSON export is variable length, so it streams
// without a Content-Length; a mid-stream error simply truncates it.
//
// An optional ?shard=N exports just that shard, so clients can pull all shards in
// parallel over separate connections.
func (w *Worker) handleExport(rw http.ResponseWriter, r *http.Request) {
	shard, hasShard, serr := w.parseShard(r)
	if serr != nil {
		http.Error(rw, serr.Error(), http.StatusBadRequest)
		return
	}
	var sc scope = w.store
	if hasShard {
		sc = w.store.Shard(shard)
	}

	if strings.Contains(r.Header.Get("Accept"), "json") {
		rw.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.ExportJSON(rw, sc)
		return
	}
	n := sc.Len() // snapshot count → declared length
	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.Itoa(n*recordSize))
	_, _ = w.ExportBinary(rw, n, sc)
}

// handleDigest returns the order-independent content digest, for verifying a
// transfer or comparing replicas. ?shard=N scopes it to one shard.
//
//	GET /job/digest            → {"digest":"<hex>","entries":N}
//	GET /job/digest?shard=K    → {"digest":"<hex>","entries":M,"shard":K}
func (w *Worker) handleDigest(rw http.ResponseWriter, r *http.Request) {
	shard, hasShard, err := w.parseShard(r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	var sc scope = w.store
	if hasShard {
		sc = w.store.Shard(shard)
	}
	resp := map[string]any{
		"digest":  fmt.Sprintf("%016x", sc.Digest()),
		"entries": sc.Len(),
	}
	if hasShard {
		resp["shard"] = shard
	}
	writeJSON(rw, http.StatusOK, resp)
}

// handleBulk stores a JSON array of Jobz in one sharded bulk operation.
//
//	POST /job/bulk  body: [{"item":"a","value":1},{"item":"b","value":2}]
func (w *Worker) handleBulk(rw http.ResponseWriter, r *http.Request) {
	var jobs []Jobz
	if err := json.NewDecoder(r.Body).Decode(&jobs); err != nil {
		http.Error(rw, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	valid := jobs[:0] // reuse backing array; drop entries without an item
	for _, j := range jobs {
		if j.Item != "" {
			valid = append(valid, j)
		}
	}
	inserted, err := w.PutBatch(valid)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(rw, http.StatusOK, map[string]any{
		"received": len(jobs),
		"stored":   len(valid),
		"inserted": inserted,
		"entries":  w.Len(),
	})
}

func (w *Worker) handleJob(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		job := w.Get(r.PathValue("item"))
		status := http.StatusOK
		if !job.Found {
			status = http.StatusNotFound
		}
		writeJSON(rw, status, job)

	case http.MethodPost:
		var job Job
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
			http.Error(rw, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if job.Item == "" {
			http.Error(rw, "missing \"item\"", http.StatusBadRequest)
			return
		}
		created, err := w.Put(job)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		job.Found = true
		if created {
			job.Status = http.StatusCreated
		} else {
			job.Status = http.StatusOK
		}
		writeJSON(rw, job.Status, job)

	default:
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeJSON(rw http.ResponseWriter, status int, v any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(v)
}
