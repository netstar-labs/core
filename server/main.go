// Command server is a minimal net/http front end for a core store: POST a Job
// to store an item->value mapping, GET it back by item, or POST a batch.
//
//	go run ./server -addr :8080 -store /tmp/srv/kv -shards 8
//	curl -s -XPOST localhost:8080/job -d '{"item":"alice","value":42}'
//	curl -s localhost:8080/job/alice
//	curl -s -XPOST localhost:8080/job/bulk -d '[{"item":"a","value":1},{"item":"b","value":2}]'
//	printf '{"item":"a","uuid":1}\n{"item":"b","uuid":2}\n' | curl -s -XPOST --data-binary @- localhost:8080/job/stream
//	curl -s -XPOST --data-binary @records.bin -H 'Content-Type: application/octet-stream' localhost:8080/job/stream
//	curl -s localhost:8080/job/export -o dump.bin                       # binary export
//	curl -s -H 'Accept: application/x-ndjson' localhost:8080/job/export # NDJSON export
//
// For a large pre-sized bulk load (e.g. applying partition files), start the server
// reserved and unsynced, then push the parts with "core push-parts":
//
//	go run ./server -addr :8080 -store kv.core -shards 8 -reserve 350000000 -nosync
//	go run ./cmd/core push-parts -parts part -url http://localhost:8080
//
// With -patch <dir> the server gains a WAL-backed patch write-buffer: POST /patch
// streams coalesced upserts/deletes into a core/patch Log, and a flush loop folds
// them into the store every -flush seconds (or at -flushat pending). The Log
// replays any un-applied window on restart, so an acked batch survives a crash:
//
//	go run ./server -addr :8080 -store kv.core -keylen 8 -vallen 8 -patch ./wal -flush 5
//	printf '{"item":"a","value":1}\n{"item":"b","op":"delete"}\n' | curl -s -XPOST --data-binary @- localhost:8080/patch
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/netstar-labs/core/internal/cli"
	"github.com/netstar-labs/core/patch"
)

type params struct {
	Addr     string `env:"a,order,require" default:"localhost:1455" help:"listen address"`
	Store    string `default:"kv.core" help:"store path prefix"`
	Shards   int    `default:"8" help:"number of shards for a new store (existing: shard count read from <store>.NNN)"`
	Capacity int    `env:"cap" default:"1024" help:"initial capacity per shard"`
	Reserve  int    `help:"pre-size for n entries (0=off; avoids mid-load resizes)"`
	Keylen   int    `default:"32" help:"key length in bytes for a new store (existing: auto-detected); 32=SHA-256, 16=XXH3-128, 8=xxHash64"`
	Vallen   int    `default:"8" help:"value length in bytes for a new store (existing: auto-detected)"`
	NoSync   bool   `help:"skip per-write msync; rely on Sync at graceful shutdown"`
	Patch    string `help:"enable a WAL-backed patch write-buffer at this dir (empty = off); feed it via POST /patch"`
	Flush    int    `default:"5" help:"flush the patch buffer every N seconds (0 = only on size threshold + shutdown)"`
	FlushAt  int    `default:"1000000" help:"flush the patch buffer once it reaches N pending changes"`
	Log      string `help:"log timestamps on|off (default: off on linux, on elsewhere)"`
}

func main() {

	var param params
	cli.Configure(&param)

	// Production (linux) logs without timestamps because the supervisor adds its own.
	if runtime.GOOS == "linux" {
		log.SetFlags(0)
	}
	switch param.Log {
	case "on", "yes", "true":
		log.SetFlags(log.Ldate | log.Ltime)
	case "off", "no", "false":
		log.SetFlags(0)
	}

	// A bare store name lives under the platform var dir (/var/core on linux,
	// _dev/var/core elsewhere); anything containing a slash is used as given.
	if !strings.Contains(param.Store, "/") {
		varDir := "/var"
		if runtime.GOOS != "linux" {
			varDir = "_dev/var"
		}
		param.Store = filepath.Join(varDir, "core", param.Store)
		if err := os.MkdirAll(filepath.Dir(param.Store), 0o755); err != nil {
			log.Fatalf("create store dir: %v", err)
		}
	}

	w, err := NewWorker(param.Store, param.Shards, param.Capacity, param.Reserve, param.Keylen, param.Vallen, param.NoSync)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer w.Close() // flushes + closes the store on exit

	// The effective geometry/shards come from an existing store's files, which can
	// differ from the flags shown in the startup banner — log what is actually used.
	log.Printf("store ready: %s — %d shards, keyLen=%d valLen=%d, %d entries",
		param.Store, w.Shards(), keyLen, valLen, w.Len())

	// Cancel the context on SIGINT/SIGTERM to trigger graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Optional patch write-buffer: POST /patch appends coalesced upserts/deletes to
	// a WAL-backed Log, and this loop folds them into the store on an interval or
	// once they pile up. Open replays any window a prior run left un-applied.
	// flushDone closes when the flush loop has returned. Shutdown must wait on it
	// before draining: cancelling ctx only asks the loop to stop, and a loop caught
	// mid-Flush is still calling Sync on the store that drain and Close are about to
	// tear down. The race detector catches exactly that (mmap.RW.Close racing
	// RW.Sync) if the wait is missing.
	flushDone := make(chan struct{})
	if param.Patch != "" {
		lg, err := patch.Open(param.Patch, keyLen, valLen)
		if err != nil {
			log.Fatalf("open patch buffer: %v", err)
		}
		w.log = lg
		log.Printf("patch buffer at %s — flush every %ds or at %d pending", param.Patch, param.Flush, param.FlushAt)
		go func() {
			defer close(flushDone)
			patchFlushLoop(ctx, w, param.Flush, param.FlushAt)
		}()
	} else {
		close(flushDone)
	}

	srv := &http.Server{
		Addr:    param.Addr,
		Handler: w.routes(),
		// No Read/Write timeout: /job/stream and /job/binary uploads can run
		// arbitrarily long. ReadHeaderTimeout still guards against slowloris,
		// and IdleTimeout reaps idle keep-alive connections.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("listening on %s", param.Addr)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	stop() // restore default signal handling so a second Ctrl-C force-quits
	log.Println("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}

	// No more requests are accepted, and no flush is in flight: drain the patch
	// buffer into the store. The wait is load-bearing — draining while the loop is
	// inside Flush means two Flushes on one Log and a Sync on a store that the
	// deferred Close is about to unmap.
	<-flushDone
	if err := drain(w); err != nil {
		log.Printf("drain: %v", err)
	}
	log.Println("stopped")
}

// drain folds the patch buffer into the store and closes the WAL, leaving nothing
// un-applied behind. It is the shutdown sequence, factored out of main so a test
// can assert the ordering — main itself is flag binding and signal handling, which
// is the part not worth contorting to reach.
//
// It is safe to call with no patch buffer configured, and reports the first error
// rather than stopping at it: a failed flush must not skip the close.
func drain(w *Worker) error {
	if w == nil || w.log == nil {
		return nil
	}
	var firstErr error
	if st, err := w.log.Flush(w.store); err != nil {
		firstErr = fmt.Errorf("final patch flush: %w", err)
	} else if st.Upserts+st.Deletes > 0 {
		log.Printf("final patch flush: %d upserts, %d deletes", st.Upserts, st.Deletes)
	}
	if err := w.log.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("patch buffer close: %w", err)
	}
	return firstErr
}

// patchFlushLoop folds the patch buffer into the store on a cadence: every
// flushSecs seconds (when > 0), or as soon as the buffer reaches flushAt pending
// changes. It wakes once a second to check both triggers and skips empty windows.
// It returns when ctx is cancelled (shutdown), leaving the final drain to main.
func patchFlushLoop(ctx context.Context, w *Worker, flushSecs, flushAt int) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			pending, _ := w.log.Pending()
			if pending == 0 {
				last = now
				continue
			}
			timeUp := flushSecs > 0 && now.Sub(last) >= time.Duration(flushSecs)*time.Second
			sizeUp := flushAt > 0 && pending >= flushAt
			if timeUp || sizeUp {
				if _, err := w.log.Flush(w.store); err != nil {
					log.Printf("patch flush: %v", err)
				}
				last = now
			}
		}
	}
}
