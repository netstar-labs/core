// "push-parts" subcommand: stream a partition set to a running core server in
// destination order, then verify via the server's content digest.
//
//	core push-parts -parts part -url http://localhost:8080
//
// Each <prefix>.pNNN file is POSTed as application/octet-stream to the whole store
// (never ?shard=N — parts are group-index-partitioned across all shards, so the
// server must route each record by hash). Posting the parts in order preserves the
// front-to-back write sweep that keeps an out-of-RAM load from thrashing.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type pushPartsCfg struct {
	Parts string `default:"part" help:"partition prefix (reads <prefix>.pNNN in order)"`
	Url   string `default:"http://localhost:8080" help:"server base URL"`
}

// streamPath is the server's fixed binary-ingest route. It is a constant, not a
// config field: a field named Path is silently overwritten by the ubiquitous $PATH
// environment variable (the env loader overlays os.Environ by upper-cased field
// name, last, so it beats even an explicit flag) — which pointed every POST at a
// garbage URL.
const streamPath = "/job/stream"

func runPushParts(c *pushPartsCfg) {
	base := strings.TrimRight(c.Url, "/")
	endpoint := base + streamPath
	total, inserted := 0, 0
	start := time.Now()

	for p := 0; ; p++ {
		fp := fmt.Sprintf("%s.p%03d", c.Parts, p)
		if _, err := os.Stat(fp); err != nil {
			break // no more partitions
		}
		f, err := os.Open(fp)
		if err != nil {
			fatal(err)
		}
		// Pass the *os.File directly: the body streams (chunked), no RAM blowup.
		resp, err := http.Post(endpoint, "application/octet-stream", f)
		f.Close()
		if err != nil {
			fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fatal(fmt.Errorf("%s: %s: %s", fp, resp.Status, strings.TrimSpace(string(body))))
		}
		var r struct {
			Records  int `json:"records"`
			Inserted int `json:"inserted"`
			Entries  int `json:"entries"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			fatal(fmt.Errorf("%s: bad response %q: %w", fp, string(body), err))
		}
		total += r.Records
		inserted += r.Inserted
		fmt.Printf("  %s: %d records, %d new (store entries %d)\n", filepath.Base(fp), r.Records, r.Inserted, r.Entries)
	}

	if total == 0 {
		fatal(fmt.Errorf("no partition files found at %s.pNNN", c.Parts))
	}
	dur := time.Since(start)
	fmt.Printf("push-parts: %d records, %d new entries in %v (%.2f M/s)\n",
		total, inserted, dur.Round(time.Millisecond), float64(total)/dur.Seconds()/1e6)

	// Verify: fetch the server's order-independent content digest.
	if resp, err := http.Get(base + "/job/digest"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var d struct {
			Digest  string `json:"digest"`
			Entries int    `json:"entries"`
		}
		if json.Unmarshal(body, &d) == nil && d.Digest != "" {
			fmt.Printf("server digest: %s  (%d entries)\n", d.Digest, d.Entries)
		}
	}
}
