// Command core is the end-to-end CLI for the core hash table: analyze a dataset,
// bulk-ingest it into an mmap-backed sharded store, pack a store for transport,
// serve point lookups, and benchmark a self-generated, machine-sized test set.
//
//	go run ./cmd/core plan   -data testdata/records.encode            # recommend shards/reserve/parts
//	go run ./cmd/core ingest -data testdata/kv350mm -store kv.core -shards 8
//	go run ./cmd/core lookup -store kv.core -shards 8 key0000042
//	go run ./cmd/core bench                                       # size + build + bench + clean up
//
// When the store is larger than RAM, ingest in destination order to avoid
// page-cache thrashing — partition the dataset by group index, then load the
// partitions in order (-text reads "key<space>uint64" lines instead of records):
//
//	go run ./cmd/core partition -text -data testdata/kv350mm -out part -shards 8 -reserve 1000000 -parts 16
//	go run ./cmd/core ingest-parts -parts part -store kv.core -shards 8 -reserve 1000000
//
// For sharing, bundle the parts into one tar.gz (stdlib archive/tar + gzip) with a
// manifest carrying per-blob CRC-32C and the store's order-independent Digest, then
// unpack + rebuild + verify on the far side:
//
//	go run ./cmd/core pack   -glob 'part.p*' -out bundle.tar.gz -store kv.core -shards 8 -reserve 1000000 -parts 16
//	go run ./cmd/core unpack -in bundle.tar.gz -dir restore -store kv2.core
//
// For a ship-whole/rebuild (no delta), omit -reserve: export-parts and pack derive
// the group mask from the store's actual per-shard capacity and record it in the
// manifest (capPerShard), so the rebuild needs no remembered reserve and never
// resizes. A part-wise delta still needs an explicit -reserve shared by both sides.
//
//	go run ./cmd/core export-parts -store kv.core -out part -parts 16   # auto (ship-whole)
//
// To update a store in place instead of rebuilding it, build a patch (coalesced
// upserts + deletes) against it and apply it on the far side; apply verifies the
// store still matches the digest the patch was built against:
//
//	go run ./cmd/core patch-build -upserts add.txt -deletes del.txt -store kv.core -out patch.tar.gz
//	go run ./cmd/core patch-apply -in patch.tar.gz -store kv.core
//
// Or derive that patch as a delta: export two store snapshots back to parts and
// diff them part-by-part (incremental replication — ship only what changed):
//
//	go run ./cmd/core export-parts -store old.core -out old -reserve 1000000 -parts 16
//	go run ./cmd/core delta old new -store old.core -out patch.tar.gz
//
// Parts can also be applied to a running server (start it -reserve/-nosync for the
// fast path), streamed in order and verified by its content digest:
//
//	go run ./cmd/core push-parts -parts part -url http://localhost:8080
//
// A -vallen 0 store is a key-only set (membership only); load it from a newline-
// delimited list of key strings with ingest-keys, or from packed key-only records
// with ingest-bin, and lookup reports present/<not found>:
//
//	go run ./cmd/core ingest-keys -data set.txt -store set.core -shards 8   # one key per line
//	go run ./cmd/core lookup -store set.core key0000042                     # -> present
//
// Keys are hashed into the fixed key slot by geometry — SHA-256 for a 32-byte key,
// XXH3-128 for a 16-byte key, xxHash64 for an 8-byte key (a 2*keyLen-char hex
// argument is taken as the raw key instead); text/lookup values are a uint64 stored
// little-endian. Geometry defaults to a 32-byte key /
// 8-byte value (40-byte record). Commands that open an existing store auto-detect
// its geometry from the header (via core.Geometry on "<store>.000"), so
// -keylen/-vallen are needed only to create a new store (partition, a
// fresh ingest); unpack reads geometry from the bundle manifest. lookup also
// auto-detects the shard count from the "<store>.NNN" files, so it needs only
// -store and the key(s); for partition/ingest-parts, -shards and -reserve must match.
//
// Subcommands are dispatched by internal/cli: "core" (or "core help") prints the
// menu, "core help <sub>" or "core <sub> -help" lists a subcommand's flags, and
// "core version" prints the build stamp. Flags are parsed by the same package (see
// its doc): each also accepts -flag=value or -flag:value, a bare -flag for bools,
// and an uppercased env-var override (e.g. SHARDS=16).
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/internal/cli"
	"github.com/netstar-labs/core/internal/mmap"
	"github.com/netstar-labs/core/internal/xxh3"
	"github.com/netstar-labs/core/internal/xxhash"
)

// Default key/value geometry, overridden per-command by -keylen/-vallen via
// setGeometry. cmd/core runs exactly one subcommand per process, so these
// globals are set once before any store/record work and never race.
var (
	keyLen = 32
	valLen = 8
)

// main wires every subcommand into cli.Dispatch for go-tool style dispatch:
// "core help <cmd>" / "core <cmd> -help" prints a command's flag table, "core
// version" prints the build banner, and "core <cmd> [flags]" parses the command's
// own flags (the subcommand word is stripped from os.Args first) and runs it. Each
// Run handler receives the already-populated config. The bare/`help`/unknown-command
// top-level menu is rendered here by usage() in the familiar `go help` layout
// instead of by the dispatcher.
func main() {
	var (
		plan        planCfg
		ingest      ingestCfg
		ingestBin   ingestBinCfg
		ingestKeys  ingestKeysCfg
		partition   partitionCfg
		ingestParts ingestPartsCfg
		digest      digestCfg
		header      headerCfg
		pack        packCfg
		unpack      unpackCfg
		pushParts   pushPartsCfg
		lookup      lookupCfg
		bench       benchCfg
		patchBuild  patchBuildCfg
		patchApply  patchApplyCfg
		exportParts exportPartsCfg
		delta       deltaCfg
	)

	cmds := []cli.Command{
		{Name: "notices", Help: "print the third-party license notices compiled into this binary",
			Cfg: &noticesCfg, Run: func([]string) {
				if err := writeNotices(os.Stdout); err != nil {
					fmt.Fprintln(os.Stderr, "core: notices:", err)
					os.Exit(1)
				}
			}},
		{Name: "plan", Help: "recommend shards/reserve/parts for a dataset", Cfg: &plan, Run: func([]string) { runPlan(&plan) }},
		{Name: "ingest", Help: "bulk-load a \"key<sp>uint64\" text dataset into a store", Cfg: &ingest, Run: func([]string) { runIngest(&ingest) }},
		{Name: "ingest-bin", Help: "bulk-load a packed-binary dataset into a store", Cfg: &ingestBin, Run: func([]string) { runIngestBin(&ingestBin) }},
		{Name: "ingest-keys", Help: "bulk-load a newline-delimited key list into a key-only set", Cfg: &ingestKeys, Run: func([]string) { runIngestKeys(&ingestKeys) }},
		{Name: "partition", Help: "bucket records by destination group for an out-of-RAM load", Cfg: &partition, Run: func([]string) { runPartition(&partition) }},
		{Name: "ingest-parts", Help: "load partition files in group order into one store", Cfg: &ingestParts, Run: func([]string) { runIngestParts(&ingestParts) }},
		{Name: "digest", Help: "print a store's order-independent content digest", Cfg: &digest, Run: func([]string) { runDigest(&digest) }},
		{Name: "header", Help: "print a store's on-disk header in table form", Cfg: &header, Run: func([]string) { runHeader(&header) }},
		{Name: "pack", Help: "bundle a store or its parts into one tar.gz", Cfg: &pack, Run: func([]string) { runPack(&pack) }},
		{Name: "unpack", Help: "extract, verify, and optionally rebuild a bundle", Cfg: &unpack, Run: func([]string) { runUnpack(&unpack) }},
		{Name: "patch-build", Help: "build a transportable patch (upserts+deletes) bundle", Cfg: &patchBuild, Run: func([]string) { runPatchBuild(&patchBuild) }},
		{Name: "patch-apply", Help: "apply a patch bundle to a store (verifies base digest)", Cfg: &patchApply, Run: func([]string) { runPatchApply(&patchApply) }},
		{Name: "export-parts", Help: "re-bucket a live store back into partition files (inverse of ingest-parts)", Cfg: &exportParts, Run: func([]string) { runExportParts(&exportParts) }},
		{Name: "delta", Help: "diff prior vs current partition sets into a patch bundle", Cfg: &delta, Run: func([]string) { runDelta(&delta) }},
		{Name: "push-parts", Help: "stream partition files to a running core server", Cfg: &pushParts, Run: func([]string) { runPushParts(&pushParts) }},
		{Name: "lookup", Help: "print values for the given keys (or bulk CSV via -in)", Cfg: &lookup, Args: true, Run: func(keys []string) { runLookup(&lookup, keys) }},
		{Name: "bench", Help: "build a machine-sized test set and benchmark lookups", Cfg: &bench, Run: func([]string) { runBench(&bench) }},
	}

	// Top-level menu, go-tool style, for the bare invocation, the `help`/`-h`/`-help`
	// words, and any unknown command, so `core` and `core help` match the `go help`
	// layout. Anything cli.Dispatch handles — `version`, `help <cmd>`, `<cmd> -help`,
	// and the commands themselves — falls through to it untouched.
	switch {
	case len(os.Args) < 2, len(os.Args) == 2 && isHelpWord(os.Args[1]):
		usage(os.Stdout, cmds)
		return
	case !knownArg(os.Args[1], cmds):
		fmt.Fprintf(os.Stderr, "core: unknown command %q\n\n", os.Args[1])
		usage(os.Stderr, cmds)
		os.Exit(2)
	}

	cli.Dispatch(cmds, os.Args[1:])
}

// usage prints a go-tool style top-level menu: a one-line synopsis, the usage line,
// the command table, and the "help <command>" footer.
func usage(w io.Writer, cmds []cli.Command) {
	name := filepath.Base(os.Args[0])
	fmt.Fprintf(w, "core is the end-to-end CLI for the core hash table.\n\n")
	fmt.Fprintf(w, "Usage:\n\n\t%s <command> [arguments]\n\n", name)
	fmt.Fprintf(w, "The commands are:\n\n")
	tw := tabwriter.NewWriter(w, 0, 0, 4, ' ', 0)
	for _, c := range cmds {
		fmt.Fprintf(tw, "\t%s\t%s\n", c.Name, c.Help)
	}
	tw.Flush()
	fmt.Fprintf(w, "\nUse \"%s help <command>\" for more information about a command.\n", name)
}

// isHelpWord reports whether a single top-level token requests the menu: help, -h,
// -help, or --help (leading dashes are ignored).
func isHelpWord(s string) bool {
	switch strings.TrimLeft(s, "-") {
	case "help", "h":
		return true
	}
	return false
}

// knownArg reports whether the first token is something cli.Dispatch will handle:
// a defined command, or the reserved version/help words (the latter as a `help
// <cmd>` drill-in). Anything else is an unknown command and gets the menu.
func knownArg(arg string, cmds []cli.Command) bool {
	switch strings.TrimLeft(arg, "-") {
	case "version", "help", "h":
		return true
	}
	for _, c := range cmds {
		if c.Name == arg {
			return true
		}
	}
	return false
}

// recordSize is the packed binary record: key | value (little-endian uint64).
// Set alongside keyLen/valLen by setGeometry.
var recordSize = keyLen + valLen

// setGeometry applies the chosen key/value lengths to the package-level geometry
// used across the CLI. Safe because each process runs exactly one subcommand.
func setGeometry(klen, vlen int) {
	keyLen, valLen = klen, vlen
	recordSize = keyLen + valLen
}

// setGeometryFromStore adopts an existing store's on-disk geometry when its first
// shard (<store>.000) is present — the header is authoritative — and otherwise
// falls back to the -keylen/-vallen flag values (for creating a new store). So
// commands that open an existing store need no geometry flags.
func setGeometryFromStore(store string, klen, vlen int) {
	if k, v, err := core.Geometry(store + ".000"); err == nil {
		setGeometry(k, v)
		return
	}
	setGeometry(klen, vlen)
}

// keylenForHash maps a hash name to the key length cmd/core uses for it — the inverse
// of makeKey: xxHash64 → 8, XXH3-128 → 16, SHA-256 → 32. Matching is loose and
// case-insensitive: xx/64/xxhash64, 128/xxh128/xxh3 (the 128-bit form is the only XXH3
// supported), sha/256/sha256. Names that would silently mean something else are
// rejected: sha128 (truncated SHA-256 is not a named choice) and xxh3-64. Keep in
// sync with makeKey.
func keylenForHash(name string) (int, error) {
	s := strings.ToLower(strings.TrimSpace(name))
	has := func(sub string) bool { return strings.Contains(s, sub) }
	sha := has("sha") || has("256")
	xxh3Only := s == "xxh3" || s == "xxhash3"
	switch {
	case sha && has("128"), has("xxh3") && !has("128") && !xxh3Only, has("xxhash3") && !has("128") && !xxh3Only:
		// ambiguous: fall through to the error below
	case xxh3Only, !sha && has("128"):
		return 16, nil
	case has("xx"), has("64"):
		return 8, nil
	case sha:
		return 32, nil
	}
	return 0, fmt.Errorf("unknown -hash %q; use xxhash64 (keylen 8), xxh128 (keylen 16) or sha256 (keylen 32)", name)
}

// resolveKeylen picks the key length: from -hash when given (overrides -keylen), else
// the -keylen flag. Used by every command that creates/opens a store geometry.
func resolveKeylen(hash string, keylen int) int {
	if hash == "" {
		return keylen
	}
	k, err := keylenForHash(hash)
	if err != nil {
		fatal(err)
	}
	return k
}

// detectShards counts the consecutive shard files <prefix>.000, .001, … present on
// disk, so a read command can open a store without being told its shard count.
// Returns 0 when no <prefix>.000 exists. The header records geometry but not the
// shard count, so this fills that gap — and opening with the wrong count is
// destructive (it creates empty shards and misroutes lookups).
func detectShards(prefix string) int {
	n := 0
	for {
		if _, err := os.Stat(fmt.Sprintf("%s.%03d", prefix, n)); err != nil {
			return n
		}
		n++
	}
}

// ---- ingest-bin: zero-parse, zero-alloc binary ingest ----
type ingestBinCfg struct {
	Data   string `env:"order,require" help:"input binary dataset path"`
	Store  string `default:"kv.core" help:"output store path prefix"`
	Shards int    `default:"8" help:"number of shards (power of two)"`
	Cap    int    `default:"1024" help:"initial capacity per shard"`
	Keylen int    `default:"32" help:"key length in bytes (must match the records)"`
	Vallen int    `default:"8" help:"value length in bytes (must match the records)"`
	Hash   string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runIngestBin(c *ingestBinCfg) {
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)

	// mmap the input read-only: records are read straight from the page cache.
	ra, err := mmap.Open(c.Data)
	if err != nil {
		fatal(err)
	}
	defer ra.Close()
	buf := ra.Bytes()
	if len(buf)%recordSize != 0 {
		fatal(fmt.Errorf("file size %d is not a multiple of record size %d", len(buf), recordSize))
	}
	nrec := len(buf) / recordSize

	sh, err := core.OpenSharded(c.Store, keyLen, valLen, c.Shards, c.Cap, core.WithNoSync())
	if err != nil {
		fatal(err)
	}
	defer sh.Close()
	if err := sh.Reserve(nrec); err != nil { // one resize per shard
		fatal(err)
	}

	const chunkRecs = 1 << 18 // bound per-shard index buffers
	inserted := 0
	start := time.Now()
	for off := 0; off < len(buf); off += chunkRecs * recordSize {
		end := off + chunkRecs*recordSize
		if end > len(buf) {
			end = len(buf)
		}
		ins, err := sh.PutRecords(buf[off:end])
		if err != nil {
			fatal(err)
		}
		inserted += ins
	}
	if err := sh.Sync(); err != nil {
		fatal(err)
	}

	dur := time.Since(start)
	fmt.Printf("ingest-bin: %d records, %d new entries, %d shards in %v (%.2f M/s)\n",
		nrec, inserted, sh.Shards(), dur.Round(time.Millisecond), float64(nrec)/dur.Seconds()/1e6)
	fmt.Printf("store on disk: %s\n", storeSize(c.Store, c.Shards))
}

// makeKey writes the fixed-size key for s into dst, choosing the hash by key width
// to match the store geometry: a 32-byte key takes the SHA-256 digest, a 16-byte key
// the XXH3-128 digest (canonical big-endian), an 8-byte key the xxHash64 digest
// (little-endian). A string of exactly 2*len(dst) hex
// chars is decoded straight into its raw bytes instead (so hex-printed keys round-
// trip). Any other width falls back to a SHA-256 digest, truncated or zero-padded
// in the tail to fit. At a 32-byte key this matches the server's derivation exactly.
func makeKey(dst []byte, s string) {
	if len(s) == 2*len(dst) {
		if tmp, err := hex.DecodeString(s); err == nil {
			copy(dst, tmp)
			return
		}
	}
	switch len(dst) {
	case 8:
		binary.LittleEndian.PutUint64(dst, xxhash.Sum([]byte(s)))
	case 16:
		sum := xxh3.HashString128(s).Bytes()
		copy(dst, sum[:])
	case 32:
		sum := sha256.Sum256([]byte(s))
		copy(dst, sum[:])
	default:
		for i := range dst {
			dst[i] = 0
		}
		sum := sha256.Sum256([]byte(s))
		copy(dst, sum[:]) // first min(len(dst), 32) digest bytes
	}
}

// ---- ingest: bulk-load the dataset into a sharded store ----

type ingestCfg struct {
	Data    string `env:"order,require" help:"input dataset path"`
	Store   string `default:"kv.core" help:"output store path prefix"`
	Shards  int    `default:"8" help:"number of shards (power of two)"`
	Reserve int    `help:"pre-size for this many entries (0 = auto-grow)"`
	Cap     int    `default:"1024" help:"initial capacity per shard"`
	Keylen  int    `default:"32" help:"key length in bytes"`
	Vallen  int    `default:"8" help:"value length in bytes (>=8; value is a uint64)"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

func runIngest(c *ingestCfg) {
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)
	if valLen == 0 {
		fatal(fmt.Errorf("ingest reads key<sp>uint64 text; a key-only store needs ingest-bin with key-only records"))
	}

	sh, err := core.OpenSharded(c.Store, keyLen, valLen, c.Shards, c.Cap, core.WithNoSync())
	if err != nil {
		fatal(err)
	}
	defer sh.Close()
	if c.Reserve > 0 {
		if err := sh.Reserve(c.Reserve); err != nil {
			fatal(err)
		}
	}

	f, err := os.Open(c.Data)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)

	const chunk = 100_000
	keys := make([][]byte, 0, chunk)
	vals := make([][]byte, 0, chunk)
	total, inserted := 0, 0
	start := time.Now()

	flush := func() {
		if len(keys) == 0 {
			return
		}
		ins, err := sh.PutBatch(keys, vals)
		if err != nil {
			fatal(err)
		}
		inserted += ins
		keys = keys[:0]
		vals = vals[:0]
	}

	for sc.Scan() {
		ks, vs, ok := splitKV(sc.Bytes())
		if !ok {
			continue
		}
		v, err := strconv.ParseUint(vs, 10, 64)
		if err != nil {
			continue
		}
		k := make([]byte, keyLen)
		makeKey(k, ks)
		b := make([]byte, valLen)
		binary.LittleEndian.PutUint64(b, v)
		keys = append(keys, k)
		vals = append(vals, b)
		total++
		if len(keys) == chunk {
			flush()
		}
	}
	flush()
	if err := sc.Err(); err != nil {
		fatal(err)
	}
	if err := sh.Sync(); err != nil {
		fatal(err)
	}

	dur := time.Since(start)
	fmt.Printf("ingest: %d lines, %d new entries, %d shards in %v (%.2f M/s)\n",
		total, inserted, sh.Shards(), dur.Round(time.Millisecond), float64(total)/dur.Seconds()/1e6)
	fmt.Printf("store on disk: %s\n", storeSize(c.Store, c.Shards))
}

// ---- ingest-keys: bulk-load a newline-delimited key list into a key-only set ----

type ingestKeysCfg struct {
	Data    string `env:"order,require" help:"input dataset path (one key per line)"`
	Store   string `default:"kv.core" help:"output store path prefix"`
	Shards  int    `default:"8" help:"number of shards (power of two)"`
	Reserve int    `help:"pre-size for this many entries (0 = auto-grow)"`
	Cap     int    `default:"1024" help:"initial capacity per shard"`
	Keylen  int    `default:"32" help:"key length in bytes"`
	Hash    string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
}

// runIngestKeys bulk-loads a newline-delimited list of key strings into a key-only
// (valLen==0) set store. Each line is hashed with makeKey — SHA-256 for a 32-byte
// key, xxHash64 for an 8-byte key — so membership routes on the same key bytes the
// store stores and matches lookup's derivation. Blank lines (after trimming) are
// skipped. This is the text-only counterpart to ingest-bin's key-only records.
func runIngestKeys(c *ingestKeysCfg) {
	// Force a value-less geometry: a key-only set has no value column. An existing
	// store's header is authoritative, so reject one that was built with values.
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), 0)
	if valLen != 0 {
		fatal(fmt.Errorf("ingest-keys builds a key-only set; store %s has valLen=%d (use ingest)", c.Store, valLen))
	}

	sh, err := core.OpenSharded(c.Store, keyLen, valLen, c.Shards, c.Cap, core.WithNoSync())
	if err != nil {
		fatal(err)
	}
	defer sh.Close()
	if c.Reserve > 0 {
		if err := sh.Reserve(c.Reserve); err != nil {
			fatal(err)
		}
	}

	f, err := os.Open(c.Data)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)

	const chunk = 100_000
	keys := make([][]byte, 0, chunk)
	noVal := []byte{} // shared zero-length value: valLen==0 reads/writes nothing
	vals := make([][]byte, 0, chunk)
	total, inserted := 0, 0
	start := time.Now()

	flush := func() {
		if len(keys) == 0 {
			return
		}
		ins, err := sh.PutBatch(keys, vals)
		if err != nil {
			fatal(err)
		}
		inserted += ins
		keys = keys[:0]
		vals = vals[:0]
	}

	for sc.Scan() {
		ks := bytes.TrimSpace(sc.Bytes())
		if len(ks) == 0 {
			continue
		}
		k := make([]byte, keyLen)
		makeKey(k, string(ks))
		keys = append(keys, k)
		vals = append(vals, noVal)
		total++
		if len(keys) == chunk {
			flush()
		}
	}
	flush()
	if err := sc.Err(); err != nil {
		fatal(err)
	}
	if err := sh.Sync(); err != nil {
		fatal(err)
	}

	dur := time.Since(start)
	fmt.Printf("ingest-keys: %d keys, %d new entries, %d shards in %v (%.2f M/s)\n",
		total, inserted, sh.Shards(), dur.Round(time.Millisecond), float64(total)/dur.Seconds()/1e6)
	fmt.Printf("store on disk: %s\n", storeSize(c.Store, c.Shards))
}

// ---- lookup: print values for the given keys ----

type lookupCfg struct {
	Store  string `default:"kv.core" help:"store path prefix"`
	Shards int    `help:"number of shards (0 = auto-detect from <store>.NNN files)"`
	Keylen int    `default:"32" help:"key length in bytes (auto-detected from the store)"`
	Vallen int    `default:"8" help:"value length in bytes (auto-detected from the store)"`
	Hash   string `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
	Codex  string `help:"decode value into le bit fields, e.g. 'P:16,S:16,S:16,A:4,R:4' or a single 'A:4:4'; or @path to load the spec from a file"`
	In     string `help:"bulk mode: read keys from this file (one per line; - for stdin) and write CSV"`
	Out    string `help:"CSV output path for bulk mode (default: stdout)"`
}

func runLookup(c *lookupCfg, keys []string) {
	// Geometry (key/value lengths) and shard count both come from the existing
	// store, so a plain `lookup -store <prefix> <key>` needs no other flags.
	setGeometryFromStore(c.Store, resolveKeylen(c.Hash, c.Keylen), c.Vallen)
	shards := c.Shards
	if shards <= 0 {
		if shards = detectShards(c.Store); shards == 0 {
			fatal(fmt.Errorf("no store found at %s.000", c.Store))
		}
	}

	// Optional -codex bit-field decoder: parse the spec once, up front. Fields may
	// span the full value width (valLen*8 bits), so 16-byte values decode fully.
	var codex []codexField
	if c.Codex != "" {
		if valLen == 0 {
			fatal(fmt.Errorf("-codex needs a value store (valLen>=8); this is a key-only set"))
		}
		spec, err := loadCodexSpec(c.Codex) // inline, or @path to read from a file
		if err != nil {
			fatal(err)
		}
		if codex, err = parseCodex(spec, valLen*8); err != nil {
			fatal(err)
		}
	}

	sh, err := core.OpenSharded(c.Store, keyLen, valLen, shards, 1024)
	if err != nil {
		fatal(err)
	}
	defer sh.Close()

	// Bulk mode: read a list of keys from -in and write a CSV to -out.
	if c.In != "" {
		if err := lookupCSV(sh, c.In, c.Out, codex); err != nil {
			fatal(err)
		}
		return
	}

	// keys are the positional (non-flag) args left over after the flags are bound.
	k := make([]byte, keyLen)
	for _, arg := range keys {
		makeKey(k, arg)
		v, ok := sh.Get(k)
		switch {
		case !ok:
			fmt.Printf("%s = <not found>\n", arg)
		case valLen == 0: // key-only (set): report membership
			fmt.Printf("%s = present\n", arg)
		case valLen == 8: // single uint64: print as decimal
			fmt.Printf("%s = %d\n", arg, binary.LittleEndian.Uint64(v))
			if codex != nil {
				renderCodex(os.Stdout, v, codex)
			}
		default: // wider value: not a single uint64, print as big-endian hex
			fmt.Printf("%s = 0x%s\n", arg, hexBE(v))
			if codex != nil {
				renderCodex(os.Stdout, v, codex)
			}
		}
	}
}

// ---- bench: self-contained, machine-sized lookup benchmark ----
//
// bench needs no dataset or pre-built store. It sizes a synthetic test set from
// the machine the way `plan` does (shards from CPU cores, entry count from a slice
// of RAM so the store stays in memory), builds it in a throwaway store, runs
// concurrent random point lookups, prints a summary table, and deletes the store.
// -n / -shards override the machine-derived sizing.

type benchCfg struct {
	N       int `help:"entries in the test set (0 = size from machine RAM)"`
	Lookups int `default:"10000000" help:"total random point lookups"`
	Shards  int `help:"shard count (0 = recommend from CPU cores)"`
	Procs   int `help:"concurrent reader goroutines (0 = GOMAXPROCS)"`
	Keylen  int `default:"32" help:"key length in bytes"`
	Vallen  int `default:"8" help:"value length in bytes (>=8; value is a uint64)"`
}

func runBench(c *benchCfg) {
	setGeometry(c.Keylen, c.Vallen)

	// --- machine-derived sizing (mirrors plan: shards from cores, entries from RAM) ---
	cores := runtime.NumCPU()
	shards := c.Shards
	if shards <= 0 {
		shards = recShards(cores)
	}
	ram := detectRAM()
	entries := c.N
	autoSized := entries <= 0
	if autoSized {
		entries = autoEntries(ram, recordSize)
	}
	nprocs := c.Procs
	if nprocs <= 0 {
		nprocs = runtime.GOMAXPROCS(0)
	}

	// --- throwaway store in a temp dir; always removed, even on a fatal build ---
	dir, err := os.MkdirTemp("", "core-bench-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(dir)
	store := dir + "/demo.core"

	sh, err := core.OpenSharded(store, keyLen, valLen, shards, 1024, core.WithNoSync())
	if err != nil {
		fatal(err)
	}
	defer sh.Close()
	if err := sh.Reserve(entries); err != nil { // one resize per shard to the planned capacity
		fatal(err)
	}

	// --- build the test set: synthetic deterministic key%09d records (as gen did) ---
	const chunkRecs = 1 << 18
	buf := make([]byte, chunkRecs*recordSize)
	buildStart := time.Now()
	for base := 0; base < entries; base += chunkRecs {
		n := chunkRecs
		if base+n > entries {
			n = entries - base
		}
		chunk := buf[:n*recordSize]
		for j := 0; j < n; j++ {
			rec := chunk[j*recordSize : (j+1)*recordSize]
			makeKey(rec[:keyLen], fmt.Sprintf("key%09d", base+j))
			if valLen > 0 { // valLen==0 ⇒ key-only (set) records
				binary.LittleEndian.PutUint64(rec[keyLen:], uint64(base+j)*2654435761+1)
			}
		}
		if _, err := sh.PutRecords(chunk); err != nil {
			fatal(err)
		}
	}
	if err := sh.Sync(); err != nil {
		fatal(err)
	}
	buildDur := time.Since(buildStart)

	// --- precompute a hashed-key sample so the timed loop is pure store lookups ---
	sampleN := entries
	if sampleN > 1_000_000 {
		sampleN = 1_000_000
	}
	sample := make([][]byte, sampleN)
	for i := range sample {
		k := make([]byte, keyLen)
		makeKey(k, fmt.Sprintf("key%09d", i))
		sample[i] = k
	}

	// --- run the benchmark: concurrent random point lookups ---
	perProc := c.Lookups / nprocs
	var wg sync.WaitGroup
	var totalHits int64
	runStart := time.Now()
	for p := 0; p < nprocs; p++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			dst := make([]byte, valLen)
			hits := 0
			for i := 0; i < perProc; i++ {
				if sh.GetInto(sample[rng.Intn(len(sample))], dst) {
					hits++
				}
			}
			atomic.AddInt64(&totalHits, int64(hits))
		}(int64(p) + 1)
	}
	wg.Wait()
	runDur := time.Since(runStart)
	ops := perProc * nprocs

	// --- summary table (store is removed by the deferred RemoveAll on return) ---
	ramStr := "unknown"
	if ram > 0 {
		ramStr = humanBytes(ram)
	}
	sizing := "from -n"
	if autoSized {
		sizing = "auto (RAM-sized)"
	}
	hitPct := 0.0
	if ops > 0 {
		hitPct = float64(totalHits) / float64(ops) * 100
	}
	fmt.Println("\nbenchmark summary")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  machine\t%d cores · %s RAM\n", cores, ramStr)
	fmt.Fprintf(tw, "  geometry\tkeyLen=%d valLen=%d (%d B/record)\n", keyLen, valLen, recordSize)
	fmt.Fprintf(tw, "  shards\t%d\n", shards)
	fmt.Fprintf(tw, "  test set\t%s entries · %s · %s\n", comma(int64(entries)), sizing, storeSize(store, shards))
	fmt.Fprintf(tw, "  build\t%v · %.2f M entries/s\n", buildDur.Round(time.Millisecond), float64(entries)/buildDur.Seconds()/1e6)
	fmt.Fprintf(tw, "  lookups\t%s · %d goroutines\n", comma(int64(ops)), nprocs)
	fmt.Fprintf(tw, "  hits\t%s (%.1f%%)\n", comma(totalHits), hitPct)
	fmt.Fprintf(tw, "  throughput\t%.2f Mops/s · %.1f ns/op\n", float64(ops)/runDur.Seconds()/1e6, float64(runDur.Nanoseconds())/float64(ops))
	tw.Flush()
	fmt.Println()
}

// autoEntries picks a test-set size that fits a slice of RAM, so the benchmark
// store stays in memory. It mirrors plan's per-entry cost (slot + 1 control byte,
// divided by the 7/8 load factor) and is bounded so an auto run never balloons.
func autoEntries(ram int64, recordSize int) int {
	const (
		floor = 1_000_000
		ceil  = 25_000_000
		frac  = 0.10 // fraction of RAM the test store may occupy
	)
	if ram <= 0 {
		return 4 * floor // RAM unknown: a modest fixed set
	}
	perEntry := float64(recordSize+1) * float64(loadDen) / float64(loadNum)
	n := int(float64(ram) * frac / perEntry)
	if n < floor {
		n = floor
	}
	if n > ceil {
		n = ceil
	}
	return n
}

// ---- helpers ----

func splitKV(line []byte) (key, val string, ok bool) {
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			return string(line[:i]), string(line[i+1:]), true
		}
	}
	return "", "", false
}

// humanBytes formats a byte count as GiB/MiB/KiB.
func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
}

func storeSize(prefix string, shards int) string {
	var total int64
	for i := 0; i < shards; i++ {
		if fi, err := os.Stat(fmt.Sprintf("%s.%03d", prefix, i)); err == nil {
			total += fi.Size()
		}
	}
	if total >= 1<<30 {
		return strconv.FormatFloat(float64(total)/(1<<30), 'f', 2, 64) + " GiB (apparent)"
	}
	return strconv.FormatFloat(float64(total)/(1<<20), 'f', 1, 64) + " MiB (apparent)"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
