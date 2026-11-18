// "plan" subcommand: analyze a dataset and recommend ingest parameters for a
// sharded core store — row count, power-of-two capacity, resulting load factor, a
// shard setting, and partition sizing for an out-of-RAM load. The capacity and
// file-layout sizing come from core.PlanLayout (the same math a real Reserve
// uses), and the load factor is the same 7/8 the store grows at.
//
// -cores and -ram override the target machine; when unspecified they are
// autodetected from the machine running the command.
//
//	core plan -data testdata/records.encode
//	core plan -data data.bin -format bin -shards 16
//	core plan -rows 380000000                            # skip the file scan
//	core plan -data testdata/records.encode -sample 1000000  # estimate dup rate
//	core plan -rows 380000000 -cores 64 -ram 256         # size for a different box
//	core plan -rows 380000000 -keylen 8 -commit records.core # create the empty pre-sized store
//
// With -commit <name>, plan also materializes the recommendation as an empty,
// pre-sized (sparse) sharded store at <name>.000…NNN — geometry on disk, so later
// ingest/digest/lookup auto-detect it. It refuses to overwrite an existing store.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/netstar-labs/core"
)

// The 7/8 grow threshold, shown in the report. The capacity/layout sizing now
// comes from core.PlanLayout, so this is the only constant the plan still mirrors.
const (
	loadNum = 7
	loadDen = 8
)

type planCfg struct {
	Data    string  `help:"dataset to analyze (counts rows)"`
	Format  string  `default:"text" help:"input format: text (\"key<sp>uint64\" lines) or bin (packed records)"`
	Rows    int64   `help:"row count (skip scanning -data)"`
	Keylen  int     `default:"32" help:"key length in bytes"`
	Vallen  int     `default:"8" help:"value length in bytes"`
	Hash    string  `help:"hash accepts xxhash64, xxh128 or sha256 to set -keylen n"`
	Shards  int     `help:"shard count (0 = recommend from cores)"`
	Reserve int64   `help:"reserve count (0 = use row count)"`
	Ram     float64 `help:"target usable RAM in GiB (0 = use this machine)"`
	Cores   int     `help:"target CPU cores (0 = use this machine)"`
	Budget  float64 `default:"0.40" help:"fraction of RAM a single partition's resident window may use"`
	Sample  int     `help:"estimate duplicate rate from a reservoir sample of this many rows (0 = off)"`
	Commit  string  `help:"create an empty pre-sized store at this path prefix from the plan"`
}

func runPlan(c *planCfg) {
	// -hash overrides -keylen; resolve once so the sizing, the geometry report, the
	// suggested commands, and -commit all use the same key length.
	c.Keylen = resolveKeylen(c.Hash, c.Keylen)

	slotSize := c.Keylen + c.Vallen
	recSize := int64(slotSize)

	// --- row count (+ optional duplicate sample) ---
	var nrows int64
	var reservoir []string
	sampled := false
	switch {
	case c.Rows > 0:
		nrows = c.Rows
		if c.Sample > 0 {
			fmt.Fprintln(os.Stderr, "note: -sample needs -data; ignored with -rows")
		}
	case c.Data == "":
		fmt.Fprintln(os.Stderr, "error: provide -data <file> or -rows <n>")
		os.Exit(2)
	default:
		st, err := os.Stat(c.Data)
		if err != nil {
			fatal(err)
		}
		if c.Format != "text" && c.Format != "bin" {
			fatal(fmt.Errorf("-format must be text or bin, got %q", c.Format))
		}
		if c.Sample > 0 {
			fmt.Fprintf(os.Stderr, "scanning %s (%.2f GiB) for rows + duplicate sample…\n", c.Data, float64(st.Size())/(1<<30))
			if c.Format == "bin" {
				if st.Size()%recSize != 0 {
					fatal(fmt.Errorf("file size %d is not a multiple of record size %d", st.Size(), recSize))
				}
				nrows, reservoir, err = sampleBin(c.Data, int(recSize), c.Keylen, c.Sample)
			} else {
				nrows, reservoir, err = sampleText(c.Data, c.Keylen, c.Sample)
			}
			if err != nil {
				fatal(err)
			}
			sampled = true
		} else if c.Format == "bin" {
			if st.Size()%recSize != 0 {
				fatal(fmt.Errorf("file size %d is not a multiple of record size %d", st.Size(), recSize))
			}
			nrows = st.Size() / recSize
		} else {
			fmt.Fprintf(os.Stderr, "counting rows in %s (%.2f GiB)…\n", c.Data, float64(st.Size())/(1<<30))
			if nrows, err = countLines(c.Data); err != nil {
				fatal(err)
			}
		}
	}
	if nrows <= 0 {
		fatal(fmt.Errorf("row count is zero"))
	}

	// --- machine ---
	nc := c.Cores
	if nc <= 0 {
		nc = runtime.NumCPU()
	}
	ram := int64(c.Ram * (1 << 30))
	if ram <= 0 {
		ram = detectRAM()
	}

	// --- recommendation ---
	nsh := c.Shards
	if nsh <= 0 {
		nsh = recShards(nc)
	}
	res := c.Reserve
	if res <= 0 {
		res = nrows
	}
	// The library owns the sizing math (per-shard capacity rounding, file layout),
	// so this report can't drift from what a real store ends up with.
	lay, err := core.PlanLayout(c.Keylen, c.Vallen, nsh, int(res))
	if err != nil {
		fatal(err)
	}
	capPerShard := lay.CapPerShard
	totalCap := lay.TotalCapacity
	load := float64(nrows) / float64(totalCap)
	storeBytes := lay.StoreBytes
	slotsBytes := int64(capPerShard) * int64(slotSize) * int64(nsh)
	ctrlBytes := int64(capPerShard) * int64(nsh)
	verBytes := int64(0)
	if c.Vallen > 8 {
		verBytes = int64(capPerShard) * 4 * int64(nsh)
	}

	// --- partition sizing ---
	parts := 1
	var budget int64
	fits := true
	if ram > 0 {
		budget = int64(float64(ram) * c.Budget)
		if storeBytes > budget && budget > 0 {
			fits = false
			need := (storeBytes + budget - 1) / budget // ceil
			for int64(parts) < need {
				parts <<= 1
			}
		}
	}
	residentPerPart := storeBytes
	if parts > 1 {
		residentPerPart = storeBytes / int64(parts)
	}

	// --- report ---
	fmt.Printf("\ninput\n")
	if c.Data != "" {
		fmt.Printf("  file:    %s (%s)\n", c.Data, c.Format)
	}
	fmt.Printf("  rows:    %s\n", comma(nrows))

	fmt.Printf("\ngeometry (keyLen=%d, valLen=%d)\n", c.Keylen, c.Vallen)
	perSlot := slotSize + 1
	verNote := "no version array, valLen<=8"
	if c.Vallen > 8 {
		perSlot += 4
		verNote = "+4 version array, valLen>8"
	}
	fmt.Printf("  slot size:     %d B (key+val)\n", slotSize)
	fmt.Printf("  per-slot cost: %d B (+1 ctrl; %s)\n", perSlot, verNote)

	fmt.Printf("\nmachine\n")
	fmt.Printf("  cores: %d\n", nc)
	if ram > 0 {
		fmt.Printf("  ram:   %s\n", humanBytes(ram))
	} else {
		fmt.Printf("  ram:   unknown (pass -ram <GiB> for partition sizing)\n")
	}

	fmt.Printf("\nrecommendation\n")
	fmt.Printf("  shards:           %d%s\n", nsh, ifElse(c.Shards <= 0, "  (largest power of two <= cores)", ""))
	fmt.Printf("  reserve:          %s%s\n", comma(res), ifElse(c.Reserve <= 0, "  (= row count)", ""))
	fmt.Printf("  capacity/shard:   %s = 2^%d\n", comma(int64(capPerShard)), log2(capPerShard))
	fmt.Printf("  total capacity:   %s = 2^%d\n", comma(int64(totalCap)), log2(totalCap))
	fmt.Printf("  load factor:      %.3f  (grows at %.3f)\n", load, float64(loadNum)/float64(loadDen))
	fmt.Printf("  store on disk:    ~%s  (ctrl %s + ", humanBytes(storeBytes), humanBytes(ctrlBytes))
	if verBytes > 0 {
		fmt.Printf("version %s + ", humanBytes(verBytes))
	}
	fmt.Printf("slots %s)\n", humanBytes(slotsBytes))
	if load > 0.85 {
		fmt.Printf("  ! load is near the 0.875 grow threshold; bump -reserve a few %% to avoid a mid-load resize\n")
	}

	fmt.Printf("\npartition sizing\n")
	if ram <= 0 {
		fmt.Printf("  RAM unknown — cannot size partitions; pass -ram <GiB>\n")
	} else if fits {
		fmt.Printf("  store %s <= budget %s (%.2f x %s)  → fits in RAM, no partitioning needed\n",
			humanBytes(storeBytes), humanBytes(budget), c.Budget, humanBytes(ram))
	} else {
		fmt.Printf("  store %s > budget %s (%.2f x %s)  → partition the load\n",
			humanBytes(storeBytes), humanBytes(budget), c.Budget, humanBytes(ram))
		fmt.Printf("  parts:            %d  (per-partition resident ~%s)\n", parts, humanBytes(residentPerPart))
	}

	// --- duplicates (sampled) ---
	if sampled {
		n := int64(len(reservoir))
		set := make(map[string]struct{}, len(reservoir))
		for _, k := range reservoir {
			set[k] = struct{}{}
		}
		distinct := int64(len(set))
		exact := nrows <= int64(c.Sample) // whole file fit in the reservoir
		uniqueRate := 1.0
		if n > 0 {
			uniqueRate = float64(distinct) / float64(n)
		}
		estUnique := int64(float64(nrows)*uniqueRate + 0.5)
		if exact {
			estUnique = distinct
		}
		label := "sampled via reservoir"
		if exact {
			label = "exact — whole file fit in the sample"
		}
		fmt.Printf("\nduplicates (%s)\n", label)
		fmt.Printf("  sample:            %s rows\n", comma(n))
		fmt.Printf("  distinct keys:     %s  (distinct key text; keys are SHA-256-hashed, so only exact dups collapse)\n", comma(distinct))
		fmt.Printf("  unique rate:       %.4f  (dup rate %.2f%%)\n", uniqueRate, (1-uniqueRate)*100)
		fmt.Printf("  est. live entries: ~%s  (vs %s rows)\n", comma(estUnique), comma(nrows))
		fmt.Printf("  reserve stays at the row count — over-provisioning is safe\n")
	}

	// --- suggested commands ---
	// -text only for text input with a value column; a key-only set (valLen==0) is
	// packed key-only records, which partition -text rejects.
	textFlag := ""
	if c.Format == "text" && c.Vallen != 0 {
		textFlag = "-text "
	}
	store := "kv.core"
	if c.Commit != "" {
		store = c.Commit
	}
	// Carry a non-default geometry through, so the suggested commands build the
	// partitions and store at the same key/value widths that were planned for
	// (partition sizes records by it; ingest-parts creates the store with it).
	geom := ""
	if c.Keylen != 32 || c.Vallen != 8 {
		geom = fmt.Sprintf(" -keylen %d -vallen %d", c.Keylen, c.Vallen)
	}
	fmt.Printf("\nsuggested commands\n")
	fmt.Printf("  core partition %s-data %s -out part -shards %d -reserve %d -parts %d%s\n",
		textFlag, orName(c.Data, "<data>"), nsh, res, parts, geom)
	fmt.Printf("  core ingest-parts -parts part -store %s -shards %d -reserve %d%s\n", store, nsh, res, geom)
	fmt.Println()

	// --- commit: materialize the planned geometry as an empty, pre-sized store ---
	if c.Commit != "" {
		if _, err := os.Stat(c.Commit + ".000"); err == nil {
			fatal(fmt.Errorf("commit: a store already exists at %s.* — remove it first", c.Commit))
		}
		if err := os.MkdirAll(filepath.Dir(c.Commit), 0o755); err != nil {
			fatal(err)
		}
		sh, err := core.OpenSharded(c.Commit, c.Keylen, c.Vallen, nsh, 1024, core.WithNoSync())
		if err != nil {
			fatal(err)
		}
		if err := sh.Reserve(int(res)); err != nil { // one resize per shard to the planned capacity
			sh.Close()
			fatal(err)
		}
		if err := sh.Sync(); err != nil {
			sh.Close()
			fatal(err)
		}
		sh.Close()
		fmt.Printf("commit: created empty store %s (%d shards, keyLen=%d, valLen=%d, reserve %s, load %.3f)\n",
			c.Commit, nsh, c.Keylen, c.Vallen, comma(res), load)
		fmt.Printf("store on disk: %s\n", storeSize(c.Commit, nsh))
	}
}

// ---- sizing helpers ----

// recShards returns the largest power of two <= cores (at least 1).
func recShards(cores int) int {
	s := 1
	for s*2 <= cores {
		s <<= 1
	}
	return s
}

func log2(x uint64) int {
	n := 0
	for x > 1 {
		x >>= 1
		n++
	}
	return n
}

// ---- machine detection ----

func detectRAM() int64 {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err == nil {
			if v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); err == nil {
				return v
			}
		}
	case "linux":
		f, err := os.Open("/proc/meminfo")
		if err == nil {
			defer f.Close()
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				line := sc.Text()
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
							return kb * 1024
						}
					}
				}
			}
		}
	}
	return 0
}

// ---- helpers ----

// sampleText scans a text dataset, returning the row count and a uniform
// reservoir sample of up to sampleN distinct keys. The store SHA-256-hashes the
// full key text, so distinctness is over the whole key (not a truncated prefix).
// The key is everything before the first space, or the whole line.
func sampleText(path string, keyLen, sampleN int) (rows int64, reservoir []string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	rng := rand.New(rand.NewSource(1)) // fixed seed → reproducible estimate
	reservoir = make([]string, 0, sampleN)
	for sc.Scan() {
		line := sc.Bytes()
		key := line
		if sp := bytes.IndexByte(line, ' '); sp >= 0 {
			key = line[:sp]
		}
		reservoir = reservoirPut(reservoir, sampleN, rows, rng, key)
		rows++
	}
	return rows, reservoir, sc.Err()
}

// sampleBin scans packed records, sampling the keyLen-byte key of each.
func sampleBin(path string, recSize, keyLen, sampleN int) (rows int64, reservoir []string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	rng := rand.New(rand.NewSource(1))
	reservoir = make([]string, 0, sampleN)
	rec := make([]byte, recSize)
	for {
		_, e := io.ReadFull(br, rec)
		if e == io.EOF {
			break
		}
		if e != nil {
			return rows, reservoir, e
		}
		reservoir = reservoirPut(reservoir, sampleN, rows, rng, rec[:keyLen])
		rows++
	}
	return rows, reservoir, nil
}

// reservoirPut applies one step of reservoir sampling for the item at 0-based
// index i, copying tok into the reservoir only if it is selected.
func reservoirPut(res []string, n int, i int64, rng *rand.Rand, tok []byte) []string {
	if i < int64(n) {
		return append(res, string(tok))
	}
	if j := rng.Int63n(i + 1); j < int64(n) {
		res[j] = string(tok)
	}
	return res
}

// countLines counts '\n' in a file, plus one for a final unterminated line.
func countLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	var n int64
	var last byte
	read := false
	for {
		c, err := f.Read(buf)
		if c > 0 {
			read = true
			for _, b := range buf[:c] {
				if b == '\n' {
					n++
				}
			}
			last = buf[c-1]
		}
		if err != nil {
			break
		}
	}
	if read && last != '\n' {
		n++ // final line without trailing newline
	}
	return n, nil
}

func comma(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := ""
	if strings.HasPrefix(s, "-") {
		neg, s = "-", s[1:]
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return neg + b.String()
}

func ifElse(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func orName(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
