package main

// bulk: the CSV mode of `lookup`. With -in, read a list of keys (one per line),
// look each up, and write a CSV to -out — key,value, or key + decoded codex field
// integers when -codex is given.

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/netstar-labs/core"
)

// lookupCSV reads keys from in (one per line; "-" is stdin), looks each up in sh,
// and writes a CSV to out ("" or "-" is stdout). With a non-nil codex, each row is
// the key followed by the decoded field integers; otherwise it is key,value. A
// not-found key writes an empty-valued row so rows stay aligned 1:1 with the input.
// Blank input lines are skipped. A one-line summary is printed to stderr (so it
// never corrupts a stdout CSV).
func lookupCSV(sh *core.Sharded, in, out string, codex []codexField) error {
	var r io.Reader = os.Stdin
	if in != "-" {
		f, err := os.Open(in)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}

	var w io.Writer = os.Stdout
	if out != "" && out != "-" {
		f, err := os.Create(out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	bw := bufio.NewWriterSize(w, 1<<16)
	defer bw.Flush()
	cw := csv.NewWriter(bw)

	header := []string{"key"}
	if codex != nil {
		for _, f := range codex {
			header = append(header, f.name)
		}
	} else {
		header = append(header, "value")
	}
	if err := cw.Write(header); err != nil {
		return err
	}

	k := make([]byte, keyLen)
	row := make([]string, 0, len(header))
	keys, found, missed := 0, 0, 0
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		key := strings.TrimSpace(sc.Text())
		if key == "" {
			continue
		}
		keys++
		makeKey(k, key)
		v, ok := sh.Get(k)

		row = append(row[:0], key)
		switch {
		case ok && codex != nil:
			found++
			for _, f := range codex {
				row = append(row, strconv.FormatUint(f.decode(v), 10))
			}
		case ok:
			found++
			row = append(row, formatValue(v))
		default: // not found: empty cell(s), keeping row alignment
			missed++
			for n := len(header) - 1; n > 0; n-- {
				row = append(row, "")
			}
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	dest := out
	if dest == "" || dest == "-" {
		dest = "stdout"
	}
	fmt.Fprintf(os.Stderr, "bulk: %d keys, %d found, %d missed -> %s\n", keys, found, missed, dest)
	return nil
}

// formatValue renders a found value for the no-codex CSV column: uint64 decimal for
// an 8-byte value, big-endian hex for a wider one, "present" for a key-only set.
func formatValue(v []byte) string {
	switch {
	case valLen == 0:
		return "present"
	case valLen == 8:
		return strconv.FormatUint(binary.LittleEndian.Uint64(v), 10)
	default:
		return "0x" + hexBE(v)
	}
}
