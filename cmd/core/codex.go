package main

// codex: an optional, string-driven bit-field decoder for `lookup`. A stored value
// is a uint64, but it is often a packed bit-record (see sandbox/codex.go for a
// hard-coded example). A -codex spec names the fields so `lookup` can mask/shift the
// value apart and print each field.

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
)

// loadCodexSpec resolves a -codex argument that is either an inline spec or a
// "@path" reference to a file holding one. A file lets a large/reused layout live
// on disk instead of a shell-quoted one-liner: fields may be one per line or
// comma-separated, blank lines and "#" comments are ignored, and the result is
// rejoined into the same comma-separated form parseCodex expects.
func loadCodexSpec(arg string) (string, error) {
	if !strings.HasPrefix(arg, "@") {
		return arg, nil
	}
	path := arg[1:]
	if path == "" {
		return "", fmt.Errorf("-codex: @ needs a file path")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("-codex: %w", err)
	}
	var fields []string
	for _, line := range strings.Split(string(b), "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i] // strip trailing comment
		}
		for _, f := range strings.Split(line, ",") {
			if f = strings.TrimSpace(f); f != "" {
				fields = append(fields, f)
			}
		}
	}
	if len(fields) == 0 {
		return "", fmt.Errorf("-codex: file %s has no fields", path)
	}
	return strings.Join(fields, ","), nil
}

// hexBE renders little-endian value bytes as a big-endian hex string, so a packed
// value reads most-significant byte first (e.g. a 16-byte value as one number).
func hexBE(v []byte) string {
	b := make([]byte, len(v))
	for i := range v {
		b[i] = v[len(v)-1-i]
	}
	return hex.EncodeToString(b)
}

// codexField is one named bit slice of a uint64 value: width bits starting at
// offset, counted from the LSB (bit 0).
type codexField struct {
	name   string
	offset int
	width  int
}

// parseCodex parses a -codex spec into ordered fields. A spec is a comma-separated
// list of fields, each in one of two forms:
//
//	Name:width         sequential — fields are packed MSB→LSB with the LAST one at
//	                   bit 0, so unspecified high bits are reserved/ignored.
//	Name:offset:width  explicit  — offset is the bit position from the LSB.
//
// A spec is all-sequential or all-explicit; mixing the two forms is an error. maxBits
// is the value width in bits (valLen*8, e.g. 64 for an 8-byte value, 128 for 16); a
// single field is still capped at 64 bits (it must fit a uint64), but offsets may
// span the whole value.
func parseCodex(spec string, maxBits int) ([]codexField, error) {
	type raw struct {
		name string
		nums []int // [width] (sequential) or [offset, width] (explicit)
	}
	var raws []raw
	explicit, sequential := false, false
	for _, t := range strings.Split(spec, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		parts := strings.Split(t, ":")
		if len(parts) != 2 && len(parts) != 3 {
			return nil, fmt.Errorf("field %q: want Name:width or Name:offset:width", t)
		}
		name := strings.TrimSpace(parts[0])
		if name == "" {
			return nil, fmt.Errorf("field %q: missing name", t)
		}
		nums := make([]int, 0, 2)
		for _, p := range parts[1:] {
			n, err := strconv.Atoi(strings.TrimSpace(p))
			if err != nil {
				return nil, fmt.Errorf("field %q: %q is not a number", t, p)
			}
			nums = append(nums, n)
		}
		if len(nums) == 2 {
			explicit = true
		} else {
			sequential = true
		}
		raws = append(raws, raw{name: name, nums: nums})
	}
	if len(raws) == 0 {
		return nil, fmt.Errorf("empty codex spec")
	}
	if explicit && sequential {
		return nil, fmt.Errorf("codex mixes Name:width and Name:offset:width forms; use one or the other")
	}

	fields := make([]codexField, len(raws))
	if explicit {
		for i, r := range raws {
			off, w := r.nums[0], r.nums[1]
			if w < 1 || w > 64 {
				return nil, fmt.Errorf("field %q: width %d out of range 1..64", r.name, w)
			}
			if off < 0 || off+w > maxBits {
				return nil, fmt.Errorf("field %q: bits %d..%d out of range 0..%d (value is %d bytes)", r.name, off, off+w-1, maxBits-1, maxBits/8)
			}
			fields[i] = codexField{name: r.name, offset: off, width: w}
		}
		return fields, nil
	}

	// Sequential: the last field sits at bit 0, so accumulate the offset right→left.
	running := 0
	for i := len(raws) - 1; i >= 0; i-- {
		w := raws[i].nums[0]
		if w < 1 || w > 64 {
			return nil, fmt.Errorf("field %q: width %d out of range 1..64", raws[i].name, w)
		}
		fields[i] = codexField{name: raws[i].name, offset: running, width: w}
		running += w
	}
	if running > maxBits {
		return nil, fmt.Errorf("codex fields total %d bits, exceeds %d (value is %d bytes)", running, maxBits, maxBits/8)
	}
	return fields, nil
}

// decode extracts the field's value from the little-endian value bytes. A field is
// at most 64 bits and may sit anywhere in the (8-, 16-, … byte) value, possibly
// straddling a byte/word boundary, so it is read bit by bit from the LSB. Bits past
// the end of v read as 0.
func (f codexField) decode(v []byte) uint64 {
	var out uint64
	for i := 0; i < f.width; i++ {
		bit := f.offset + i
		if bit/8 >= len(v) {
			break
		}
		out |= uint64((v[bit/8]>>(bit%8))&1) << i
	}
	return out
}

// renderCodex prints the decoded fields of the value as an aligned table.
func renderCodex(out io.Writer, v []byte, fields []codexField) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  field\tbits\tvalue\thex")
	for _, f := range fields {
		d := f.decode(v)
		fmt.Fprintf(tw, "  %s\t%d-%d\t%d\t%#x\n", f.name, f.offset, f.offset+f.width-1, d, d)
	}
	tw.Flush()
}
