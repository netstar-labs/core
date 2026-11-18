// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Modified from the original: see the LICENSE file in this directory.

package mmap

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	const filename = "mmap_test.go"
	r, err := Open(filename)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := make([]byte, r.Len())
	if _, err := r.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}
	want, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("os.ReadFile: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("\ngot  %q\nwant %q", string(got), string(want))
	}
}

func TestMapRW(t *testing.T) {
	const size = 4096
	path := filepath.Join(t.TempDir(), "rw.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	m, err := MapRW(f, size)
	if err != nil {
		t.Fatalf("MapRW: %v", err)
	}
	if m.Len() != size {
		t.Fatalf("Len: got %d want %d", m.Len(), size)
	}

	want := []byte("centralized writable mapping")
	copy(m.Bytes(), want)
	// A second mapping must observe the write (on the file-backed path); on the
	// heap fallback this still holds within the same mapping, so read it back.
	got := append([]byte(nil), m.Bytes()[:len(want)]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("readback: got %q want %q", got, want)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := m.Close(); err != nil { // idempotent
		t.Fatalf("second Close: %v", err)
	}

	if Backed {
		// File-backed: the store should have reached the file's bytes.
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read file: %v", err)
		}
		if !bytes.Equal(raw[:len(want)], want) {
			t.Fatalf("file contents: got %q want %q", raw[:len(want)], want)
		}
	}
}

func TestMapRWZero(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "empty.bin"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	m, err := MapRW(f, 0)
	if err != nil {
		t.Fatalf("MapRW(0): %v", err)
	}
	if m.Len() != 0 {
		t.Fatalf("Len: got %d want 0", m.Len())
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
