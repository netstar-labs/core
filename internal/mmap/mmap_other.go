// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Modified from the original: see the LICENSE file in this directory.

//go:build !linux && !windows && !darwin

// Package mmap provides a way to memory-map a file.
//
// On platforms without a syscall-level mmap binding here, Open reads the
// file fully into memory at open time and serves all access from that
// slice. This preserves the Bytes()/ReadAt() API but loses the lazy,
// kernel-managed page-cache behavior of the linux/darwin/windows paths.
package mmap

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// ReaderAt reads a memory-mapped file.
//
// Like any io.ReaderAt, clients can execute parallel ReadAt calls, but it is
// not safe to call Close and reading methods concurrently.
type ReaderAt struct {
	data []byte
}

// Close releases the underlying data.
func (r *ReaderAt) Close() error {
	r.data = nil
	return nil
}

// Len returns the length of the underlying file.
func (r *ReaderAt) Len() int {
	return len(r.data)
}

// Bytes returns the underlying data as a byte slice.
//
// Callers MUST treat the returned slice as read-only. The slice is only
// valid until Close is called; do not retain it past that point.
func (r *ReaderAt) Bytes() []byte {
	return r.data
}

// At returns the byte at index i.
func (r *ReaderAt) At(i int) byte {
	if i < 0 || len(r.data) <= i {
		panic("index out of range")
	}
	return r.data[i]
}

// ReadAt implements the io.ReaderAt interface.
func (r *ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.data == nil {
		return 0, errors.New("mmap: closed")
	}
	if off < 0 || int64(len(r.data)) < off {
		return 0, fmt.Errorf("mmap: invalid ReadAt offset %d", off)
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Open reads the named file into memory and returns a *ReaderAt.
func Open(filename string) (*ReaderAt, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return &ReaderAt{data: data}, nil
}
