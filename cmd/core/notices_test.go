package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// The embedded notices must match their sources. BSD clause 2 requires the
// notice to accompany a BINARY redistribution, and a binary is all a recipient ever
// gets — so a stale embedded copy is a compliance gap rather than a formatting nit.
// build/core.sh refuses to ship on drift; this catches it in CI, before a release is
// even attempted.
func TestEmbeddedNoticeMatchesSource(t *testing.T) {
	for _, n := range []struct{ src, embed string }{
		{"../../internal/mmap/LICENSE", "notices/golang.org-x-exp-mmap.LICENSE"},
		{"../../internal/xxh3/LICENSE", "notices/github.com-zeebo-xxh3.LICENSE"},
	} {
		src, err := os.ReadFile(n.src)
		if err != nil {
			t.Fatalf("reading the notice source: %v", err)
		}
		got, err := noticeFS.ReadFile(n.embed)
		if err != nil {
			t.Fatalf("reading the embedded notice: %v", err)
		}
		if !bytes.Equal(src, got) {
			t.Errorf("cmd/core/%s has drifted from %s — re-copy it", n.embed, n.src)
		}
	}
}

// The notice must actually carry the BSD-3-Clause terms; an empty or truncated embed
// would satisfy the drift check above while shipping nothing.
func TestNoticesOutputCarriesTheLicense(t *testing.T) {
	var buf bytes.Buffer
	if err := writeNotices(&buf); err != nil {
		t.Fatalf("writeNotices: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"The Go Authors",
		"Redistribution and use in source and binary forms",
		"THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS",
		"golang.org/x/exp/mmap",
		"Yann Collet",                 // the xxh3 notice
		"Apache License, Version 2.0", // core's own license, so a reader is not misled
	} {
		if !strings.Contains(out, want) {
			t.Errorf("notices output is missing %q", want)
		}
	}
	if len(out) < 1000 {
		t.Errorf("notices output is only %d bytes — suspiciously short for a full license", len(out))
	}
}
