package main

import (
	"encoding/hex"
	"testing"
)

// The server must derive the same 16-byte key as cmd/core so a store is interchangeable
// between them. Digests are XXH3-128 (canonical big-endian) from the reference libxxhash.
func TestMakeKey16MatchesCLI(t *testing.T) {
	for in, want := range map[string]string{
		"example.com": "c481d7301ccf29bb4bfbb7c48c9c3712",
		"hello":       "b5e9c1ad071b3e7fc779cfaa5e523818",
		"":            "99aa06d3014798d86001c324468d497f",
	} {
		k := make([]byte, 16)
		makeKey(k, in)
		if got := hex.EncodeToString(k); got != want {
			t.Errorf("makeKey16(%q) = %s; want %s", in, got, want)
		}
	}
}
