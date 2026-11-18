package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/netstar-labs/core"
	"github.com/netstar-labs/core/internal/xxhash"
)

func TestKeylenForHash(t *testing.T) {
	ok := map[string]int{
		"xxhash64": 8, "xx": 8, "64": 8, "XXH64": 8, "xxhash": 8,
		"xxh128": 16, "xxhash128": 16, "xxh3-128": 16, "XXH3_128": 16, "128": 16, "xxh3": 16, "xxhash3": 16,
		"sha256": 32, "sha": 32, "256": 32, "SHA-256": 32,
	}
	for name, want := range ok {
		got, err := keylenForHash(name)
		if err != nil || got != want {
			t.Errorf("keylenForHash(%q) = %d, %v; want %d", name, got, err, want)
		}
	}
	// Names that would silently mean something other than what they say are refused.
	for _, name := range []string{"", "md5", "sha128", "sha-128", "xxh3-64", "xxh364", "xxhash3-64", "blake2"} {
		if got, err := keylenForHash(name); err == nil {
			t.Errorf("keylenForHash(%q) = %d; want an error", name, got)
		}
	}
}

// The 16-byte key is XXH3-128 in canonical big-endian form; these digests come from the
// reference libxxhash 0.8.2 (python xxhash xxh3_128), not from this code.
func TestMakeKey16(t *testing.T) {
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
		if sum := core.Sum128String(in); hex.EncodeToString(sum[:]) != want {
			t.Errorf("core.Sum128String(%q) = %x; want %s", in, sum, want)
		}
	}
}

// A 2*keyLen-char hex argument is a raw key, at every width.
func TestMakeKeyHexIsRaw(t *testing.T) {
	for _, n := range []int{8, 16, 32} {
		raw := make([]byte, n)
		for i := range raw {
			raw[i] = byte(i*17 + 3)
		}
		k := make([]byte, n)
		makeKey(k, hex.EncodeToString(raw))
		if hex.EncodeToString(k) != hex.EncodeToString(raw) {
			t.Errorf("width %d: hex argument was hashed, not taken as the raw key", n)
		}
	}
}

// The 8- and 32-byte derivations must not move when the 16-byte case is added.
func TestMakeKeyExistingWidthsUnchanged(t *testing.T) {
	k8 := make([]byte, 8)
	makeKey(k8, "hello")
	if binary.LittleEndian.Uint64(k8) != xxhash.Sum([]byte("hello")) {
		t.Error("8-byte key is no longer xxHash64 little-endian")
	}
	k32 := make([]byte, 32)
	makeKey(k32, "hello")
	if s := sha256.Sum256([]byte("hello")); hex.EncodeToString(k32) != hex.EncodeToString(s[:]) {
		t.Error("32-byte key is no longer SHA-256")
	}
}
