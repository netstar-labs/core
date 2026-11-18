package core

import (
	"github.com/netstar-labs/core/internal/xxh3"
	"github.com/netstar-labs/core/internal/xxhash"
)

// sum hashes a fixed-size key. xxHash64 is fast (hand-written amd64 asm in the
// internal package) and mixes well enough that both the H1 group index and the
// H2 control tag derived from it are well distributed.
func sum(key []byte) uint64 { return xxhash.Sum(key) }

// Sum returns the 64-bit xxHash (XXH64) digest of key. It is the exact hash
// the store uses internally to route keys, exposed so callers can pre-compute
// digests, partition keys, or shard data the same way Core does.
func Sum(key []byte) uint64 { return xxhash.Sum(key) }

// SumString returns the 64-bit xxHash (XXH64) digest of s. It is zero-alloc
// at any input size and matches Sum([]byte(s)).
func SumString(s string) uint64 { return xxhash.SSum(s) }

// Sum128 returns the 128-bit XXH3 digest (XXH3_128bits, seed 0) of key in canonical
// big-endian form — the 16-byte key that cmd/core and the server derive for a store
// with keyLen=16. It is not collision-resistant against an adversary; use SHA-256
// where an attacker chooses the input.
func Sum128(key []byte) [16]byte { return xxh3.Hash128(key).Bytes() }

// Sum128String returns the 128-bit XXH3 digest of s; it matches Sum128([]byte(s))
// and does not allocate.
func Sum128String(s string) [16]byte { return xxh3.HashString128(s).Bytes() }
