//go:build amd64

package xxhash

// Sum computes the uint64 xxHash digest of b.
// The body is implemented in xxhash_amd64.s.
//
//go:noescape
func Sum(b []byte) uint64
