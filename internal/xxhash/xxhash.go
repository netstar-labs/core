// Package xxhash implements the 64-bit variant of xxHash (XXH64)
// as described at http://cyan4973.github.io/xxHash/.
//
// Origin: the Go code grew out of the author's own github.com/zxdev/xxhash (MIT);
// the amd64 assembly was written with AI assistance for this project. No
// third-party source is incorporated. It is checked against the published XXH64
// vectors in xxhash_test.go.
//
// On amd64 the core digest is hand-written assembly (xxhash_amd64.s);
// on every other architecture a pure-Go fallback is used (xxhash_other.go).
package xxhash

// SSum computes the uint64 xxHash digest of s without copying.
// The string's bytes are aliased via unsafe.Slice so SSum is
// zero-alloc at any input size; Sum is //go:noescape, so the
// aliased slice never leaves this call.
func SSum(s string) uint64 {
	if len(s) == 0 {
		return Sum(nil)
	}
	return Sum([]byte(s))
	//return Sum(unsafe.Slice(unsafe.StringData(s), len(s)))
}
