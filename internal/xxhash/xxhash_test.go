package xxhash

import (
	"strings"
	"testing"
)

// Reference XXH64 vectors (seed = 0).
var vectors = []struct {
	in   string
	want uint64
}{
	{"", 0xef46db3751d8e999},
	{"a", 0xd24ec4f1a98c6e5b},
	{"abc", 0x44bc2cf5ad770999},
	{"message digest", 0x066ed728fceeb3be},
	{"abcdefghijklmnopqrstuvwxyz", 0xcfe1f278fa89835c},
}

func TestSumVectors(t *testing.T) {
	for _, v := range vectors {
		if got := Sum([]byte(v.in)); got != v.want {
			t.Errorf("Sum(%q) = %016x, want %016x", v.in, got, v.want)
		}
		if got := SSum(v.in); got != v.want {
			t.Errorf("SSum(%q) = %016x, want %016x", v.in, got, v.want)
		}
	}
}

// Stress every length 0..256 to cover all tail combinations.
func TestSumLengths(t *testing.T) {
	buf := make([]byte, 256)
	for i := range buf {
		buf[i] = byte(i)
	}
	for n := 0; n <= 256; n++ {
		// Compare amd64 asm vs a known-good pure-Go reference inline.
		got := Sum(buf[:n])
		want := refSum(buf[:n])
		if got != want {
			t.Errorf("n=%d: got %016x, want %016x", n, got, want)
		}
	}
}

// refSum is the pure-Go reference used to cross-check the asm.
func refSum(b []byte) uint64 {
	var n, h uint64 = uint64(len(b)), 2870177450012600261
	if n >= 32 {
		v1 := uint64(6983438078262162902)
		v2 := uint64(14029467366897019727)
		v3 := uint64(0)
		v4 := uint64(7046029288634856825)
		for len(b) >= 32 {
			v1 = refRound(v1, refU64(b[0:8]))
			v2 = refRound(v2, refU64(b[8:16]))
			v3 = refRound(v3, refU64(b[16:24]))
			v4 = refRound(v4, refU64(b[24:32]))
			b = b[32:]
		}
		h = (v1<<1 | v1>>63) + (v2<<7 | v2>>57) + (v3<<12 | v3>>52) + (v4<<18 | v4>>46)
		h = (h^refRound(0, v1))*11400714785074694791 + 9650029242287828579
		h = (h^refRound(0, v2))*11400714785074694791 + 9650029242287828579
		h = (h^refRound(0, v3))*11400714785074694791 + 9650029242287828579
		h = (h^refRound(0, v4))*11400714785074694791 + 9650029242287828579
	}
	h += n
	for len(b) >= 8 {
		h ^= refRound(0, refU64(b[:8]))
		h = (h<<27|h>>37)*11400714785074694791 + 9650029242287828579
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(refU32(b[:4])) * 11400714785074694791
		h = (h<<23|h>>41)*14029467366897019727 + 1609587929392839161
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * 2870177450012600261
		h = (h<<11 | h>>53) * 11400714785074694791
	}
	h ^= h >> 33
	h *= 14029467366897019727
	h ^= h >> 29
	h *= 1609587929392839161
	h ^= h >> 32
	return h
}

func refRound(a, v uint64) uint64 {
	a += v * 14029467366897019727
	return (a<<31 | a>>33) * 11400714785074694791
}
func refU64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}
func refU32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func benchSize(b *testing.B, n int) {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(int64(n))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Sum(buf)
	}
}

func BenchmarkSum8(b *testing.B)   { benchSize(b, 8) }
func BenchmarkSum31(b *testing.B)  { benchSize(b, 31) }
func BenchmarkSum64(b *testing.B)  { benchSize(b, 64) }
func BenchmarkSum256(b *testing.B) { benchSize(b, 256) }
func BenchmarkSum1K(b *testing.B)  { benchSize(b, 1024) }
func BenchmarkSum64K(b *testing.B) { benchSize(b, 65536) }

func BenchmarkSSum1K(b *testing.B) {
	s := strings.Repeat("x", 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SSum(s)
	}
}
