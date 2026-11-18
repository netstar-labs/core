package core

import (
	"path/filepath"
	"testing"
)

func benchMap(b *testing.B, kl, vl, n int) *Map {
	p := filepath.Join(b.TempDir(), "b.core")
	m, err := Open(p, kl, vl, n*2)
	if err != nil {
		b.Fatal(err)
	}
	key := make([]byte, kl)
	val := make([]byte, vl)
	for i := 0; i < n; i++ {
		putUint(key, uint64(i))
		m.Put(key, val)
	}
	return m
}

func putUint(b []byte, u uint64) {
	for i := 0; i < len(b); i += 8 {
		b[i] = byte(u)
		b[i+1] = byte(u >> 8)
		b[i+2] = byte(u >> 16)
		b[i+3] = byte(u >> 24)
		b[i+4] = byte(u >> 32)
		b[i+5] = byte(u >> 40)
		b[i+6] = byte(u >> 48)
		b[i+7] = byte(u >> 56)
	}
}

func BenchmarkGet8_8(b *testing.B) {
	const n = 1_000_000
	m := benchMap(b, 8, 8, n)
	defer m.Close()
	key := make([]byte, 8)
	dst := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		putUint(key, uint64(i%n))
		m.GetInto(key, dst)
	}
}

func BenchmarkUpdate8_8(b *testing.B) {
	const n = 1_000_000
	m := benchMap(b, 8, 8, n)
	defer m.Close()
	key := make([]byte, 8)
	val := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		putUint(key, uint64(i%n))
		m.Update(key, val)
	}
}

func BenchmarkGetParallel8_8(b *testing.B) {
	const n = 1_000_000
	m := benchMap(b, 8, 8, n)
	defer m.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 8)
		dst := make([]byte, 8)
		i := uint64(0)
		for pb.Next() {
			i++
			putUint(key, i%n)
			m.GetInto(key, dst)
		}
	})
}
