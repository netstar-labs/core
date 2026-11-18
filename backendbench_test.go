package core

import (
	"path/filepath"
	"testing"
)

// A common surface across all four backends so one benchmark body can drive each.
type kv interface {
	Put(key, val []byte) (bool, error)
	GetInto(key, dst []byte) bool
	Close() error
}

const benchN = 1_000_000
const benchShards = 16

func openBackend(b *testing.B, kind string) kv {
	b.Helper()
	dir := b.TempDir()
	var m kv
	var err error
	switch kind {
	case "map":
		m, err = Open(filepath.Join(dir, "b.core"), 8, 8, benchN*2)
	case "sharded":
		m, err = OpenSharded(filepath.Join(dir, "b"), 8, 8, benchShards, benchN*2/benchShards)
	case "shared":
		m, err = OpenShared(filepath.Join(dir, "b.core"), 8, 8, benchN*2)
	case "sharedsharded":
		m, err = OpenSharedSharded(filepath.Join(dir, "b"), 8, 8, benchShards, benchN*2/benchShards)
	}
	if err != nil {
		b.Fatal(err)
	}
	return m
}

func fill(b *testing.B, m kv) {
	key := make([]byte, 8)
	val := make([]byte, 8)
	for i := 0; i < benchN; i++ {
		putUint(key, uint64(i))
		if _, err := m.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func benchGet(b *testing.B, kind string) {
	m := openBackend(b, kind)
	defer m.Close()
	fill(b, m)
	key := make([]byte, 8)
	dst := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		putUint(key, uint64(i%benchN))
		m.GetInto(key, dst)
	}
}

func benchGetParallel(b *testing.B, kind string) {
	m := openBackend(b, kind)
	defer m.Close()
	fill(b, m)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 8)
		dst := make([]byte, 8)
		i := uint64(0)
		for pb.Next() {
			i++
			putUint(key, i%benchN)
			m.GetInto(key, dst)
		}
	})
}

func benchPutParallel(b *testing.B, kind string) {
	m := openBackend(b, kind)
	defer m.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 8)
		val := make([]byte, 8)
		var i uint64
		for pb.Next() {
			// Spread writes across the key space; unique-ish keys per goroutine.
			i++
			putUint(key, (i*2654435761)%(benchN*4))
			m.Put(key, val)
		}
	})
}

func BenchmarkGet_Map(b *testing.B)           { benchGet(b, "map") }
func BenchmarkGet_Sharded(b *testing.B)       { benchGet(b, "sharded") }
func BenchmarkGet_Shared(b *testing.B)        { benchGet(b, "shared") }
func BenchmarkGet_SharedSharded(b *testing.B) { benchGet(b, "sharedsharded") }

func BenchmarkGetParallel_Map(b *testing.B)           { benchGetParallel(b, "map") }
func BenchmarkGetParallel_Sharded(b *testing.B)       { benchGetParallel(b, "sharded") }
func BenchmarkGetParallel_Shared(b *testing.B)        { benchGetParallel(b, "shared") }
func BenchmarkGetParallel_SharedSharded(b *testing.B) { benchGetParallel(b, "sharedsharded") }

func BenchmarkPutParallel_Map(b *testing.B)           { benchPutParallel(b, "map") }
func BenchmarkPutParallel_Sharded(b *testing.B)       { benchPutParallel(b, "sharded") }
func BenchmarkPutParallel_Shared(b *testing.B)        { benchPutParallel(b, "shared") }
func BenchmarkPutParallel_SharedSharded(b *testing.B) { benchPutParallel(b, "sharedsharded") }
