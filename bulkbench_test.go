package core

import (
	"path/filepath"
	"testing"
)

func BenchmarkSharedPutOneByOne(b *testing.B) {
	p := filepath.Join(b.TempDir(), "s.core")
	m, _ := OpenShared(p, 8, 8, b.N*2, WithNoSync())
	defer m.Close()
	key := make([]byte, 8)
	val := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		putUint(key, uint64(i))
		m.Put(key, val)
	}
}

func BenchmarkSharedBatch(b *testing.B) {
	p := filepath.Join(b.TempDir(), "s.core")
	m, _ := OpenShared(p, 8, 8, b.N*2, WithNoSync())
	defer m.Close()
	val := make([]byte, 8)
	b.ResetTimer()
	m.Batch(func(put func(k, v []byte) (bool, error)) error {
		key := make([]byte, 8)
		for i := 0; i < b.N; i++ {
			putUint(key, uint64(i))
			put(key, val)
		}
		return nil
	})
}
