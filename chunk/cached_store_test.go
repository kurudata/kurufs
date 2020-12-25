package chunk

import (
	"context"
	"github.com/juicedata/juicesync/object"
	"testing"
	"time"
)

func BenchmarkCachedRead(b *testing.B) {
	blob := object.CreateStorage("mem", "", "", "")
	config := defaultConf
	config.PageSize = 4 << 20
	store := NewCachedStore(blob, config)
	w := store.NewWriter(1)
	w.WriteAt(make([]byte, 1024), 0)
	if err := w.Finish(1024); err != nil {
		b.Fatalf("write fail: %s", err)
	}
	time.Sleep(time.Millisecond * 100)
	p := NewPage(make([]byte, 1024))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := store.NewReader(1, 1024)
		if n, err := r.ReadAt(context.Background(), p, 0); err != nil || n != 1024 {
			b.FailNow()
		}
	}
}

func BenchmarkUncachedRead(b *testing.B) {
	blob := object.CreateStorage("mem", "", "", "")
	config := defaultConf
	config.PageSize = 4 << 20
	config.CacheSize = 0
	store := NewCachedStore(blob, config)
	w := store.NewWriter(2)
	w.WriteAt(make([]byte, 1024), 0)
	if err := w.Finish(1024); err != nil {
		b.Fatalf("write fail: %s", err)
	}
	p := NewPage(make([]byte, 1024))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := store.NewReader(2, 1024)
		if n, err := r.ReadAt(context.Background(), p, 0); err != nil || n != 1024 {
			b.FailNow()
		}
	}
}
