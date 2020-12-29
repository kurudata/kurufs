package chunk

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestExpand(t *testing.T) {
	rs := expandDir("/not/exists/jfsCache")
	if len(rs) != 1 || rs[0] != "/not/exists/jfsCache" {
		t.Errorf("expand: %v", rs)
		t.FailNow()
	}

	os.Mkdir("/tmp/aaa1", 0755)
	os.Mkdir("/tmp/aaa2", 0755)
	os.Mkdir("/tmp/aaa3", 0755)
	os.Mkdir("/tmp/aaa3/jfscache", 0755)
	os.Mkdir("/tmp/aaa3/jfscache/jfs", 0755)

	rs = expandDir("/tmp/aaa*/jfscache/jfs")
	if len(rs) != 3 || rs[0] != "/tmp/aaa1/jfscache/jfs" {
		t.Errorf("expand: %v", rs)
		t.FailNow()
	}
}

func TestCacheStore(t *testing.T) {
	s := newCacheStore("/tmp/diskCache1", 10240, 3, 1, &defaultConf)
	key := "chunks/1"
	s.stage(key, []byte{1}, true)
	f, err := s.load(key)
	if err != nil {
		t.Errorf("load %s: %s", key, err)
		t.FailNow()
	} else {
		f.Close()
		os.Remove(s.stagePath(key))
		s.uploaded(key, 1)
	}
	for i := 0; i < 10; i++ {
		s.add(strconv.Itoa(i), 10, uint32(time.Now().Unix()))
		if len(s.keys) > 2 {
			t.Errorf("should not cache more than 2 items, but got %d", len(s.keys))
			t.FailNow()
		}
		if s.used > 10240 {
			t.Errorf("should not cache more than 10240 bytes, but got %d", s.used)
			t.FailNow()
		}
	}
}

func BenchmarkLoadCached(b *testing.B) {
	s := newCacheStore("/tmp/diskCache", 1<<30, 1<<10, 1, &defaultConf)
	p := NewPage(make([]byte, 1024))
	key := "/chunks/1_1024"
	s.cache(key, p)
	time.Sleep(time.Millisecond * 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f, e := s.load(key); e == nil {
			f.Close()
		} else {
			b.FailNow()
		}
	}
}

func BenchmarkLoadUncached(b *testing.B) {
	s := newCacheStore("/tmp/diskCache", 1<<30, 1<<10, 1, &defaultConf)
	key := "/chunks/222_1024"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f, e := s.load(key); e != nil {
			f.Close()
		}
	}
}
