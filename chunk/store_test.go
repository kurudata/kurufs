package chunk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicesync/object"
	"jfs/utils"
)

func testStore(t *testing.T, store ChunkStore) {
	writer := store.NewWriter(1)
	data := []byte("hello world")
	if n, err := writer.WriteAt(data, 0); n != 11 || err != nil {
		t.Fatalf("write fail: %d %s", n, err)
	}
	offset := defaultConf.PageSize - 3
	if n, err := writer.WriteAt(data, int64(offset)); err != nil || n != 11 {
		t.Fatalf("write fail: %d %s", n, err)
	}
	if err := writer.FlushTo(defaultConf.PageSize + 3); err != nil {
		t.Fatalf("flush fail: %s", err)
	}
	size := offset + len(data)
	if err := writer.Finish(size); err != nil {
		t.Fatalf("finish fail: %s", err)
	}
	defer store.Remove(1, size)

	reader := store.NewReader(1, size)
	p := NewPage(make([]byte, 5))
	if n, err := reader.ReadAt(context.Background(), p, 6); n != 5 || err != nil {
		t.Fatalf("read failed: %d %s", n, err)
	} else if string(p.Data[:n]) != "world" {
		t.Fatalf("not expected: %s", string(p.Data[:n]))
	}
	p = NewPage(make([]byte, 20))
	if n, err := reader.ReadAt(context.Background(), p, offset); n != 11 || err != nil && err != io.EOF {
		t.Fatalf("read failed: %d %s", n, err)
	} else if string(p.Data[:n]) != "hello world" {
		t.Fatalf("not expected: %s", string(p.Data[:n]))
	}
}

func TestDiskStore(t *testing.T) {
	testStore(t, NewDiskStore("/tmp/diskStore"))
}

var defaultConf = Config{
	PageSize:    1024,
	Partitions:  1,
	CacheDir:    "/tmp/diskCache",
	CacheSize:   10,
	AsyncUpload: false,
	MaxUpload:   1,
	UploadLimit: 0,
	PutTimeout:  time.Second,
	GetTimeout:  time.Second * 2,
}

func TestPermissionOfCachedBlock(t *testing.T) {
	cfg := defaultConf
	cfg.CacheMode = 0640
	cfg.CacheDir = "/tmp/testdir"
	cfg.AutoCreate = true
	mem := object.CreateStorage("mem", "", "", "")
	store := NewCachedStore(mem, cfg)
	testStore(t, store)
	dirs := []string{cfg.CacheDir, filepath.Join(cfg.CacheDir, "raw"), filepath.Join(cfg.CacheDir, "raw", "chunks")}
	for _, d := range dirs {
		st, err := os.Stat(d)
		if err != nil {
			t.Errorf("no cache dir %s: %s", d, err)
		} else if st.Mode()&0777 != 0770 {
			t.Errorf("mode of %s should be 0770, but got %o", d, st.Mode()&0777)
		}
	}
}

func TestCachedStore(t *testing.T) {
	mem := object.CreateStorage("mem", "", "", "")
	store := NewCachedStore(mem, defaultConf)
	testStore(t, store)
}

func TestNotCompressedStore(t *testing.T) {
	mem := object.CreateStorage("mem", "", "", "")
	conf := defaultConf
	conf.Compress = ""
	conf.CacheSize = 0
	store := NewCachedStore(mem, conf)
	testStore(t, store)
}

func TestPartitionedStore(t *testing.T) {
	mem := object.CreateStorage("mem", "", "", "")
	conf := defaultConf
	conf.Partitions = 10
	store := NewCachedStore(mem, conf)
	testStore(t, store)
}

func TestCachedStoreWithLimit(t *testing.T) {
	mem := object.CreateStorage("mem", "", "", "")
	conf := defaultConf
	conf.UploadLimit = 128
	store := NewCachedStore(mem, conf)
	writer := store.NewWriter(1)
	data := make([]byte, 256)
	// make them hard to compress
	for i := range data {
		data[i] = byte(i)
	}
	st := time.Now()
	if _, err := writer.WriteAt(data, 0); err != nil {
		t.Error(err)
	}
	writer.Finish(len(data))
	if time.Now().Sub(st) < time.Second {
		t.Error(fmt.Errorf("Writing too fast: %s", time.Now().Sub(st)))
	}
}

func TestAsyncStore(t *testing.T) {
	mem := object.CreateStorage("mem", "", "", "")
	conf := defaultConf
	conf.CacheDir = "/tmp/testdirAsync"
	p := filepath.Join(conf.CacheDir, stagingDir, "chunks/0/0/123_0")
	os.MkdirAll(filepath.Dir(p), 0744)
	f, _ := os.Create(p)
	f.WriteString("good")
	f.Close()
	conf.AsyncUpload = true
	conf.UploadLimit = 0
	store := NewCachedStore(mem, conf)
	time.Sleep(time.Millisecond * 10) // wait for scan to finish
	if mem.Exists("chunks/0/0/123_0_4") != nil {
		t.Fatalf("staging object should be upload")
	}
	testStore(t, store)
}

func TestRecoverAppendedObject(t *testing.T) {
	mem := object.CreateStorage("mem", "", "", "")
	comp := utils.NewCompressor("zstd")
	var block = make([]byte, 2)
	err := recoverAppendedKey(mem, "chunks/1_0_2", comp, block)
	if err == nil {
		t.Fatalf("recover should fail")
		t.FailNow()
	}

	data := []byte("hello")
	buf := make([]byte, 1024)
	n, _ := comp.Compress(buf, data)
	mem.Put("chunks/1_0_5", bytes.NewReader(buf[:n]))
	err = recoverAppendedKey(mem, "chunks/1_0_2", comp, block)
	if err != nil {
		t.Fatalf("recover fail: %s", err)
	}
	if string(block) != "he" {
		t.Fatalf("unexpected result: %v != %v", string(block), "he")
	}
}
