package utils

import (
	"io"
	"os"
	"testing"
)

func testCompress(t *testing.T, c Compressor) {
	src := []byte("hello")
	dst := make([]byte, c.CompressBound(len(src)))
	n, err := c.Compress(dst, src)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	src2 := make([]byte, len(src))
	n, err = c.Decompress(src2, dst[:n])
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if string(src2[:n]) != string(src) {
		t.Error("not matched", string(src2))
		t.FailNow()
	}
}

func TestUncompressed(t *testing.T) {
	testCompress(t, NewCompressor("none"))
}

func TestZstd0(t *testing.T) {
	testCompress(t, NewCompressor("zstd0"))
}

func TestZstd(t *testing.T) {
	testCompress(t, NewCompressor("zstd"))
}

func TestGoLZ4(t *testing.T) {
	testCompress(t, NewCompressor("LZ4"))
}

func TestLZ4Stream(t *testing.T) {
	testCompress(t, NewCompressor("LZ4Stream"))
}

func benchmarkDecompress(b *testing.B, comp Compressor) {
	f, _ := os.Open(os.Getenv("PAYLOAD"))
	var c = make([]byte, 5<<20)
	var d = make([]byte, 4<<20)
	n, err := io.ReadFull(f, d)
	f.Close()
	if err != nil {
		b.Skip()
		return
	}
	d = d[:n]
	n, err = comp.Compress(c[:4<<20], d)
	if err != nil {
		b.Errorf("compress: %s", err)
		b.FailNow()
	}
	c = c[:n]
	// println("compres", comp.Name(), len(c), len(d))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := comp.Decompress(d, c)
		if err != nil {
			b.Errorf("decompress %d %s", n, err)
			b.FailNow()
		}
		b.SetBytes(int64(len(d)))
	}
}

func BenchmarkDecompressZstd0(b *testing.B) {
	benchmarkDecompress(b, NewCompressor("zstd0"))
}

func BenchmarkDecompressZstd(b *testing.B) {
	benchmarkDecompress(b, NewCompressor("zstd"))
}

func BenchmarkDecompressGoLZ4Stream(b *testing.B) {
	benchmarkDecompress(b, NewCompressor("lz4stream"))
}

func BenchmarkDecompressGoLZ4(b *testing.B) {
	benchmarkDecompress(b, NewCompressor("lz4"))
}

func BenchmarkDecompressLZ4(b *testing.B) {
	benchmarkDecompress(b, LZ4{})
}

func BenchmarkDecompressNone(b *testing.B) {
	benchmarkDecompress(b, NewCompressor("none"))
}

func benchmarkCompress(b *testing.B, comp Compressor) {
	f, _ := os.Open(os.Getenv("PAYLOAD"))
	var d = make([]byte, 4<<20)
	n, err := io.ReadFull(f, d)
	f.Close()
	if err != nil {
		b.Skip()
		return
	}
	d = d[:n]
	var c = make([]byte, 5<<20)
	// println("compres", comp.Name(), len(c), len(d))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := comp.Compress(c, d)
		if err != nil {
			b.Errorf("compress %d %s", n, err)
			b.FailNow()
		}
		b.SetBytes(int64(len(d)))
	}
}

func BenchmarkCompressZstd0(b *testing.B) {
	benchmarkCompress(b, NewCompressor("Zstd0"))
}

func BenchmarkCompressZstd(b *testing.B) {
	benchmarkCompress(b, NewCompressor("Zstd"))
}
func BenchmarkCompressLZ4Stream(b *testing.B) {
	benchmarkCompress(b, NewCompressor("LZ4Stream"))
}
func BenchmarkCompressGoLZ4(b *testing.B) {
	benchmarkCompress(b, NewCompressor("LZ4"))
}
func BenchmarkCompressCLZ4(b *testing.B) {
	benchmarkCompress(b, LZ4{})
}
func BenchmarkCompressNone(b *testing.B) {
	benchmarkCompress(b, NewCompressor("none"))
}
