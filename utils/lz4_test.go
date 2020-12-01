package utils

import (
	"bytes"
	"testing"
)

func TestLZ4(t *testing.T) {
	src := []byte("hello world")
	dst := make([]byte, LZ4_compressBound((len(src))))
	comp := dst[:LZ4_compress_default(src, dst)]
	decomp := make([]byte, len(src))
	if _, err := LZ4_decompress_safe(comp); err != nil {
		t.Fatal(err)
	}
	if _, err := LZ4_decompress_fast(comp, decomp); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(src, decomp) {
		t.Fatalf("expect %s, but got %s", string(src), string(decomp))
	}
}
