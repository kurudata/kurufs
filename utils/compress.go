package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/DataDog/zstd"
	"github.com/pierrec/lz4"
)

const ZSTD_LEVEL = 1 // fastest

type Compressor interface {
	Name() string
	CompressBound(int) int
	Compress(dst, src []byte) (int, error)
	Decompress(dst, src []byte) (int, error)
	NewReader(r io.Reader) io.ReadCloser
}

func NewCompressor(algr string) Compressor {
	algr = strings.ToLower(algr)
	if algr == "zstd0" || algr == "zstd" {
		return &ZStandard{ZSTD_LEVEL}
	} else if algr == "lz4" {
		return &LZ4{}
	} else if algr == "lz4stream" {
		return &LZ4Stream{}
	} else if algr == "none" || algr == "" {
		return noOp{}
	}
	return nil
}

type noOp struct{}

func (n noOp) Name() string            { return "Noop" }
func (n noOp) CompressBound(l int) int { return l }
func (n noOp) Compress(dst, src []byte) (int, error) {
	if len(dst) < len(src) {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(src))
	}
	copy(dst, src)
	return len(src), nil
}
func (n noOp) Decompress(dst, src []byte) (int, error) {
	if len(dst) < len(src) {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(src))
	}
	copy(dst, src)
	return len(src), nil
}
func (n noOp) NewReader(r io.Reader) io.ReadCloser { return ioutil.NopCloser(r) }

type ZStandard struct {
	level int
}

func (n *ZStandard) Name() string            { return "Zstd" }
func (n *ZStandard) CompressBound(l int) int { return zstd.CompressBound(l) }
func (n *ZStandard) Compress(dst, src []byte) (int, error) {
	d, err := zstd.CompressLevel(dst, src, n.level)
	if err != nil {
		return 0, err
	}
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("buffer too short: %d < %d", cap(dst), cap(d))
	}
	return len(d), err
}
func (n *ZStandard) Decompress(dst, src []byte) (int, error) {
	d, err := zstd.Decompress(dst, src)
	if err != nil {
		return 0, err
	}
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(d))
	}
	return len(d), err
}
func (n *ZStandard) NewReader(r io.Reader) io.ReadCloser {
	return zstd.NewReader(r)
}

type LZ4 struct{}

func (l LZ4) Name() string               { return "LZ4" }
func (l LZ4) CompressBound(size int) int { return LZ4_compressBound(size) }
func (l LZ4) Compress(dst, src []byte) (int, error) {
	return int(LZ4_compress_default(src, dst)), nil
}
func (l LZ4) Decompress(dst, src []byte) (int, error) {
	return LZ4_decompress_fast(src, dst)
}
func (l LZ4) NewReader(r io.Reader) io.ReadCloser {
	panic("not supported")
}

const lz4BlockSize = 64 << 10

type LZ4Stream struct{}

func (z *LZ4Stream) Name() string            { return "LZ4Stream" }
func (z *LZ4Stream) CompressBound(l int) int { return 15 + l + (l/lz4BlockSize+1)*4 + 4 }
func (z *LZ4Stream) Compress(dst, src []byte) (int, error) {
	buf := bytes.NewBuffer(dst[:0])
	w := lz4.NewWriter(buf)
	w.NoChecksum = true
	w.BlockMaxSize = lz4BlockSize
	_, err := w.Write(src)
	if err != nil {
		return 0, err
	}
	err = w.Close()
	if err != nil {
		return 0, err
	}
	return buf.Len(), err
}
func (z *LZ4Stream) Decompress(dst, src []byte) (int, error) {
	r := lz4.NewReader(bytes.NewBuffer(src))
	var got int
	for got < len(dst) {
		n, err := r.Read(dst[got:])
		if err != nil {
			return got, err
		}
		got += n
	}
	return got, nil
}
func (z *LZ4Stream) NewReader(r io.Reader) io.ReadCloser {
	r = bufio.NewReaderSize(r, 4<<10)
	return ioutil.NopCloser(lz4.NewReader(r))
}
