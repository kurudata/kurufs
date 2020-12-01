package utils

/*
#include "lz4.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func LZ4_compressBound(leng int) int {
	return int(C.LZ4_compressBound(C.int(leng)))
}

func LZ4_compress_default(src []byte, dst []byte) uint32 {
	return uint32(C.LZ4_compress_default(
		(*C.char)(unsafe.Pointer(&src[0])),
		(*C.char)(unsafe.Pointer(&dst[0])),
		C.int(len(src)),
		C.int(len(dst))))
}

func LZ4_decompress_safe(src []byte) ([]byte, error) {
	dst := make([]byte, len(src)*3)
	n := int(C.LZ4_decompress_safe(
		(*C.char)(unsafe.Pointer(&src[0])),
		(*C.char)(unsafe.Pointer(&dst[0])),
		C.int(len(src)), C.int(len(dst))))
	for n < 0 {
		dst = make([]byte, len(dst)*2)
		n = int(C.LZ4_decompress_safe(
			(*C.char)(unsafe.Pointer(&src[0])),
			(*C.char)(unsafe.Pointer(&dst[0])),
			C.int(len(src)), C.int(len(dst))))
	}
	return dst[:n], nil
}

func LZ4_decompress_fast(src []byte, dst []byte) (int, error) {
	n := int(C.LZ4_decompress_safe(
		(*C.char)(unsafe.Pointer(&src[0])),
		(*C.char)(unsafe.Pointer(&dst[0])),
		C.int(len(src)), C.int(len(dst))))
	if n < 0 {
		return 0, fmt.Errorf("LZ4 decompresss failed: %d", n)
	}
	return n, nil
}
