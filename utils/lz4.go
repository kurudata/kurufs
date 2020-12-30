/*
 * JuiceFS, Copyright (C) 2020 Juicedata, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
