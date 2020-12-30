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

// #include <stdlib.h>
import "C"
import (
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

var useHeap = os.Getenv("JFS_OFFHEAP") == ""
var slabs = make(map[uintptr][]byte)
var slabsMutex sync.Mutex

func Alloc(size int) []byte {
	if useHeap {
		b := make([]byte, size)
		ptr := unsafe.Pointer(&b[0])
		slabsMutex.Lock()
		slabs[uintptr(ptr)] = b
		slabsMutex.Unlock()
		return b
	}
	var p unsafe.Pointer
	p = C.malloc(C.size_t(size))
	for p == nil {
		time.Sleep(time.Millisecond)
		p = C.malloc(C.size_t(size))
	}
	return (*[1 << 30]byte)(p)[:size:size]
}

func Free(buf []byte) {
	// buf could be zero when writing
	p := unsafe.Pointer(&buf[:1][0])
	if useHeap {
		slabsMutex.Lock()
		if _, ok := slabs[uintptr(p)]; !ok {
			panic("invalid pointer")
		}
		delete(slabs, uintptr(p))
		slabsMutex.Unlock()
	} else {
		C.free(p)
	}
}

func init() {
	if useHeap {
		go func() {
			for {
				runtime.GC()
				time.Sleep(time.Minute * 10)
			}
		}()
	}
}
