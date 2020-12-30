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
