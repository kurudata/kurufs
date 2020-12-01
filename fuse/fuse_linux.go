package fuse

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

func getUmask(in *fuse.MknodIn) uint16 {
	return uint16(in.Umask)
}

func setBlksize(out *fuse.Attr, size uint32) {
	out.Blksize = size
}
