package fuse

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

func getUmask(in *fuse.MknodIn) uint16 {
	return 0
}

func setBlksize(out *fuse.Attr, size uint32) {
}
