package fuse

import (
	"github.com/juicedata/juicefs/meta"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func attrToStat(inode Ino, attr *Attr, out *fuse.Attr) {
	out.Ino = uint64(inode)
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Mode = attr.SMode()
	out.Nlink = attr.Nlink
	out.Atime = uint64(attr.Atime)
	out.Atimensec = attr.Atimensec
	out.Mtime = uint64(attr.Mtime)
	out.Mtimensec = attr.Mtimensec
	out.Ctime = uint64(attr.Ctime)
	out.Ctimensec = attr.Ctimensec

	var size, blocks uint64
	switch attr.Typ {
	case meta.TYPE_DIRECTORY:
		fallthrough
	case meta.TYPE_SYMLINK:
		fallthrough
	case meta.TYPE_FILE:
		size = attr.Length
		blocks = (size + 511) / 512
	case meta.TYPE_BLOCKDEV:
		fallthrough
	case meta.TYPE_CHARDEV:
		out.Rdev = attr.Rdev
	}
	out.Size = size
	out.Blocks = blocks
	setBlksize(out, 0x10000)
}
