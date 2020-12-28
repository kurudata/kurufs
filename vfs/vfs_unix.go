// +build !windows

package vfs

import (
	"fmt"
	"syscall"

	"github.com/juicedata/juicefs/meta"

	"golang.org/x/sys/unix"
)

const O_ACCMODE = syscall.O_ACCMODE

const (
	MODE_MASK_R = 4
	MODE_MASK_W = 2
	MODE_MASK_X = 1
)

type Statfs struct {
	Bsize  uint32
	Blocks uint64
	Bavail uint64
	Files  uint64
	Favail uint64
}

func StatFS(ctx Context, ino Ino) (st *Statfs, err int) {
	var totalspace, availspace, iused, iavail uint64
	m.StatFS(ctx, &totalspace, &availspace, &iused, &iavail)
	var bsize uint64 = 0x10000
	blocks := totalspace / bsize
	bavail := blocks - (totalspace-availspace+bsize-1)/bsize

	st = new(Statfs)
	st.Bsize = uint32(bsize)
	st.Blocks = blocks
	st.Bavail = bavail
	st.Files = iused + iavail
	st.Favail = iavail
	logit(ctx, "statfs (%d): OK (%d,%d,%d,%d)", ino, totalspace-availspace, availspace, iused, iavail)
	return
}

func accessTest(attr *Attr, mmode uint16, uid uint32, gid uint32) syscall.Errno {
	if uid == 0 {
		return 0
	}
	mode := attr.Mode
	var effected uint16
	if uid == attr.Uid {
		effected = (mode >> 6) & 7
	} else {
		effected = mode & 7
		if gid == attr.Gid {
			effected = (mode >> 3) & 7
		}
	}
	if mmode&effected != mmode {
		return syscall.EACCES
	}
	return 0
}

func Access(ctx Context, ino Ino, mask int) (err syscall.Errno) {
	defer func() { logit(ctx, "access (%d,0x%X): %s", ino, mask, strerr(err)) }()
	var mmode uint16
	if mask&unix.R_OK != 0 {
		mmode |= MODE_MASK_R
	}
	if mask&unix.W_OK != 0 {
		mmode |= MODE_MASK_W
	}
	if mask&unix.X_OK != 0 {
		mmode |= MODE_MASK_X
	}
	if IsSpecialNode(ino) {
		node := getInternalNode(ino)
		err = accessTest(node.attr, mmode, ctx.Uid(), ctx.Gid())
		return
	}

	err = m.Access(ctx, ino, mmode)
	return
}

func setattrStr(set int, mode, uid, gid uint32, atime, mtime int64, size uint64) string {
	s := ""
	if set&meta.SET_ATTR_MODE != 0 {
		s += fmt.Sprintf("mode=%s:0%04o;", smode(uint16(mode)), (mode & 07777))
	}
	if set&meta.SET_ATTR_UID != 0 {
		s += fmt.Sprintf("uid=%d;", uid)
	}
	if set&meta.SET_ATTR_GID != 0 {
		s += fmt.Sprintf("gid=%d;", gid)
	}
	if (set&meta.SET_ATTR_ATIME) != 0 && atime < 0 {
		s += fmt.Sprintf("atime=NOW;")
	} else if set&meta.SET_ATTR_ATIME != 0 {
		s += fmt.Sprintf("atime=%d;", atime)
	}
	if (set&meta.SET_ATTR_MTIME) != 0 && mtime < 0 {
		s += fmt.Sprintf("mtime=NOW;")
	} else if set&meta.SET_ATTR_MTIME != 0 {
		s += fmt.Sprintf("mtime=%d;", mtime)
	}
	if (set & meta.SET_ATTR_SIZE) != 0 {
		s += fmt.Sprintf("size=%d;", size)
	}
	return s
}

func SetAttr(ctx Context, ino Ino, set int, opened uint8, mode, uid, gid uint32, atime, mtime int64, atimensec, mtimensec uint32, size uint64) (entry *meta.Entry, err syscall.Errno) {
	str := setattrStr(set, mode, uid, gid, atime, mtime, size)
	defer func() {
		logit(ctx, "setattr (%d,0x%X,[%s]): %s%s", ino, set, str, strerr(err), (*Entry)(entry))
	}()
	if IsSpecialNode(ino) {
		n := getInternalNode(ino)
		entry = &meta.Entry{Inode: ino, Attr: n.attr}
		return
	}
	err = syscall.EINVAL
	var attr = &Attr{}
	if (set & (meta.SET_ATTR_MODE | meta.SET_ATTR_UID | meta.SET_ATTR_GID | meta.SET_ATTR_ATIME | meta.SET_ATTR_MTIME | meta.SET_ATTR_SIZE)) == 0 {
		// change other flags or change nothing
		err = m.SetAttr(ctx, ino, opened, 0, 0, attr)
		if err != 0 {
			return
		}
	}
	if (set & (meta.SET_ATTR_MODE | meta.SET_ATTR_UID | meta.SET_ATTR_GID | meta.SET_ATTR_ATIME | meta.SET_ATTR_MTIME | meta.SET_ATTR_ATIME_NOW | meta.SET_ATTR_MTIME_NOW)) != 0 {
		if (set & meta.SET_ATTR_MODE) != 0 {
			attr.Mode = uint16(mode & 07777)
		}
		if (set & meta.SET_ATTR_UID) != 0 {
			attr.Uid = uid
		}
		if (set & meta.SET_ATTR_GID) != 0 {
			attr.Gid = gid
		}
		if set&meta.SET_ATTR_ATIME != 0 {
			attr.Atime = atime
			attr.Atimensec = atimensec
		}
		if (set & meta.SET_ATTR_MTIME) != 0 {
			attr.Mtime = mtime
			attr.Mtimensec = mtimensec
		}
		err = m.SetAttr(ctx, ino, opened, uint16(set), 0, attr)
		if err != 0 {
			return
		}
	}
	if set&meta.SET_ATTR_SIZE != 0 {
		err = Truncate(ctx, ino, int64(size), opened, attr)
	}
	entry = &meta.Entry{Inode: ino, Attr: attr}
	return
}
