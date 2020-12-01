package vfs

import (
	"jfs/meta"
	"os"
	"time"
)

const (
	minInternalNode = 0x7FFFFFFFFFFFF0

	oplogInode  = minInternalNode + 1
	configInode = minInternalNode + 3
)

type internalNode struct {
	inode Ino
	name  string
	attr  *Attr
}

var internalNodes = []*internalNode{
	{oplogInode, ".oplog", &Attr{Mode: 0400}},
	{configInode, ".jfsconfig", &Attr{Mode: 0400}},
}

func init() {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	now := time.Now().Unix()
	for _, v := range internalNodes {
		v.attr.Typ = meta.TYPE_FILE
		v.attr.Uid = uid
		v.attr.Gid = gid
		v.attr.Atime = now
		v.attr.Mtime = now
		v.attr.Ctime = now
		v.attr.Nlink = 1
	}
}

func IsSpecialNode(ino Ino) bool {
	return ino >= minInternalNode
}

func isSpecialName(name string) bool {
	if name[0] != '.' {
		return false
	}
	for _, n := range internalNodes {
		if name == n.name {
			return true
		}
	}
	return false
}

func getInternalNode(ino Ino) *internalNode {
	for _, n := range internalNodes {
		if ino == n.inode {
			return n
		}
	}
	return nil
}

func getInternalNodeByName(name string) *internalNode {
	for _, n := range internalNodes {
		if name == n.name {
			return n
		}
	}
	return nil
}
