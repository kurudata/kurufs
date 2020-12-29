package redis

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	. "github.com/juicedata/juicefs/meta"
	"github.com/juicedata/juicefs/utils"

	"github.com/go-redis/redis/v8"
)

/*
	Node: i$inode -> Attribute{type,mode,uid,gid,atime,mtime,ctime,nlink,length,rdev}
	Dir:   d$inode -> {name -> {inode,type}}
	File:  c$inode_$indx -> [Slice{pos,id,length,off,len},]
	Symlink: s$inode -> target

	TODO:
	Xattr: x$inode -> {name -> value}
	ACL:
	Posix Lock:
	Flock:

	Sessions
	Removed chunks
*/

var logger = utils.GetLogger("juicefs")

const usedSpace = "usedSpace"
const totalInodes = "totalInodes"
const delchunks = "delchunks"
const allSessions = "sessions"

type RedisConfig struct {
	Strict  bool // update ctime
	Retries int
}

type redisMeta struct {
	sync.Mutex
	conf *RedisConfig
	rdb  *redis.Client

	sid          int64
	openFiles    map[Ino]int
	removedFiles map[Ino]bool
	msgCallbacks *msgCallbacks
}

type msgCallbacks struct {
	sync.Mutex
	callbacks map[uint32]MsgCallback
}

func NewRedisMeta(url string, conf *RedisConfig) (Meta, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", url, err)
	}
	m := &redisMeta{
		conf:         conf,
		rdb:          redis.NewClient(opt),
		openFiles:    make(map[Ino]int),
		removedFiles: make(map[Ino]bool),
		msgCallbacks: &msgCallbacks{
			callbacks: make(map[uint32]MsgCallback),
		},
	}
	m.sid, err = m.rdb.Incr(c, "nextsession").Result()
	if err != nil {
		return nil, fmt.Errorf("create session: %s", err)
	}
	logger.Debugf("session is is %d", m.sid)
	go m.refreshSession()
	go m.cleanupChunks()
	return m, nil
}

func (r *redisMeta) Init(format Format) error {
	body, err := r.rdb.Get(c, "setting").Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if err == nil {
		return fmt.Errorf("this volume is already formated as: %s", body)
	}
	data, err := json.MarshalIndent(format, "", "")
	if err != nil {
		logger.Fatalf("json: %s", err)
	}
	return r.rdb.Set(c, "setting", data, 0).Err()
}

func (r *redisMeta) Load() (*Format, error) {
	body, err := r.rdb.Get(c, "setting").Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("no volume found")
	}
	if err != nil {
		return nil, err
	}
	var format Format
	err = json.Unmarshal([]byte(body), &format)
	if err != nil {
		return nil, fmt.Errorf("json: %s", err)
	}
	return &format, nil
}

func (r *redisMeta) OnMsg(mtype uint32, cb MsgCallback) {
	r.msgCallbacks.Lock()
	defer r.msgCallbacks.Unlock()
	r.msgCallbacks.callbacks[mtype] = cb
}

func (r *redisMeta) newMsg(mid uint32, args ...interface{}) error {
	r.msgCallbacks.Lock()
	cb, ok := r.msgCallbacks.callbacks[mid]
	r.msgCallbacks.Unlock()
	if ok {
		return cb(args...)
	}
	panic("not callback for " + strconv.Itoa(int(mid)))
}

var c = context.TODO()

func (r *redisMeta) sessionKey(sid int64) string {
	return fmt.Sprintf("session%d", r.sid)
}

func (r *redisMeta) symKey(inode Ino) string {
	return fmt.Sprintf("s%d", inode)
}

func (r *redisMeta) inodeKey(inode Ino) string {
	return fmt.Sprintf("i%d", inode)
}

func (r *redisMeta) entryKey(parent Ino) string {
	return fmt.Sprintf("d%d", parent)
}

func (r *redisMeta) chunkKey(inode Ino, indx uint32) string {
	return fmt.Sprintf("c%d_%d", inode, indx)
}

func (r *redisMeta) nextInode() (Ino, error) {
	ino, err := r.rdb.Incr(c, "nextinode").Uint64()
	if ino == 1 {
		ino, err = r.rdb.Incr(c, "nextinode").Uint64()
	}
	return Ino(ino), err
}

func (r *redisMeta) packEntry(_type uint8, inode Ino) []byte {
	wb := utils.NewBuffer(9)
	wb.Put8(_type)
	wb.Put64(uint64(inode))
	return wb.Bytes()
}

func (r *redisMeta) parseEntry(buf []byte) (uint8, Ino) {
	if len(buf) != 9 {
		panic("invalid entry")
	}
	return buf[0], Ino(binary.BigEndian.Uint64(buf[1:]))
}

func (r *redisMeta) parseAttr(buf []byte, attr *Attr) {
	if attr == nil {
		return
	}
	rb := utils.FromBuffer(buf)
	attr.Flags = rb.Get8()
	attr.Mode = rb.Get16()
	attr.Typ = uint8(attr.Mode >> 12)
	attr.Mode &= 0xfff
	attr.Uid = rb.Get32()
	attr.Gid = rb.Get32()
	attr.Atime = int64(rb.Get64())
	attr.Atimensec = rb.Get32()
	attr.Mtime = int64(rb.Get64())
	attr.Mtimensec = rb.Get32()
	attr.Ctime = int64(rb.Get64())
	attr.Ctimensec = rb.Get32()
	attr.Nlink = rb.Get32()
	attr.Length = rb.Get64()
	attr.Rdev = rb.Get32()
	attr.Full = true
	logger.Tracef("attr: %+v -> %+v", buf, attr)
}

func (r *redisMeta) marshal(attr *Attr) []byte {
	w := utils.NewBuffer(36 + 24 + 4)
	w.Put8(attr.Flags)
	w.Put16((uint16(attr.Typ) << 12) | (attr.Mode & 0xfff))
	w.Put32(attr.Uid)
	w.Put32(attr.Gid)
	w.Put64(uint64(attr.Atime))
	w.Put32(attr.Atimensec)
	w.Put64(uint64(attr.Mtime))
	w.Put32(attr.Mtimensec)
	w.Put64(uint64(attr.Ctime))
	w.Put32(attr.Ctimensec)
	w.Put32(attr.Nlink)
	w.Put64(attr.Length)
	w.Put32(attr.Rdev)
	logger.Tracef("attr: %+v -> %+v", attr, w.Bytes())
	return w.Bytes()
}

func align4K(length uint64) int64 {
	if length == 0 {
		return 0
	}
	return int64((((length - 1) >> 12) + 1) << 12)
}

func (r *redisMeta) StatFS(ctx Context, totalspace, availspace, iused, iavail *uint64) syscall.Errno {
	*totalspace = 1 << 50
	used, _ := r.rdb.IncrBy(c, usedSpace, 0).Result()
	used = ((used >> 16) + 1) << 16 // aligned to 64K
	*availspace = *totalspace - uint64(used)
	inodes, _ := r.rdb.IncrBy(c, totalInodes, 0).Result()
	*iused = uint64(inodes)
	*iavail = 10 << 20
	return 0
}

func (r *redisMeta) Lookup(ctx Context, parent Ino, name string, inode *Ino, attr *Attr) syscall.Errno {
	buf, err := r.rdb.HGet(c, r.entryKey(parent), name).Bytes()
	if err != nil {
		return errno(err)
	}
	_, ino := r.parseEntry(buf)
	a, err := r.rdb.Get(c, r.inodeKey(ino)).Bytes()
	if err == nil && attr != nil {
		r.parseAttr(a, attr)
		if attr.Typ == TYPE_DIRECTORY && r.conf.Strict {
			cnt, err := r.rdb.HLen(c, r.entryKey(ino)).Result()
			if err == nil {
				attr.Nlink = uint32(cnt + 2)
			}
		}
	}
	if inode != nil {
		*inode = ino
	}
	return errno(err)
}

func (r *redisMeta) Access(ctx Context, inode Ino, modemask uint16) syscall.Errno {
	return 0 // handled by kernel
}

func (r *redisMeta) GetAttr(ctx Context, inode Ino, opened uint8, attr *Attr) syscall.Errno {
	a, err := r.rdb.Get(c, r.inodeKey(inode)).Bytes()
	if inode == 1 && err == redis.Nil {
		// root inode
		attr.Flags = 0
		attr.Typ = TYPE_DIRECTORY
		attr.Mode = 0777
		attr.Uid = 0
		attr.Uid = 0
		ts := time.Now().Unix()
		attr.Atime = ts
		attr.Mtime = ts
		attr.Ctime = ts
		attr.Nlink = 2
		attr.Length = 4 << 10
		attr.Rdev = 0
		r.rdb.Set(c, r.inodeKey(inode), r.marshal(attr), 0)
		return 0
	}
	if err == nil {
		r.parseAttr(a, attr)
		if attr.Typ == TYPE_DIRECTORY && r.conf.Strict {
			cnt, err := r.rdb.HLen(c, r.entryKey(inode)).Result()
			if err == nil {
				attr.Nlink = uint32(cnt + 2)
			}
		}
	}
	return errno(err)
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if eno, ok := err.(syscall.Errno); ok {
		return eno
	}
	if err == redis.Nil {
		return syscall.ENOENT
	}
	logger.Errorf("error: %s", err)
	return syscall.EIO
}

func (r *redisMeta) txn(txf func(tx *redis.Tx) error, keys ...string) syscall.Errno {
	var err error
	for i := 0; i < 10; i++ {
		err = r.rdb.Watch(c, txf, keys...)
		if err == nil {
			return 0
		}
		if err == redis.TxFailedErr {
			continue
		}
		return errno(err)
	}
	return errno(err)
}

func (r *redisMeta) Truncate(ctx Context, inode Ino, flags uint8, length uint64, attr *Attr) syscall.Errno {
	return r.txn(func(tx *redis.Tx) error {
		var t Attr
		a, err := tx.Get(c, r.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		r.parseAttr(a, &t)
		old := t.Length
		t.Length = length
		now := time.Now()
		t.Ctime = now.Unix()
		t.Ctimensec = uint32(now.Nanosecond())
		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.Set(c, r.inodeKey(inode), r.marshal(&t), 0)
			if old > length {
				pipe.ZAdd(c, delchunks, &redis.Z{float64(now.Unix()), r.delChunks(inode, length, old)})
			} else if length > (old/CHUNKSIZE+1)*CHUNKSIZE {
				// zero out last chunks
				w := utils.NewBuffer(24)
				w.Put32(uint32(old % CHUNKSIZE))
				w.Put64(0)
				w.Put32(0)
				w.Put32(0)
				w.Put32(CHUNKSIZE - uint32(old%CHUNKSIZE))
				pipe.RPush(c, r.chunkKey(inode, uint32(old/CHUNKSIZE)), w.Bytes())
			}
			pipe.IncrBy(c, usedSpace, align4K(length)-align4K(old))
			return nil
		})
		if err == nil {
			if attr != nil {
				*attr = t
			}
			go r.deleteChunks(inode, length, old)
		}
		return err
	}, r.inodeKey(inode))
}

func (r *redisMeta) SetAttr(ctx Context, inode Ino, opened uint8, set uint16, sugidclearmode uint8, attr *Attr) syscall.Errno {
	return r.txn(func(tx *redis.Tx) error {
		var cur Attr
		a, err := tx.Get(c, r.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		r.parseAttr(a, &cur)
		if (set&(SET_ATTR_UID|SET_ATTR_GID)) != 0 && (set&SET_ATTR_MODE) != 0 {
			attr.Mode |= (cur.Mode & 06000)
		}
		if (cur.Mode&06000) != 0 && (set&(SET_ATTR_UID|SET_ATTR_GID)) != 0 {
			cur.Mode &= 01777
			attr.Mode &= 01777
		}
		if set&SET_ATTR_UID != 0 {
			cur.Uid = attr.Uid
		}
		if set&SET_ATTR_GID != 0 {
			cur.Gid = attr.Gid
		}
		if set&SET_ATTR_MODE != 0 {
			if ctx.Uid() != 0 && (attr.Mode&02000) != 0 {
				if ctx.Gid() != cur.Gid {
					attr.Mode &= 05777
				}
			}
			cur.Mode = attr.Mode
		}
		now := time.Now()
		if set&SET_ATTR_ATIME != 0 {
			cur.Atime = attr.Atime
			cur.Atimensec = attr.Atimensec
		}
		if set&SET_ATTR_ATIME_NOW != 0 {
			cur.Atime = now.Unix()
			cur.Atimensec = uint32(now.Nanosecond())
		}
		if set&SET_ATTR_MTIME != 0 {
			cur.Mtime = attr.Mtime
			cur.Mtimensec = attr.Mtimensec
		}
		if set&SET_ATTR_MTIME_NOW != 0 {
			cur.Mtime = now.Unix()
			cur.Mtimensec = uint32(now.Nanosecond())
		}
		cur.Ctime = now.Unix()
		cur.Ctimensec = uint32(now.Nanosecond())
		_, err = tx.Set(c, r.inodeKey(inode), r.marshal(&cur), 0).Result()
		if err == nil {
			*attr = cur
		}
		return err
	}, r.inodeKey(inode))
}

func (r *redisMeta) ReadLink(ctx Context, inode Ino, path *[]byte) syscall.Errno {
	buf, err := r.rdb.Get(c, r.symKey(inode)).Bytes()
	if err == nil {
		*path = buf
	}
	return errno(err)
}

func (r *redisMeta) Symlink(ctx Context, parent Ino, name string, path string, inode *Ino, attr *Attr) syscall.Errno {
	return r.mknod(ctx, parent, name, TYPE_SYMLINK, 0644, 022, 0, path, inode, attr)
}

func (r *redisMeta) Mknod(ctx Context, parent Ino, name string, _type uint8, mode, cumask uint16, rdev uint32, inode *Ino, attr *Attr) syscall.Errno {
	return r.mknod(ctx, parent, name, _type, mode, cumask, rdev, "", inode, attr)
}

func (r *redisMeta) mknod(ctx Context, parent Ino, name string, _type uint8, mode, cumask uint16, rdev uint32, path string, inode *Ino, attr *Attr) syscall.Errno {
	ino, err := r.nextInode()
	if err != nil {
		return errno(err)
	}
	attr.Typ = _type
	attr.Mode = mode & ^cumask
	attr.Uid = ctx.Uid()
	attr.Gid = ctx.Gid()
	if _type == TYPE_DIRECTORY {
		attr.Nlink = 2
		attr.Length = 4 << 10
	} else {
		attr.Nlink = 1
		if _type == TYPE_SYMLINK {
			attr.Length = uint64(len(path))
		} else {
			attr.Length = 0
			attr.Rdev = rdev
		}
	}

	*inode = ino
	return r.txn(func(tx *redis.Tx) error {
		var patt Attr
		a, err := tx.Get(c, r.inodeKey(parent)).Bytes()
		if err != nil {
			return err
		}
		r.parseAttr(a, &patt)
		if patt.Typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}

		err = tx.HGet(c, r.entryKey(parent), name).Err()
		if err != nil && err != redis.Nil {
			return err
		} else if err == nil {
			return syscall.EEXIST
		}

		now := time.Now()
		patt.Mtime = now.Unix()
		patt.Mtimensec = uint32(now.Nanosecond())
		patt.Ctime = now.Unix()
		patt.Ctimensec = uint32(now.Nanosecond())
		attr.Atime = now.Unix()
		attr.Atimensec = uint32(now.Nanosecond())
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())

		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.HSet(c, r.entryKey(parent), name, r.packEntry(_type, ino))
			pipe.Set(c, r.inodeKey(parent), r.marshal(&patt), 0)
			pipe.Set(c, r.inodeKey(ino), r.marshal(attr), 0)
			if _type == TYPE_SYMLINK {
				pipe.Set(c, r.symKey(ino), path, 0)
			} else if _type == TYPE_FILE {
				pipe.IncrBy(c, usedSpace, align4K(0))
			}
			pipe.Incr(c, totalInodes)
			return nil
		})
		return err
	}, r.inodeKey(parent), r.entryKey(parent))
}

func (r *redisMeta) Mkdir(ctx Context, parent Ino, name string, mode uint16, cumask uint16, copysgid uint8, inode *Ino, attr *Attr) syscall.Errno {
	return r.Mknod(ctx, parent, name, TYPE_DIRECTORY, mode, cumask, 0, inode, attr)
}

func (r *redisMeta) Create(ctx Context, parent Ino, name string, mode uint16, cumask uint16, inode *Ino, attr *Attr) syscall.Errno {
	err := r.Mknod(ctx, parent, name, TYPE_FILE, mode, cumask, 0, inode, attr)
	if err == 0 {
		r.Lock()
		r.openFiles[*inode] = 1
		r.Unlock()
	}
	return err
}

func (r *redisMeta) Unlink(ctx Context, parent Ino, name string) syscall.Errno {
	buf, err := r.rdb.HGet(c, r.entryKey(parent), name).Bytes()
	if err != nil {
		return errno(err)
	}
	_type, inode := r.parseEntry(buf)
	if _type == TYPE_DIRECTORY {
		return syscall.EPERM
	}

	return r.txn(func(tx *redis.Tx) error {
		rs, _ := tx.MGet(c, r.inodeKey(parent), r.inodeKey(inode)).Result()
		if rs[0] == nil || rs[1] == nil {
			return redis.Nil
		}
		var pattr, attr Attr
		r.parseAttr([]byte(rs[0].(string)), &pattr)
		if pattr.Typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}
		now := time.Now()
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())
		r.parseAttr([]byte(rs[1].(string)), &attr)
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())

		buf, err := tx.HGet(c, r.entryKey(parent), name).Bytes()
		if err != nil {
			return err
		}
		_type2, inode2 := r.parseEntry(buf)
		if _type2 != _type || inode2 != inode {
			return syscall.EAGAIN
		}

		attr.Nlink--
		var opened bool
		if _type == TYPE_FILE && attr.Nlink == 0 {
			r.Lock()
			opened = r.openFiles[inode] > 0
			r.Unlock()
		}

		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.HDel(c, r.entryKey(parent), name)
			pipe.Set(c, r.inodeKey(parent), r.marshal(&pattr), 0)
			if attr.Nlink > 0 {
				pipe.Set(c, r.inodeKey(inode), r.marshal(&attr), 0)
			} else {
				switch _type {
				case TYPE_SYMLINK:
					pipe.Del(c, r.symKey(inode))
					pipe.Del(c, r.inodeKey(inode))
				case TYPE_FILE:
					if opened {
						pipe.SAdd(c, r.sessionKey(r.sid), strconv.Itoa(int(inode)))
					} else {
						pipe.ZAdd(c, delchunks, &redis.Z{float64(now.Unix()), r.delChunks(inode, 0, attr.Length)})
						pipe.Del(c, r.inodeKey(inode))
						pipe.IncrBy(c, usedSpace, -align4K(attr.Length))
					}
				}
				pipe.IncrBy(c, totalInodes, -1)
			}
			return nil
		})
		if err == nil && _type == TYPE_FILE && attr.Nlink == 0 {
			if opened {
				r.Lock()
				r.removedFiles[inode] = true
				r.Unlock()
			} else {
				go r.deleteChunks(inode, 0, attr.Length)
			}
		}
		return err
	}, r.entryKey(parent), r.inodeKey(parent), r.inodeKey(inode))
}

func (r *redisMeta) Rmdir(ctx Context, parent Ino, name string) syscall.Errno {
	if name == "." {
		return syscall.EINVAL
	}
	if name == ".." {
		return syscall.ENOTEMPTY
	}
	buf, err := r.rdb.HGet(c, r.entryKey(parent), name).Bytes()
	if err != nil {
		return errno(err)
	}
	typ, inode := r.parseEntry(buf)
	if typ != TYPE_DIRECTORY {
		return syscall.ENOTDIR
	}

	return r.txn(func(tx *redis.Tx) error {
		a, err := tx.Get(c, r.inodeKey(parent)).Bytes()
		if err != nil {
			return err
		}
		var pattr Attr
		r.parseAttr(a, &pattr)
		if pattr.Typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}
		now := time.Now()
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())

		buf, err := tx.HGet(c, r.entryKey(parent), name).Bytes()
		if err != nil {
			return err
		}
		typ, inode = r.parseEntry(buf)
		if typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}

		cnt, err := tx.HLen(c, r.entryKey(inode)).Result()
		if err != nil {
			return err
		}
		if cnt > 0 {
			return syscall.ENOTEMPTY
		}
		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.HDel(c, r.entryKey(parent), name)
			pipe.Set(c, r.inodeKey(parent), r.marshal(&pattr), 0)
			pipe.Del(c, r.inodeKey(inode))
			// pipe.Del(c, r.entryKey(inode))
			pipe.IncrBy(c, totalInodes, -1)
			return nil
		})
		return err
	}, r.inodeKey(parent), r.entryKey(parent), r.inodeKey(inode), r.entryKey(inode))
}

func (r *redisMeta) Rename(ctx Context, parentSrc Ino, nameSrc string, parentDst Ino, nameDst string, inode *Ino, attr *Attr) syscall.Errno {
	buf, err := r.rdb.HGet(c, r.entryKey(parentSrc), nameSrc).Bytes()
	if err != nil {
		return errno(err)
	}
	_, ino := r.parseEntry(buf)
	if parentSrc == parentDst && nameSrc == nameDst {
		if inode != nil {
			*inode = ino
		}
		return 0
	}
	buf, err = r.rdb.HGet(c, r.entryKey(parentDst), nameDst).Bytes()
	if err != nil && err != redis.Nil {
		return errno(err)
	}
	keys := []string{r.entryKey(parentSrc), r.inodeKey(parentSrc), r.inodeKey(ino), r.entryKey(parentDst), r.inodeKey(parentDst)}

	var dino Ino
	var dtyp uint8
	if err == nil {
		dtyp, dino = r.parseEntry(buf)
		keys = append(keys, r.inodeKey(dino))
		if dtyp == TYPE_DIRECTORY {
			keys = append(keys, r.entryKey(dino))
		}
	}

	return r.txn(func(tx *redis.Tx) error {
		buf, err = tx.HGet(c, r.entryKey(parentDst), nameDst).Bytes()
		if err != nil && err != redis.Nil {
			return err
		}
		var tattr Attr
		var opened bool
		if err == nil {
			typ1, dino1 := r.parseEntry(buf)
			if dino1 != dino || typ1 != dtyp {
				return syscall.EAGAIN
			}
			if typ1 == TYPE_DIRECTORY {
				cnt, err := tx.HLen(c, r.entryKey(dino)).Result()
				if err != nil {
					return err
				}
				if cnt != 0 {
					return syscall.ENOTEMPTY
				}
			} else {
				a, err := tx.Get(c, r.inodeKey(dino)).Bytes()
				if err != nil {
					return err
				}
				r.parseAttr(a, &tattr)
				tattr.Nlink--
				if tattr.Nlink > 0 {
					now := time.Now()
					tattr.Ctime = now.Unix()
					tattr.Ctimensec = uint32(now.Nanosecond())
				} else if dtyp == TYPE_FILE {
					r.Lock()
					opened = r.openFiles[dino] > 0
					r.Unlock()
				}
			}
		} else {
			dino = 0
		}

		buf, err := tx.HGet(c, r.entryKey(parentSrc), nameSrc).Bytes()
		if err != nil {
			return err
		}
		_, ino1 := r.parseEntry(buf)
		if ino != ino1 {
			return syscall.EAGAIN
		}

		rs, _ := tx.MGet(c, r.inodeKey(parentSrc), r.inodeKey(parentDst), r.inodeKey(ino)).Result()
		if rs[0] == nil || rs[1] == nil || rs[2] == nil {
			return redis.Nil
		}
		var sattr, dattr, iattr Attr
		r.parseAttr([]byte(rs[0].(string)), &sattr)
		if sattr.Typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}
		now := time.Now()
		sattr.Mtime = now.Unix()
		sattr.Mtimensec = uint32(now.Nanosecond())
		sattr.Ctime = now.Unix()
		sattr.Ctimensec = uint32(now.Nanosecond())
		r.parseAttr([]byte(rs[1].(string)), &dattr)
		if dattr.Typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}
		dattr.Mtime = now.Unix()
		dattr.Mtimensec = uint32(now.Nanosecond())
		dattr.Ctime = now.Unix()
		dattr.Ctimensec = uint32(now.Nanosecond())
		r.parseAttr([]byte(rs[2].(string)), &iattr)
		iattr.Ctime = now.Unix()
		iattr.Ctimensec = uint32(now.Nanosecond())
		if attr != nil {
			*attr = iattr
		}

		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.HDel(c, r.entryKey(parentSrc), nameSrc)
			pipe.Set(c, r.inodeKey(parentSrc), r.marshal(&sattr), 0)
			if dino > 0 {
				if dtyp == TYPE_FILE && tattr.Nlink > 0 {
					pipe.Set(c, r.inodeKey(dino), r.marshal(&tattr), 0)
				} else {
					if dtyp == TYPE_DIRECTORY {
						// pipe.Del(c, r.entryKey(dino))
						pipe.Del(c, r.inodeKey(dino))
					} else if dtyp == TYPE_SYMLINK {
						pipe.Del(c, r.symKey(dino))
						pipe.Del(c, r.inodeKey(dino))
					} else if dtyp == TYPE_FILE {
						if opened {
							pipe.SAdd(c, r.sessionKey(r.sid), strconv.Itoa(int(dino)))
						} else {
							pipe.ZAdd(c, delchunks, &redis.Z{float64(now.Unix()), r.delChunks(dino, 0, tattr.Length)})
							pipe.Del(c, r.inodeKey(dino))
							pipe.IncrBy(c, usedSpace, -align4K(tattr.Length))
						}
					}
					pipe.IncrBy(c, totalInodes, -1)
				}
				pipe.HDel(c, r.entryKey(parentDst), nameDst)
			}
			pipe.HSet(c, r.entryKey(parentDst), nameDst, buf)
			if parentDst != parentSrc {
				pipe.Set(c, r.inodeKey(parentDst), r.marshal(&dattr), 0)
			}
			pipe.Set(c, r.inodeKey(ino), r.marshal(&iattr), 0)
			return nil
		})
		if err == nil && dino > 0 && dtyp == TYPE_FILE {
			if opened {
				r.Lock()
				r.removedFiles[dino] = true
				r.Unlock()
			} else {
				go r.deleteChunks(dino, 0, tattr.Length)
			}
		}
		return err
	}, keys...)
}

func (r *redisMeta) Link(ctx Context, inode, parent Ino, name string, attr *Attr) syscall.Errno {
	return r.txn(func(tx *redis.Tx) error {
		rs, _ := tx.MGet(c, r.inodeKey(parent), r.inodeKey(inode)).Result()
		if rs[0] == nil || rs[1] == nil {
			return redis.Nil
		}
		var pattr, iattr Attr
		r.parseAttr([]byte(rs[0].(string)), &pattr)
		if pattr.Typ != TYPE_DIRECTORY {
			return syscall.ENOTDIR
		}
		now := time.Now()
		pattr.Mtime = now.Unix()
		pattr.Mtimensec = uint32(now.Nanosecond())
		pattr.Ctime = now.Unix()
		pattr.Ctimensec = uint32(now.Nanosecond())
		r.parseAttr([]byte(rs[1].(string)), &iattr)
		if iattr.Typ == TYPE_DIRECTORY {
			return syscall.EPERM
		}
		iattr.Ctime = now.Unix()
		iattr.Ctimensec = uint32(now.Nanosecond())
		iattr.Nlink++

		err := tx.HGet(c, r.entryKey(parent), name).Err()
		if err != nil && err != redis.Nil {
			return err
		} else if err == nil {
			return syscall.EEXIST
		}

		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.HSet(c, r.entryKey(parent), name, r.packEntry(iattr.Typ, inode))
			pipe.Set(c, r.inodeKey(parent), r.marshal(&pattr), 0)
			pipe.Set(c, r.inodeKey(inode), r.marshal(&iattr), 0)
			return nil
		})
		if err == nil && attr != nil {
			*attr = iattr
		}
		return err
	}, r.inodeKey(inode), r.entryKey(parent), r.inodeKey(parent))
}

func (r *redisMeta) Readdir(ctx Context, inode Ino, plus uint8, entries *[]*Entry) syscall.Errno {
	vals, err := r.rdb.HGetAll(c, r.entryKey(inode)).Result()
	if err != nil {
		return errno(err)
	}
	for name, val := range vals {
		typ, inode := r.parseEntry([]byte(val))
		*entries = append(*entries, &Entry{
			Inode: inode,
			Name:  []byte(name),
			Attr:  &Attr{Typ: typ},
		})
	}
	if plus != 0 {
		var keys []string
		for _, e := range *entries {
			keys = append(keys, r.inodeKey(e.Inode))
		}
		rs, _ := r.rdb.MGet(c, keys...).Result()
		for i, re := range rs {
			if re != nil {
				if a, ok := re.([]byte); ok {
					r.parseAttr(a, (*entries)[i].Attr)
				}
			}
		}
	}
	return 0
}

func (r *redisMeta) refreshSession() {
	for {
		now := time.Now()
		r.rdb.ZAdd(c, allSessions, &redis.Z{float64(now.Unix()), strconv.Itoa(int(r.sid))})
		rng := &redis.ZRangeBy{"", strconv.Itoa(int(now.Add(time.Minute * -10).Unix())), 0, 100}
		staleSessions, _ := r.rdb.ZRangeByScore(c, allSessions, rng).Result()
		for _, ssid := range staleSessions {
			sid, _ := strconv.Atoi(ssid)
			inodes, err := r.rdb.LRange(c, r.sessionKey(int64(sid)), 0, 1000000).Result()
			if err == nil {
				for _, sinode := range inodes {
					inode, _ := strconv.Atoi(sinode)
					if err = r.deleteInode(Ino(inode)); err != nil {
						break
					}
				}
				if err == nil {
					r.rdb.Del(c, r.sessionKey(int64(sid)))
					r.rdb.ZRem(c, allSessions, ssid)
				}
			}
		}
		time.Sleep(time.Minute)
	}
}

func (r *redisMeta) deleteInode(inode Ino) error {
	var attr Attr
	a, err := r.rdb.Get(c, r.inodeKey(inode)).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	r.parseAttr(a, &attr)
	_, err = r.rdb.TxPipelined(c, func(pipe redis.Pipeliner) error {
		pipe.ZAdd(c, delchunks, &redis.Z{float64(time.Now().Unix()), r.delChunks(inode, 0, attr.Length)})
		pipe.Del(c, r.inodeKey(inode))
		pipe.IncrBy(c, usedSpace, -align4K(attr.Length))
		return nil
	})
	if err == nil {
		go r.deleteChunks(inode, 0, attr.Length)
	}
	return err
}

func (r *redisMeta) Open(ctx Context, inode Ino, flags uint8, attr *Attr) syscall.Errno {
	var err syscall.Errno
	if attr != nil {
		err = r.GetAttr(ctx, inode, 1, attr)
	}
	if err == 0 {
		r.Lock()
		r.openFiles[inode] = r.openFiles[inode] + 1
		r.Unlock()
	}
	return 0
}

func (r *redisMeta) Close(ctx Context, inode Ino) syscall.Errno {
	r.Lock()
	defer r.Unlock()
	refs := r.openFiles[inode]
	if refs <= 1 {
		delete(r.openFiles, inode)
		if r.removedFiles[inode] {
			delete(r.removedFiles, inode)
			go func() {
				if err := r.deleteInode(inode); err == nil {
					r.rdb.SRem(c, r.sessionKey(r.sid), strconv.Itoa(int(inode)))
				}
			}()
		}
	} else {
		r.openFiles[inode] = refs - 1
	}
	return 0
}

func (r *redisMeta) Read(inode Ino, indx uint32, chunks *[]Slice) syscall.Errno {
	vals, err := r.rdb.LRange(c, r.chunkKey(inode, indx), 0, 1000000).Result()
	if err != nil {
		return errno(err)
	}
	var root *slice
	for _, val := range vals {
		rb := utils.ReadBuffer([]byte(val))
		pos := rb.Get32()
		chunkid := rb.Get64()
		cleng := rb.Get32()
		soff := rb.Get32()
		slen := rb.Get32()
		s := newSlice(pos, chunkid, cleng, soff, slen)
		if root != nil {
			var right *slice
			s.left, right = root.cut(pos)
			_, s.right = right.cut(pos + slen)
		}
		root = s
	}
	root.visit(func(s *slice) {
		*chunks = append(*chunks, Slice{s.chunkid, s.cleng, s.off, s.len})
	})
	// TODO: compact
	return 0
}

func (r *redisMeta) NewChunk(ctx Context, inode Ino, indx uint32, offset uint32, chunkid *uint64) syscall.Errno {
	cid, err := r.rdb.Incr(c, "nextchunk").Uint64()
	if err == nil {
		*chunkid = cid
	}
	return errno(err)
}

func (r *redisMeta) Write(ctx Context, inode Ino, indx uint32, off uint32, slice Slice) syscall.Errno {
	return r.txn(func(tx *redis.Tx) error {
		// TODO: refcount for chunkid
		var attr Attr
		a, err := tx.Get(c, r.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		r.parseAttr(a, &attr)
		newleng := uint64(indx)*CHUNKSIZE + uint64(off) + uint64(slice.Len)
		var added int64
		if newleng > attr.Length {
			added = align4K(newleng) - align4K(attr.Length)
			attr.Length = newleng
		}
		now := time.Now()
		attr.Mtime = now.Unix()
		attr.Mtimensec = uint32(now.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())

		w := utils.NewBuffer(24)
		w.Put32(off)
		w.Put64(slice.Chunkid)
		w.Put32(slice.Clen)
		w.Put32(slice.Off)
		w.Put32(slice.Len)

		_, err = tx.TxPipelined(c, func(pipe redis.Pipeliner) error {
			pipe.RPush(c, r.chunkKey(inode, indx), w.Bytes())
			pipe.Set(c, r.inodeKey(inode), r.marshal(&attr), 0)
			if added > 0 {
				pipe.IncrBy(c, usedSpace, added)
			}
			return nil
		})
		return err
	}, r.inodeKey(inode), r.chunkKey(inode, indx))
}

func (r *redisMeta) delChunks(inode Ino, start, end uint64) string {
	return fmt.Sprintf("%d:%d:%d", inode, start, end)
}

func (r *redisMeta) cleanupChunks() {
	for {
		now := time.Now()
		members, _ := r.rdb.ZRangeByScore(c, delchunks, &redis.ZRangeBy{strconv.Itoa(0), strconv.Itoa(int(now.Add(time.Hour).Unix())), 0, 1000}).Result()
		for _, member := range members {
			ps := strings.Split(member, ":")
			if len(ps) != 3 {
				logger.Errorf("invalid del chunks: %s", member)
				continue
			}
			inode, _ := strconv.Atoi(ps[0])
			start, _ := strconv.Atoi(ps[1])
			end, _ := strconv.Atoi(ps[2])
			r.deleteChunks(Ino(inode), uint64(start), uint64(end))
		}
		time.Sleep(time.Minute)
	}
}

func (r *redisMeta) deleteChunks(inode Ino, start, end uint64) {
	var i uint32
	if start > 0 {
		i = uint32((start-1)/CHUNKSIZE) + 1
	}
	var rs []*redis.StringSliceCmd
	for uint64(i)*CHUNKSIZE <= end {
		p := r.rdb.Pipeline()
		var indx = i
		for j := 0; uint64(i)*CHUNKSIZE <= end && j < 1000; j++ {
			rs = append(rs, p.LRange(c, r.chunkKey(inode, i), 0, 1000000))
			i++
		}
		vals, err := p.Exec(c)
		if err != nil {
			logger.Errorf("LRange %d[%d-%d]: %s", inode, start, end, err)
			return
		}
		for j := range vals {
			val, err := rs[j].Result()
			if err == redis.Nil {
				continue
			}
			for _, cs := range val {
				rb := utils.ReadBuffer([]byte(cs))
				_ = rb.Get32() // pos
				chunkid := rb.Get64()
				cleng := rb.Get32()
				err := r.newMsg(CHUNK_DEL, chunkid, cleng)
				if err != nil {
					logger.Warnf("delete chunk %d fail: %s, retry later", inode, err)
					now := time.Now()
					key := r.delChunks(inode, uint64((indx+uint32(j)))*CHUNKSIZE, uint64((indx+uint32(j)+1))*CHUNKSIZE)
					r.rdb.ZAdd(c, delchunks, &redis.Z{float64(now.Unix()), key})
					return
				}
			}
			r.rdb.Del(c, r.chunkKey(inode, indx+uint32(j)))
		}
	}
	r.rdb.ZRem(c, delchunks, r.delChunks(inode, start, end))
}
