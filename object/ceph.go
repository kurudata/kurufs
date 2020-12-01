// +build ceph

package object

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/ceph/go-ceph/rados"
)

type ceph struct {
	defaultObjectStorage
	name string
	conn *rados.Conn
	free chan *rados.IOContext
}

func (c *ceph) String() string {
	return fmt.Sprintf("ceph://%s", c.name)
}

func (c *ceph) Create() error {
	names, err := c.conn.ListPools()
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == c.name {
			return nil
		}
	}
	return c.conn.MakePool(c.name)
}

func (c *ceph) newContext() (*rados.IOContext, error) {
	select {
	case ctx := <-c.free:
		return ctx, nil
	default:
		return c.conn.OpenIOContext(c.name)
	}
}

func (c *ceph) release(ctx *rados.IOContext) {
	select {
	case c.free <- ctx:
	default:
		ctx.Destroy()
	}
}

func (c *ceph) do(f func(ctx *rados.IOContext) error) (err error) {
	ctx, err := c.newContext()
	if err != nil {
		return err
	}
	err = f(ctx)
	if err != nil {
		ctx.Destroy()
	} else {
		c.release(ctx)
	}
	return
}

type cephReader struct {
	c     *ceph
	ctx   *rados.IOContext
	key   string
	off   int64
	limit int64
}

func (r *cephReader) Read(buf []byte) (n int, err error) {
	if r.limit > 0 && int64(len(buf)) > r.limit {
		buf = buf[:r.limit]
	}
	n, err = r.ctx.Read(r.key, buf, uint64(r.off))
	r.off += int64(n)
	if r.limit > 0 {
		r.limit -= int64(n)
	}
	if err == nil && n < len(buf) {
		err = io.EOF
	}
	return
}

func (r *cephReader) Close() error {
	if r.ctx != nil {
		r.c.release(r.ctx)
		r.ctx = nil
	}
	return nil
}

func (c *ceph) Get(key string, off, limit int64) (io.ReadCloser, error) {
	ctx, err := c.newContext()
	if err != nil {
		return nil, err
	}
	return &cephReader{c, ctx, key, off, limit}, nil
}

var cephPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1<<20)
	},
}

func (c *ceph) Put(key string, in io.ReadSeeker) error {
	return c.do(func(ctx *rados.IOContext) error {
		if b, ok := in.(*bytes.Reader); ok {
			v := reflect.ValueOf(b)
			data := v.Elem().Field(0).Bytes()
			return ctx.WriteFull(key, data)
		}
		buf := cephPool.Get().([]byte)
		defer cephPool.Put(buf)
		var off uint64
		for {
			n, err := in.Read(buf)
			if n > 0 {
				if err = ctx.Write(key, buf[:n], off); err != nil {
					return err
				}
				off += uint64(n)
			} else {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	})
}

func (c *ceph) Exists(key string) error {
	return c.do(func(ctx *rados.IOContext) error {
		_, err := ctx.Stat(key)
		return err
	})
}

func (c *ceph) Delete(key string) error {
	return c.do(func(ctx *rados.IOContext) error {
		return ctx.Delete(key)
	})
}

func (c *ceph) List(prefix, marker string, limit int64) ([]*Object, error) {
	var objs = make([]*Object, 0, limit)
	err := c.do(func(ctx *rados.IOContext) error {
		iter, err := ctx.Iter()
		if err != nil {
			return err
		}
		defer iter.Close()

		// FIXME: this will be really slow for many objects
		keys := make([]string, 0, limit)
		for iter.Next() {
			key := iter.Value()
			if key <= marker || !strings.HasPrefix(key, prefix) {
				continue
			}
			keys = append(keys, key)
		}
		// the keys are not ordered, sort them first
		sort.Strings(keys)
		if len(keys) > int(limit) {
			keys = keys[:limit]
		}
		for _, key := range keys {
			st, err := ctx.Stat(key)
			if err != nil {
				continue // FIXME
			}
			ts := int(st.ModTime.Unix())
			objs = append(objs, &Object{key, int64(st.Size), ts, ts})
		}
		return nil
	})
	return objs, err
}

func newCeph(endpoint, cluster, user string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint %s: %s", endpoint, err)
	}
	name := uri.Host
	conn, err := rados.NewConnWithClusterAndUser(cluster, user)
	if err != nil {
		logger.Fatalf("Can't create connection to cluster %s for user %s: %s", cluster, user, err)
	}
	if err := conn.ReadDefaultConfigFile(); err != nil {
		logger.Fatalf("Can't read default config file: %s", err)
	}
	if err := conn.Connect(); err != nil {
		logger.Fatalf("Can't connect to cluster %s: %s", cluster, err)
	}
	return &ceph{
		name: name,
		conn: conn,
		free: make(chan *rados.IOContext, 50),
	}
}

func init() {
	RegisterStorage("ceph", newCeph)
}
