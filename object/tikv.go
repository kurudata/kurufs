// +build tikv

package object

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

type tikv struct {
	defaultObjectStorage
	c    *rawkv.Client
	addr string
}

func (t *tikv) String() string {
	return fmt.Sprintf("tikv://%s", t.addr)
}

func (t *tikv) Copy(dst, src string) error {
	return copyObj(t, dst, src)
}

func (t *tikv) Get(key string, off, limit int64) (io.ReadCloser, error) {
	d, err := t.c.Get(ctx, []byte(key))
	if len(d) == 0 {
		err = errors.New("not found")
	}
	if err != nil {
		return nil, err
	}
	data := d[off:]
	if limit > 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return ioutil.NopCloser(bytes.NewBuffer(data)), nil
}

func (t *tikv) Put(key string, in io.ReadSeeker) error {
	var d []byte
	var err error
	if b, ok := in.(*bytes.Reader); ok {
		v := reflect.ValueOf(b)
		d = v.Elem().Field(0).Bytes()
	} else {
		d, err = ioutil.ReadAll(in)
		if err != nil {
			return err
		}
	}
	return t.c.Put(ctx, []byte(key), d)
}

func (t *tikv) Exists(key string) error {
	_, err := t.Get(key, 0, -1)
	return err
}

func (t *tikv) Delete(key string) error {
	return t.c.Delete(ctx, []byte(key))
}

func (t *tikv) List(prefix, marker string, limit int64) ([]*Object, error) {
	return nil, errors.New("not supported")
}

func newTiKV(endpoint, accesskey, secretkey string) ObjectStorage {
	conf := config.Default()
	// config.Raw.MaxBatchPutSize = 4 << 20
	pds := strings.Split(endpoint, ",")
	for i, pd := range pds {
		pd = strings.TrimSpace(pd)
		if !strings.Contains(pd, ":") {
			pd += ":2379"
		}
		pds[i] = pd
	}
	c, err := rawkv.NewClient(ctx, pds, conf)
	if err != nil {
		panic(err.Error())
	}
	return &tikv{c: c, addr: endpoint}
}

func init() {
	RegisterStorage("tikv", newTiKV)
}
