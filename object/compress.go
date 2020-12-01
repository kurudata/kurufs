package object

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"jfs/utils"
	"reflect"
)

type compressed struct {
	ObjectStorage
	compressor utils.Compressor
}

func NewCompressed(obj ObjectStorage, compressor utils.Compressor) ObjectStorage {
	return &compressed{obj, compressor}
}

func (c *compressed) String() string {
	return "compressed " + c.ObjectStorage.String()
}

type cReader struct {
	io.Reader
	r1 io.Closer
	r2 io.Closer
}

func (c *cReader) Close() error {
	c.r2.Close()
	return c.r1.Close()
}

func (c *compressed) Get(key string, off, limit int64) (io.ReadCloser, error) {
	in, err := c.ObjectStorage.Get(key, 0, -1)
	if err != nil {
		return nil, err
	}
	cr := c.compressor.NewReader(in)
	for off > 0 {
		buf := make([]byte, off)
		n, err := cr.Read(buf)
		if err != nil {
			in.Close()
			return nil, err
		}
		off -= int64(n)
	}
	if limit > 0 {
		return &cReader{io.LimitReader(cr, limit), cr, in}, nil
	}
	return &cReader{cr, cr, in}, nil
}

func (c *compressed) Put(key string, in io.ReadSeeker) error {
	var data []byte
	if b, ok := in.(*bytes.Reader); ok {
		v := reflect.ValueOf(b)
		data = v.Elem().Field(0).Bytes()
	} else {
		var err error
		data, err = ioutil.ReadAll(in)
		if err != nil {
			return err
		}
	}
	// TODO: alloc from buffer pool
	buf := make([]byte, c.compressor.CompressBound(len(data)))
	n, err := c.compressor.Compress(buf, data)
	if err != nil {
		return err
	}
	return c.ObjectStorage.Put(key, bytes.NewReader(buf[:n]))
}

func (c *compressed) CreateMultipartUpload(key string) (*MultipartUpload, error) {
	return nil, errors.New("not supported")
}
