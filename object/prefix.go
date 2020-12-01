package object

import (
	"fmt"
	"io"
)

type withPrefix struct {
	os     ObjectStorage
	prefix string
}

// WithPrefix retuns a object storage that add a prefix to keys.
func WithPrefix(os ObjectStorage, prefix string) ObjectStorage {
	return &withPrefix{os, prefix}
}

func (p *withPrefix) Create() error {
	return p.os.Create()
}

func (p *withPrefix) String() string {
	return fmt.Sprintf("%s/%s", p.os, p.prefix)
}

func (p *withPrefix) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return p.os.Get(p.prefix+key, off, limit)
}

func (p *withPrefix) Put(key string, in io.ReadSeeker) error {
	return p.os.Put(p.prefix+key, in)
}

func (p *withPrefix) Exists(key string) error {
	return p.os.Exists(p.prefix + key)
}

func (p *withPrefix) Delete(key string) error {
	return p.os.Delete(p.prefix + key)
}

func (p *withPrefix) List(prefix, marker string, limit int64) ([]*Object, error) {
	if marker != "" {
		marker = p.prefix + marker
	}
	objs, err := p.os.List(p.prefix+prefix, marker, limit)
	ln := len(p.prefix)
	for _, obj := range objs {
		obj.Key = obj.Key[ln:]
	}
	return objs, err
}

var _ ObjectStorage = &withPrefix{}
