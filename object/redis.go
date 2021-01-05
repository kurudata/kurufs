package object

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/juicedata/juicesync/object"
)

// redisStore stores data chunks into Redis.
type redisStore struct {
	object.DefaultObjectStorage
	rdb *redis.Client
}

var c = context.TODO()

func (r *redisStore) String() string {
	return fmt.Sprintf("redis://%s", r.rdb.Options().Addr)
}

func (r *redisStore) Head(key string) (*object.Object, error) {
	v, err := r.rdb.Get(c, key).Bytes()
	if err != nil {
		return nil, err
	}
	return &object.Object{Key: key, Size: int64(len(v)), IsDir: strings.HasSuffix(key, "/")}, nil
}

func (r *redisStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	data, err := r.rdb.Get(c, key).Bytes()
	if err != nil {
		return nil, err
	}
	data = data[off:]
	if limit > 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return ioutil.NopCloser(bytes.NewBuffer(data)), nil
}

func (r *redisStore) Put(key string, in io.Reader) error {
	data, err := ioutil.ReadAll(in)
	if err != nil {
		return err
	}
	return r.rdb.Set(c, key, data, 0).Err()
}

func (r *redisStore) Delete(key string) error {
	return r.rdb.Del(c, key).Err()
}

func newRedis(url, user, passwd string) (object.ObjectStorage, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", url, err)
	}
	if user != "" {
		opt.Username = user
	}
	if passwd != "" {
		opt.Password = passwd
	}
	rdb := redis.NewClient(opt)
	return &redisStore{object.DefaultObjectStorage{}, rdb}, nil
}

func init() {
	object.Register("redis", newRedis)
}
