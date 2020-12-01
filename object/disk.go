package object

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"jfs/utils"
)

type diskStore struct {
	dir    string
	delay  time.Duration
	eratio float64
}

func (d *diskStore) String() string {
	return "file://" + d.dir
}

func (d *diskStore) Create() error {
	return os.MkdirAll(d.dir, os.FileMode(0700))
}

func (d *diskStore) path(key string) string {
	return filepath.Join(d.dir, strings.TrimLeft(key, "/"))
}

func (d *diskStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	time.Sleep(d.delay)
	if rand.Float64() < d.eratio {
		return nil, errors.New("random failure")
	}
	p := d.path(key)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, 0); err != nil {
			f.Close()
			return nil, err
		}
	}
	if limit > 0 {
		buf := make([]byte, limit)
		n, err := f.Read(buf)
		if err != nil {
			return nil, err
		}
		return ioutil.NopCloser(bytes.NewBuffer(buf[:n])), nil
	}
	return f, err
}

func (d *diskStore) Put(key string, in io.ReadSeeker) error {
	time.Sleep(d.delay)
	if rand.Float64() < d.eratio {
		return errors.New("random failure")
	}
	p := d.path(key)
	if err := os.MkdirAll(filepath.Dir(p), os.FileMode(0700)); err != nil {
		return err
	}
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, in)
	return err
}

func (d *diskStore) Exists(key string) error {
	time.Sleep(d.delay)
	if rand.Float64() < d.eratio {
		return errors.New("random failure")
	}
	if utils.Exists(d.path(key)) {
		return nil
	}
	return errors.New("not exists")
}

func (d *diskStore) Delete(key string) error {
	time.Sleep(d.delay)
	if rand.Float64() < d.eratio {
		return errors.New("random failure")
	}
	os.Remove(d.path(key))
	return nil
}

func (d *diskStore) List(prefix, marker string, limit int64) ([]*Object, error) {
	time.Sleep(d.delay)
	if rand.Float64() < d.eratio {
		return nil, errors.New("random failure")
	}
	var objs []*Object
	filepath.Walk(d.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || path == d.dir {
			return nil
		}
		key := path
		if strings.HasPrefix(path, d.dir) && d.dir != "." {
			key = key[len(d.dir)+1:]
		}
		if info.IsDir() {
			if len(key) < len(prefix) && !strings.HasPrefix(prefix, key) {
				return filepath.SkipDir
			}
			if key < marker && !strings.HasPrefix(marker, key) {
				return filepath.SkipDir
			}
		} else {
			if key > marker && strings.HasPrefix(key, prefix) {
				t := int(info.ModTime().Unix())
				objs = append(objs, &Object{key, info.Size(), t, t})
				if len(objs) == int(limit) {
					return errors.New("enough")
				}
			}
		}
		return nil
	})
	return objs, nil
}

func newDisk(endpoint, delays, eratios string) ObjectStorage {
	store := &diskStore{dir: endpoint}
	if delays != "" {
		delay, err := time.ParseDuration(delays)
		if err != nil {
			logger.Errorf("invalid duration %s: %s", delays, err)
		} else if delay > 0 {
			logger.Infof("delay all requests by %s", delay)
			store.delay = delay
		}
	}
	if eratios != "" {
		eratio, err := strconv.ParseFloat(eratios, 64)
		if err != nil {
			logger.Errorf("invalid error ratio %s: %s", eratios, err)
		} else if eratio > 0.0 {
			logger.Infof("simulate request faiure by chance: %f", eratio)
			store.eratio = eratio
		}
	}
	return store
}

func init() {
	RegisterStorage("file", newDisk)
}
