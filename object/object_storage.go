package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"jfs/utils"
)

var logger = utils.GetLogger("juicefs")
var ctx = context.Background()

var UserAgent = "JuiceFS"

type Object struct {
	Key   string
	Size  int64
	Ctime int // Unix seconds
	Mtime int // Unix seconds
}

type MultipartUpload struct {
	MinPartSize int
	MaxCount    int
	UploadID    string
}

type Part struct {
	Num  int
	Size int
	ETag string
}

type PendingPart struct {
	Key      string
	UploadID string
	Created  time.Time
}

type ObjectStorage interface {
	String() string
	Create() error
	Get(key string, off, limit int64) (io.ReadCloser, error)
	Put(key string, in io.ReadSeeker) error
	Exists(key string) error
	Delete(key string) error
	List(prefix, marker string, limit int64) ([]*Object, error)
}

var notSupported = errors.New("not supported")

type defaultObjectStorage struct{}

func (s defaultObjectStorage) Create() error {
	return nil
}

func (s defaultObjectStorage) List(prefix, marker string, limit int64) ([]*Object, error) {
	return nil, notSupported
}

type Register func(endpoint, accessKey, secretKey string) ObjectStorage

var storages = make(map[string]Register)

func RegisterStorage(name string, register Register) {
	storages[name] = register
}

func CreateStorage(name, endpoint, accessKey, secretKey string) ObjectStorage {
	f, ok := storages[name]
	if ok {
		return f(endpoint, accessKey, secretKey)
	}
	panic(fmt.Sprintf("invalid storage: %s", name))
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func doTesting(store ObjectStorage, key string, data []byte) error {
	if err := store.Put(key, bytes.NewReader(data)); err != nil {
		if strings.Contains(err.Error(), "Access Denied") {
			return fmt.Errorf("Failed to put: %s", err)
		}
		if err2 := store.Create(); err2 != nil {
			return fmt.Errorf("Failed to create %s: %s,  previous error: %s\nplease create bucket %s manually, then mount again",
				store, err2, err, store)
		}
		if err := store.Put(key, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("Failed to put: %s", err)
		}
	}
	p, err := store.Get(key, 0, -1)
	if err != nil {
		return fmt.Errorf("Failed to get: %s", err)
	}
	data2, err := ioutil.ReadAll(p)
	p.Close()
	if !bytes.Equal(data, data2) {
		return fmt.Errorf("Read wrong data")
	}
	err = store.Delete(key)
	if err != nil {
		fmt.Printf("Failed to delete: %s", err)
	}
	return nil
}

func DoTesting(store ObjectStorage) error {
	rand.Seed(int64(time.Now().UnixNano()))
	key := "testing/" + randSeq(10)
	data := make([]byte, 100)
	rand.Read(data)
	nRetry := 3
	var err error
	for i := 0; i < nRetry; i++ {
		err = doTesting(store, key, data)
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(i*3+1))
	}
	return err
}
