package object

import (
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
)

type bosclient struct {
	defaultObjectStorage
	bucket string
	c      *bos.Client
}

func (q *bosclient) String() string {
	return fmt.Sprintf("bos://%s", q.bucket)
}

func (q *bosclient) Create() error {
	_, err := q.c.PutBucket(q.bucket)
	if err != nil && strings.Contains(err.Error(), "BucketAlreadyExists") {
		err = nil
	}
	return err
}

func (q *bosclient) Get(key string, off, limit int64) (io.ReadCloser, error) {
	var r *api.GetObjectResult
	var err error
	if limit > 0 {
		r, err = q.c.GetObject(q.bucket, key, nil, off, off+limit-1)
	} else if off > 0 {
		r, err = q.c.GetObject(q.bucket, key, nil, off)
	} else {
		r, err = q.c.GetObject(q.bucket, key, nil)
	}
	if err != nil {
		return nil, err
	}
	return r.Body, nil
}

func (q *bosclient) Put(key string, in io.ReadSeeker) error {
	b, vlen, err := findLen(in)
	if err != nil {
		return err
	}
	body, err := bce.NewBodyFromSizedReader(b, vlen)
	if err != nil {
		return err
	}
	_, err = q.c.BasicPutObject(q.bucket, key, body)
	return err
}

func (q *bosclient) Exists(key string) error {
	_, err := q.c.GetObjectMeta(q.bucket, key)
	return err
}

func (q *bosclient) Delete(key string) error {
	return q.c.DeleteObject(q.bucket, key)
}

func (q *bosclient) List(prefix, marker string, limit int64) ([]*Object, error) {
	if limit > 1000 {
		limit = 1000
	}
	limit_ := int(limit)
	out, err := q.c.SimpleListObjects(q.bucket, prefix, limit_, marker, "")
	for err == nil && len(out.Contents) == 0 && out.IsTruncated {
		out, err = q.c.SimpleListObjects(q.bucket, prefix, limit_, out.NextMarker, "")
	}
	if err != nil {
		return nil, err
	}
	n := len(out.Contents)
	objs := make([]*Object, n)
	for i := 0; i < n; i++ {
		k := out.Contents[i]
		mod, _ := time.Parse("2006-01-02T15:04:05Z", k.LastModified)
		objs[i] = &Object{k.Key, int64(k.Size), int(mod.Unix()), int(mod.Unix())}
	}
	return objs, nil
}

func newBOS(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint: %v, error: %v", endpoint, err)
	}
	hostParts := strings.SplitN(uri.Host, ".", 2)
	bucketName := hostParts[0]
	endpoint = fmt.Sprintf("https://%s", hostParts[1])
	bosClient, err := bos.NewClient(accessKey, secretKey, endpoint)
	return &bosclient{bucket: bucketName, c: bosClient}
}

func init() {
	RegisterStorage("bos", newBOS)
}
