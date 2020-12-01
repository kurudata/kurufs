package object

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/kurin/blazer/b2"
)

type b2client struct {
	defaultObjectStorage
	client *b2.Client
	bucket *b2.Bucket
	cursor *b2.Cursor
}

func (c *b2client) String() string {
	return fmt.Sprintf("b2://%s", c.bucket.Name())
}

func (c *b2client) Create() error {
	return nil
}

func (c *b2client) Get(key string, off, limit int64) (io.ReadCloser, error) {
	obj := c.bucket.Object(key)
	if _, err := obj.Attrs(ctx); err != nil {
		return nil, err
	}
	return obj.NewRangeReader(ctx, off, limit), nil
}

func (c *b2client) Put(key string, data io.ReadSeeker) error {
	w := c.bucket.Object(key).NewWriter(ctx)
	if _, err := w.ReadFrom(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (c *b2client) Exists(key string) error {
	_, err := c.bucket.Object(key).Attrs(ctx)
	return err
}

func (c *b2client) Delete(key string) error {
	return c.bucket.Object(key).Delete(ctx)
}

func (c *b2client) List(prefix, marker string, limit int64) ([]*Object, error) {
	if limit > 1000 {
		limit = 1000
	}
	var cursor *b2.Cursor
	if marker != "" {
		if c.cursor == nil {
			return nil, errors.New("not supported")
		}
		cursor = c.cursor
	} else {
		cursor = &b2.Cursor{Prefix: prefix}
	}
	c.cursor = nil
	objects, nc, err := c.bucket.ListCurrentObjects(ctx, int(limit), cursor)
	if err != nil && err != io.EOF {
		return nil, err
	}
	c.cursor = nc

	n := len(objects)
	objs := make([]*Object, n)
	for i := 0; i < n; i++ {
		attr, err := objects[i].Attrs(ctx)
		if err == nil {
			// attr.LastModified is not correct
			objs[i] = &Object{attr.Name, attr.Size, int(attr.UploadTimestamp.Unix()), int(attr.UploadTimestamp.Unix())}
		}
	}
	return objs, nil
}

func newB2(endpoint, account, key string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint: %v, error: %v", endpoint, err)
	}
	hostParts := strings.Split(uri.Host, ".")
	bucketName := hostParts[0]
	client, err := b2.NewClient(ctx, account, key, b2.Transport(httpClient.Transport))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucket, err := client.Bucket(ctx, bucketName)
	if err != nil {
		bucket, err = client.NewBucket(ctx, bucketName, &b2.BucketAttrs{
			Type: "allPrivate",
		})
		if err != nil {
			log.Fatalf("Failed to create bucket %s: %s, please create it manually.", bucketName, err)
		}
	}
	return &b2client{client: client, bucket: bucket}
}

func init() {
	RegisterStorage("b2", newB2)
}
