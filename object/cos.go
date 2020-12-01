package object

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

const cosChecksumKey = "x-cos-meta-" + checksumAlgr

type COS struct {
	c        *cos.Client
	endpoint string
}

func (c *COS) String() string {
	return fmt.Sprintf("cos://%s", strings.Split(c.endpoint, ".")[0])
}

func (c *COS) Create() error {
	_, err := c.c.Bucket.Put(ctx, nil)
	if err != nil {
		if e, ok := err.(*cos.ErrorResponse); ok && e.Code == "BucketAlreadyOwnedByYou" {
			err = nil
		}
	}
	return err
}

func (c *COS) Get(key string, off, limit int64) (io.ReadCloser, error) {
	params := &cos.ObjectGetOptions{}
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		params.Range = r
	}
	resp, err := c.c.Object.Get(ctx, key, params)
	if err != nil {
		return nil, err
	}
	if off == 0 && limit == -1 {
		resp.Body = verifyChecksum(resp.Body, resp.Header.Get(cosChecksumKey))
	}
	return resp.Body, nil
}

func (c *COS) Put(key string, in io.ReadSeeker) error {
	header := http.Header(map[string][]string{
		cosChecksumKey: {generateChecksum(in)},
	})
	options := &cos.ObjectPutOptions{nil, &cos.ObjectPutHeaderOptions{XCosMetaXXX: &header}}
	_, err := c.c.Object.Put(ctx, key, in, options)
	return err
}

func (c *COS) Copy(dst, src string) error {
	source := fmt.Sprintf("%s/%s", c.endpoint, src)
	_, _, err := c.c.Object.Copy(ctx, dst, source, nil)
	return err
}

func (c *COS) Exists(key string) error {
	_, err := c.c.Object.Head(ctx, key, nil)
	return err
}

func (c *COS) Delete(key string) error {
	_, err := c.c.Object.Delete(ctx, key)
	return err
}

func (c *COS) List(prefix, marker string, limit int64) ([]*Object, error) {
	param := cos.BucketGetOptions{
		Prefix:  prefix,
		Marker:  marker,
		MaxKeys: int(limit),
	}
	resp, _, err := c.c.Bucket.Get(ctx, &param)
	for err == nil && len(resp.Contents) == 0 && resp.IsTruncated {
		param.Marker = resp.NextMarker
		resp, _, err = c.c.Bucket.Get(ctx, &param)
	}
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]*Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		t, _ := time.Parse(time.RFC3339, o.LastModified)
		objs[i] = &Object{o.Key, int64(o.Size), int(t.Unix()), int(t.Unix())}
	}
	return objs, nil
}

func newCOS(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint %s: %s", endpoint, err)
	}
	b := &cos.BaseURL{BucketURL: uri}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  accessKey,
			SecretKey: secretKey,
		},
	})
	client.UserAgent = UserAgent
	return &COS{client, uri.Host}
}

func init() {
	RegisterStorage("cos", newCOS)
}
