package object

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type obsClient struct {
	bucket string
	region string
	c      *obs.ObsClient
}

func (s *obsClient) String() string {
	return fmt.Sprintf("obs://%s", s.bucket)
}

func (s *obsClient) Create() error {
	params := &obs.CreateBucketInput{}
	params.Bucket = s.bucket
	params.Location = s.region
	_, err := s.c.CreateBucket(params)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok && obsError.Code == "BucketAlreadyOwnedByYou" {
			err = nil
		}
	}
	return err
}

func (s *obsClient) Get(key string, off, limit int64) (io.ReadCloser, error) {
	params := &obs.GetObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.RangeStart = off
	if limit > 0 {
		params.RangeEnd = off + limit - 1
	}
	resp, err := s.c.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *obsClient) Put(key string, in io.ReadSeeker) error {
	var body io.ReadSeeker
	if b, ok := in.(io.ReadSeeker); ok {
		body = b
	} else {
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	params := &obs.PutObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.Body = body

	_, err := s.c.PutObject(params)
	return err
}

func (s *obsClient) Copy(dst, src string) error {
	params := &obs.CopyObjectInput{}
	params.Bucket = s.bucket
	params.Key = dst
	params.CopySourceBucket = s.bucket
	params.CopySourceKey = src
	_, err := s.c.CopyObject(params)
	return err
}

func (s *obsClient) Exists(key string) error {
	params := &obs.GetObjectMetadataInput{}
	params.Bucket = s.bucket
	params.Key = key
	_, err := s.c.GetObjectMetadata(params)
	return err
}

func (s *obsClient) Delete(key string) error {
	params := obs.DeleteObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	_, err := s.c.DeleteObject(&params)
	return err
}

func (s *obsClient) List(prefix, marker string, limit int64) ([]*Object, error) {
	input := &obs.ListObjectsInput{
		Bucket: s.bucket,
		Marker: marker,
	}
	input.Prefix = prefix
	input.MaxKeys = int(limit)
	resp, err := s.c.ListObjects(input)
	for err == nil && len(resp.Contents) == 0 && resp.IsTruncated {
		input.Marker = resp.NextMarker
		resp, err = s.c.ListObjects(input)
	}
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]*Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		mtime := int(o.LastModified.Unix())
		objs[i] = &Object{o.Key, o.Size, mtime, mtime}
	}
	return objs, nil
}

func newObs(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint %s: %s", endpoint, err)
	}
	hostParts := strings.SplitN(uri.Host, ".", 2)
	bucket := hostParts[0]
	region := strings.Split(hostParts[1], ".")[1]
	endpoint = fmt.Sprintf("%s://%s", uri.Scheme, hostParts[1])
	c, err := obs.New(accessKey, secretKey, endpoint)
	if err != nil {
		logger.Fatal(err)
	}
	return &obsClient{bucket, region, c}
}

func init() {
	RegisterStorage("obs", newObs)
}
