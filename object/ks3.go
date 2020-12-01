package object

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ks3sdklib/aws-sdk-go/aws"
	"github.com/ks3sdklib/aws-sdk-go/aws/credentials"
	"github.com/ks3sdklib/aws-sdk-go/service/s3"
)

type ks3 struct {
	bucket string
	s3     *s3.S3
	ses    *session.Session
}

func (s *ks3) String() string {
	return fmt.Sprintf("ks3://%s", s.bucket)
}

func (s *ks3) Create() error {
	_, err := s.s3.CreateBucket(&s3.CreateBucketInput{Bucket: &s.bucket})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case awss3.ErrCodeBucketAlreadyExists:
				err = nil
			case awss3.ErrCodeBucketAlreadyOwnedByYou:
				err = nil
			}
		}
	}
	return err
}

func (s *ks3) Get(key string, off, limit int64) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		params.Range = &r
	}
	resp, err := s.s3.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *ks3) Put(key string, in io.ReadSeeker) error {
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
	params := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   body,
	}
	_, err := s.s3.PutObject(params)
	return err
}
func (s *ks3) Copy(dst, src string) error {
	src = s.bucket + "/" + src
	params := &s3.CopyObjectInput{
		Bucket:     &s.bucket,
		Key:        &dst,
		CopySource: &src,
	}
	_, err := s.s3.CopyObject(params)
	return err
}

func (s *ks3) Exists(key string) error {
	param := s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.HeadObject(&param)
	return err
}

func (s *ks3) Delete(key string) error {
	param := s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.DeleteObject(&param)
	return err
}

func (s *ks3) List(prefix, marker string, limit int64) ([]*Object, error) {
	param := s3.ListObjectsInput{
		Bucket:  &s.bucket,
		Prefix:  &prefix,
		Marker:  &marker,
		MaxKeys: &limit,
	}
	resp, err := s.s3.ListObjects(&param)
	if err != nil {
		return nil, err
	}
	n := len(resp.Contents)
	objs := make([]*Object, n)
	for i := 0; i < n; i++ {
		o := resp.Contents[i]
		mtime := int(o.LastModified.Unix())
		objs[i] = &Object{*o.Key, *o.Size, mtime, mtime}
	}
	return objs, nil
}

var ks3Regions = map[string]string{
	"cn-beijing":   "BEIJING",
	"cn-shanghai":  "SHANGHAI",
	"cn-guangzhou": "GUANGZHOU",
	"cn-qingdao":   "QINGDAO",
	"jr-beijing":   "JR_BEIJING",
	"jr-shanghai":  "JR_SHANGHAI",
	"":             "HANGZHOU",
	"cn-hk-1":      "HONGKONG",
	"rus":          "RUSSIA",
	"sgp":          "SINGAPORE",
}

func newKS3(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, _ := url.ParseRequestURI(endpoint)
	ssl := strings.ToLower(uri.Scheme) == "https"
	hostParts := strings.Split(uri.Host, ".")
	bucket := hostParts[0]
	region := hostParts[1][3:]
	if strings.HasPrefix(region, "-") {
		region = region[1:]
	}
	if strings.HasSuffix(uri.Host, "ksyun.com") {
		if strings.HasSuffix(region, "-internal") {
			region = region[:len(region)-len("-internal")]
		}
		region = ks3Regions[region]
	}

	if region == "" {
		region = "ks3"
	}

	awsConfig := &aws.Config{
		Region:                  region,
		Endpoint:                strings.SplitN(uri.Host, ".", 2)[1],
		DisableSSL:              !ssl,
		HTTPClient:              httpClient,
		S3ForcePathStyle:        true,
		Credentials:             credentials.NewStaticCredentials(accessKey, secretKey, ""),
		DisableComputeChecksums: os.Getenv("DISABLE_MD5") != "",
	}

	return &ks3{bucket, s3.New(awsConfig), nil}
}

func init() {
	RegisterStorage("ks3", newKS3)
}
