package object

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/awserr"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

type ibmcos struct {
	bucket string
	s3     *s3.S3
}

func (s *ibmcos) String() string {
	return fmt.Sprintf("ibmcos://%s", s.bucket)
}

func (s *ibmcos) Create() error {
	_, err := s.s3.CreateBucket(&s3.CreateBucketInput{Bucket: &s.bucket})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				err = nil
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				err = nil
			}
		}
	}
	return err
}

func (s *ibmcos) Get(key string, off, limit int64) (io.ReadCloser, error) {
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

func (s *ibmcos) Put(key string, in io.ReadSeeker) error {
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

func (s *ibmcos) Copy(dst, src string) error {
	src = s.bucket + "/" + src
	params := &s3.CopyObjectInput{
		Bucket:     &s.bucket,
		Key:        &dst,
		CopySource: &src,
	}
	_, err := s.s3.CopyObject(params)
	return err
}

func (s *ibmcos) Exists(key string) error {
	param := s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.HeadObject(&param)
	return err
}

func (s *ibmcos) Delete(key string) error {
	param := s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.DeleteObject(&param)
	return err
}

func (s *ibmcos) List(prefix, marker string, limit int64) ([]*Object, error) {
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

func newIBMCOS(endpoint, apiKey, serviceInstanceID string) ObjectStorage {
	uri, _ := url.ParseRequestURI(endpoint)
	hostParts := strings.Split(uri.Host, ".")
	bucket := hostParts[0]
	region := hostParts[2]
	authEndpoint := "https://iam.cloud.ibm.com/identity/token"
	serviceEndpoint := "https://" + strings.SplitN(uri.Host, ".", 2)[1]
	conf := aws.NewConfig().
		WithRegion(region).
		WithEndpoint(serviceEndpoint).
		WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(),
			authEndpoint, apiKey, serviceInstanceID)).
		WithS3ForcePathStyle(true)
	sess := session.Must(session.NewSession())
	client := s3.New(sess, conf)
	return &ibmcos{bucket, client}
}

func init() {
	RegisterStorage("ibmcos", newIBMCOS)
}
