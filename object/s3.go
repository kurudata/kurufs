package object

import (
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3client struct {
	bucket string
	s3     *s3.S3
	ses    *session.Session
}

func (s *s3client) String() string {
	return fmt.Sprintf("s3://%s", s.bucket)
}

func (s *s3client) Create() error {
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

func (s *s3client) Get(key string, off, limit int64) (io.ReadCloser, error) {
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
	if off == 0 && limit == -1 {
		cs := resp.Metadata[checksumAlgr]
		if cs != nil {
			resp.Body = verifyChecksum(resp.Body, *cs)
		}
	}
	return resp.Body, nil
}

func (s *s3client) Put(key string, in io.ReadSeeker) error {
	checksum := generateChecksum(in)
	params := &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     in,
		Metadata: map[string]*string{checksumAlgr: &checksum},
	}
	_, err := s.s3.PutObject(params)
	return err
}

func (s *s3client) Copy(dst, src string) error {
	src = s.bucket + "/" + src
	params := &s3.CopyObjectInput{
		Bucket:     &s.bucket,
		Key:        &dst,
		CopySource: &src,
	}
	_, err := s.s3.CopyObject(params)
	return err
}

func (s *s3client) Exists(key string) error {
	param := s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.HeadObject(&param)
	return err
}

func (s *s3client) Delete(key string) error {
	param := s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3.DeleteObject(&param)
	return err
}

func (s *s3client) List(prefix, marker string, limit int64) ([]*Object, error) {
	param := s3.ListObjectsInput{
		Bucket:  &s.bucket,
		Prefix:  &prefix,
		Marker:  &marker,
		MaxKeys: &limit,
	}
	resp, err := s.s3.ListObjects(&param)
	for err == nil && len(resp.Contents) == 0 && *resp.IsTruncated {
		param.Marker = resp.NextMarker
		resp, err = s.s3.ListObjects(&param)
	}
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

func newS3(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint %s: %s", endpoint, err)
	}
	ssl := strings.ToLower(uri.Scheme) == "https"

	var (
		bucket string
		region string
		ep     string
	)

	if strings.Contains(uri.Host, ".amazonaws.com") {
		// standard s3
		// [BUCKET].s3-[REGION].[REST_OF_ENDPOINT]
		// [BUCKET].s3.[REGION].amazonaws.com[.cn]
		hostParts := strings.SplitN(uri.Host, ".s3", 2)
		bucket = hostParts[0]
		endpoint = "s3" + hostParts[1]
		if strings.HasPrefix(endpoint, "s3-") || strings.HasPrefix(endpoint, "s3.") {
			endpoint = endpoint[3:]
		}
		if strings.HasPrefix(endpoint, "dualstack") {
			endpoint = endpoint[len("dualstack."):]
		}
		if endpoint == "amazonaws.com" {
			endpoint = "us-east-1." + endpoint
		}

		region = strings.Split(endpoint, ".")[0]
		if region == "external-1" {
			region = "us-east-1"
		}
	} else {
		// compatible s3
		// [BUCKET].[ENDPOINT]
		hostParts := strings.SplitN(uri.Host, ".", 2)
		bucket = hostParts[0]
		ep = hostParts[1]
		region = "us-east-1"
	}

	awsConfig := &aws.Config{
		Region:     aws.String(region),
		DisableSSL: aws.Bool(!ssl),
		HTTPClient: httpClient,
	}
	if accessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}
	if ep != "" {
		awsConfig.Endpoint = aws.String(ep)
		awsConfig.S3ForcePathStyle = aws.Bool(true)
	}

	ses := session.New(awsConfig) //.WithLogLevel(aws.LogDebugWithHTTPBody))
	return &s3client{bucket, s3.New(ses), ses}
}

func init() {
	RegisterStorage("s3", newS3)
}
