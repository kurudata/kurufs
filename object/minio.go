package object

import (
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type minio struct {
	s3client
}

func (m *minio) String() string {
	return *m.s3client.ses.Config.Endpoint
}

func (m *minio) Create() error {
	return nil
}

func newMinio(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint %s: %s", endpoint, err)
	}
	ssl := strings.ToLower(uri.Scheme) == "https"
	awsConfig := &aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         &uri.Host,
		DisableSSL:       aws.Bool(!ssl),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
	}
	if accessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	ses := session.New(awsConfig) //.WithLogLevel(aws.LogDebugWithHTTPBody))
	return &minio{s3client{uri.Path[1:], s3.New(ses), ses}}
}

func init() {
	RegisterStorage("minio", newMinio)
}
