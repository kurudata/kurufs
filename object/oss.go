package object

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type ossClient struct {
	client *oss.Client
	bucket *oss.Bucket
}

func (o *ossClient) String() string {
	return fmt.Sprintf("oss://%s", o.bucket.BucketName)
}

func (o *ossClient) Create() error {
	err := o.bucket.Client.CreateBucket(o.bucket.BucketName)
	// ignore error if bucket is already created
	if err != nil && strings.Contains(err.Error(), "BucketAlreadyExists") {
		err = nil
	}
	return err
}

func (o *ossClient) checkError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "InvalidAccessKeyId") || strings.Contains(msg, "SecurityTokenExpired") {
		logger.Warnf("refresh security token: %s", err)
		go o.refreshToken()
	}
	return err
}

func (o *ossClient) Get(key string, off, limit int64) (resp io.ReadCloser, err error) {
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("%d-", off)
		}
		resp, err = o.bucket.GetObject(key, oss.NormalizedRange(r), oss.RangeBehavior("standard"))
	} else {
		resp, err = o.bucket.GetObject(key)
		if err == nil {
			resp = verifyChecksum(resp,
				resp.(*oss.Response).Headers.Get(oss.HTTPHeaderOssMetaPrefix+checksumAlgr))
		}
	}
	o.checkError(err)
	return resp, err
}

func (o *ossClient) Put(key string, in io.ReadSeeker) error {
	option := oss.Meta(checksumAlgr, generateChecksum(in))
	return o.checkError(o.bucket.PutObject(key, in, option))
}

func (o *ossClient) Copy(dst, src string) error {
	_, err := o.bucket.CopyObject(src, dst)
	return o.checkError(err)
}

func (o *ossClient) Exists(key string) error {
	_, err := o.bucket.GetObjectDetailedMeta(key)
	return o.checkError(err)
}

func (o *ossClient) Delete(key string) error {
	return o.checkError(o.bucket.DeleteObject(key))
}

func (o *ossClient) List(prefix, marker string, limit int64) ([]*Object, error) {
	if limit > 1000 {
		limit = 1000
	}
	result, err := o.bucket.ListObjects(oss.Prefix(prefix),
		oss.Marker(marker), oss.MaxKeys(int(limit)))
	o.checkError(err)
	for err == nil && len(result.Objects) == 0 && result.IsTruncated {
		result, err = o.bucket.ListObjects(oss.Prefix(prefix),
			oss.Marker(result.NextMarker), oss.MaxKeys(int(limit)))
	}
	if err != nil {
		return nil, err
	}
	n := len(result.Objects)
	objs := make([]*Object, n)
	for i := 0; i < n; i++ {
		o := result.Objects[i]
		mtime := int(o.LastModified.Unix())
		objs[i] = &Object{o.Key, o.Size, mtime, mtime}
	}
	return objs, nil
}

type stsCred struct {
	AccessKeyId     string
	AccessKeySecret string
	Expiration      string
	SecurityToken   string
	LastUpdated     string
	Code            string
}

func fetch(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}

func fetchStsToken() (*stsCred, error) {
	if cred, err := fetchStsCred(); err == nil {
		return cred, nil
	}
	url := "http://127.0.0.1:10011/"
	token, err := fetch(url + "role-security-token")
	if err != nil {
		return nil, err
	}
	accessKey, err := fetch(url + "role-access-key-id")
	if err != nil {
		return nil, err
	}
	secretKey, err := fetch(url + "role-access-key-secret")
	if err != nil {
		return nil, err
	}
	return &stsCred{
		SecurityToken:   string(token),
		AccessKeyId:     string(accessKey),
		AccessKeySecret: string(secretKey),
		Expiration:      time.Now().Add(time.Hour * 24 * 100).Format("2006-01-02T15:04:05Z"),
	}, nil
}

func fetchStsCred() (*stsCred, error) {
	url := "http://100.100.100.200/latest/meta-data/Ram/security-credentials/"
	role, err := fetch(url)
	if err != nil {
		return nil, err
	}
	d, err := fetch(url + string(role))
	if err != nil {
		return nil, err
	}
	var cred stsCred
	err = json.Unmarshal(d, &cred)
	return &cred, err
}

func (o *ossClient) refreshToken() time.Time {
	cred, err := fetchStsToken()
	if err != nil {
		logger.Errorf("refresh token: %s", err)
		return time.Now().Add(time.Second)
	}
	o.client.Config.AccessKeyID = cred.AccessKeyId
	o.client.Config.AccessKeySecret = cred.AccessKeySecret
	o.client.Config.SecurityToken = cred.SecurityToken
	logger.Debugf("Refreshed STS, will be expired at %s", cred.Expiration)
	expire, err := time.Parse("2006-01-02T15:04:05Z", cred.Expiration)
	if err != nil {
		logger.Errorf("invalid expiration: %s, %s", cred.Expiration, err)
		return time.Now().Add(time.Minute)
	}
	return expire
}

func newOSS(endpoint, accessKey, secretKey string) ObjectStorage {
	uri, err := url.ParseRequestURI(endpoint)
	if err != nil {
		logger.Fatalf("Invalid endpoint: %v, error: %v", endpoint, err)
	}
	hostParts := strings.SplitN(uri.Host, ".", 2)
	bucketName := hostParts[0]
	domain := uri.Scheme + "://" + hostParts[1]

	var client *oss.Client
	if accessKey != "" {
		client, err = oss.New(domain, accessKey, secretKey)
	} else {
		cred, err := fetchStsToken()
		if err != nil {
			logger.Fatalf("No credential provided for OSS")
		}
		client, err = oss.New(domain, cred.AccessKeyId, cred.AccessKeySecret,
			oss.SecurityToken(cred.SecurityToken))
	}
	if err != nil {
		logger.Fatalf("Cannot create OSS client with endpoint %s: %s", endpoint, err)
	}
	client.Config.Timeout = 10
	client.Config.RetryTimes = 1
	client.Config.HTTPTimeout.ConnectTimeout = time.Second * 2   // 30s
	client.Config.HTTPTimeout.ReadWriteTimeout = time.Second * 5 // 60s
	client.Config.HTTPTimeout.HeaderTimeout = time.Second * 5    // 60s
	client.Config.HTTPTimeout.LongTimeout = time.Second * 30     // 300s
	client.Config.IsEnableCRC = false                            // CRC64ECMA is much slower than CRC32C
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		logger.Fatalf("Cannot create bucket %s: %s", bucketName, err)
	}
	o := &ossClient{client: client, bucket: bucket}
	if o.client.Config.SecurityToken != "" {
		go func() {
			for {
				next := o.refreshToken()
				time.Sleep(next.Sub(time.Now()) / 2)
			}
		}()
	}
	return o
}

func init() {
	RegisterStorage("oss", newOSS)
}
