package object

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type ufile struct {
	RestfulStorage
}

func (u *ufile) String() string {
	uri, _ := url.ParseRequestURI(u.endpoint)
	return fmt.Sprintf("ufile://%s", uri.Host)
}

func ufileSigner(req *http.Request, accessKey, secretKey, signName string) {
	if accessKey == "" {
		return
	}
	toSign := req.Method + "\n"
	for _, n := range HEADER_NAMES {
		toSign += req.Header.Get(n) + "\n"
	}
	bucket := strings.Split(req.URL.Host, ".")[0]
	key := req.URL.Path
	// Hack for UploadHit
	if len(req.URL.RawQuery) > 0 {
		vs, _ := url.ParseQuery(req.URL.RawQuery)
		if _, ok := vs["FileName"]; ok {
			key = "/" + vs.Get("FileName")
		}
	}
	toSign += "/" + bucket + key
	h := hmac.New(sha1.New, []byte(secretKey))
	h.Write([]byte(toSign))
	sig := base64.StdEncoding.EncodeToString(h.Sum(nil))
	token := signName + " " + accessKey + ":" + sig
	req.Header.Add("Authorization", token)
}

func (u *ufile) Create() error {
	uri, _ := url.ParseRequestURI(u.endpoint)
	parts := strings.Split(uri.Host, ".")
	name := parts[0]
	region := parts[1] // www.cn-bj.ufileos.com
	if region == "ufile" {
		region = parts[2] // www.ufile.cn-north-02.ucloud.cn
	}
	if strings.HasPrefix(region, "internal") {
		// www.internal-hk-01.ufileos.cn
		// www.internal-cn-gd-02.ufileos.cn
		ps := strings.Split(region, "-")
		region = strings.Join(ps[1:len(ps)-1], "-")
	}

	query := url.Values{}
	query.Add("Action", "CreateBucket")
	query.Add("BucketName", name)
	query.Add("PublicKey", u.accessKey)
	query.Add("Region", region)

	// generate signature
	toSign := fmt.Sprintf("ActionCreateBucketBucketName%sPublicKey%sRegion%s",
		name, u.accessKey, region)
	h := sha1.New()
	h.Write([]byte(toSign))
	h.Write([]byte(u.secretKey))
	sig := hex.EncodeToString(h.Sum(nil))
	query.Add("Signature", sig)

	req, err := http.NewRequest("GET", "https://api.ucloud.cn/?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	err = parseError(resp)
	if strings.Contains(err.Error(), "duplicate bucket name") ||
		strings.Contains(err.Error(), "CreateBucketResponse") {
		err = nil
	}
	return err
}

func (u *ufile) parseResp(resp *http.Response, out interface{}) error {
	defer resp.Body.Close()
	if resp.ContentLength <= 0 || resp.ContentLength > (1<<31) {
		return fmt.Errorf("invalid content length: %d", resp.ContentLength)
	}
	data := make([]byte, resp.ContentLength)
	if _, err := io.ReadFull(resp.Body, data); err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("status: %v, message: %s", resp.StatusCode, string(data))
	}
	err := json.Unmarshal(data, out)
	if err != nil {
		return err
	}
	return nil
}

type DataItem struct {
	FileName   string
	Size       int64
	ModifyTime int
}

// ListObjectsOutput presents output for ListObjects.
type uFileListObjectsOutput struct {
	// Object keys
	DataSet []*DataItem `json:"DataSet,omitempty"`
}

func (u *ufile) List(prefix, marker string, limit int64) ([]*Object, error) {
	query := url.Values{}
	query.Add("list", "")
	query.Add("prefix", prefix)
	query.Add("marker", marker)
	if limit > 1000 {
		limit = 1000
	}
	query.Add("limit", strconv.Itoa(int(limit)))
	resp, err := u.request("GET", "?"+query.Encode(), nil, nil)
	if err != nil {
		return nil, err
	}

	var out uFileListObjectsOutput
	if err := u.parseResp(resp, &out); err != nil {
		return nil, err
	}
	objs := make([]*Object, len(out.DataSet))
	for i, item := range out.DataSet {
		mtime := item.ModifyTime
		objs[i] = &Object{item.FileName, item.Size, mtime, mtime}
	}
	return objs, nil
}

func newUFile(endpoint, accessKey, secretKey string) ObjectStorage {
	return &ufile{RestfulStorage{defaultObjectStorage{}, endpoint, accessKey, secretKey, "UCloud", ufileSigner}}
}

func init() {
	RegisterStorage("ufile", newUFile)
}
