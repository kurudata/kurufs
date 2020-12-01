package object

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
)

type gs struct {
	defaultObjectStorage
	region     string
	bucketName string
	bucket     *storage.BucketHandle
	ot         *storage.ObjectIterator
}

func (g *gs) String() string {
	return fmt.Sprintf("gs://%s", g.bucketName)
}

func (g *gs) Create() error {
	// check if the bucket is already exists
	if objs, err := g.List("", "", 1); err == nil && len(objs) > 0 {
		return nil
	}

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		projectID, _ = metadata.ProjectID()
	}
	if projectID == "" {
		cred, err := google.FindDefaultCredentials(context.Background())
		if err == nil {
			projectID = cred.ProjectID
		}
	}
	if projectID == "" {
		log.Fatalf("GOOGLE_CLOUD_PROJECT environment variable must be set")
	}

	attr := &storage.BucketAttrs{StorageClass: "regional", Location: g.region}
	err := g.bucket.Create(context.Background(), projectID, attr)
	if err != nil && strings.Contains(err.Error(), "You already own this bucket") {
		return nil
	}
	return err
}

func (g *gs) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return g.bucket.Object(key).NewRangeReader(context.Background(), off, limit)
}

func (g *gs) Put(key string, data io.ReadSeeker) error {
	wc := g.bucket.Object(key).NewWriter(context.Background())
	if _, err := io.Copy(wc, data); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

func (g *gs) Copy(dst, src string) error {
	srcObj := g.bucket.Object(src)
	dstObj := g.bucket.Object(dst)
	_, err := dstObj.CopierFrom(srcObj).Run(context.Background())
	return err
}

func (g *gs) Exists(key string) error {
	obj := g.bucket.Object(key)
	_, err := obj.Attrs(context.Background())
	return err
}

func (g *gs) Delete(key string) error {
	obj := g.bucket.Object(key)
	return obj.Delete(context.Background())
}

func (g *gs) List(prefix, marker string, limit int64) ([]*Object, error) {
	objects := []*Object{}
	if marker == "" {
		g.ot = g.bucket.Objects(context.Background(), &storage.Query{Prefix: prefix})
	}
	if g.ot == nil {
		return objects, nil
	}

	count := int64(0)
	for count < limit {
		attrs, err := g.ot.Next()
		if err != nil {
			break
		}
		obj := &Object{
			Key:   attrs.Name,
			Size:  int64(attrs.Size),
			Ctime: int(attrs.Created.Unix()),
			Mtime: int(attrs.Updated.Unix()),
		}
		objects = append(objects, obj)
		count++
	}
	return objects, nil
}

func newGS(host, accessKey, secretKey string) ObjectStorage {
	hostParts := strings.Split(host, ".")
	bucket := hostParts[0]
	region := hostParts[1]
	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Create storage client: %s", err)
	}

	return &gs{
		region:     region,
		bucketName: bucket,
		bucket:     client.Bucket(bucket),
	}
}

func init() {
	RegisterStorage("gs", newGS)
}
