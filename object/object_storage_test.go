package object

import (
	"bytes"
	crand "crypto/rand"
	"crypto/rsa"
	"io/ioutil"
	"jfs/utils"
	"os"
	"strings"
	"testing"
	"time"
)

func get(s ObjectStorage, k string, off, limit int64) (string, error) {
	r, err := s.Get("/test", off, limit)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func testStorage(t *testing.T, s ObjectStorage) {
	if err := s.Create(); err != nil {
		t.Fatalf("Can't create bucket %s: %s", s, err)
	}

	s = WithPrefix(s, "unit-test")
	defer s.Delete("/test")
	defer s.Delete("/test2")
	k := "/large"
	defer s.Delete(k)

	_, err := s.Get("/not_exists", 0, -1)
	if err == nil {
		t.Fatalf("Get should failed: %s", err)
	}

	br := []byte("hello")
	if err := s.Put("/test", bytes.NewReader(br)); err != nil {
		t.Fatalf("PUT failed: %s", err)
	}

	if d, e := get(s, "/test", 0, -1); d != "hello" {
		t.Fatalf("expect hello, but got %v, error:%s", d, e)
	}
	if d, e := get(s, "/test", 2, -1); d != "llo" {
		t.Fatalf("expect llo, but got %v, error:%s", d, e)
	}
	if d, e := get(s, "/test", 2, 2); d != "ll" {
		t.Fatalf("expect ll, but got %v, error:%s", d, e)
	}
	if d, e := get(s, "/test", 4, 1); d != "o" {
		t.Errorf("out-of-range get: 'o', but got %v, error:%s", d, e)
	}

	objs, err2 := s.List("", "", 1)
	if err2 == nil {
		if len(objs) != 1 {
			t.Fatalf("List should return 1 keys, but got %d", len(objs))
		}
		if objs[0].Key != "/test" {
			t.Fatalf("First key should be /test, but got %s", objs[0].Key)
		}
		if !strings.Contains(s.String(), "encrypted") && !strings.Contains(s.String(), "compressed") && objs[0].Size != 5 {
			t.Fatalf("Size of first key should be 5, but got %v", objs[0].Size)
		}
		now := int(time.Now().Unix())
		if objs[0].Mtime < now-30 || objs[0].Mtime > now+30 {
			t.Fatalf("Mtime of key should be within 30 seconds: %d %d", objs[0].Mtime, now)
		}
	} else {
		t.Fatal("list failed", err2)
	}

	if err := s.Put("/test2", bytes.NewReader(br)); err != nil {
		t.Fatalf("PUT failed: %s", err)
	}
	objs, err2 = s.List("", "/test", 10240)
	if err2 == nil {
		if len(objs) != 1 {
			t.Fatalf("List should return 1 keys, but got %d", len(objs))
		}
		if objs[0].Key != "/test2" {
			t.Fatalf("Second key should be /test2")
		}
		if !strings.Contains(s.String(), "encrypted") && !strings.Contains(s.String(), "compressed") && objs[0].Size != 5 {
			t.Fatalf("Size of first key should be 5, but got %v", objs[0].Size)
		}
		now := int(time.Now().Unix())
		if objs[0].Mtime < now-30 || objs[0].Mtime > now+30 {
			t.Fatalf("Mtime of key should be within 30 seconds: %d %d", objs[0].Mtime, now)
		}
	} else {
		t.Fatal("list2 failed", err2)
	}

	objs, err2 = s.List("", "/test2", 1)
	if err2 != nil {
		t.Errorf("list3 failed: %s", err2)
	} else if len(objs) != 0 {
		t.Fatalf("list3 should not return anything, but got %d", len(objs))
	}

	f, _ := ioutil.TempFile("", "test")
	f.Write([]byte("this is a file"))
	f.Seek(0, 0)
	os.Remove(f.Name())
	defer f.Close()
	if err := s.Put("/file", f); err != nil {
		t.Fatalf("failed to put from file")
	} else if s.Exists("/file") != nil {
		t.Fatalf("/file should exists")
	} else {
		s.Delete("/file")
	}

	if err := s.Exists("/test"); err != nil {
		t.Fatalf("check exists failed: %s", err)
	}

	if err := s.Delete("/test"); err != nil {
		t.Fatalf("delete failed: %s", err)
	}

	if err := s.Exists("/test"); err == nil {
		t.Fatalf("Exists should failed")
	}
}

func TestMem(t *testing.T) {
	m := newMem("", "", "")
	testStorage(t, m)
}

func TestQingStor(t *testing.T) {
	s := newQingStor(os.Getenv("QY_ENDPOINT"), os.Getenv("QY_ACCESS_KEY"), os.Getenv("QY_SECRET_KEY"))
	testStorage(t, s)
}

func TestS3(t *testing.T) {
	s := newS3(os.Getenv("AWS_ENDPOINT"), os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
	testStorage(t, s)
}

func TestOSS(t *testing.T) {
	s := newOSS(os.Getenv("OSS_ENDPOINT"), os.Getenv("OSS_ACCESS_KEY"), os.Getenv("OSS_SECRET_KEY"))
	testStorage(t, s)
}

func TestUFile(t *testing.T) {
	ufile := newUFile(os.Getenv("UCLOUD_ENDPOINT"),
		os.Getenv("UCLOUD_PUBLIC_KEY"), os.Getenv("UCLOUD_PRIVATE_KEY"))
	testStorage(t, ufile)
}

func TestGS(t *testing.T) {
	gs := newGS(os.Getenv("QY_ENDPOINT"), "", "")
	testStorage(t, gs)
}

func TestQiniu(t *testing.T) {
	qiniu := newQiniu(os.Getenv("QINIU_ENDPOINT"), os.Getenv("QINIU_ACCESS_KEY"), os.Getenv("QINIU_SECRET_KEY"))
	testStorage(t, qiniu)
}

func TestCompress(t *testing.T) {
	algrs := []string{"zstd0", "zstd", "none"}
	for _, name := range algrs {
		s1 := newMem("", "", "")
		c := NewCompressed(s1, utils.NewCompressor(name))
		testStorage(t, c)
	}
}

func TestKS3(t *testing.T) {
	ks3 := newKS3(os.Getenv("KS3_ENDPOINT"), os.Getenv("KS3_ACCESS_KEY"), os.Getenv("KS3_SECRET_KEY"))
	testStorage(t, ks3)
}

func TestCOS(t *testing.T) {
	cos := newCOS(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_ACCESS_KEY"), os.Getenv("COS_SECRET_KEY"))
	testStorage(t, cos)
}

func TestAzure(t *testing.T) {
	abs := newAbs(os.Getenv("AZURE_ENDPOINT"),
		os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_KEY"))
	testStorage(t, abs)
}

// func TestCeph(t *testing.T) {
// 	s := newCeph("http://jfs-test", "ceph", "client.admin")
// 	testStorage(t, s)
// }

func TestDisk(t *testing.T) {
	s := newDisk("/tmp/abc", "", "")
	testStorage(t, s)
}

func TestB2(t *testing.T) {
	b := newB2(os.Getenv("B2_ENDPOINT"), os.Getenv("B2_KEY_ID"), os.Getenv("B2_APP_KEY"))
	testStorage(t, b)
}

func TestSpace(t *testing.T) {
	b := newSpace(os.Getenv("SPACE_ENDPOINT"), os.Getenv("SPACE_ACCESS_KEY"), os.Getenv("SPACE_SECRET_KEY"))
	testStorage(t, b)
}

func TestBOS(t *testing.T) {
	b := newBOS(os.Getenv("BOS_ENDPOINT"), os.Getenv("BOS_ACCESS_KEY"), os.Getenv("BOS_SECRET_KEY"))
	testStorage(t, b)
}

func TestMinio(t *testing.T) {
	b := newMinio("http://localhost:9000/test", os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY"))
	testStorage(t, b)
}

func TestOBS(t *testing.T) {
	b := newObs(os.Getenv("OBS_ENDPOINT"), os.Getenv("OBS_ACCESS_KEY"), os.Getenv("OBS_SECRET_KEY"))
	testStorage(t, b)
}

func TestWasabi(t *testing.T) {
	s := newWasabi(os.Getenv("WASABI_ENDPOINT"), os.Getenv("WASABI_ACCESS_KEY"), os.Getenv("WASABI_SECRET_KEY"))
	testStorage(t, s)
}

func TestBeansdb(t *testing.T) {
	s := newBeansdb("localhost:7900", "", "")
	testStorage(t, s)
}

// func TestTiKV(t *testing.T) {
// 	s := newTiKV("127.0.0.1:2379", "", "")
// 	testStorage(t, s)
// }

func TestIBMCOS(t *testing.T) {
	s := newIBMCOS(os.Getenv("IBMCOS_ENDPOINT"), os.Getenv("IBMCOS_ACCESS_KEY"), os.Getenv("IBMCOS_SECRET_KEY"))
	testStorage(t, s)
}

func TestEncrypted(t *testing.T) {
	s := CreateStorage("mem", "", "", "")
	privkey, _ := rsa.GenerateKey(crand.Reader, 2048)
	kc := NewRSAEncryptor(privkey)
	dc := NewAESEncryptor(kc)
	es := NewEncrypted(s, dc)
	testStorage(t, es)
}
