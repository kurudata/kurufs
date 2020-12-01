package object_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"io/ioutil"
	"jfs/object"
	"os/exec"
	"testing"
)

var testkey = GenerateRsaKeyPair()

func GenerateRsaKeyPair() *rsa.PrivateKey {
	privkey, _ := rsa.GenerateKey(rand.Reader, 2048)
	return privkey
}

func TestRSA(t *testing.T) {
	c1 := object.NewRSAEncryptor(testkey)
	ciphertext, _ := c1.Encrypt([]byte("hello"))

	privPEM := object.ExportRsaPrivateKeyToPem(testkey, "abc")
	key2, _ := object.ParseRsaPrivateKeyFromPem(privPEM, "abc")
	c2 := object.NewRSAEncryptor(key2)
	plaintext, _ := c2.Decrypt(ciphertext)
	if string(plaintext) != "hello" {
		t.Fail()
	}

	_, err := object.ParseRsaPrivateKeyFromPem(privPEM, "")
	if err == nil {
		t.Errorf("parse without passphrase should fail")
		t.Fail()
	}
	_, err = object.ParseRsaPrivateKeyFromPem(privPEM, "ab")
	if err != x509.IncorrectPasswordError {
		t.Errorf("parse without passphrase should return IncorrectPasswordError")
		t.Fail()
	}

	exec.Command("openssl", "genrsa", "-out", "/tmp/private.pem", "2048").Run()
	if _, err = object.ParseRsaPrivateKeyFromPath("/tmp/private.pem", ""); err != nil {
		t.Error(err)
		t.Fail()
	}
	exec.Command("openssl", "genrsa", "-out", "/tmp/private.pem", "-aes256", "-passout", "pass:abcd", "2048").Run()
	if _, err = object.ParseRsaPrivateKeyFromPath("/tmp/private.pem", "abcd"); err != nil {
		t.Error(err)
		t.Fail()
	}
}

func BenchmarkRSA4096Encrypt(b *testing.B) {
	secret := make([]byte, 32)
	kc := object.NewRSAEncryptor(testkey)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		kc.Encrypt(secret)
	}
}

func BenchmarkRSA4096Decrypt(b *testing.B) {
	secret := make([]byte, 32)
	kc := object.NewRSAEncryptor(testkey)
	ciphertext, _ := kc.Encrypt(secret)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		kc.Decrypt(ciphertext)
	}
}

func TestAESGCM(t *testing.T) {
	kc := object.NewRSAEncryptor(testkey)
	dc := object.NewAESEncryptor(kc)
	data := []byte("hello")
	ciphertext, _ := dc.Encrypt(data)
	plaintext, _ := dc.Decrypt(ciphertext)
	if !bytes.Equal(data, plaintext) {
		t.Errorf("decrypt fail")
		t.Fail()
	}
}

func TestEncryptedStore(t *testing.T) {
	s := object.CreateStorage("mem", "", "", "")
	kc := object.NewRSAEncryptor(testkey)
	dc := object.NewAESEncryptor(kc)
	es := object.NewEncrypted(s, dc)
	es.Put("a", bytes.NewReader([]byte("hello")))
	r, err := es.Get("a", 1, 2)
	if err != nil {
		t.Errorf("Get a: %s", err)
		t.Fail()
	}
	d, _ := ioutil.ReadAll(r)
	if string(d) != "el" {
		t.Fail()
	}

	r, _ = es.Get("a", 0, -1)
	d, _ = ioutil.ReadAll(r)
	if string(d) != "hello" {
		t.Fail()
	}
}
