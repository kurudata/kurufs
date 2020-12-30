/*
 * JuiceFS, Copyright (C) 2020 Juicedata, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/juicedata/juicefs/meta"
	"github.com/juicedata/juicefs/redis"
	"github.com/juicedata/juicefs/utils"
	"github.com/juicedata/juicesync/object"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func fixObjectSize(s int) int {
	var bits uint
	for s > 1 {
		bits++
		s /= 2
	}
	s = s << bits
	if s < 64 {
		s = 64
	} else if s > 16<<10 {
		s = 16 << 10
	}
	return s
}

func createStorage(fmt *meta.Format) object.ObjectStorage {
	blob := object.CreateStorage(strings.ToLower(fmt.Storage), fmt.Bucket, fmt.AccessKey, fmt.SecretKey)
	if blob == nil {
		logger.Fatalf("Invalid storage type: %s", fmt.Storage)
	}
	return object.WithPrefix(blob, fmt.Volume+"/")
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func doTesting(store object.ObjectStorage, key string, data []byte) error {
	if err := store.Put(key, bytes.NewReader(data)); err != nil {
		if strings.Contains(err.Error(), "Access Denied") {
			return fmt.Errorf("Failed to put: %s", err)
		}
		// TODO: upgrade juicesync
		// if err2 := store.Create(); err2 != nil {
		// 	return fmt.Errorf("Failed to create %s: %s,  previous error: %s\nplease create bucket %s manually, then mount again",
		// 		store, err2, err, store)
		// }
		if err := store.Put(key, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("Failed to put: %s", err)
		}
	}
	p, err := store.Get(key, 0, -1)
	if err != nil {
		return fmt.Errorf("Failed to get: %s", err)
	}
	data2, err := ioutil.ReadAll(p)
	p.Close()
	if !bytes.Equal(data, data2) {
		return fmt.Errorf("Read wrong data")
	}
	err = store.Delete(key)
	if err != nil {
		fmt.Printf("Failed to delete: %s", err)
	}
	return nil
}

func test(store object.ObjectStorage) error {
	rand.Seed(int64(time.Now().UnixNano()))
	key := "testing/" + randSeq(10)
	data := make([]byte, 100)
	rand.Read(data)
	nRetry := 3
	var err error
	for i := 0; i < nRetry; i++ {
		err = doTesting(store, key, data)
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(i*3+1))
	}
	return err
}

func format(c *cli.Context) error {
	if c.Bool("trace") {
		utils.SetLogLevel(logrus.TraceLevel)
	} else if c.Bool("debug") {
		utils.SetLogLevel(logrus.DebugLevel)
	} else if c.Bool("quiet") {
		utils.SetLogLevel(logrus.ErrorLevel)
		utils.InitLoggers(!c.Bool("nosyslog"))
	}

	logger.Infof("Meta address: %s", c.String("meta"))

	accesskey := c.String("accesskey")
	if accesskey == "" && os.Getenv("ACCESS_KEY") != "" {
		accesskey = os.Getenv("ACCESS_KEY")
		os.Unsetenv("ACCESS_KEY")
	}
	secretkey := c.String("secretkey")
	if secretkey == "" && os.Getenv("SECRET_KEY") != "" {
		secretkey = os.Getenv("SECRET_KEY")
		os.Unsetenv("SECRET_KEY")
	}
	if c.Args().Len() < 1 {
		logger.Fatalf("please give it a name")
	}
	name := c.Args().Get(0)
	blockSize := fixObjectSize(c.Int("blockSize"))
	if blockSize != 4096 {
		logger.Infof("files is splitted into blocks up to %d KiB", blockSize)
	}
	format := meta.Format{
		Volume:      name,
		Storage:     c.String("storage"),
		Bucket:      c.String("bucket"),
		AccessKey:   accesskey,
		SecretKey:   secretkey,
		BlockSize:   blockSize,
		Compression: c.String("compress"),
	}

	if format.Storage == "file" && !strings.HasSuffix(format.Bucket, "/") {
		format.Bucket += "/"
	}

	object.UserAgent = "JuiceFS-CE-" + Build()
	blob := createStorage(&format)
	logger.Infof("Data uses %s", blob)
	if err := test(blob); err != nil {
		logger.Fatalf("Storage %s is not configured correctly: %s", blob, err)
		return err
	}

	var rc = redis.RedisConfig{Retries: 10}
	m, err := redis.NewRedisMeta(c.String("meta"), &rc)
	if err != nil {
		logger.Fatalf("Meta is not available: %s", err)
	}
	err = m.Init(format)
	if err != nil {
		logger.Fatalf("format: %s", err)
		return err
	}
	logger.Infof("Volume is formatted as %+v", format)
	return nil
}

func formatFlags() *cli.Command {
	var defaultBucket = "/var/jfs"
	if runtime.GOOS == "darwin" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
			return nil
		}
		defaultBucket = path.Join(homeDir, ".juicefs", "local")
	}
	return &cli.Command{
		Name:  "format",
		Usage: "format a volume",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "meta",
				Aliases: []string{"m"},
				Value:   "redis://127.0.0.1:6379/1",
				Usage:   "Address for metadata",
			},
			&cli.IntFlag{
				Name:  "blockSize",
				Value: 4096,
				Usage: "size of block in KiB",
			},
			&cli.StringFlag{
				Name:  "compress",
				Value: "lz4",
				Usage: "compression algorithm",
			},
			&cli.StringFlag{
				Name:  "storage",
				Value: "file",
				Usage: "Address for metadata",
			},
			&cli.StringFlag{
				Name:  "bucket",
				Value: defaultBucket,
				Usage: "A bucket to store data",
			},
			&cli.StringFlag{
				Name:  "accesskey",
				Usage: "Access key for object storage",
			},
			&cli.StringFlag{
				Name:  "secretkey",
				Usage: "Secret key for object storage",
			},
		},
		ArgsUsage: "NAME",
		Action:    format,
	}
}
