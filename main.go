package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/chunk"
	"github.com/juicedata/juicefs/fuse"
	"github.com/juicedata/juicefs/meta"
	"github.com/juicedata/juicefs/redis"
	"github.com/juicedata/juicefs/utils"
	"github.com/juicedata/juicefs/vfs"

	"github.com/juicedata/juicesync/object"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/google/gops/agent"
)

func installHandler(mp string) {
	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 10)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for {
			<-signalChan
			go func() {
				if runtime.GOOS == "linux" {
					exec.Command("umount", mp, "-l").Run()
				} else if runtime.GOOS == "darwin" {
					exec.Command("diskutil", "umount", "force", mp).Run()
				}
			}()
			go func() {
				time.Sleep(time.Second * 3)
				os.Exit(1)
			}()
		}
	}()
}

var logger = utils.GetLogger("juicefs")

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

func main() {
	var defaultCacheDir = "/var/jfsCache"
	var defaultBucket = "/var/jfs"
	var defaultMountpoint = "/jfs"
	if runtime.GOOS == "darwin" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Errorf("%v", err)
			return
		}
		defaultMountpoint = path.Join(homeDir, "jfs")
		defaultBucket = path.Join(homeDir, ".juicefs", "local")
		defaultCacheDir = path.Join(homeDir, ".juicefs", "cache")
	}

	app := &cli.App{
		Name:      "juicefs",
		Usage:     "A POSIX filesystem built on redis and object storage.",
		Version:   Build(),
		Copyright: "AGPLv3",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "enable debug log",
			},
			&cli.BoolFlag{
				Name:    "quiet",
				Aliases: []string{"q"},
				Usage:   "only warning and errors",
			},
			&cli.BoolFlag{
				Name:  "trace",
				Usage: "enable trace log",
			},
			&cli.BoolFlag{
				Name:  "nosyslog",
				Usage: "disable syslog",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "format",
				Usage: "format a volume",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "name",
						Usage: "Volume name",
					},
					&cli.StringFlag{
						Name:    "meta",
						Aliases: []string{"m"},
						Value:   "redis://localhost:6379/1",
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
			},
			{
				Name:   "mount",
				Usage:  "mount a volume",
				Action: mount,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "meta",
						Value: "redis://localhost:6379/1",
						Usage: "Address for metadata",
					},
					&cli.StringFlag{
						Name:    "mountpoint",
						Aliases: []string{"m"},
						Value:   defaultMountpoint,
						Usage:   "mount point",
					},
					&cli.StringFlag{
						Name:  "o",
						Usage: "other fuse options",
					},
					&cli.Float64Flag{
						Name:  "attrcacheto",
						Value: 1.0,
						Usage: "attributes cache timeout in seconds",
					},
					&cli.Float64Flag{
						Name:  "entrycacheto",
						Value: 1.0,
						Usage: "file entry cache timeout in seconds",
					},
					&cli.Float64Flag{
						Name:  "direntrycacheto",
						Value: 1.0,
						Usage: "dir entry cache timeout in seconds",
					},

					&cli.IntFlag{
						Name:  "getTimeout",
						Value: 60,
						Usage: "the max number of seconds to download an object",
					},
					&cli.IntFlag{
						Name:  "putTimeout",
						Value: 60,
						Usage: "the max number of seconds to upload an object",
					},
					&cli.IntFlag{
						Name:  "ioretries",
						Value: 10,
						Usage: "number of retries after network failure",
					},
					&cli.IntFlag{
						Name:  "maxUpload",
						Value: 20,
						Usage: "number of connections to upload",
					},
					&cli.IntFlag{
						Name:  "bufferSize",
						Value: 300,
						Usage: "total read/write buffering in MB",
					},
					&cli.IntFlag{
						Name:  "prefetch",
						Value: 3,
						Usage: "prefetch N blocks in parallel",
					},

					&cli.BoolFlag{
						Name:  "writeback",
						Usage: "Upload objects in background",
					},
					&cli.StringFlag{
						Name:  "cacheDir",
						Value: defaultCacheDir,
						Usage: "directory to cache object",
					},
					&cli.IntFlag{
						Name:  "cacheSize",
						Value: 1 << 10,
						Usage: "size of cached objects in MiB",
					},
					&cli.Float64Flag{
						Name:  "freeSpace",
						Value: 0.1,
						Usage: "min free space (ratio)",
					},
					&cli.BoolFlag{
						Name:  "partialOnly",
						Usage: "cache only random/small read",
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}

func createStorage(fmt *meta.Format) object.ObjectStorage {
	blob := object.CreateStorage(strings.ToLower(fmt.Storage), fmt.Bucket, fmt.AccessKey, fmt.SecretKey)
	if blob == nil {
		logger.Fatalf("Invalid storage type: %s", fmt.Storage)
	}
	if fmt.Storage != "file" && fmt.Storage != "mem" {
		blob = object.WithPrefix(blob, fmt.Volume)
	}
	return blob
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

	if format.Storage == "file" && strings.HasSuffix(format.Bucket, "/") {
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
	m := redis.NewRedisMeta(c.String("meta"), &rc)
	err := m.Init(format)
	if err != nil {
		logger.Fatalf("format: %s", err)
		return err
	}
	logger.Infof("Volume is formatted as %+v", format)
	return nil
}

func mount(c *cli.Context) error {
	go func() {
		for port := 6060; port < 6100; port++ {
			http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), nil)
		}
	}()
	go func() {
		for port := 6070; port < 6100; port++ {
			agent.Listen(agent.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
		}
	}()
	if c.Bool("trace") {
		utils.SetLogLevel(logrus.TraceLevel)
	} else if c.Bool("debug") {
		utils.SetLogLevel(logrus.DebugLevel)
	} else if c.Bool("quiet") {
		utils.SetLogLevel(logrus.ErrorLevel)
		utils.InitLoggers(!c.Bool("nosyslog"))
	}

	logger.Infof("Meta address: %s", c.String("meta"))
	var rc = redis.RedisConfig{Retries: 10}
	m := redis.NewRedisMeta(c.String("meta"), &rc)
	format, err := m.Load()
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}

	chunkConf := chunk.Config{
		PageSize: format.BlockSize * 1024,
		Compress: format.Compression,

		GetTimeout:  time.Second * time.Duration(c.Int("getTimeout")),
		PutTimeout:  time.Second * time.Duration(c.Int("putTimeout")),
		MaxUpload:   c.Int("maxUpload"),
		AsyncUpload: c.Bool("writeback"),
		Prefetch:    c.Int("prefetch"),
		BufferSize:  c.Int("bufferSize") << 20,

		CacheDir:       c.String("cacheDir"),
		CacheSize:      int64(c.Int("cacheSize")),
		FreeSpace:      float32(c.Float64("freeRatio")),
		CacheMode:      os.FileMode(0600),
		CacheFullBlock: !c.Bool("partialOnly"),
		AutoCreate:     true,
	}
	blob := createStorage(format)
	logger.Infof("Data use %s", blob)

	store := chunk.NewCachedStore(blob, chunkConf)
	m.OnMsg(meta.CHUNK_DEL, meta.MsgCallback(func(args ...interface{}) error {
		chunkid := args[0].(uint64)
		length := args[1].(uint32)
		return store.Remove(chunkid, int(length))
	}))

	mp, _ := filepath.Abs(c.String("m"))
	conf := &vfs.Config{
		Meta: &meta.Config{
			IORetries: 10,
		},
		Version:    Build(),
		Mountpoint: mp,
		Primary: &vfs.StorageConfig{
			Name:      format.Storage,
			Endpoint:  format.Bucket,
			AccessKey: format.AccessKey,
			SecretKey: format.AccessKey,
		},
		Chunk: &chunkConf,
	}
	vfs.Init(conf, m, store)

	installHandler(mp)
	logger.Infof("mount volume %s at %s", format.Volume, mp)
	err = fuse.Main(conf, c.String("o"), c.Float64("attrcacheto"), c.Float64("entrycacheto"), c.Float64("direntrycacheto"))
	if err != nil {
		logger.Errorf("%s", err)
		os.Exit(1)
	}
	return nil
}
