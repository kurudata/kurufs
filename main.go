package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"jfs/chunk"
	"jfs/fuse"
	"jfs/meta"
	"jfs/redis"
	"jfs/utils"
	"jfs/vfs"

	"github.com/juicedata/juicesync/object"

	"github.com/google/gops/agent"
	"github.com/sirupsen/logrus"
)

func installHandler() {
	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 10)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for {
			<-signalChan
			go func() {
				if runtime.GOOS == "linux" {
					exec.Command("umount", *mountpoint, "-l").Run()
				} else if runtime.GOOS == "darwin" {
					exec.Command("diskutil", "umount", "force", *mountpoint).Run()
				}
			}()
			go func() {
				time.Sleep(time.Second * 3)
				os.Exit(1)
			}()
		}
	}()
}

var mountpoint = flag.String("mountpoint", "/jfs", "mount point")
var fuseopts = flag.String("o", "", "other fuse options")
var attrcacheto = flag.Float64("attrcacheto", 1.0, "attributes cache timeout in seconds")
var entrycacheto = flag.Float64("entrycacheto", 1.0, "file entry cache timeout in seconds")
var direntrycacheto = flag.Float64("direntrycacheto", 1.0, "dir entry cache timeout in seconds")

var metaAddr = flag.String("meta", "redis://127.0.0.1:6379/1", "address for Redis")

var storageName = flag.String("storage", "file", "type of object storage: file, s3, ufile, qingstor, oss")

var localDir = flag.String("dir", "/var/jfs", "root of chunk store")
var diskFailRatio = flag.Float64("failRatio", 0.0, "simulate request failure")
var reqDelay = flag.String("reqDelay", "0ms", "simulate slowness of object storage")
var test = flag.Bool("test", false, "test accessing to object storage")

var endpoint = flag.String("endpoint", "", "bucket and endpoint for object storage")
var accesskey = flag.String("accesskey", "", "Access key for object storage")
var secretkey = flag.String("secretkey", "", "Secret key for object storage")

var objectSize = flag.Int("objectSize", 4096, "size of object in KiB")
var partitions = flag.Int("partitions", 1, "number of hash partition for objects")
var compress = flag.String("compress", "zstd0", "compression algorithm")
var getTimeout = flag.Int("getTimeout", 60, "the max number of seconds to download an object")
var putTimeout = flag.Int("putTimeout", 60, "the max number of seconds to upload an object")
var bufferSize = flag.Int("bufferSize", 300, "total read/write buffering in MB")
var readahead = flag.Int("readahead", 0, "max readahead in MiB (default: bufferSize/5)")
var prefetch = flag.Int("prefetch", 3, "prefetch N blocks in parallel")
var ioretries = flag.Int("ioretries", 30, "number of retries after network failure")
var maxUpload = flag.Int("maxUpload", 20, "number of connections to upload")

var cacheDir = flag.String("cacheDir", "/var/jfsCache", "directory to cache object")
var cacheSize = flag.Int64("cacheSize", 2<<10, "size of cached objects in MB")
var freeSpace = flag.Float64("freeSpace", 0.2, "min free space (ratio)")
var cacheMode = flag.String("cacheMode", "0600", "file permissions for cached blocks")
var writeback = flag.Bool("async", false, "Upload objects in background")
var cachePartialOnly = flag.Bool("cachePartialOnly", false, "cache only random/small read")

var version = flag.Bool("V", false, "show version")
var logger = utils.GetLogger("juicefs")
var trace = flag.Bool("vv", false, "turn on trace log")
var debug = flag.Bool("v", false, "turn on debug log")
var quiet = flag.Bool("q", false, "change log level to ERROR")
var nosyslog = flag.Bool("nosyslog", false, "log to syslog")

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
	if runtime.GOOS == "darwin" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Errorf("%v", err)
			return
		}
		*mountpoint = path.Join(homeDir, "jfs")
		*localDir = path.Join(homeDir, ".juicefs", "local")
		*cacheDir = path.Join(homeDir, ".juiefs", "cache")
	}

	flag.Parse()
	if *version {
		fmt.Println("JuiceFS CE", Build())
		return
	}

	if *trace {
		utils.SetLogLevel(logrus.TraceLevel)
	} else if *debug {
		utils.SetLogLevel(logrus.DebugLevel)
	} else if *quiet {
		utils.SetLogLevel(logrus.ErrorLevel)
	}
	if !*debug {
		utils.InitLoggers(!*nosyslog)
	}
	logger.Infof("JuiceFS CE %s", Build())

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

	var rc = redis.RedisConfig{Retries: *ioretries}
	logger.Infof("Meta address: %s", *metaAddr)
	m := redis.NewRedisMeta(*metaAddr, &rc)

	if *accesskey == "" && os.Getenv("ACCESS_KEY") != "" {
		*accesskey = os.Getenv("ACCESS_KEY")
		os.Unsetenv("ACCESS_KEY")
	}
	if *secretkey == "" && os.Getenv("SECRET_KEY") != "" {
		*secretkey = os.Getenv("SECRET_KEY")
		os.Unsetenv("SECRET_KEY")
	}
	storeConf := &vfs.StorageConfig{
		Name:      *storageName,
		Endpoint:  *endpoint,
		AccessKey: *accesskey,
		SecretKey: *secretkey,
	}
	if storeConf.Name == "file" && storeConf.Endpoint == "" {
		storeConf.Endpoint = *localDir
		storeConf.AccessKey = *reqDelay
		storeConf.SecretKey = fmt.Sprintf("%.3f", *diskFailRatio)
	}
	object.UserAgent = "JuiceFS-CE-" + Build()
	blob := createStorage(storeConf)
	logger.Infof("Data uses %s", blob)
	if *test {
		// if err := obj.DoTesting(blob); err != nil {
		// 	logger.Fatalf("Storage %s is not configured correctly: %s", blob, err)
		// }
		os.Exit(0)
	}

	*objectSize = fixObjectSize(*objectSize)
	if *objectSize != 4096 {
		logger.Infof("files is splitted into objects up to %d KB", *objectSize)
	}
	mode, _ := strconv.ParseUint(*cacheMode, 8, 32)
	chunkConf := chunk.Config{
		CacheDir:       *cacheDir,
		CacheSize:      *cacheSize,
		FreeSpace:      float32(*freeSpace),
		CacheMode:      os.FileMode(mode),
		CacheFullBlock: !*cachePartialOnly,
		AutoCreate:     true,
		Compress:       *compress,
		MaxUpload:      *maxUpload,
		AsyncUpload:    *writeback,
		Partitions:     *partitions,
		Prefetch:       *prefetch,
		PageSize:       *objectSize * 1024,
		BufferSize:     *bufferSize << 20,
		Readahead:      *readahead << 20,
		GetTimeout:     time.Second * time.Duration(*getTimeout),
		PutTimeout:     time.Second * time.Duration(*putTimeout),
	}
	store := chunk.NewCachedStore(blob, chunkConf)
	m.OnMsg(meta.CHUNK_DEL, meta.MsgCallback(func(args ...interface{}) error {
		chunkid := args[0].(uint64)
		length := args[1].(uint32)
		return store.Remove(chunkid, int(length))
	}))

	*mountpoint, _ = filepath.Abs(*mountpoint)
	conf := &vfs.Config{
		Version:    Build(),
		Mountpoint: *mountpoint,
		Meta: &meta.Config{
			IORetries: *ioretries,
		},
		Primary: storeConf,
		Chunk:   &chunkConf,
	}
	vfs.Init(conf, m, store)

	installHandler()
	logger.Infof("mount juicefs at %s", *mountpoint)
	err := fuse.Main(conf, *fuseopts, *attrcacheto, *entrycacheto, *direntrycacheto)
	if err != nil {
		logger.Errorf("%s", err)
		os.Exit(1)
	}
}

func createStorage(conf *vfs.StorageConfig) object.ObjectStorage {
	blob := object.CreateStorage(strings.ToLower(conf.Name), conf.Endpoint, conf.AccessKey, conf.SecretKey)
	if blob == nil {
		logger.Fatalf("Invalid storage type: %s", conf.Name)
	}
	if conf.Name != "file" && conf.Name != "mem" {
		uri, _ := url.ParseRequestURI(conf.Endpoint)
		if uri.Path != "" && uri.Path != "/" {
			if !strings.HasSuffix(uri.Path, "/") {
				uri.Path += "/"
			}
			blob = object.WithPrefix(blob, uri.Path)
		}
	}
	return blob
}
