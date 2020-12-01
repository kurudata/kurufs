package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"jfs/object"
	"jfs/utils"

	"github.com/davies/groupcache"
	"github.com/golang/protobuf/proto"
)

const CHUNK_SIZE = 1 << 26 // 64M
const BLOCK_SIZE = 1 << 16 // 64K
const SlowRequest = time.Second * time.Duration(10)

var (
	logger = utils.GetLogger("juicefs")
)

// blockSink is a Sink that accept bytes from ByteView without copying.
type blockSink struct {
	bytes []byte
}

func (sink *blockSink) SetString(s string) error { return nil }
func (sink *blockSink) SetBytes(v []byte) error {
	sink.bytes = v
	return nil
}
func (sink *blockSink) SetProto(m proto.Message) error { return nil }
func (sink *blockSink) View() (groupcache.ByteView, error) {
	return groupcache.NewByteView(sink.bytes, ""), nil
}

var _ groupcache.Sink = &blockSink{}

// chunk for read only
type rChunk struct {
	id     uint64
	length int
	store  *cachedStore
}

func chunkForRead(id uint64, length int, store *cachedStore) *rChunk {
	return &rChunk{id, length, store}
}

func (c *rChunk) blockSize(indx int) int {
	bsize := c.length - indx*c.store.conf.PageSize
	if bsize > c.store.conf.PageSize {
		bsize = c.store.conf.PageSize
	}
	return bsize
}

func (c *rChunk) key(indx int) string {
	if c.store.conf.Partitions > 1 {
		return fmt.Sprintf("chunks/%02X/%v/%v_%v_%v", c.id%256, c.id/1000/1000, c.id, indx, c.blockSize(indx))
	}
	return fmt.Sprintf("chunks/%v/%v/%v_%v_%v", c.id/1000/1000, c.id/1000, c.id, indx, c.blockSize(indx))
}

func (c *rChunk) index(off int) int {
	return off / c.store.conf.PageSize
}

func (c *rChunk) Keys() []string {
	if c.length <= 0 {
		return nil
	}
	lastIndx := (c.length - 1) / c.store.conf.PageSize
	keys := make([]string, lastIndx+1)
	for i := 0; i <= lastIndx; i++ {
		keys[i] = c.key(i)
	}
	return keys
}

func groupKey(ctx context.Context, conf *Config) string {
	if conf.CacheGroupSize == 0 {
		return ""
	}
	if v := ctx.Value(CtxKey("inode")); v != nil {
		var group interface{} = 0
		if g := ctx.Value(CtxKey("group")); g != nil {
			group = g
		}
		return fmt.Sprintf("#%v-%v", v, group)
	}
	return ""
}

func (c *rChunk) loadPage(ctx context.Context, indx int) (b *Page, err error) {
	// start := time.Now()
	key := c.key(indx) + groupKey(ctx, &c.store.conf)
	var sink blockSink
	for i := 0; i < 3 && sink.bytes == nil; i++ {
		// for concurrent Get(), only one will got the bytes
		err = c.store.gcache.Get(nil, key, &sink)
		time.Sleep(time.Second * time.Duration(i*i))
	}
	if err == nil && len(sink.bytes) == 0 {
		err = fmt.Errorf("can not download %s after tried 3 times", key)
	}
	if err != nil {
		return nil, err
	}
	block := NewPage(sink.bytes)
	// logger.Debugf("load %v_%v: %s", c.id, indx, time.Since(start))
	return block, nil
}

func (c *rChunk) ReadAt(ctx context.Context, page *Page, off int) (n int, err error) {
	p := page.Data
	if len(p) == 0 {
		return 0, nil
	}
	if int(off) >= c.length {
		return 0, io.EOF
	}

	indx := c.index(off)
	boff := int(off) % c.store.conf.PageSize
	blockSize := c.blockSize(indx)
	if boff+len(p) > blockSize {
		// read beyond currend page
		var got int
		for got < len(p) {
			// aligned to current page
			l := utils.Min(len(p)-got, c.blockSize(c.index(off))-int(off)%c.store.conf.PageSize)
			pp := page.Slice(got, l)
			n, err = c.ReadAt(ctx, pp, off)
			pp.Release()
			if err != nil {
				return got + n, err
			}
			if n == 0 {
				return got, io.EOF
			}
			got += n
			off += n
		}
		return got, nil
	}

	key := c.key(indx)
	inGcache := c.store.gcache.IsCached(key)
	if c.store.conf.CacheSize > 0 && !inGcache {
		r, err := c.store.bcache.load(key)
		if err == nil {
			n, err = r.ReadAt(p, int64(boff))
			r.Close()
			if err == nil {
				return n, nil
			}
			if f, ok := r.(*os.File); ok {
				logger.Warnf("remove partial cached block %s: %d %s", f.Name(), n, err)
				os.Remove(f.Name())
			}
		}
	}

	if !c.store.shouldCache(len(p)) || c.store.conf.CacheGroup == "" && !inGcache {
		if c.store.seekable && boff > 0 && len(p) <= blockSize/4 && !inGcache {
			// partial read
			st := time.Now()
			in, err := c.store.storage.Get(key, int64(boff), int64(len(p)))
			used := time.Since(st)
			logger.Debugf("GET %s RANGE(%d,%d) (%s, %.3fs)", key, boff, len(p), err, used.Seconds())
			if used > SlowRequest {
				logger.Infof("slow request: GET %s (%s, %.3fs)", key, err, used.Seconds())
			}
			c.store.fetcher.fetch(key)
			if err == nil {
				defer in.Close()
				return io.ReadFull(in, p)
			}
		}
		block, err := c.store.group.Do(key, func() (*Page, error) {
			tmp := page
			if boff > 0 || len(p) < blockSize {
				tmp = NewOffPage(blockSize)
			} else {
				tmp.Acquire()
			}
			tmp.Acquire()
			err := withTimeout(func() error {
				defer tmp.Release()
				return c.store.load(key, tmp, c.store.shouldCache(blockSize))
			}, c.store.conf.GetTimeout)
			return tmp, err
		})
		defer block.Release()
		if err != nil {
			return 0, err
		}
		if block != page {
			copy(p, block.Data[boff:])
		}
		return len(p), nil
	}

	if len(p) < blockSize/2 && c.store.conf.CacheGroup != "" {
		// partial read
		key = fmt.Sprintf("%s,%d,%d", key, boff, len(p)) + groupKey(ctx, &c.store.conf)
		var sink blockSink
		for i := 0; i < 3; i++ {
			// for concurrent Get(), only one will got the bytes
			c.store.gcache.Get(nil, key, &sink)
			if sink.bytes != nil {
				return copy(p, sink.bytes), nil
			}
		}
	}
	block, err := c.loadPage(ctx, indx)
	if err != nil {
		return 0, err
	}
	defer block.Release()
	n = copy(p, block.Data[boff:])
	return n, nil
}

func (c *rChunk) delete(indx int) error {
	key := c.key(indx)
	st := time.Now()
	err := c.store.storage.Delete(key)
	used := time.Since(st)
	logger.Debugf("DELETE %v (%v, %.3fs)", key, err, used.Seconds())
	if used > SlowRequest {
		logger.Infof("slow request: DELETE %v (%s, %.3fs)", key, err, used.Seconds())
	}
	return err
}

func (c *rChunk) Remove() error {
	if c.length == 0 {
		// no block
		return nil
	}

	lastIndx := (c.length - 1) / c.store.conf.PageSize
	deleted := false
	for i := 0; i <= lastIndx; i++ {
		// there could be multiple clients try to remove the same chunk in the same time,
		// any of them should succeed if any blocks is removed
		key := c.key(i)
		c.store.pendingMutex.Lock()
		delete(c.store.pendingKeys, key)
		c.store.pendingMutex.Unlock()
		c.store.bcache.remove(key)
		if c.delete(i) == nil {
			deleted = true
		}
	}

	if !deleted {
		return errors.New("chunk not found")
	}
	return nil
}

var pagePool = make(chan *Page, 128)

func allocPage(sz int) *Page {
	if sz != BLOCK_SIZE {
		return NewOffPage(sz)
	}
	select {
	case p := <-pagePool:
		return p
	default:
		return NewOffPage(BLOCK_SIZE)
	}
}

func freePage(p *Page) {
	if cap(p.Data) != BLOCK_SIZE {
		p.Release()
		return
	}
	select {
	case pagePool <- p:
	default:
		p.Release()
	}
}

// chunk for write only
type wChunk struct {
	rChunk
	pages       [][]*Page
	uploaded    int
	errors      chan error
	uploadError error
	pendings    int
}

func chunkForWrite(id uint64, store *cachedStore) *wChunk {
	return &wChunk{
		rChunk: rChunk{id, 0, store},
		pages:  make([][]*Page, CHUNK_SIZE/store.conf.PageSize),
		errors: make(chan error, CHUNK_SIZE/store.conf.PageSize),
	}
}

func (c *wChunk) SetID(id uint64) {
	c.id = id
}

func (c *wChunk) WriteAt(p []byte, off int64) (n int, err error) {
	if int(off)+len(p) > CHUNK_SIZE {
		return 0, fmt.Errorf("write out of chunk boudary: %d > %d", int(off)+len(p), CHUNK_SIZE)
	}
	if off < int64(c.uploaded) {
		return 0, fmt.Errorf("Cannot overwrite uploaded block: %d < %d", off, c.uploaded)
	}

	// Fill previous blocks with zeros
	if c.length < int(off) {
		zeros := make([]byte, int(off)-c.length)
		c.WriteAt(zeros, int64(c.length))
	}

	for n < len(p) {
		indx := c.index(int(off) + n)
		boff := (int(off) + n) % c.store.conf.PageSize
		var bs = BLOCK_SIZE
		if indx > 0 {
			bs = c.store.conf.PageSize
		}
		bi := boff / bs
		bo := boff % bs
		var page *Page
		if bi < len(c.pages[indx]) {
			page = c.pages[indx][bi]
		} else {
			page = allocPage(bs)
			page.Data = page.Data[:0]
			c.pages[indx] = append(c.pages[indx], page)
		}
		left := len(p) - n
		if bo+left > bs {
			page.Data = page.Data[:bs]
		} else if len(page.Data) < bo+left {
			page.Data = page.Data[:bo+left]
		}
		n += copy(page.Data[bo:], p[n:])
	}
	if int(off)+n > c.length {
		c.length = int(off) + n
	}
	return n, nil
}

func withTimeout(f func() error, timeout time.Duration) error {
	var done = make(chan int, 1)
	var t = time.NewTimer(timeout)
	var err error
	go func() {
		err = f()
		done <- 1
	}()
	select {
	case <-done:
		t.Stop()
	case <-t.C:
		err = fmt.Errorf("timeout after %s", timeout)
	}
	return err
}

func (c *wChunk) put(key string, p *Page) error {
	p.Acquire()
	return withTimeout(func() error {
		defer p.Release()
		st := time.Now()
		err := c.store.storage.Put(key, bytes.NewReader(p.Data))
		used := time.Since(st)
		logger.Debugf("PUT %s (%s, %.3fs)", key, err, used.Seconds())
		if used > SlowRequest {
			logger.Infof("slow request: PUT %v (%s, %.3fs)", key, err, used.Seconds())
		}
		return err
	}, c.store.conf.PutTimeout)
}

func (c *wChunk) syncUpload(key string, block *Page) {
	blen := len(block.Data)
	bufSize := c.store.compressor.CompressBound(blen)
	var buf *Page
	if bufSize > blen {
		buf = NewOffPage(bufSize)
	} else {
		buf = block
		buf.Acquire()
	}
	n, err := c.store.compressor.Compress(buf.Data, block.Data)
	if err != nil {
		logger.Fatalf("compress chunk %v: %s", c.id, err)
		return
	}
	buf.Data = buf.Data[:n]
	if blen < c.store.conf.PageSize {
		// block will be freed after written into disk
		c.store.bcache.cache(key, block)
	}
	block.Release()

	c.store.currentUpload <- true
	defer func() {
		buf.Release()
		<-c.store.currentUpload
	}()

	try := 0
	for try <= 10 && c.uploadError == nil {
		err = c.put(key, buf)
		if err == nil {
			c.errors <- nil
			return
		}
		try++
		logger.Warnf("upload %s: %s (try %d)", key, err, try)
		time.Sleep(time.Second * time.Duration(try*try))
	}
	c.errors <- fmt.Errorf("upload block %s: %s (after %d tries)", key, err, try)
}

func (c *wChunk) asyncUpload(key string, block *Page, stagingPath string) {
	blockSize := len(block.Data)
	defer c.store.bcache.uploaded(key, blockSize)
	defer func() {
		<-c.store.currentUpload
	}()
	select {
	case c.store.currentUpload <- true:
	default:
		// release the memory and wait
		block.Release()
		c.store.pendingMutex.Lock()
		c.store.pendingKeys[key] = true
		c.store.pendingMutex.Unlock()
		defer func() {
			c.store.pendingMutex.Lock()
			delete(c.store.pendingKeys, key)
			c.store.pendingMutex.Unlock()
		}()

		logger.Debugf("wait to upload %s", key)
		c.store.currentUpload <- true

		// load from disk
		f, err := os.Open(stagingPath)
		if err != nil {
			c.store.pendingMutex.Lock()
			ok := c.store.pendingKeys[key]
			c.store.pendingMutex.Unlock()
			if ok {
				logger.Errorf("read stagging file %s: %s", stagingPath, err)
			} else {
				logger.Debugf("%s is not needed, drop it", key)
			}
			return
		}

		block = NewOffPage(blockSize)
		_, err = io.ReadFull(f, block.Data)
		f.Close()
		if err != nil {
			logger.Errorf("read stagging file %s: %s", stagingPath, err)
			block.Release()
			return
		}
	}
	bufSize := c.store.compressor.CompressBound(blockSize)
	var buf *Page
	if bufSize > blockSize {
		buf = NewOffPage(bufSize)
	} else {
		buf = block
		buf.Acquire()
	}
	n, err := c.store.compressor.Compress(buf.Data, block.Data)
	if err != nil {
		logger.Fatalf("compress chunk %v: %s", c.id, err)
		return
	}
	buf.Data = buf.Data[:n]
	block.Release()

	try := 0
	for c.uploadError == nil {
		err = c.put(key, buf)
		if err == nil {
			break
		}
		logger.Warnf("upload %s: %s (tried %d)", key, err, try)
		try++
		time.Sleep(time.Second * time.Duration(try))
	}
	buf.Release()
	os.Remove(stagingPath)
}

func (c *wChunk) upload(indx int) {
	blen := c.blockSize(indx)
	key := c.key(indx)
	pages := c.pages[indx]
	c.pages[indx] = nil
	c.pendings++

	go func() {
		var block *Page
		if len(pages) == 1 {
			block = pages[0]
		} else {
			block = NewOffPage(blen)
			var off int
			for _, b := range pages {
				off += copy(block.Data[off:], b.Data)
				freePage(b)
			}
			if off != blen {
				logger.Fatalf("block length does not match: %v != %v", off, blen)
			}
		}
		if c.store.conf.AsyncUpload {
			stagingPath, err := c.store.bcache.stage(key, block.Data, c.store.shouldCache(blen))
			if err != nil {
				logger.Warnf("write %s to disk: %s, upload it directly", stagingPath, err)
				c.syncUpload(key, block)
			} else {
				c.errors <- nil
				go c.asyncUpload(key, block, stagingPath)
			}
		} else {
			c.syncUpload(key, block)
		}
	}()
}

func (c *wChunk) ID() uint64 {
	return c.id
}

func (c *wChunk) Len() int {
	return c.length
}

func (c *wChunk) Bytes() []byte {
	if c.length > c.store.conf.PageSize {
		logger.Fatalf("get bytes from multi blocks")
	}
	var buf = make([]byte, 0, c.length)
	for _, pages := range c.pages {
		for _, p := range pages {
			buf = append(buf, p.Data...)
		}
	}
	return buf
}

func (c *wChunk) FlushTo(offset int) error {
	if offset < c.uploaded {
		logger.Fatalf("Invalid offset: %d < %d", offset, c.uploaded)
	}
	for i, block := range c.pages {
		start := i * c.store.conf.PageSize
		end := start + c.store.conf.PageSize
		if start >= c.uploaded && end <= offset {
			if block != nil {
				c.upload(i)
			}
			c.uploaded = end
		}
	}

	return nil
}

func (c *wChunk) Finish(length int) error {
	if c.length != length {
		return fmt.Errorf("Length mismatch: %v != %v", c.length, length)
	}

	n := (length-1)/c.store.conf.PageSize + 1
	if err := c.FlushTo(n * c.store.conf.PageSize); err != nil {
		return err
	}
	for i := 0; i < c.pendings; i++ {
		if err := <-c.errors; err != nil {
			c.uploadError = err
			return err
		}
	}
	return nil
}

func (c *wChunk) Abort() {
	for i := range c.pages {
		for _, b := range c.pages[i] {
			freePage(b)
		}
		c.pages[i] = nil
	}
}

// Config contains options for cachedStore
type Config struct {
	CacheDir       string
	CacheMode      os.FileMode
	CacheSize      int64
	FreeSpace      float32
	AutoCreate     bool
	Compress       string
	MaxUpload      int
	AsyncUpload    bool
	Partitions     int
	PageSize       int
	UploadLimit    int
	GetTimeout     time.Duration
	PutTimeout     time.Duration
	CacheGroup     string
	CacheGroupSize int
	CacheFullBlock bool
	BufferSize     int
	Readahead      int
	Prefetch       int
}

type RateLimiter interface {
	Wait(bytes int64)
}

type cachedStore struct {
	storage       object.ObjectStorage
	gcache        *groupcache.Group
	bcache        CacheManager
	fetcher       *prefetcher
	conf          Config
	group         *Group
	currentUpload chan bool
	pendingKeys   map[string]bool
	pendingMutex  sync.Mutex
	compressor    utils.Compressor
	seekable      bool
}

func (store *cachedStore) load(key string, page *Page, cache bool) (err error) {
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("recovered from %s", e)
		}
	}()

	err = errors.New("Not downloaded")
	var in io.ReadCloser
	tried := 0
	start := time.Now()
	// it will be retried outside
	for err != nil && tried < 2 {
		time.Sleep(time.Second * time.Duration(tried*tried))
		st := time.Now()
		in, err = store.storage.Get(key, 0, -1)
		used := time.Since(st)
		logger.Debugf("GET %s (%s, %.3fs)", key, err, used.Seconds())
		if used > SlowRequest {
			logger.Infof("slow request: GET %s (%s, %.3fs)", key, err, used.Seconds())
		}
		// try to recover once after failed to get 2 times
		if err != nil && tried == 1 && len(page.Data) < store.conf.PageSize {
			if e := recoverAppendedKey(store.storage, key, store.compressor, page.Data); e == nil {
				if cache {
					store.bcache.cache(key, page)
				}
				return nil
			}
		}
		tried++
	}
	if err != nil {
		return fmt.Errorf("get %s: %s", key, err)
	}
	needed := store.compressor.CompressBound(len(page.Data))
	var n int
	if needed > len(page.Data) {
		c := NewOffPage(needed)
		defer c.Release()
		var cn int
		cn, err = io.ReadFull(in, c.Data)
		in.Close()
		if err != nil && (cn == 0 || err != io.ErrUnexpectedEOF) {
			return err
		}
		n, err = store.compressor.Decompress(page.Data, c.Data[:cn])
	} else {
		n, err = io.ReadFull(in, page.Data)
	}
	if err != nil || n < len(page.Data) {
		return fmt.Errorf("read %s fully: %s (%d < %d) after %s (tried %d)", key, err, n, len(page.Data),
			time.Since(start), tried)
	}
	if cache {
		store.bcache.cache(key, page)
	}
	return nil
}

// NewCachedStore create a cached store.
func NewCachedStore(storage object.ObjectStorage, config Config) ChunkStore {
	compressor := utils.NewCompressor(config.Compress)
	if config.GetTimeout == 0 {
		config.GetTimeout = time.Second * 60
	}
	if config.PutTimeout == 0 {
		config.PutTimeout = time.Second * 60
	}
	store := &cachedStore{
		storage:       storage,
		conf:          config,
		currentUpload: make(chan bool, config.MaxUpload),
		compressor:    compressor,
		seekable:      compressor.CompressBound(0) == 0 && !strings.Contains(storage.String(), "(encrypted)"),
		bcache:        newCacheManager(&config),
		pendingKeys:   make(map[string]bool),
		group:         &Group{},
	}
	if config.CacheSize == 0 {
		config.Prefetch = 0 // disable prefetch if cache is disabled
	}
	store.fetcher = newPrefetcher(config.Prefetch, func(key string) {
		store.gcache.Get(nil, key, &blockSink{})
	})
	name := config.CacheGroup
	if name == "" {
		name = fmt.Sprintf("block-%p", store)
	}
	store.gcache = groupcache.GetGroup(name)
	if store.gcache == nil {
		store.gcache = groupcache.NewGroup(name, 32<<20, groupcache.GetterFunc(
			func(ctx groupcache.Context, key string, dest groupcache.Sink) (err error) {
				return withTimeout(func() error {
					if strings.Contains(key, "#") {
						key = key[:strings.Index(key, "#")]
					}
					var boff, limit int
					if strings.Contains(key, ",") {
						parts := strings.SplitN(key, ",", 3)
						key = parts[0]
						boff, _ = strconv.Atoi(parts[1])
						limit, _ = strconv.Atoi(parts[2])
					}
					size := parseObjOrigSize(key)
					if size == 0 || size > store.conf.PageSize {
						logger.Fatalf("Invalid key: %s", key)
					}
					if limit == 0 {
						limit = size
					}
					// the buffer will be kept in groupcache, can't be re-cycled.
					r, err := store.bcache.load(key)
					if err == nil {
						block := make([]byte, limit)
						n, err := r.ReadAt(block, int64(boff))
						r.Close()
						if err == nil {
							dest.SetBytes(block)
							return nil
						}
						if f, ok := r.(*os.File); ok {
							logger.Errorf("short chunk %s: %d < %d", key, n, size)
							os.Remove(f.Name())
						}
					}
					block := make([]byte, size)
					err = store.load(key, NewPage(block), true)
					if err == nil {
						dest.SetBytes(block[boff : boff+limit])
					}
					return err
				}, config.GetTimeout)
			}))
	}

	go store.uploadStaging()
	// TODO: Scan all the block with old style key
	return store
}

func (c *cachedStore) Seekable() bool {
	return c.seekable
}

func (c *cachedStore) shouldCache(size int) bool {
	return size < c.conf.PageSize || c.conf.CacheFullBlock
}

func parseObjOrigSize(key string) int {
	p := strings.LastIndexByte(key, '_')
	l, _ := strconv.Atoi(key[p+1:])
	return l
}

func recoverAppendedKey(store object.ObjectStorage, key string, compressor utils.Compressor, block []byte) error {
	prefix := key[:strings.LastIndexByte(key, '_')]
	objs, err := store.List(prefix, "", 1000)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		l := parseObjOrigSize(obj.Key)
		if l >= len(block) {
			in, err := store.Get(obj.Key, 0, -1)
			if err != nil {
				logger.Warnf("get %s: %s", obj.Key, err)
				continue
			}
			src, err := ioutil.ReadAll(in)
			in.Close()
			if err != nil {
				continue
			}
			p := NewOffPage(l)
			defer p.Release()
			n, err := compressor.Decompress(p.Data, src)
			if err != nil || n < len(block) {
				logger.Warnf("load %s: %s (%d < %d)", obj.Key, err, n, len(block))
				continue
			}
			copy(block, p.Data[:n])
			logger.Infof("recovered %s using %s", key, obj.Key)
			return nil
		}
	}
	return errors.New("not recoverable")
}

func (s *cachedStore) uploadStaging() {
	staging := s.bcache.scanStaging()
	for key, path := range staging {
		s.currentUpload <- true
		go func(key, stagingPath string) {
			defer func() {
				<-s.currentUpload
			}()
			block, err := ioutil.ReadFile(stagingPath)
			if err != nil {
				logger.Errorf("open %s: %s", stagingPath, err)
				return
			}
			buf := make([]byte, s.compressor.CompressBound(len(block)))
			n, err := s.compressor.Compress(buf, block)
			if err != nil {
				logger.Errorf("compress chunk %s: %s", stagingPath, err)
				return
			}
			compressed := buf[:n]

			if strings.Count(key, "_") == 1 {
				// add size at the end
				key = fmt.Sprintf("%s_%d", key, len(block))
			}
			try := 0
			for {
				err := s.storage.Put(key, bytes.NewReader(compressed))
				if err == nil {
					break
				}
				logger.Infof("upload %s: %s (try %d)", key, err, try)
				try++
				time.Sleep(time.Second * time.Duration(try*try))
			}
			s.bcache.uploaded(key, len(block))
			os.Remove(stagingPath)
		}(key, path)
	}
}

func (s *cachedStore) NewReader(chunkid uint64, length int) Reader {
	return chunkForRead(chunkid, length, s)
}

func (s *cachedStore) NewWriter(chunkid uint64) Writer {
	return chunkForWrite(chunkid, s)
}

func (s *cachedStore) Remove(chunkid uint64, length int) error {
	r := chunkForRead(chunkid, length, s)
	return r.Remove()
}

var _ ChunkStore = &cachedStore{}
