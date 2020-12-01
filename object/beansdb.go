package object

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

const connTimeout = time.Second
const reqTimeout = time.Second * 10

type beansdb struct {
	defaultObjectStorage
	addr     string
	ips      []string
	idleConn chan net.Conn // idle connections

	marker string
	keys   []string
}

func (s *beansdb) String() string {
	return fmt.Sprintf("beansdb://%s", s.addr)
}

func (s *beansdb) getConn() (c net.Conn, err error) {
	select {
	case c = <-s.idleConn:
	default:
		addr := s.ips[rand.Int()%len(s.ips)]
		c, err = net.DialTimeout("tcp", addr, connTimeout)
	}
	return
}

func (s *beansdb) releaseConn(conn net.Conn) {
	select {
	case s.idleConn <- conn:
	default:
		conn.Close()
	}
}

func isSpace(r rune) bool {
	return r == ' '
}

func splitKeys(s string) []string {
	// s[:len(s) - 2] remove "\r\n"
	return strings.FieldsFunc(s[:len(s)-2], isSpace)
}

type Response struct {
	Status string
	Msg    string
	items  map[string][]byte
}

func (resp *Response) Read(b *bufio.Reader) error {
	resp.items = make(map[string][]byte, 1)
	for {
		s, e := b.ReadString('\n')
		if e != nil {
			return e
		}
		parts := splitKeys(s)
		if len(parts) < 1 {
			return errors.New("invalid response")
		}

		resp.Status = parts[0]
		switch resp.Status {
		case "VALUE":
			if len(parts) < 4 {
				return errors.New("invalid response")
			}
			key := parts[1]
			length, e2 := strconv.Atoi(parts[3])
			if e2 != nil {
				return errors.New("invalid response")
			}
			body := make([]byte, length)
			if _, e = io.ReadFull(b, body); e != nil {
				return e
			}
			b.ReadByte() // \r
			b.ReadByte() // \n
			resp.items[key] = body
			continue

		case "END":
		case "STORED", "NOT_STORED", "DELETED", "NOT_FOUND":
		case "OK":

		case "ERROR", "SERVER_ERROR", "CLIENT_ERROR":
			if len(parts) > 1 {
				resp.Msg = parts[1]
			}
		default:
			return errors.New("unknown response:" + resp.Status)
		}
		break
	}
	return nil
}

func (s *beansdb) execute(cmd, key string, body []byte) (resp *Response, err error) {
	conn, err := s.getConn()
	if err != nil {
		return
	}

	w := bufio.NewWriter(conn)
	switch cmd {
	case "get", "delete":
		_, err = fmt.Fprintf(w, "%s %s\r\n", cmd, key)
	case "set":
		fmt.Fprintf(w, "%s %s 0 0 %d\r\n", cmd, key, len(body))
		if _, err = w.Write(body); err != nil {
			conn.Close()
			return
		}
		_, err = io.WriteString(w, "\r\n")
	}
	if err == nil {
		err = w.Flush()
	}
	if err != nil {
		conn.Close()
		return
	}

	resp = new(Response)
	reader := bufio.NewReader(conn)
	if err = resp.Read(reader); err != nil {
		conn.Close()
		return nil, err
	}

	s.releaseConn(conn)
	return
}

func (s *beansdb) executeWithTimeout(cmd, key string, body []byte) (resp *Response, err error) {
	done := make(chan bool, 1)
	go func() {
		resp, err = s.execute(cmd, key, body)
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(reqTimeout):
		err = errors.New("timeout")
	}
	return resp, err
}

func (s *beansdb) Get(key string, off, limit int64) (io.ReadCloser, error) {
	resp, err := s.executeWithTimeout("get", key, nil)
	if err != nil {
		return nil, err
	}
	d := resp.items[key]
	if d == nil {
		return nil, errors.New("not found")
	}
	if off >= 0 {
		if int(off) < len(d) {
			d = d[off:]
		} else {
			d = nil
		}
	}
	if limit >= 0 && int(limit) < len(d) {
		d = d[:limit]
	}
	return ioutil.NopCloser(bytes.NewBuffer(d)), nil
}

func (s *beansdb) Put(key string, in io.ReadSeeker) error {
	data, err := ioutil.ReadAll(in)
	if err != nil {
		return err
	}
	resp, err := s.executeWithTimeout("set", key, data)
	if err == nil && resp.Status != "STORED" {
		err = errors.New("not stored")
	}
	return err
}

func (s *beansdb) Exists(key string) error {
	r, err := s.Get("?"+key, 0, -1)
	if err == nil {
		d, _ := ioutil.ReadAll(r)
		if len(d) == 0 {
			err = errors.New("not found")
		} else {
			meta := strings.Split(string(d), " ")
			ver, _ := strconv.Atoi(meta[0])
			if ver < 0 {
				err = errors.New("deleted")
			}
		}
	}
	return err
}

func (s *beansdb) Delete(key string) error {
	resp, err := s.executeWithTimeout("delete", key, nil)
	if err == nil && resp.Status != "DELETED" {
		err = errors.New("not deleted")
	}
	return err
}

func (s *beansdb) list(pos, prefix, marker string) ([]string, error) {
	r, err := s.Get("@"+pos, 0, -1)
	if err != nil {
		return nil, err
	}
	d, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(d, []byte{'\n'})
	var all []string
	for _, line := range lines {
		if len(line) <= 1 {
			break
		}
		parts := bytes.Split(line, []byte{' '})
		key := string(parts[0])
		if len(key) == 2 && strings.HasSuffix(key, "/") {
			keys, err := s.list(pos+key[:1], prefix, marker)
			if err != nil {
				return nil, err
			}
			all = append(all, keys...)
		} else if strings.HasPrefix(key, prefix) && key > marker {
			ver, _ := strconv.Atoi(string(parts[2]))
			if ver > 0 {
				all = append(all, key)
			}
		}
	}
	return all, nil
}

func (s *beansdb) List(prefix, marker string, limit int64) ([]*Object, error) {
	var keys []string
	if marker != "" && marker == s.marker {
		keys = s.keys
	} else {
		var err error
		keys, err = s.list("", prefix, marker)
		if err != nil {
			return nil, err
		}
		sort.Sort(sort.StringSlice(keys))
	}
	if limit < int64(len(keys)) {
		s.keys = keys[limit:]
		s.marker = keys[limit-1]
		keys = keys[:limit]
	} else {
		s.marker = ""
		s.keys = nil
	}
	var objs = make([]*Object, len(keys))
	for i, key := range keys {
		var size, mtime int
		// r, _ := s.Get("?"+key, 0, -1)
		// d, _ := ioutil.ReadAll(r)
		// meta := strings.Split(string(d), " ")
		// size, _ = strconv.Atoi(meta[3])
		// mtime, _ = strconv.Atoi(meta[4])
		objs[i] = &Object{Key: key, Size: int64(size), Mtime: mtime}
	}
	return objs, nil
}

func (s *beansdb) resolve() {

}

func newBeansdb(addr, ak, sk string) ObjectStorage {
	s := new(beansdb)
	s.addr = addr
	s.ips = []string{addr}
	s.idleConn = make(chan net.Conn, 10)
	go s.resolve()
	return s
}

func init() {
	RegisterStorage("beansdb", newBeansdb)
}

var _ ObjectStorage = &beansdb{}
