package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	proto "google.golang.org/protobuf/proto"
)

const APPEND_FILENAME = ".append"
const CYCLE_PERIOD = 60

type FileCache struct {
	ProtoCache
	FileName string

	lock sync.RWMutex

	appendfile *os.File
}

func (self *FileCache) init() {
	self.mkdir()

	self.read()

	go self.loop()
}

func (self *FileCache) read() {
	self.lock.Lock()
	defer self.lock.Unlock()

	buffers, err := ioutil.ReadFile(self.FileName)
	if err == nil || len(buffers) > 0 {
		proto.Unmarshal(buffers, self)
	}
	if self.Datas == nil {
		self.Datas = make(map[string]*ProtoVal)
	}

	buffers, err = ioutil.ReadFile(self.FileName + APPEND_FILENAME)
	reader := bytes.NewReader(buffers)
	for {
		lenbytes := make([]byte, 4)
		n, err := reader.Read(lenbytes)
		if n <= 0 || err != nil {
			break
		}
		length := binary.LittleEndian.Uint32(lenbytes)
		buffers = make([]byte, length)
		n, err = reader.Read(buffers)
		if n <= 0 || err != nil {
			break
		}
		opt := &Opt{}
		proto.Unmarshal(buffers, opt)

		switch opt.Type {
		case File_Opt_put:
			self.put(opt.Key, unmarshal(opt.Data), opt.Timeout)
		case File_Opt_del:
			self.del(opt.Key)
		}
	}
}

func (self *FileCache) mkdir() {
	_, err := os.Stat(path.Dir(self.FileName))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path.Dir(self.FileName), os.ModePerm)
		}
	}
}

func (self *FileCache) loop() {
	for {
		func() {
			self.lock.Lock()
			defer self.lock.Unlock()
			delkey := []string{}
			now := time.Now().Unix()
			for k, v := range self.Datas {
				if v.Timeout > 0 && v.Timeout < now {
					delkey = append(delkey, k)
				}
			}
			for _, v := range delkey {
				delete(self.Datas, v)
			}
			self.clear_append()
			self.write()
		}()
		time.Sleep(time.Second * CYCLE_PERIOD)
	}
}

func (self *FileCache) append(o File_Opt, key string, timeout int64, data []byte) {
	var err error
	if self.appendfile == nil {
		_, err = os.Stat(self.FileName + APPEND_FILENAME)
		if err != nil {
			if os.IsNotExist(err) {
				self.appendfile, err = os.Create(self.FileName + APPEND_FILENAME)
			}
		} else {
			self.appendfile, err = os.OpenFile(self.FileName+APPEND_FILENAME, os.O_APPEND, 0666)
		}
	}
	if err == nil {
		lendata := make([]byte, 4)
		binary.LittleEndian.PutUint32(lendata, uint32(len(data)))
		self.appendfile.Write(lendata)
		self.appendfile.Write(data)
	} else {
		panic(err)
	}
}

func (self *FileCache) clear_append() {
	self.appendfile.Close()
	self.appendfile = nil
	ioutil.WriteFile(self.FileName+APPEND_FILENAME, []byte{}, 0600)
}

func (self *FileCache) write() {
	data, err := proto.Marshal(self)
	if err == nil {
		ioutil.WriteFile(self.FileName, data, 0600)
	} else {
		panic(err)
	}
}

func (self *FileCache) Put(key string, val interface{}, timeout int64) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.put(key, val, timeout)
}

func (self *FileCache) PutNX(key string, val interface{}, timeout int64) (bool, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.exist(key) {
		return false, nil
	} else {
		return true, self.put(key, val, timeout)
	}
}

func (self *FileCache) put(key string, val interface{}, timeout int64) error {
	if timeout > 0 {
		timeout += time.Now().Unix()
	}
	dataBytes := marshal(val)
	self.Datas[key] = &ProtoVal{
		Timeout: timeout,
		Data:    dataBytes,
	}
	self.append(File_Opt_put, key, timeout, dataBytes)
	return nil
}

func (self *FileCache) Exist(key string) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()

	return self.exist(key)
}

func (self *FileCache) exist(key string) bool {
	v, ok := self.Datas[key]
	if ok {
		if v.Timeout == 0 || v.Timeout >= time.Now().Unix() {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (self *FileCache) GetMulti(key ...string) ([]interface{}, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	ret := []interface{}{}
	for _, k := range key {
		v, err := self.get(k)
		if err == nil {
			ret = append(ret, v)
		}
	}
	return ret, nil
}

func (self *FileCache) Get(key string) (interface{}, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	return self.get(key)
}

func (self *FileCache) get(key string) (interface{}, error) {
	val, ok := self.Datas[key]
	if ok {
		if val.Timeout == 0 || val.Timeout >= time.Now().Unix() {
			return unmarshal(val.Data), nil
		} else {
			return nil, fmt.Errorf("data expire")
		}
	} else {
		return nil, fmt.Errorf("data is nil")
	}
}

func (self *FileCache) Keys(prefix_key string) []string {
	self.lock.RLock()
	defer self.lock.RUnlock()

	return self.keys(prefix_key)
}

func (self *FileCache) keys(prefix_key string) []string {
	prefix_key = "^" + strings.ReplaceAll(prefix_key, "*", `[\s\S]+`) + "$"
	ret := []string{}
	for k, val := range self.Datas {
		if matched, err := regexp.MatchString(prefix_key, k); err == nil && matched && (val.Timeout == 0 || val.Timeout >= time.Now().Unix()) {
			ret = append(ret, k)
		}
	}
	return ret
}

func (self *FileCache) Del(key string) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.del(key)
}

func (self *FileCache) del(key string) error {
	_, ok := self.Datas[key]
	if ok {
		delete(self.Datas, key)
		self.append(File_Opt_del, key, 0, nil)
		return nil
	} else {
		return fmt.Errorf("not exist key")
	}
}

func (self *FileCache) LPop(key string) (string, error) {
	return self.lPop(key)
}

func (self *FileCache) lPop(key string) (string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	vals, err := self.get(key)
	if err != nil {
		return "", err
	}
	if v, ok := vals.([][]byte); ok {
		if len(v) > 0 {
			ret := v[0]
			v = v[1:]
			err = self.put(key, v, 0)
			if err != nil {
				return "", err
			}
			i_ret := unmarshal(ret)
			switch i_ret.(type) {
			case []byte:
				return string(i_ret.([]byte)), nil
			case string:
				return string(i_ret.(string)), nil
			default:
				return "", fmt.Errorf("unknow data type")
			}
		} else {
			return "", fmt.Errorf("list length is zero")
		}
	} else {
		return "", fmt.Errorf("not is list")
	}
}

func (self *FileCache) RPush(key string, values ...interface{}) (uint64, error) {
	return self.rPush(key, values...)
}

func (self *FileCache) rPush(key string, values ...interface{}) (uint64, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.exist(key) {
		vals, err := self.get(key)
		if err != nil {
			return 0, err
		}
		if v, ok := vals.([][]byte); ok {
			for _, i_v := range values {
				dataBytes := marshal(i_v)
				v = append(v, dataBytes)
			}
			err = self.put(key, v, 0)
			if err != nil {
				return 0, err
			}
			return uint64(len(values)), nil
		} else {
			return 0, fmt.Errorf("Not List Key")
		}
	} else {
		err := self.put(key, values, 0)
		if err != nil {
			return 0, err
		}
		return uint64(len(values)), nil
	}
}

func (self *FileCache) Incr(key string) (int64, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.incr(key)
}

func (self *FileCache) incr(key string) (int64, error) {
	var int_param int64
	param, err := self.get(key)
	if err == nil {
		int_param = param.(int64)
	} else {
		int_param = 0
	}
	int_param++
	return int_param, self.put(key, int_param, 0)
}

func (self *FileCache) Decr(key string) (int64, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.decr(key)
}

func (self *FileCache) decr(key string) (int64, error) {
	var int_param int64
	param, err := self.get(key)
	if err == nil {
		int_param = param.(int64)
	} else {
		int_param = 0
	}
	int_param--
	return int_param, self.put(key, int_param, 0)
}

func (self *FileCache) getDel(key string) (string, error) {
	data, err := self.get(key)
	if err == nil {
		self.del(key)
	}
	return fmt.Sprintf("%v", data), err
}

func (self *FileCache) GetDel(key string) (string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.getDel(key)
}

func (self *FileCache) LockRun(key string, f func()) {
	renewclose := make(chan struct{})
	// 续约
	renewal := func() {
		defer self.Del(key)
		timer := time.NewTimer(time.Second * 10)
		for {
			select {
			case <-renewclose:
				timer.Stop()
				return
			case <-timer.C:
				timer.Reset(time.Second * 10)
				self.Put(key, true, 20)
			}
		}
	}
	// 注册key
	registerkey := func() bool {
		if ok, _ := self.PutNX(key, true, 20); ok {
			// 开启自动续约
			go renewal()
			return true
		}
		return false
	}
	for !registerkey() {
		// 延时 0.1 s
		time.Sleep(time.Millisecond * 100)
	}
	defer close(renewclose)

	f()
}

func newFile(filename string) *FileCache {
	file := &FileCache{FileName: filename}
	file.init()
	return file
}
