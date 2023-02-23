package cache

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

type MemoryCache struct {
	sync.RWMutex
	mem map[string]*cacheData
}

func (self *MemoryCache) init() {
	self.mem = make(map[string]*cacheData)
	go self.loop()
}

func (self *MemoryCache) loop() {
	for {
		func() {
			self.Lock()
			defer self.Unlock()
			delkey := []string{}
			now := time.Now().Unix()
			for k, v := range self.mem {
				if v.timeout > 0 && v.timeout < now {
					delkey = append(delkey, k)
				}
			}
			for _, v := range delkey {
				delete(self.mem, v)
			}
		}()
		time.Sleep(time.Second * 120)
	}
}

func (self *MemoryCache) Put(key string, val interface{}, timeout int64) error {
	self.Lock()
	defer self.Unlock()

	return self.put(key, val, timeout)
}

func (self *MemoryCache) put(key string, val interface{}, timeout int64) error {
	if timeout > 0 {
		timeout += time.Now().Unix()
	}
	self.mem[key] = &cacheData{
		timeout: timeout,
		data:    val,
	}
	return nil
}

func (self *MemoryCache) PutNX(key string, val interface{}, timeout int64) (bool, error) {
	self.Lock()
	defer self.Unlock()

	if self.exist(key) {
		return false, nil
	} else {
		return true, self.put(key, val, timeout)
	}
}

func (self *MemoryCache) Exist(key string) bool {
	self.RLock()
	defer self.RUnlock()

	return self.exist(key)
}

func (self *MemoryCache) exist(key string) bool {
	v, ok := self.mem[key]
	if ok {
		if v.timeout == 0 || v.timeout >= time.Now().Unix() {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (self *MemoryCache) GetMulti(key ...string) ([]interface{}, error) {
	self.RLock()
	defer self.RUnlock()

	ret := []interface{}{}
	for _, k := range key {
		v, err := self.get(k)
		if err == nil {
			ret = append(ret, v)
		}
	}
	return ret, nil
}

func (self *MemoryCache) Get(key string) (interface{}, error) {
	self.RLock()
	defer self.RUnlock()

	return self.get(key)
}

func (self *MemoryCache) get(key string) (interface{}, error) {
	val, ok := self.mem[key]
	if ok {
		if val.timeout == 0 || val.timeout >= time.Now().Unix() {
			return val.data, nil
		} else {
			return nil, fmt.Errorf("data expire")
		}
	} else {
		return nil, fmt.Errorf("data is nil")
	}
}

func (self *MemoryCache) Keys(prefix_key string) []string {
	self.RLock()
	defer self.RUnlock()

	return self.keys(prefix_key)
}

func (self *MemoryCache) keys(prefix_key string) []string {
	prefix_key = strings.ReplaceAll(prefix_key, "*", `[\s\S]+`)
	ret := []string{}
	for k, val := range self.mem {
		if matched, err := regexp.MatchString(prefix_key, k); err == nil && matched && val.timeout == 0 || val.timeout >= time.Now().Unix() {
			ret = append(ret, prefix_key)
		}
	}
	return ret
}

func (self *MemoryCache) Incr(key string) (int64, error) {
	self.Lock()
	defer self.Unlock()

	return self.incr(key)
}

func (self *MemoryCache) incr(key string) (int64, error) {
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

func (self *MemoryCache) Decr(key string) (int64, error) {
	self.Lock()
	defer self.Unlock()

	return self.decr(key)
}

func (self *MemoryCache) decr(key string) (int64, error) {
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

func (self *MemoryCache) getDel(key string) (string, error) {
	data, err := self.get(key)
	if err == nil {
		self.del(key)
	}
	return fmt.Sprintf("%v", data), err
}

func (self *MemoryCache) GetDel(key string) (string, error) {
	self.Lock()
	defer self.Unlock()

	return self.getDel(key)
}

func (self *MemoryCache) LPop(key string) (string, error) {
	return self.lPop(key)
}

func (self *MemoryCache) lPop(key string) (string, error) {
	self.Lock()
	defer self.Unlock()

	vals, err := self.get(key)
	if err != nil {
		return "", err
	}
	if v, ok := vals.([]interface{}); ok {
		if len(v) > 0 {
			ret := v[0]
			v = v[1:]
			err = self.put(key, v, 0)
			if err != nil {
				return "", err
			}
			switch ret.(type) {
			case []byte:
				return string(ret.([]byte)), nil
			case string:
				return string(ret.(string)), nil
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

func (self *MemoryCache) RPush(key string, values ...interface{}) (uint64, error) {
	return self.rPush(key, values...)
}

func (self *MemoryCache) rPush(key string, values ...interface{}) (uint64, error) {
	self.Lock()
	defer self.Unlock()

	if self.exist(key) {
		vals, err := self.get(key)
		if err != nil {
			return 0, err
		}
		if v, ok := vals.([]interface{}); ok {
			v = append(v, values...)
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

func (self *MemoryCache) Del(key string) error {
	self.Lock()
	defer self.Unlock()

	return self.del(key)
}

func (self *MemoryCache) del(key string) error {
	_, ok := self.mem[key]
	if ok {
		delete(self.mem, key)
		return nil
	} else {
		return fmt.Errorf("not exist key")
	}
}

func (self *MemoryCache) LockRun(key string, f func()) {
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

func newMemory() *MemoryCache {
	m := &MemoryCache{}
	m.init()
	return m
}
