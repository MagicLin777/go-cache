package cache

import (
	"bytes"
	"encoding/gob"
)

func init() {
	gob.Register([]interface{}{})
	gob.Register(map[int]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register(map[interface{}]interface{}{})
	gob.Register(map[string]string{})
	gob.Register(map[int]string{})
	gob.Register(map[int]int{})
	gob.Register(map[int]int64{})
	gob.Register([][]byte{})
}

type CacheInterface interface {
	Incr(key string) (int64, error)
	Decr(key string) (int64, error)
	Exist(key string) bool
	Get(key string) (interface{}, error)
	GetDel(key string) (string, error)
	GetMulti(key ...string) ([]interface{}, error)
	Put(key string, val interface{}, timeout int64) error
	PutNX(key string, val interface{}, timeout int64) (bool, error)
	Del(string) error
	LockRun(key string, f func())
	Keys(key string) []string
	LPop(key string) (string, error)
	RPush(key string, values ...interface{}) (uint64, error)
}

type cacheData struct {
	timeout int64
	data    interface{}
}

type cacheval struct {
	Data interface{}
}

type CACHEMODE int

const (
	CACHEMODE_FILE         = CACHEMODE(1)
	CACHEMODE_MEMORY       = CACHEMODE(2)
	CACHEMODE_REDIS        = CACHEMODE(3)
	CACHEMODE_REDISCLUSTER = CACHEMODE(4)
)

func marshal(data interface{}) []byte {
	var result bytes.Buffer
	encoder := gob.NewEncoder(&result)
	encoder.Encode(&cacheval{Data: data})
	dataBytes := result.Bytes()
	return dataBytes
}

func unmarshal(databytes []byte) interface{} {
	var val cacheval
	decoder := gob.NewDecoder(bytes.NewReader(databytes))
	decoder.Decode(&val)
	return val.Data
}

func Marshal(data interface{}) []byte {
	if d, ok := data.(map[interface{}]interface{}); ok {
		for _, v := range d {
			gob.Register(v)
		}
	}
	var result bytes.Buffer
	encoder := gob.NewEncoder(&result)
	encoder.Encode(data)
	dataBytes := result.Bytes()
	return dataBytes
}

func Unmarshal(databytes []byte, val interface{}) error {
	decoder := gob.NewDecoder(bytes.NewReader(databytes))
	return decoder.Decode(val)
}

func NewCache(mode CACHEMODE, filename string, addr string, addrs []string, pwd string) CacheInterface {
	switch mode {
	case CACHEMODE_MEMORY:
		return newMemory()
	case CACHEMODE_FILE:
		return newFile(filename)
	case CACHEMODE_REDIS:
		return newRedis(addr, pwd)
	case CACHEMODE_REDISCLUSTER:
		return newRedisCluster(addrs, pwd)
	}
	return nil
}
