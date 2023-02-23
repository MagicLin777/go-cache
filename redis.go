package cache

import (
	"context"
	"time"

	redis "github.com/go-redis/redis/v8"
)

type RedisCache struct {
	redis *redis.Client
}

func (self *RedisCache) init(addr string, pwd string) {
	self.redis = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
	})
}

func (self *RedisCache) Put(key string, val interface{}, timeout int64) error {
	return self.put(key, val, timeout)
}

func (self *RedisCache) put(key string, val interface{}, timeout int64) error {
	cmd := self.redis.Set(context.Background(), key, marshal(val), time.Duration(timeout)*time.Second)
	return self.error(cmd.Err())
}

func (self *RedisCache) PutNX(key string, val interface{}, timeout int64) (bool, error) {
	return self.putNX(key, val, timeout)
}

func (self *RedisCache) putNX(key string, val interface{}, timeout int64) (bool, error) {
	cmd := self.redis.SetNX(context.Background(), key, val, time.Duration(timeout)*time.Second)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisCache) keys(key string) []string {
	cmd := self.redis.Keys(context.Background(), key)
	return cmd.Val()
}

func (self *RedisCache) Keys(key string) []string {
	return self.keys(key)
}

func (self *RedisCache) incr(key string) (int64, error) {
	cmd := self.redis.Incr(context.Background(), key)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisCache) Incr(key string) (int64, error) {
	return self.incr(key)
}

func (self *RedisCache) decr(key string) (int64, error) {
	cmd := self.redis.Decr(context.Background(), key)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisCache) Decr(key string) (int64, error) {
	return self.incr(key)
}

func (self *RedisCache) getDel(key string) (string, error) {
	cmd := self.redis.GetDel(context.Background(), key)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisCache) GetDel(key string) (string, error) {
	return self.getDel(key)
}

func (self *RedisCache) LPop(key string) (string, error) {
	return self.lPop(key)
}

func (self *RedisCache) lPop(key string) (string, error) {
	cmd := self.redis.LPop(context.Background(), key)
	return cmd.String(), self.error(cmd.Err())
}

func (self *RedisCache) RPush(key string, values ...interface{}) (uint64, error) {
	return self.rPush(key, values...)
}

func (self *RedisCache) rPush(key string, values ...interface{}) (uint64, error) {
	cmd := self.redis.RPush(context.Background(), key, values...)
	i, e := cmd.Uint64()
	return i, self.error(e)
}

func (self *RedisCache) Exist(key string) bool {
	return self.exist(key)
}

func (self *RedisCache) exist(keys string) bool {
	cmd := self.redis.Exists(context.Background(), keys)
	return cmd.Val() > 0
}

func (self *RedisCache) GetMulti(key ...string) ([]interface{}, error) {
	ret := []interface{}{}
	for _, k := range key {
		v, err := self.get(k)
		if err == nil {
			ret = append(ret, v)
		}
	}
	return ret, nil
}

func (self *RedisCache) Get(key string) (interface{}, error) {
	return self.get(key)
}

func (self *RedisCache) get(key string) (interface{}, error) {
	cmd := self.redis.Get(context.Background(), key)
	buffer, err := cmd.Bytes()
	return unmarshal(buffer), self.error(err)
}

func (self *RedisCache) Del(key string) error {
	return self.del(key)
}

func (self *RedisCache) error(err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == redis.Nil.Error() {
		return nil
	}
	return err
}

func (self *RedisCache) del(key string) error {
	cmd := self.redis.Del(context.Background(), key)
	return self.error(cmd.Err())
}

func (self *RedisCache) LockRun(key string, f func()) {
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
		if ok, _ := self.putNX(key, true, 20); ok {
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

func newRedis(addr string, pwd string) *RedisCache {
	r := &RedisCache{}
	r.init(addr, pwd)
	return r
}
