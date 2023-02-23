package cache

import (
	"context"
	"time"

	redis "github.com/go-redis/redis/v8"
)

type RedisClusterCache struct {
	redisCluster *redis.ClusterClient
}

func (self *RedisClusterCache) init(addrs []string, pwd string) {
	self.redisCluster = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: pwd,
	})
}

func (self *RedisClusterCache) Put(key string, val interface{}, timeout int64) error {
	return self.put(key, val, timeout)
}

func (self *RedisClusterCache) put(key string, val interface{}, timeout int64) error {
	cmd := self.redisCluster.Set(context.Background(), key, marshal(val), time.Duration(timeout)*time.Second)
	return self.error(cmd.Err())
}

func (self *RedisClusterCache) PutNX(key string, val interface{}, timeout int64) (bool, error) {
	return self.putNX(key, val, timeout)
}

func (self *RedisClusterCache) putNX(key string, val interface{}, timeout int64) (bool, error) {
	cmd := self.redisCluster.SetNX(context.Background(), key, val, time.Duration(timeout)*time.Second)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisClusterCache) Exist(key string) bool {
	return self.exist(key)
}

func (self *RedisClusterCache) exist(keys string) bool {
	cmd := self.redisCluster.Exists(context.Background(), keys)
	return cmd.Val() > 0
}

func (self *RedisClusterCache) GetMulti(key ...string) ([]interface{}, error) {
	ret := []interface{}{}
	for _, k := range key {
		v, err := self.get(k)
		if err == nil {
			ret = append(ret, v)
		}
	}
	return ret, nil
}

func (self *RedisClusterCache) Get(key string) (interface{}, error) {
	return self.get(key)
}

func (self *RedisClusterCache) get(key string) (interface{}, error) {
	cmd := self.redisCluster.Get(context.Background(), key)
	buffer, err := cmd.Bytes()
	return unmarshal(buffer), self.error(err)
}

func (self *RedisClusterCache) incr(key string) (int64, error) {
	cmd := self.redisCluster.Incr(context.Background(), key)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisClusterCache) Incr(key string) (int64, error) {
	return self.incr(key)
}

func (self *RedisClusterCache) decr(key string) (int64, error) {
	cmd := self.redisCluster.Decr(context.Background(), key)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisClusterCache) Decr(key string) (int64, error) {
	return self.incr(key)
}

func (self *RedisClusterCache) keys(key string) []string {
	cmd := self.redisCluster.Keys(context.Background(), key)
	return cmd.Val()
}

func (self *RedisClusterCache) Keys(key string) []string {
	return self.keys(key)
}

func (self *RedisClusterCache) getDel(key string) (string, error) {
	cmd := self.redisCluster.GetDel(context.Background(), key)
	return cmd.Val(), self.error(cmd.Err())
}

func (self *RedisClusterCache) GetDel(key string) (string, error) {
	return self.getDel(key)
}

func (self *RedisClusterCache) LPop(key string) (string, error) {
	return self.lPop(key)
}

func (self *RedisClusterCache) lPop(key string) (string, error) {
	cmd := self.redisCluster.LPop(context.Background(), key)
	return cmd.String(), self.error(cmd.Err())
}

func (self *RedisClusterCache) RPush(key string, values ...interface{}) (uint64, error) {
	return self.rPush(key, values...)
}

func (self *RedisClusterCache) rPush(key string, values ...interface{}) (uint64, error) {
	cmd := self.redisCluster.RPush(context.Background(), key, values...)
	i, e := cmd.Uint64()
	return i, self.error(e)
}

func (self *RedisClusterCache) Del(key string) error {
	return self.del(key)
}

func (self *RedisClusterCache) error(err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == redis.Nil.Error() {
		return nil
	}
	return err
}

func (self *RedisClusterCache) del(key string) error {
	cmd := self.redisCluster.Del(context.Background(), key)
	return self.error(cmd.Err())
}

func (self *RedisClusterCache) LockRun(key string, f func()) {
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

func newRedisCluster(addrs []string, pwd string) *RedisClusterCache {
	r := &RedisClusterCache{}
	r.init(addrs, pwd)
	return r
}
