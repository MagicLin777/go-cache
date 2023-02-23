# go-cache

支持 文件/内存/redis/redis-cluster 的缓存包

## 初衷

当我想使用一个文件缓存，但是却给我创建出无数的文件，无限加大了文件系统的负荷，并且使得我更加不好管理缓存文件。由此，为了方便我做本地缓存，我将缓存信息放到同一个文件中，相当于 redis 的 append模式集成在应用本身。



## 安装

```
go get github.com/magiclin777/go-cache
```



## 例子

```
func main() {
	_cache := cache.NewCache(cache.CACHEMODE_FILE, "./cache.dat", "", nil, "")
	err := _cache.Put("test", "test1", 0)
	fmt.Println(err)

	var v interface{}
	for i := 0; i < 100000; i++ {
		v, err = _cache.Get("test")
	}
	fmt.Println(v, err)
}
```



## 测试

````
=== RUN   TestFileCache
    cache_test.go:8: <nil>
    cache_test.go:14: test1 <nil>
--- PASS: TestFileCache (1.41s)
=== RUN   TestMemoryCache
    cache_test.go:20: <nil>
    cache_test.go:26: test1 <nil>
--- PASS: TestMemoryCache (0.00s)
PASS
ok      github.com/magiclin777/go-cache 2.012s
````

