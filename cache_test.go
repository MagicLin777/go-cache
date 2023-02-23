package cache

import "testing"

func TestFileCache(t *testing.T) {
	_cache := NewCache(CACHEMODE_FILE, "./cache.dat", "", nil, "")
	err := _cache.Put("test", "test1", 0)
	t.Log(err)

	var v interface{}
	for i := 0; i < 100000; i++ {
		v, err = _cache.Get("test")
	}
	t.Log(v, err)
}

func TestMemoryCache(t *testing.T) {
	_cache := NewCache(CACHEMODE_MEMORY, "", "", nil, "")
	err := _cache.Put("test", "test1", 0)
	t.Log(err)

	var v interface{}
	for i := 0; i < 100000; i++ {
		v, err = _cache.Get("test")
	}
	t.Log(v, err)
}
