package calendar

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecutionCacheEvictsLeastRecentlyUsedEntry(t *testing.T) {
	cache := newExecutionCache(time.Hour, 2, time.Hour)
	t.Cleanup(cache.close)
	now := time.Now()
	cache.set("a", []time.Time{now})
	cache.set("b", []time.Time{now.Add(time.Minute)})
	require.NotNil(t, cache.get("a"))

	cache.set("c", []time.Time{now.Add(2 * time.Minute)})
	require.Equal(t, 2, cache.size())
	require.Nil(t, cache.get("b"))
	require.NotNil(t, cache.get("a"))
	require.NotNil(t, cache.get("c"))
}

func TestExecutionCacheExpiresEntriesAndReturnsCopies(t *testing.T) {
	cache := newExecutionCache(time.Millisecond, 1, time.Hour)
	t.Cleanup(cache.close)
	original := []time.Time{time.Now()}
	cache.set("key", original)
	got := cache.get("key")
	require.Equal(t, original, got)
	got[0] = time.Time{}
	require.Equal(t, original, cache.get("key"))

	time.Sleep(2 * time.Millisecond)
	require.Nil(t, cache.get("key"))
	require.Zero(t, cache.size())
}

func TestDefaultEngineUsesCacheConfigurationAndClosesIdempotently(t *testing.T) {
	engine := NewDefaultEngine(WithCaching(&CacheConfig{Enabled: true, TTL: time.Hour, MaxEntries: 1, CleanupInterval: time.Hour}))
	require.NotNil(t, engine.cache)
	require.Equal(t, 1, engine.cache.maxEntries)
	engine.Close()
	engine.Close()

	defaults := NewDefaultEngine(WithCaching(&CacheConfig{Enabled: true}))
	require.Equal(t, 24*time.Hour, defaults.cache.ttl)
	require.Equal(t, 10000, defaults.cache.maxEntries)
	require.Equal(t, 12*time.Hour, defaults.config.CacheConfig.CleanupInterval)
	defaults.Close()

	disabled := NewDefaultEngine(WithCaching(&CacheConfig{}))
	require.Nil(t, disabled.cache)
}

func TestExecutionCacheConcurrentAccessRemainsBounded(t *testing.T) {
	cache := newExecutionCache(time.Hour, 8, time.Hour)
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				key := fmt.Sprintf("%d-%d", worker, iteration)
				cache.set(key, []time.Time{time.Now()})
				_ = cache.get(key)
				if iteration%25 == 0 {
					cache.clear()
				}
			}
		}(worker)
	}
	var closers sync.WaitGroup
	closers.Add(2)
	go func() {
		defer closers.Done()
		cache.close()
	}()
	go func() {
		defer closers.Done()
		cache.close()
	}()
	workers.Wait()
	closers.Wait()
	require.LessOrEqual(t, cache.size(), 8)
}
