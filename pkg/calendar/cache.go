package calendar

import (
	"container/list"
	"sync"
	"time"
)

// executionCache provides caching for calculated execution times
type executionCache struct {
	cache      map[string]*cacheEntry
	recency    *list.List
	ttl        time.Duration
	maxEntries int
	mu         sync.Mutex
	stopChan   chan struct{}
	doneChan   chan struct{}
	stopOnce   sync.Once
}

type cacheEntry struct {
	times     []time.Time
	createdAt time.Time
	element   *list.Element
}

// newExecutionCache creates a new execution cache with the specified TTL
func newExecutionCache(ttl time.Duration, maxEntries int, cleanupInterval time.Duration) *executionCache {
	cache := &executionCache{
		cache:      make(map[string]*cacheEntry),
		recency:    list.New(),
		ttl:        ttl,
		maxEntries: maxEntries,
		stopChan:   make(chan struct{}),
		doneChan:   make(chan struct{}),
	}

	// Start cleanup goroutine
	go cache.cleanup(cleanupInterval)

	return cache
}

// get retrieves cached execution times if they exist and haven't expired
func (c *executionCache) get(key string) []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[key]
	if !exists {
		return nil
	}

	// Check if entry has expired
	if time.Since(entry.createdAt) > c.ttl {
		c.removeEntry(key, entry)
		return nil
	}
	c.recency.MoveToFront(entry.element)

	// Return a copy to prevent modification
	result := make([]time.Time, len(entry.times))
	copy(result, entry.times)
	return result
}

// set stores execution times in the cache
func (c *executionCache) set(key string, times []time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	c.cleanupExpiredLocked(now)
	timesCopy := append([]time.Time(nil), times...)
	if entry, exists := c.cache[key]; exists {
		entry.times = timesCopy
		entry.createdAt = now
		c.recency.MoveToFront(entry.element)
		return
	}
	element := c.recency.PushFront(key)
	c.cache[key] = &cacheEntry{times: timesCopy, createdAt: now, element: element}
	for len(c.cache) > c.maxEntries {
		oldest := c.recency.Back()
		if oldest == nil {
			break
		}
		oldestKey := oldest.Value.(string)
		c.removeEntry(oldestKey, c.cache[oldestKey])
	}
}

// clear removes all entries from the cache
func (c *executionCache) clear() { //nolint:unused
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*cacheEntry)
	c.recency.Init()
}

// size returns the number of entries in the cache
func (c *executionCache) size() int { //nolint:unused
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.cache)
}

// cleanup periodically removes expired entries
func (c *executionCache) cleanup(interval time.Duration) {
	defer close(c.doneChan)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopChan:
			return
		}
	}
}

// cleanupExpired removes expired entries from the cache
func (c *executionCache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredLocked(time.Now())
}

func (c *executionCache) cleanupExpiredLocked(now time.Time) {
	for key, entry := range c.cache {
		if now.Sub(entry.createdAt) > c.ttl {
			c.removeEntry(key, entry)
		}
	}
}

func (c *executionCache) close() {
	c.stopOnce.Do(func() { close(c.stopChan) })
	<-c.doneChan
}

func (c *executionCache) removeEntry(key string, entry *cacheEntry) {
	delete(c.cache, key)
	if entry != nil && entry.element != nil {
		c.recency.Remove(entry.element)
	}
}
