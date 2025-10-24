package calendar

import (
	"sync"
	"time"
)

// executionCache provides caching for calculated execution times
type executionCache struct {
	cache map[string]*cacheEntry
	ttl   time.Duration
	mu    sync.RWMutex
}

type cacheEntry struct {
	times     []time.Time
	createdAt time.Time
}

// newExecutionCache creates a new execution cache with the specified TTL
func newExecutionCache(ttl time.Duration) *executionCache {
	cache := &executionCache{
		cache: make(map[string]*cacheEntry),
		ttl:   ttl,
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

// get retrieves cached execution times if they exist and haven't expired
func (c *executionCache) get(key string) []time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.cache[key]
	if !exists {
		return nil
	}

	// Check if entry has expired
	if time.Since(entry.createdAt) > c.ttl {
		return nil
	}

	// Return a copy to prevent modification
	result := make([]time.Time, len(entry.times))
	copy(result, entry.times)
	return result
}

// set stores execution times in the cache
func (c *executionCache) set(key string, times []time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a copy to prevent modification
	timesCopy := make([]time.Time, len(times))
	copy(timesCopy, times)

	c.cache[key] = &cacheEntry{
		times:     timesCopy,
		createdAt: time.Now(),
	}
}

// clear removes all entries from the cache
func (c *executionCache) clear() { //nolint:unused
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*cacheEntry)
}

// size returns the number of entries in the cache
func (c *executionCache) size() int { //nolint:unused
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.cache)
}

// cleanup periodically removes expired entries
func (c *executionCache) cleanup() {
	ticker := time.NewTicker(c.ttl / 2) // Cleanup twice per TTL period
	defer ticker.Stop()

	for range ticker.C {
		c.cleanupExpired()
	}
}

// cleanupExpired removes expired entries from the cache
func (c *executionCache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.cache {
		if now.Sub(entry.createdAt) > c.ttl {
			delete(c.cache, key)
		}
	}
}
