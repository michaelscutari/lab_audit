package main

import (
	"container/list"
	"sync"
)

// DirCache is an LRU cache for directory children.
// Thread-safe for concurrent access.
type DirCache struct {
	capacity int
	items    map[string]*list.Element
	order    *list.List
	mu       sync.RWMutex
}

type cacheEntry struct {
	path     string
	children []*Node
}

// NewDirCache creates a new LRU cache with the given capacity.
func NewDirCache(capacity int) *DirCache {
	return &DirCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves children for a path from the cache.
// Returns nil if not found. Moves accessed item to front.
func (c *DirCache) Get(path string) []*Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[path]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*cacheEntry).children
	}
	return nil
}

// Set stores children for a path in the cache.
// Evicts oldest entry if over capacity.
func (c *DirCache) Set(path string, children []*Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already exists, update and move to front
	if elem, ok := c.items[path]; ok {
		c.order.MoveToFront(elem)
		elem.Value.(*cacheEntry).children = children
		return
	}

	// Evict oldest if at capacity
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(*cacheEntry).path)
		}
	}

	// Add new entry at front
	entry := &cacheEntry{
		path:     path,
		children: children,
	}
	elem := c.order.PushFront(entry)
	c.items[path] = elem
}

// Clear removes all entries from the cache.
func (c *DirCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.order = list.New()
}

// Len returns the current number of cached directories.
func (c *DirCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order.Len()
}

// Contains checks if a path is in the cache without affecting LRU order.
func (c *DirCache) Contains(path string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.items[path]
	return ok
}

// Invalidate removes a specific path from the cache.
func (c *DirCache) Invalidate(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[path]; ok {
		c.order.Remove(elem)
		delete(c.items, path)
	}
}
