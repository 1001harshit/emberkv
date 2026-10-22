// Package cache is a sharded LRU cache for SSTable data blocks.
//
// SSTables are immutable, so a cached block can never go stale: it is keyed
// by (table id, block number) and table ids are never reused. Blocks of a
// deleted table are simply evicted as they age out.
package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

const numShards = 16

type key struct {
	table uint64
	block uint32
}

type entry struct {
	k key
	v []byte
}

type shard struct {
	mu       sync.Mutex
	capacity int64
	size     int64
	items    map[key]*list.Element
	lru      list.List // front = most recently used
}

// Cache maps (table, block) to verified block contents. It is safe for
// concurrent use. Cached slices are shared and must not be modified.
type Cache struct {
	shards [numShards]shard
	hits   atomic.Uint64
	misses atomic.Uint64
}

// New returns a cache holding up to capacity bytes of block data.
func New(capacity int64) *Cache {
	c := &Cache{}
	for i := range c.shards {
		c.shards[i].capacity = max(capacity/numShards, 1)
		c.shards[i].items = make(map[key]*list.Element)
	}
	return c
}

func (c *Cache) shard(k key) *shard {
	h := k.table*0x9E3779B97F4A7C15 ^ uint64(k.block)*0xC2B2AE3D27D4EB4F
	return &c.shards[(h>>32)%numShards]
}

// Get returns the cached block, if present.
func (c *Cache) Get(table uint64, block int) ([]byte, bool) {
	k := key{table, uint32(block)}
	s := c.shard(k)
	s.mu.Lock()
	el, ok := s.items[k]
	if ok {
		s.lru.MoveToFront(el)
	}
	s.mu.Unlock()
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return el.Value.(*entry).v, true
}

// Put stores a block, evicting the least recently used blocks of its shard
// to stay within capacity.
func (c *Cache) Put(table uint64, block int, v []byte) {
	k := key{table, uint32(block)}
	s := c.shard(k)
	size := int64(len(v)) + 64 // rough per-entry overhead
	if size > s.capacity {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[k]; ok {
		s.lru.MoveToFront(el)
		return
	}
	s.items[k] = s.lru.PushFront(&entry{k, v})
	s.size += size
	for s.size > s.capacity {
		old := s.lru.Back()
		e := old.Value.(*entry)
		s.lru.Remove(old)
		delete(s.items, e.k)
		s.size -= int64(len(e.v)) + 64
	}
}

// Stats reports hits, misses and the bytes currently cached.
func (c *Cache) Stats() (hits, misses uint64, bytes int64) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		bytes += s.size
		s.mu.Unlock()
	}
	return c.hits.Load(), c.misses.Load(), bytes
}
