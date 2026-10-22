package cache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestGetPut(t *testing.T) {
	c := New(1 << 20)
	if _, ok := c.Get(1, 0); ok {
		t.Fatal("hit on empty cache")
	}
	c.Put(1, 0, []byte("block"))
	if v, ok := c.Get(1, 0); !ok || string(v) != "block" {
		t.Fatalf("Get = %q, %v", v, ok)
	}
	if _, ok := c.Get(2, 0); ok {
		t.Fatal("blocks of different tables collided")
	}
	hits, misses, size := c.Stats()
	if hits != 1 || misses != 2 || size == 0 {
		t.Fatalf("Stats = %d hits, %d misses, %d bytes", hits, misses, size)
	}
}

func TestEvictsLeastRecentlyUsedWithinCapacity(t *testing.T) {
	const capacity = 64 << 10
	c := New(capacity)
	block := bytes.Repeat([]byte("x"), 1024)
	for i := 0; i < 1000; i++ {
		c.Put(1, i, block)
		c.Get(1, 0) // keep block 0 hot
	}
	if _, _, size := c.Stats(); size > capacity {
		t.Fatalf("cache holds %d bytes, capacity %d", size, capacity)
	}
	if _, ok := c.Get(1, 0); !ok {
		t.Fatal("hot block was evicted")
	}
	if _, ok := c.Get(1, 1); ok {
		t.Fatal("cold early block survived")
	}
	if _, ok := c.Get(1, 999); !ok {
		t.Fatal("most recent block was evicted")
	}
}

func TestOversizedBlockIsNotCached(t *testing.T) {
	c := New(numShards * 100)
	c.Put(1, 0, make([]byte, 1000))
	if _, ok := c.Get(1, 0); ok {
		t.Fatal("block larger than a shard was cached")
	}
}

func TestConcurrent(t *testing.T) {
	c := New(256 << 10)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				b := (i * (g + 1)) % 500
				if v, ok := c.Get(uint64(g%3), b); ok && string(v) != fmt.Sprint(g%3, b) {
					t.Errorf("wrong block contents %q", v)
				}
				c.Put(uint64(g%3), b, []byte(fmt.Sprint(g%3, b)))
			}
		}(g)
	}
	wg.Wait()
}
