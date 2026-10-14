// Package memtable is the in-memory, sorted write buffer of the LSM tree,
// implemented as a skip list.
package memtable

import (
	"bytes"
	"sync"

	"github.com/1001harshit/emberkv/internal/base"
)

const maxHeight = 12

type node struct {
	entry base.Entry
	next  []*node
}

// Memtable is a concurrency-safe sorted map from key to its latest entry.
type Memtable struct {
	mu     sync.RWMutex
	head   *node
	height int
	rnd    uint64
	size   int
	count  int
}

// New returns an empty memtable.
func New() *Memtable {
	return &Memtable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rnd:    0x9E3779B97F4A7C15,
	}
}

// randomHeight returns a height with P(h) = 1/4^(h-1), via xorshift.
func (m *Memtable) randomHeight() int {
	h := 1
	for h < maxHeight {
		m.rnd ^= m.rnd << 13
		m.rnd ^= m.rnd >> 7
		m.rnd ^= m.rnd << 17
		if m.rnd&3 != 0 {
			break
		}
		h++
	}
	return h
}

// findGE returns the first node with key >= key. If prev is non-nil it is
// filled with the rightmost node before that position on every level.
func (m *Memtable) findGE(key []byte, prev []*node) *node {
	x := m.head
	for lvl := m.height - 1; lvl >= 0; lvl-- {
		for nx := x.next[lvl]; nx != nil && bytes.Compare(nx.entry.Key, key) < 0; nx = x.next[lvl] {
			x = nx
		}
		if prev != nil {
			prev[lvl] = x
		}
	}
	return x.next[0]
}

// Put inserts or replaces the entry for e.Key. Key and Value are copied.
func (m *Memtable) Put(e base.Entry) {
	e.Key = bytes.Clone(e.Key)
	e.Value = bytes.Clone(e.Value)
	if e.Key == nil {
		e.Key = []byte{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var prev [maxHeight]*node
	n := m.findGE(e.Key, prev[:])
	if n != nil && bytes.Equal(n.entry.Key, e.Key) {
		m.size += e.Size() - n.entry.Size()
		n.entry = e
		return
	}

	h := m.randomHeight()
	if h > m.height {
		for i := m.height; i < h; i++ {
			prev[i] = m.head
		}
		m.height = h
	}
	nn := &node{entry: e, next: make([]*node, h)}
	for i := 0; i < h; i++ {
		nn.next[i] = prev[i].next[i]
		prev[i].next[i] = nn
	}
	m.size += e.Size()
	m.count++
}

// Get returns the entry stored for key, including tombstones.
func (m *Memtable) Get(key []byte) (base.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := m.findGE(key, nil)
	if n != nil && bytes.Equal(n.entry.Key, key) {
		return n.entry, true
	}
	return base.Entry{}, false
}

// Size returns the approximate number of bytes held.
func (m *Memtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Len returns the number of distinct keys held.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// Iterator returns an iterator over a point-in-time snapshot of all entries
// with key >= start (nil means from the beginning).
func (m *Memtable) Iterator(start []byte) base.Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var entries []base.Entry
	for n := m.findGE(start, nil); n != nil; n = n.next[0] {
		entries = append(entries, n.entry)
	}
	return base.NewSliceIterator(entries)
}
