// Package memtable is the in-memory, sorted write buffer of the LSM tree,
// implemented as a skip list. It keeps every version of a key, ordered by
// key ascending and version descending.
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

// Memtable is a concurrency-safe sorted set of versioned entries.
type Memtable struct {
	mu         sync.RWMutex
	head       *node
	height     int
	rnd        uint64
	size       int
	count      int
	latestOnly bool
}

// New returns an empty memtable that keeps every version of a key.
func New() *Memtable {
	return &Memtable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rnd:    0x9E3779B97F4A7C15,
	}
}

// NewLatestOnly returns an empty memtable that keeps only the newest version
// of each key, replacing it in place on overwrite. Use it when no reader can
// ask for older versions (no history is kept): it keeps the memtable small
// under repeated overwrites of hot keys.
func NewLatestOnly() *Memtable {
	m := New()
	m.latestOnly = true
	return m
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

// findGE returns the first node at or after (key, version) in entry order.
// If prev is non-nil it is filled with the rightmost node before that
// position on every level.
func (m *Memtable) findGE(key []byte, version uint64, prev []*node) *node {
	x := m.head
	for lvl := m.height - 1; lvl >= 0; lvl-- {
		for nx := x.next[lvl]; nx != nil && base.Compare(nx.entry.Key, nx.entry.Version, key, version) < 0; nx = x.next[lvl] {
			x = nx
		}
		if prev != nil {
			prev[lvl] = x
		}
	}
	return x.next[0]
}

// Put inserts e. An entry with the same key and version is replaced, which
// makes the last of several writes to one key in a batch win. Key and Value
// are copied.
func (m *Memtable) Put(e base.Entry) {
	e.Key = bytes.Clone(e.Key)
	e.Value = bytes.Clone(e.Value)
	if e.Key == nil {
		e.Key = []byte{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var prev [maxHeight]*node
	if m.latestOnly {
		if n := m.findGE(e.Key, base.MaxVersion, nil); n != nil && bytes.Equal(n.entry.Key, e.Key) {
			if e.Version >= n.entry.Version {
				m.size += e.Size() - n.entry.Size()
				n.entry = e
			}
			return
		}
	}
	n := m.findGE(e.Key, e.Version, prev[:])
	if n != nil && n.entry.Version == e.Version && bytes.Equal(n.entry.Key, e.Key) {
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

// Get returns the newest entry for key with a version <= asOf, including
// tombstones.
func (m *Memtable) Get(key []byte, asOf uint64) (base.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := m.findGE(key, asOf, nil)
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

// Len returns the number of entries (versions) held.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// Versions returns an iterator over a snapshot of the versions of key alone,
// newest first. Unlike Iterator it copies nothing else.
func (m *Memtable) Versions(key []byte) base.Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var entries []base.Entry
	for n := m.findGE(key, base.MaxVersion, nil); n != nil && bytes.Equal(n.entry.Key, key); n = n.next[0] {
		entries = append(entries, n.entry)
	}
	return base.NewSliceIterator(entries)
}

// Iterator returns an iterator over a point-in-time snapshot of every
// version of every key >= start (nil means from the beginning).
func (m *Memtable) Iterator(start []byte) base.Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var entries []base.Entry
	for n := m.findGE(start, base.MaxVersion, nil); n != nil; n = n.next[0] {
		entries = append(entries, n.entry)
	}
	return base.NewSliceIterator(entries)
}
