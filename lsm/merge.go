package lsm

import (
	"bytes"
	"container/heap"

	"github.com/1001harshit/emberkv/internal/base"
)

// mergeIterator merges several sorted iterators into one. Sources are given
// newest first; when several hold the same key, only the newest is emitted.
type mergeIterator struct {
	h   mergeHeap
	cur base.Entry
	err error
}

type mergeSource struct {
	it   base.Iterator
	prio int // lower is newer
	e    *base.Entry
}

type mergeHeap []*mergeSource

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].e.Key, h[j].e.Key); c != 0 {
		return c < 0
	}
	return h[i].prio < h[j].prio
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*mergeSource)) }
func (h *mergeHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func newMergeIterator(iters []base.Iterator) *mergeIterator {
	m := &mergeIterator{}
	for i, it := range iters {
		if it.Next() {
			m.h = append(m.h, &mergeSource{it: it, prio: i, e: it.Entry()})
		} else if err := it.Err(); err != nil {
			m.err = err
		}
	}
	heap.Init(&m.h)
	return m
}

func (m *mergeIterator) advance() {
	top := m.h[0]
	if top.it.Next() {
		top.e = top.it.Entry()
		heap.Fix(&m.h, 0)
		return
	}
	if err := top.it.Err(); err != nil {
		m.err = err
	}
	heap.Pop(&m.h)
}

func (m *mergeIterator) Next() bool {
	if m.err != nil || len(m.h) == 0 {
		return false
	}
	m.cur = *m.h[0].e
	m.advance()
	// Skip older versions of the same key.
	for m.err == nil && len(m.h) > 0 && bytes.Equal(m.h[0].e.Key, m.cur.Key) {
		m.advance()
	}
	return m.err == nil
}

func (m *mergeIterator) Entry() *base.Entry { return &m.cur }
func (m *mergeIterator) Err() error         { return m.err }
