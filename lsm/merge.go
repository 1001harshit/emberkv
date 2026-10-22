package lsm

import (
	"bytes"
	"container/heap"

	"github.com/1001harshit/emberkv/internal/base"
)

// mergeIterator merges several iterators, each in entry order (key
// ascending, version descending), into one stream in entry order. It yields
// every version; only exact duplicates of a (key, version) are collapsed,
// keeping the copy from the source given first.
type mergeIterator struct {
	h   mergeHeap
	cur base.Entry
	err error
}

type mergeSource struct {
	it   base.Iterator
	prio int // lower wins on exact duplicates
	e    *base.Entry
}

type mergeHeap []*mergeSource

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := base.Compare(h[i].e.Key, h[i].e.Version, h[j].e.Key, h[j].e.Version); c != 0 {
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
	// Collapse exact duplicates, which recovery can create by replaying a
	// log whose contents were already flushed.
	for m.err == nil && len(m.h) > 0 && m.h[0].e.Version == m.cur.Version && bytes.Equal(m.h[0].e.Key, m.cur.Key) {
		m.advance()
	}
	return m.err == nil
}

func (m *mergeIterator) Entry() *base.Entry { return &m.cur }
func (m *mergeIterator) Err() error         { return m.err }

// visibleIterator turns a stream in entry order into the state of the data
// at version asOf: for each key, the newest entry with a version <= asOf.
// Tombstones and expired entries are returned; callers decide what is live.
type visibleIterator struct {
	in      base.Iterator
	asOf    uint64
	lastKey []byte
	seen    bool
}

func newVisibleIterator(in base.Iterator, asOf uint64) *visibleIterator {
	return &visibleIterator{in: in, asOf: asOf}
}

func (v *visibleIterator) Next() bool {
	for v.in.Next() {
		e := v.in.Entry()
		if e.Version > v.asOf {
			continue
		}
		if v.seen && bytes.Equal(e.Key, v.lastKey) {
			continue // an older version of a key already decided
		}
		v.lastKey, v.seen = append(v.lastKey[:0], e.Key...), true
		return true
	}
	return false
}

func (v *visibleIterator) Entry() *base.Entry { return v.in.Entry() }
func (v *visibleIterator) Err() error         { return v.in.Err() }

// gcIterator applies the history retention rule while rewriting tables.
// For each key it keeps every version newer than the horizon, plus the
// newest version at or before it (the "base", which is what a read at the
// horizon sees); older versions are dropped. When the output is the bottom
// of the tree, a base that is a tombstone or already expired at the horizon
// is dropped too, since nothing older remains for it to hide.
type gcIterator struct {
	in        base.Iterator
	horizon   uint64
	horizonMs int64
	bottom    bool

	lastKey  []byte
	seen     bool
	baseDone bool
}

func newGCIterator(in base.Iterator, horizon uint64, bottom bool) *gcIterator {
	return &gcIterator{in: in, horizon: horizon, horizonMs: base.VersionTime(horizon), bottom: bottom}
}

func (g *gcIterator) Next() bool {
	for g.in.Next() {
		e := g.in.Entry()
		if !g.seen || !bytes.Equal(e.Key, g.lastKey) {
			g.lastKey, g.seen, g.baseDone = append(g.lastKey[:0], e.Key...), true, false
		}
		if e.Version > g.horizon {
			return true // inside the history window
		}
		if g.baseDone {
			continue // shadowed for every readable time
		}
		g.baseDone = true
		if g.bottom && !e.Live(g.horizonMs) {
			continue
		}
		return true
	}
	return false
}

func (g *gcIterator) Entry() *base.Entry { return g.in.Entry() }
func (g *gcIterator) Err() error         { return g.in.Err() }
