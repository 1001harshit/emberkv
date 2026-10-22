package lsm

import (
	"hash/maphash"
	"slices"
	"sync"
)

// numStripes is the number of key locks. Keys hash onto stripes, so two
// unrelated keys share a lock with probability 1/numStripes.
const numStripes = 256

// keyLocks is a striped lock table. Writers lock the stripes of the keys they
// touch, so read-modify-write operations on different keys proceed in
// parallel while operations on the same key are serialised.
type keyLocks struct {
	seed    maphash.Seed
	stripes [numStripes]sync.Mutex
}

func (l *keyLocks) stripe(key []byte) int {
	var h maphash.Hash
	h.SetSeed(l.seed)
	h.Write(key)
	return int(h.Sum64() % numStripes)
}

// lock locks the stripes of keys and returns the function that unlocks them.
// Stripes are taken in increasing order, so concurrent callers locking
// overlapping sets of keys cannot deadlock.
func (l *keyLocks) lock(keys [][]byte) (unlock func()) {
	if len(keys) == 1 {
		m := &l.stripes[l.stripe(keys[0])]
		m.Lock()
		return m.Unlock
	}
	idx := make([]int, 0, len(keys))
	for _, k := range keys {
		idx = append(idx, l.stripe(k))
	}
	slices.Sort(idx)
	idx = slices.Compact(idx)
	for _, i := range idx {
		l.stripes[i].Lock()
	}
	return func() {
		for _, i := range idx {
			l.stripes[i].Unlock()
		}
	}
}

// lockAll locks every stripe, excluding all other writers.
func (l *keyLocks) lockAll() (unlock func()) {
	for i := range l.stripes {
		l.stripes[i].Lock()
	}
	return func() {
		for i := range l.stripes {
			l.stripes[i].Unlock()
		}
	}
}
