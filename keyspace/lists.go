package keyspace

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/1001harshit/emberkv/lsm"
)

// Errors with the exact Redis text.
var (
	ErrNoSuchKey       = errors.New("ERR no such key")
	ErrIndexOutOfRange = errors.New("ERR index out of range")
)

// listStart is where a new list's head and tail begin: the middle of the
// index space, so it can grow at both ends.
const listStart = uint64(1) << 63

func indexBytes(i uint64) []byte { return binary.BigEndian.AppendUint64(nil, i) }

func (c *container) putElement(i uint64, v []byte) {
	c.tx.Set(memberKey(c.key, c.m.version, indexBytes(i)), v, 0)
}

func (c *container) delElement(i uint64) {
	c.tx.Delete(memberKey(c.key, c.m.version, indexBytes(i)))
}

func (c *container) resize() { c.m.size = int64(c.m.tail - c.m.head) }

// elements calls fn with the list's elements at absolute indexes from..to
// (inclusive), in order, until fn returns false.
func (s *Store) elements(key []byte, m *meta, from, to uint64, fn func(i uint64, v []byte) bool) error {
	if from > to {
		return nil
	}
	prefix := memberPrefixOf(key, m.version)
	return s.db.ScanAt(append(bytes.Clone(prefix), indexBytes(from)...), s.at, func(k []byte, item lsm.Item) bool {
		if !bytes.HasPrefix(k, prefix) || len(k) != len(prefix)+8 {
			return false
		}
		i := binary.BigEndian.Uint64(k[len(prefix):])
		if i > to {
			return false
		}
		return fn(i, item.Value)
	})
}

// listRange converts Redis start/stop indexes (negative counts from the end)
// to absolute indexes, reporting false for an empty range.
func listRange(m *meta, start, stop int64) (uint64, uint64, bool) {
	n := m.size
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start = max(start, 0)
	stop = min(stop, n-1)
	if start > stop || start >= n {
		return 0, 0, false
	}
	return m.head + uint64(start), m.head + uint64(stop), true
}

func (s *Store) allElements(key []byte, m *meta) ([][]byte, error) {
	var out [][]byte
	err := s.elements(key, m, m.head, m.tail-1, func(_ uint64, v []byte) bool {
		out = append(out, v)
		return true
	})
	return out, err
}

// Push adds values to the head (left) or tail of the list and returns its
// new length. With onlyIfExists (LPUSHX/RPUSHX) a missing key stays missing
// and 0 is returned.
func (s *Store) Push(key []byte, values [][]byte, left, onlyIfExists bool) (int64, error) {
	var n int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeList, !onlyIfExists)
		if err != nil || c == nil {
			return err
		}
		if c.new {
			c.m.head, c.m.tail = listStart, listStart
		}
		for _, v := range values {
			if left {
				c.m.head--
				c.putElement(c.m.head, v)
			} else {
				c.putElement(c.m.tail, v)
				c.m.tail++
			}
		}
		c.resize()
		c.save()
		n = c.m.size
		return nil
	})
	return n, err
}

// LLen returns the length of the list.
func (s *Store) LLen(key []byte) (int64, error) { return s.size(key, TypeList) }

// LIndex returns the element at index (negative counts from the end).
func (s *Store) LIndex(key []byte, index int64) ([]byte, bool, error) {
	m, err := s.readContainer(key, TypeList)
	if err != nil || m == nil {
		return nil, false, err
	}
	from, _, ok := listRange(m, index, index)
	if !ok {
		return nil, false, nil
	}
	item, err := s.GetItem(memberKey(key, m.version, indexBytes(from)))
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, false, nil
	}
	return item.Value, err == nil, err
}

// LRange returns the elements between start and stop, inclusive.
func (s *Store) LRange(key []byte, start, stop int64) ([][]byte, error) {
	m, err := s.readContainer(key, TypeList)
	if err != nil || m == nil {
		return nil, err
	}
	from, to, ok := listRange(m, start, stop)
	if !ok {
		return nil, nil
	}
	var out [][]byte
	err = s.elements(key, m, from, to, func(_ uint64, v []byte) bool {
		out = append(out, v)
		return true
	})
	return out, err
}

// Pop removes and returns up to count elements from the head (left) or tail.
// exists reports whether the key held a list.
func (s *Store) Pop(key []byte, left bool, count int64) (vals [][]byte, exists bool, err error) {
	err = s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeList, false)
		if err != nil || c == nil {
			return err
		}
		exists = true
		vals, err = s.popFrom(c, left, count)
		if err == nil {
			c.save()
		}
		return err
	})
	return vals, exists, err
}

// popFrom removes up to count elements from one end of an open list. It
// reads before it stages any write, as reads do not see staged writes.
func (s *Store) popFrom(c *container, left bool, count int64) ([][]byte, error) {
	n := uint64(min(count, c.m.size))
	if n == 0 {
		return nil, nil
	}
	from, to := c.m.head, c.m.head+n-1
	if !left {
		from, to = c.m.tail-n, c.m.tail-1
	}
	var vals [][]byte
	if err := s.elements(c.key, &c.m, from, to, func(_ uint64, v []byte) bool {
		vals = append(vals, v)
		return true
	}); err != nil {
		return nil, err
	}
	for i := from; i <= to; i++ {
		c.delElement(i)
	}
	if left {
		c.m.head += n
	} else {
		c.m.tail -= n
		for i, j := 0, len(vals)-1; i < j; i, j = i+1, j-1 { // tail pops come out last-first
			vals[i], vals[j] = vals[j], vals[i]
		}
	}
	c.resize()
	return vals, nil
}

// LSet replaces the element at index.
func (s *Store) LSet(key []byte, index int64, value []byte) error {
	return s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeList, false)
		if err != nil {
			return err
		}
		if c == nil {
			return ErrNoSuchKey
		}
		i, _, ok := listRange(&c.m, index, index)
		if !ok {
			return ErrIndexOutOfRange
		}
		c.putElement(i, value)
		return nil
	})
}

// LTrim keeps only the elements between start and stop, inclusive.
func (s *Store) LTrim(key []byte, start, stop int64) error {
	return s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeList, false)
		if err != nil || c == nil {
			return err
		}
		from, to, ok := listRange(&c.m, start, stop)
		if !ok {
			c.m.head = c.m.tail // empty: the key goes, and its elements become garbage
			c.resize()
			c.save()
			return nil
		}
		for i := c.m.head; i < from; i++ {
			c.delElement(i)
		}
		for i := to + 1; i < c.m.tail; i++ {
			c.delElement(i)
		}
		c.m.head, c.m.tail = from, to+1
		c.resize()
		c.save()
		return nil
	})
}

// rewrite replaces an open list's contents with vals under a new
// generation, so the old elements need no individual deletes: they become
// unreachable and compaction removes them.
func (s *Store) rewrite(c *container, vals [][]byte) {
	c.m.version = s.newVersion()
	c.m.head, c.m.tail = listStart, listStart+uint64(len(vals))
	for i, v := range vals {
		c.putElement(listStart+uint64(i), v)
	}
	c.resize()
	c.save()
}

// LRem removes elements equal to value: the first count from the head if
// count > 0, the last -count from the tail if count < 0, or all if count is
// 0. It returns how many it removed.
func (s *Store) LRem(key []byte, count int64, value []byte) (int64, error) {
	var removed int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeList, false)
		if err != nil || c == nil {
			return err
		}
		vals, err := s.allElements(key, &c.m)
		if err != nil {
			return err
		}
		drop := make([]bool, len(vals))
		limit := count
		if limit < 0 {
			limit = -limit
		}
		for k := 0; k < len(vals); k++ {
			i := k
			if count < 0 {
				i = len(vals) - 1 - k
			}
			if bytes.Equal(vals[i], value) && (limit == 0 || removed < limit) {
				drop[i] = true
				removed++
			}
		}
		if removed == 0 {
			return nil
		}
		kept := vals[:0:0]
		for i, v := range vals {
			if !drop[i] {
				kept = append(kept, v)
			}
		}
		s.rewrite(c, kept)
		return nil
	})
	return removed, err
}

// LInsert inserts value before or after the first element equal to pivot.
// It returns the new length, -1 if pivot was not found, or 0 if the key
// does not exist.
func (s *Store) LInsert(key []byte, before bool, pivot, value []byte) (int64, error) {
	var n int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeList, false)
		if err != nil || c == nil {
			return err
		}
		vals, err := s.allElements(key, &c.m)
		if err != nil {
			return err
		}
		at := -1
		for i, v := range vals {
			if bytes.Equal(v, pivot) {
				at = i
				break
			}
		}
		if at < 0 {
			n = -1
			return nil
		}
		if !before {
			at++
		}
		out := make([][]byte, 0, len(vals)+1)
		out = append(append(append(out, vals[:at]...), value), vals[at:]...)
		s.rewrite(c, out)
		n = int64(len(out))
		return nil
	})
	return n, err
}

// LMove atomically pops an element from one end of src and pushes it onto
// one end of dst, returning it. src and dst may be the same list.
func (s *Store) LMove(src, dst []byte, fromLeft, toLeft bool) ([]byte, bool, error) {
	var moved []byte
	var ok bool
	err := s.txn([][]byte{src, dst}, func(tx *lsm.Txn) error {
		from, err := s.openContainer(tx, src, TypeList, false)
		if err != nil || from == nil {
			return err
		}
		to := from
		if !bytes.Equal(src, dst) {
			if to, err = s.openContainer(tx, dst, TypeList, true); err != nil {
				return err
			}
			if to.new {
				to.m.head, to.m.tail = listStart, listStart
			}
		}
		vals, err := s.popFrom(from, fromLeft, 1)
		if err != nil || len(vals) == 0 {
			return err
		}
		moved, ok = vals[0], true
		if toLeft {
			to.m.head--
			to.putElement(to.m.head, moved)
		} else {
			to.putElement(to.m.tail, moved)
			to.m.tail++
		}
		to.resize()
		to.save()
		if to != from {
			from.save()
		}
		return nil
	})
	return moved, ok, err
}
