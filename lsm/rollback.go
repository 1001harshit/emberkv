package lsm

import (
	"bytes"
	"time"

	"github.com/1001harshit/emberkv/internal/base"
)

const rollbackChunk = 1000

// RollbackTo makes the current data equal to the data as it was at time at,
// by writing, as new versions, every record that differs. Other writers are
// paused while it runs. Because it only adds versions, the state before the
// rollback stays in history and can itself be rolled back to.
//
// It works in chunks, so readers may see a partly rolled back state while
// it runs, and a crash part-way leaves it partly applied; running it again
// finishes the job. onChange, if not nil, is called with each key rewritten.
func (db *DB) RollbackTo(at time.Time, onChange func(key []byte)) error {
	if at.IsZero() {
		return nil
	}
	defer db.locks.lockAll()()

	var start []byte
	for {
		entries, next, err := db.rollbackChunk(start, at)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			c, err := db.apply(entries)
			if err != nil {
				return err
			}
			if err := c.Wait(); err != nil {
				return err
			}
			if onChange != nil {
				for _, e := range entries {
					onChange(e.Key)
				}
			}
		}
		if next == nil {
			return nil
		}
		start = next
	}
}

// rollbackChunk compares up to rollbackChunk keys >= start between now and
// time at, and returns the writes that turn the present into the past, plus
// the key to continue from (nil when done).
func (db *DB) rollbackChunk(start []byte, at time.Time) ([]base.Entry, []byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, nil, ErrClosed
	}
	nowMs := db.nowMs()
	pastVersion, pastMs := db.readPoint(at)
	cur := newVisibleIterator(newMergeIterator(db.iterators(start)), base.MaxVersion)
	past := newVisibleIterator(newMergeIterator(db.iterators(start)), pastVersion)

	// side tracks one of the two views being compared.
	type side struct {
		it    *visibleIterator
		ok    bool
		atMs  int64
		entry *base.Entry
	}
	advance := func(s *side) {
		s.ok = s.it.Next()
		s.entry = nil
		if s.ok {
			s.entry = s.it.Entry()
		}
	}
	now, then := &side{it: cur, atMs: nowMs}, &side{it: past, atMs: pastMs}
	advance(now)
	advance(then)

	var out []base.Entry
	seen := 0
	for now.ok || then.ok {
		var key []byte
		switch {
		case !then.ok:
			key = now.entry.Key
		case !now.ok:
			key = then.entry.Key
		default:
			key = now.entry.Key
			if bytes.Compare(then.entry.Key, key) < 0 {
				key = then.entry.Key
			}
		}
		if seen == rollbackChunk {
			return out, bytes.Clone(key), nil
		}
		seen++

		var have, want *base.Entry
		if now.ok && bytes.Equal(now.entry.Key, key) {
			if now.entry.Live(now.atMs) {
				have = now.entry
			}
		}
		if then.ok && bytes.Equal(then.entry.Key, key) {
			if then.entry.Live(then.atMs) {
				want = then.entry
			}
		}
		switch {
		case want == nil && have != nil:
			out = append(out, base.Entry{Key: bytes.Clone(key), Kind: base.KindDelete})
		case want != nil && (have == nil || !bytes.Equal(have.Value, want.Value) || have.ExpireAt != want.ExpireAt):
			out = append(out, base.Entry{Key: bytes.Clone(key), Value: bytes.Clone(want.Value),
				Kind: base.KindSet, ExpireAt: want.ExpireAt})
		}
		if now.ok && bytes.Equal(now.entry.Key, key) {
			advance(now)
		}
		if then.ok && bytes.Equal(then.entry.Key, key) {
			advance(then)
		}
	}
	if err := cur.Err(); err != nil {
		return nil, nil, err
	}
	return out, nil, past.Err()
}
