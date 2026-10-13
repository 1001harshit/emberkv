// Package base holds the record type shared by the WAL, memtable and SSTables,
// plus its binary encoding.
package base

import (
	"encoding/binary"
	"errors"
)

// Kind distinguishes a value from a deletion marker (tombstone).
type Kind uint8

const (
	KindSet    Kind = 1
	KindDelete Kind = 2
)

// ErrCorrupt is returned when on-disk data fails to decode or checksum.
var ErrCorrupt = errors.New("emberkv: corrupt data")

// Entry is a single versioned record. Newer entries shadow older ones with
// the same key; a KindDelete entry shadows the key entirely.
type Entry struct {
	Key      []byte
	Value    []byte
	Kind     Kind
	ExpireAt int64 // unix milliseconds; 0 means no expiry
}

// Expired reports whether the entry has a TTL that has passed at nowMs.
func (e *Entry) Expired(nowMs int64) bool {
	return e.ExpireAt > 0 && nowMs >= e.ExpireAt
}

// Live reports whether the entry is a readable value at nowMs.
func (e *Entry) Live(nowMs int64) bool {
	return e.Kind == KindSet && !e.Expired(nowMs)
}

// Size approximates the in-memory footprint of the entry.
func (e *Entry) Size() int {
	return len(e.Key) + len(e.Value) + 24
}

// AppendEntry appends the encoding of e to dst:
//
//	kind(1) | expireAt(uvarint) | keyLen(uvarint) | key | valLen(uvarint) | value
func AppendEntry(dst []byte, e *Entry) []byte {
	dst = append(dst, byte(e.Kind))
	dst = binary.AppendUvarint(dst, uint64(e.ExpireAt))
	dst = binary.AppendUvarint(dst, uint64(len(e.Key)))
	dst = append(dst, e.Key...)
	dst = binary.AppendUvarint(dst, uint64(len(e.Value)))
	dst = append(dst, e.Value...)
	return dst
}

// DecodeEntry decodes one entry from b and returns it with the number of bytes
// consumed. The returned Key and Value alias b.
func DecodeEntry(b []byte) (Entry, int, error) {
	if len(b) < 1 {
		return Entry{}, 0, ErrCorrupt
	}
	var e Entry
	e.Kind = Kind(b[0])
	if e.Kind != KindSet && e.Kind != KindDelete {
		return Entry{}, 0, ErrCorrupt
	}
	n := 1

	exp, m := binary.Uvarint(b[n:])
	if m <= 0 {
		return Entry{}, 0, ErrCorrupt
	}
	e.ExpireAt = int64(exp)
	n += m

	var err error
	if e.Key, n, err = decodeBytes(b, n); err != nil {
		return Entry{}, 0, err
	}
	if e.Value, n, err = decodeBytes(b, n); err != nil {
		return Entry{}, 0, err
	}
	return e, n, nil
}

func decodeBytes(b []byte, n int) ([]byte, int, error) {
	l, m := binary.Uvarint(b[n:])
	if m <= 0 || l > uint64(len(b)-n-m) {
		return nil, 0, ErrCorrupt
	}
	n += m
	end := n + int(l)
	return b[n:end:end], end, nil
}

// Iterator walks entries in ascending key order.
type Iterator interface {
	// Next advances to the next entry and reports whether there is one.
	Next() bool
	// Entry returns the current entry. It is valid until the next call to Next,
	// but the Key and Value slices it references are never modified.
	Entry() *Entry
	// Err returns the first error encountered, if any.
	Err() error
}

// SliceIterator iterates over an in-memory, already sorted slice of entries.
type SliceIterator struct {
	entries []Entry
	i       int
}

// NewSliceIterator returns an iterator over entries, which must be sorted by key.
func NewSliceIterator(entries []Entry) *SliceIterator {
	return &SliceIterator{entries: entries, i: -1}
}

func (it *SliceIterator) Next() bool {
	it.i++
	return it.i < len(it.entries)
}

func (it *SliceIterator) Entry() *Entry { return &it.entries[it.i] }
func (it *SliceIterator) Err() error    { return nil }
