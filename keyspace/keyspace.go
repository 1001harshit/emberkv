// Package keyspace maps Redis data types onto the flat key-value space of an
// lsm.DB, following the design used by Apache Kvrocks.
//
// Every user key has one metadata record:
//
//	'm' | key  →  type(1) | payload            (expiry lives on the record)
//
// For a string the payload is the value itself. For a hash or set it is
// version(8) | size(8), and each field or member is a separate record:
//
//	'f' | len(key)(4) | key | version(8) | field  →  value (hash) or "" (set)
//
// Reading a field first reads the metadata to learn the live version, so
// deleting, overwriting or expiring a hash of any size is one metadata write:
// the old fields keep their stale version and are never read again. A
// compaction filter later removes them from disk.
package keyspace

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

// Type is the Redis type of a key.
type Type byte

const (
	TypeNone   Type = 0
	TypeString Type = 1
	TypeHash   Type = 2
	TypeSet    Type = 3
)

func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeHash:
		return "hash"
	case TypeSet:
		return "set"
	}
	return "none"
}

// ErrWrongType is returned when an operation does not match the key's type.
// Its text is the exact Redis error.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

const (
	metaPrefix   = 'm'
	memberPrefix = 'f'
)

func metaKey(key []byte) []byte {
	return append([]byte{metaPrefix}, key...)
}

// memberPrefixOf returns the prefix shared by every member of key at version.
func memberPrefixOf(key []byte, version uint64) []byte {
	b := make([]byte, 0, 1+4+len(key)+8)
	b = append(b, memberPrefix)
	b = binary.BigEndian.AppendUint32(b, uint32(len(key)))
	b = append(b, key...)
	return binary.BigEndian.AppendUint64(b, version)
}

func memberKey(key []byte, version uint64, field []byte) []byte {
	return append(memberPrefixOf(key, version), field...)
}

// parseMemberKey splits a member record key into user key and version.
func parseMemberKey(k []byte) (key []byte, version uint64, ok bool) {
	if len(k) < 5 || k[0] != memberPrefix {
		return nil, 0, false
	}
	n := int(binary.BigEndian.Uint32(k[1:5]))
	if len(k) < 5+n+8 {
		return nil, 0, false
	}
	return k[5 : 5+n], binary.BigEndian.Uint64(k[5+n:]), true
}

// meta is a decoded metadata record.
type meta struct {
	typ      Type
	value    []byte // strings
	version  uint64 // hashes and sets
	size     int64  // hashes and sets
	expireAt int64
}

func decodeMeta(item lsm.Item) (meta, error) {
	v := item.Value
	if len(v) < 1 {
		return meta{}, errors.New("keyspace: empty metadata record")
	}
	m := meta{typ: Type(v[0]), expireAt: item.ExpireAt}
	switch m.typ {
	case TypeString:
		m.value = v[1:]
	case TypeHash, TypeSet:
		if len(v) != 17 {
			return meta{}, errors.New("keyspace: corrupt container metadata")
		}
		m.version = binary.BigEndian.Uint64(v[1:9])
		m.size = int64(binary.BigEndian.Uint64(v[9:17]))
	default:
		return meta{}, errors.New("keyspace: unknown type in metadata")
	}
	return m, nil
}

func (m *meta) encode() []byte {
	if m.typ == TypeString {
		return append([]byte{byte(TypeString)}, m.value...)
	}
	b := make([]byte, 17)
	b[0] = byte(m.typ)
	binary.BigEndian.PutUint64(b[1:], m.version)
	binary.BigEndian.PutUint64(b[9:], uint64(m.size))
	return b
}

// getter is satisfied by both *lsm.DB and *lsm.Txn.
type getter interface {
	GetItem(key []byte) (lsm.Item, error)
}

type txnGetter struct{ tx *lsm.Txn }

func (g txnGetter) GetItem(key []byte) (lsm.Item, error) { return g.tx.Get(key) }

// loadMeta returns the metadata for key, or nil if the key does not exist.
func loadMeta(g getter, key []byte) (*meta, error) {
	item, err := g.GetItem(metaKey(key))
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m, err := decodeMeta(item)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// Store is a typed keyspace on top of an lsm.DB.
type Store struct {
	db      *lsm.DB
	version atomic.Uint64

	// gcCache remembers the last container checked by the compaction filter;
	// consecutive member records almost always belong to the same container.
	gcMu    sync.Mutex
	gcKey   []byte
	gcVer   uint64
	gcStale bool
}

// Open opens the database in dir with the keyspace's compaction filter
// installed, so fields of deleted or overwritten containers are reclaimed.
func Open(dir string, opts lsm.Options) (*Store, error) {
	s := &Store{}
	var ready atomic.Bool
	opts.CompactionFilter = func(key []byte, _ lsm.Item) bool {
		return ready.Load() && s.isGarbage(key)
	}
	db, err := lsm.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	s.db = db
	ready.Store(true)
	return s, nil
}

// DB returns the underlying database.
func (s *Store) DB() *lsm.DB { return s.db }

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// newVersion returns a version number never handed out before: wall-clock
// nanoseconds, bumped if the clock has not moved past the previous one.
func (s *Store) newVersion() uint64 {
	for {
		old := s.version.Load()
		v := max(uint64(time.Now().UnixNano()), old+1)
		if s.version.CompareAndSwap(old, v) {
			return v
		}
	}
}

// isGarbage reports whether a record is a member of a container that no
// longer exists at that version. Versions are never reused, so once stale,
// a member stays stale, which is what makes dropping it safe.
func (s *Store) isGarbage(k []byte) bool {
	key, version, ok := parseMemberKey(k)
	if !ok {
		return false
	}
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	if s.gcKey != nil && bytes.Equal(s.gcKey, key) && s.gcVer == version {
		return s.gcStale
	}
	m, err := loadMeta(s.db, key)
	if err != nil {
		return false // when in doubt, keep
	}
	stale := m == nil || (m.typ != TypeHash && m.typ != TypeSet) || m.version != version
	s.gcKey, s.gcVer, s.gcStale = bytes.Clone(key), version, stale
	return stale
}

// ---- generic key operations ----

// Type returns the type of key, or TypeNone if it does not exist.
func (s *Store) Type(key []byte) (Type, error) {
	m, err := loadMeta(s.db, key)
	if err != nil || m == nil {
		return TypeNone, err
	}
	return m.typ, nil
}

// Exists reports whether key exists.
func (s *Store) Exists(key []byte) (bool, error) {
	t, err := s.Type(key)
	return t != TypeNone, err
}

// Delete removes key of any type and reports whether it existed. For hashes
// and sets this is a single write regardless of size.
func (s *Store) Delete(key []byte) (bool, error) {
	existed := false
	err := s.db.Txn(func(tx *lsm.Txn) error {
		m, err := loadMeta(txnGetter{tx}, key)
		if err != nil || m == nil {
			return err
		}
		existed = true
		tx.Delete(metaKey(key))
		return nil
	})
	return existed, err
}

// ExpireAt returns the expiry of key (unix ms, 0 for none) and whether it exists.
func (s *Store) ExpireAt(key []byte) (int64, bool, error) {
	m, err := loadMeta(s.db, key)
	if err != nil || m == nil {
		return 0, false, err
	}
	return m.expireAt, true, nil
}

// UpdateExpiry atomically changes the expiry of key. fn receives the current
// expiry and returns the new one, or write=false to leave it. A new expiry
// at or before now deletes the key. It reports whether the key existed.
func (s *Store) UpdateExpiry(key []byte, fn func(expireAt int64) (next int64, write bool)) (bool, error) {
	existed := false
	err := s.db.Txn(func(tx *lsm.Txn) error {
		m, err := loadMeta(txnGetter{tx}, key)
		if err != nil || m == nil {
			return err
		}
		existed = true
		next, write := fn(m.expireAt)
		if !write {
			return nil
		}
		if next != 0 && next <= s.db.Now().UnixMilli() {
			tx.Delete(metaKey(key))
			return nil
		}
		m.expireAt = next
		tx.Set(metaKey(key), m.encode(), next)
		return nil
	})
	return existed, err
}

// Scan calls fn for each key >= start in order, with its type, until fn
// returns false. fn must not call methods on the Store.
func (s *Store) Scan(start []byte, fn func(key []byte, t Type) bool) error {
	return s.db.Scan(metaKey(start), func(k []byte, item lsm.Item) bool {
		if len(k) == 0 || k[0] != metaPrefix {
			return false
		}
		t := TypeNone
		if len(item.Value) > 0 {
			t = Type(item.Value[0])
		}
		return fn(k[1:], t)
	})
}

// Count returns the number of keys. It scans the whole keyspace.
func (s *Store) Count() (int, error) {
	n := 0
	err := s.Scan(nil, func([]byte, Type) bool { n++; return true })
	return n, err
}

// FlushAll deletes every key.
func (s *Store) FlushAll() error {
	var keys [][]byte
	if err := s.Scan(nil, func(k []byte, _ Type) bool {
		keys = append(keys, k)
		return true
	}); err != nil {
		return err
	}
	for len(keys) > 0 {
		n := min(len(keys), 1000)
		b := new(lsm.Batch)
		for _, k := range keys[:n] {
			b.Delete(metaKey(k))
		}
		if err := s.db.Write(b); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// ---- strings ----

// Get returns the string stored at key, or nil if the key does not exist.
func (s *Store) Get(key []byte) (*lsm.Item, error) {
	m, err := loadMeta(s.db, key)
	if err != nil || m == nil {
		return nil, err
	}
	if m.typ != TypeString {
		return nil, ErrWrongType
	}
	return &lsm.Item{Value: m.value, ExpireAt: m.expireAt}, nil
}

// Update atomically replaces the string at key with fn's result, with the
// same contract as lsm.DB.Update. It fails with ErrWrongType if key holds
// another type.
func (s *Store) Update(key []byte, fn func(cur *lsm.Item) (next *lsm.Item, write bool, err error)) error {
	return s.SetUpdate(key, func(cur *lsm.Item, other bool) (*lsm.Item, bool, error) {
		if other {
			return nil, false, ErrWrongType
		}
		return fn(cur)
	})
}

// SetUpdate is Update for commands like SET that may overwrite a key of any
// type: fn is told whether the key currently holds a non-string (other).
func (s *Store) SetUpdate(key []byte, fn func(cur *lsm.Item, other bool) (next *lsm.Item, write bool, err error)) error {
	return s.db.Txn(func(tx *lsm.Txn) error {
		m, err := loadMeta(txnGetter{tx}, key)
		if err != nil {
			return err
		}
		var cur *lsm.Item
		other := m != nil && m.typ != TypeString
		if m != nil && !other {
			cur = &lsm.Item{Value: m.value, ExpireAt: m.expireAt}
		}
		next, write, err := fn(cur, other)
		if err != nil || !write {
			return err
		}
		if next == nil {
			if m != nil {
				tx.Delete(metaKey(key))
			}
			return nil
		}
		nm := meta{typ: TypeString, value: next.Value}
		tx.Set(metaKey(key), nm.encode(), next.ExpireAt)
		return nil
	})
}

// MSet sets several strings atomically, overwriting keys of any type.
func (s *Store) MSet(pairs [][]byte) error {
	b := new(lsm.Batch)
	for i := 0; i+1 < len(pairs); i += 2 {
		m := meta{typ: TypeString, value: pairs[i+1]}
		b.Set(metaKey(pairs[i]), m.encode(), 0)
	}
	return s.db.Write(b)
}

// SetEx sets a string with an expiry (unix ms, 0 for none), overwriting any type.
func (s *Store) SetEx(key, value []byte, expireAt int64) error {
	m := meta{typ: TypeString, value: value}
	return s.db.Write(new(lsm.Batch).Set(metaKey(key), m.encode(), expireAt))
}
