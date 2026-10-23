// Package keyspace maps Redis data types onto the flat key-value space of an
// lsm.DB, following the design used by Apache Kvrocks.
//
// Every user key has one metadata record:
//
//	'm' | key  →  type(1) | payload            (expiry lives on the record)
//
// For a string the payload is the value itself. For a hash, set, list or
// sorted set it starts with version(8) | size(8), and each field, member or
// element is a separate record:
//
//	'f' | len(key)(4) | key | version(8) | field     →  value (hash), "" (set)
//	'f' | len(key)(4) | key | version(8) | index(8)  →  element (list)
//	'f' | len(key)(4) | key | version(8) | member    →  score (sorted set)
//	'z' | len(key)(4) | key | version(8) | score(8) | member  →  "" (sorted set)
//
// A list's metadata also holds head(8) | tail(8): its elements occupy
// indexes [head, tail), starting from the middle of the uint64 range so it
// can grow at both ends. A sorted set keeps a second record per member, in
// the 'z' family, whose key sorts by score; scores are encoded so that byte
// order equals numeric order. This is the Apache Kvrocks layout.
//
// Reading a field first reads the metadata to learn the live version, so
// deleting, overwriting or expiring a hash of any size is one metadata write:
// the old fields keep their stale version and are never read again. A
// compaction filter later removes them from disk.
//
// Every write locks its user key (lsm.DB.TxnKeys), so operations on the same
// key are serialised while operations on different keys run in parallel.
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
	TypeList   Type = 4
	TypeZSet   Type = 5
)

// isContainer reports whether values of type t are stored as member records.
func (t Type) isContainer() bool {
	return t == TypeHash || t == TypeSet || t == TypeList || t == TypeZSet
}

func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeHash:
		return "hash"
	case TypeSet:
		return "set"
	case TypeList:
		return "list"
	case TypeZSet:
		return "zset"
	}
	return "none"
}

// ErrWrongType is returned when an operation does not match the key's type.
// Its text is the exact Redis error.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

const (
	metaPrefix   = 'm'
	memberPrefix = 'f'
	scorePrefix  = 'z'
)

func metaKey(key []byte) []byte {
	return append([]byte{metaPrefix}, key...)
}

// memberPrefixOf returns the prefix shared by every member of key at version.
func memberPrefixOf(key []byte, version uint64) []byte {
	return familyPrefix(memberPrefix, key, version)
}

func familyPrefix(family byte, key []byte, version uint64) []byte {
	b := make([]byte, 0, 1+4+len(key)+8)
	b = append(b, family)
	b = binary.BigEndian.AppendUint32(b, uint32(len(key)))
	b = append(b, key...)
	return binary.BigEndian.AppendUint64(b, version)
}

func memberKey(key []byte, version uint64, field []byte) []byte {
	return append(memberPrefixOf(key, version), field...)
}

// parseMemberKey splits a member or score record key into user key and
// version.
func parseMemberKey(k []byte) (key []byte, version uint64, ok bool) {
	if len(k) < 5 || (k[0] != memberPrefix && k[0] != scorePrefix) {
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
	typ        Type
	value      []byte // strings
	version    uint64 // containers
	size       int64  // containers
	head, tail uint64 // lists: elements occupy [head, tail); zsets: head is the floor
	expireAt   int64
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
	case TypeHash, TypeSet, TypeZSet, TypeList:
		want := 17
		if m.typ == TypeList || m.typ == TypeZSet {
			want = 33
		}
		if len(v) != want {
			return meta{}, errors.New("keyspace: corrupt container metadata")
		}
		m.version = binary.BigEndian.Uint64(v[1:9])
		m.size = int64(binary.BigEndian.Uint64(v[9:17]))
		if want == 33 {
			m.head = binary.BigEndian.Uint64(v[17:25])
			m.tail = binary.BigEndian.Uint64(v[25:33])
		}
	default:
		return meta{}, errors.New("keyspace: unknown type in metadata")
	}
	return m, nil
}

func (m *meta) encode() []byte {
	if m.typ == TypeString {
		return append([]byte{byte(TypeString)}, m.value...)
	}
	n := 17
	if m.typ == TypeList || m.typ == TypeZSet {
		n = 33
	}
	b := make([]byte, n)
	b[0] = byte(m.typ)
	binary.BigEndian.PutUint64(b[1:], m.version)
	binary.BigEndian.PutUint64(b[9:], uint64(m.size))
	if n == 33 {
		binary.BigEndian.PutUint64(b[17:], m.head)
		binary.BigEndian.PutUint64(b[25:], m.tail)
	}
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
	shared  *shared
	pending *Pending  // nil: each write waits for its own durability
	at      time.Time // zero: the present; otherwise a read-only view of the past
}

// ErrReadOnly is returned by writes through a view of the past.
var ErrReadOnly = errors.New("ERR can't write to the past: ASOF only allows read commands")

// AsOf returns a read-only view of the keyspace as it was at time t. Reads
// older than the database's History window may see incomplete data.
func (s *Store) AsOf(t time.Time) *Store {
	return &Store{db: s.db, shared: s.shared, at: t}
}

// GetItem reads a raw record at the view's time; it makes Store a getter.
func (s *Store) GetItem(key []byte) (lsm.Item, error) {
	return s.db.GetItemAt(key, s.at)
}

// shared is the state every Session of a Store has in common.
type shared struct {
	version atomic.Uint64

	// The compaction filter remembers the last container it checked;
	// consecutive member records almost always belong to the same container.
	gcMu      sync.Mutex
	gcKey     []byte
	gcVer     uint64
	gcStale   bool
	gcChecked time.Time
}

// Pending collects writes that have been applied but may not be durable
// yet. It belongs to one goroutine, typically one client connection.
type Pending struct {
	commits []lsm.Commit
}

// Wait blocks until every collected write is durable. Waiting on the first
// usually makes the rest durable too, since one log write covers them all.
func (p *Pending) Wait() error {
	var first error
	for _, c := range p.commits {
		if err := c.Wait(); err != nil && first == nil {
			first = err
		}
	}
	p.commits = p.commits[:0]
	return first
}

// Session returns a view of the Store whose writes do not wait for
// durability but are collected in p instead; the caller must call p.Wait
// before acknowledging them. Redis does the same with pipelines: it runs
// every command it has read, writes the AOF once, then sends the replies.
func (s *Store) Session(p *Pending) *Store {
	return &Store{db: s.db, shared: s.shared, pending: p}
}

// write applies a batch of blind writes holding the locks for keys, waiting
// for durability unless the Store is a Session. It skips the transaction
// machinery, which blind writes do not need.
func (s *Store) write(keys [][]byte, b *lsm.Batch) error {
	if !s.at.IsZero() {
		return ErrReadOnly
	}
	if s.pending == nil {
		return s.db.WriteKeys(keys, b)
	}
	c, err := s.db.WriteKeysNoWait(keys, b)
	if err == nil {
		s.pending.commits = append(s.pending.commits, c)
	}
	return err
}

// txn runs a transaction locking keys, waiting for durability unless the
// Store is a Session.
func (s *Store) txn(keys [][]byte, fn func(*lsm.Txn) error) error {
	if !s.at.IsZero() {
		return ErrReadOnly
	}
	if s.pending == nil {
		return s.db.TxnKeys(keys, fn)
	}
	c, err := s.db.TxnKeysNoWait(keys, fn)
	if err == nil {
		s.pending.commits = append(s.pending.commits, c)
	}
	return err
}

// Open opens the database in dir with the keyspace's compaction filter
// installed, so fields of deleted or overwritten containers are reclaimed.
func Open(dir string, opts lsm.Options) (*Store, error) {
	s := &Store{shared: &shared{}}
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
		old := s.shared.version.Load()
		v := max(uint64(time.Now().UnixNano()), old+1)
		if s.shared.version.CompareAndSwap(old, v) {
			return v
		}
	}
}

// isGarbage reports whether a record is a field or member of a container
// generation that no readable point in time can see: neither the key's
// current metadata nor any metadata version inside the History window
// (including the one in force at the window's start) refers to it.
// Generations are never reused and the window only moves forward, so once
// garbage, always garbage, which is what makes dropping it safe.
func (s *Store) isGarbage(k []byte) bool {
	key, gen, ok := parseMemberKey(k)
	if !ok {
		return false
	}
	s.shared.gcMu.Lock()
	defer s.shared.gcMu.Unlock()
	now := s.db.Now()
	if s.shared.gcKey != nil && bytes.Equal(s.shared.gcKey, key) && s.shared.gcVer == gen &&
		(s.shared.gcStale || now.Sub(s.shared.gcChecked) < time.Second) {
		// "Garbage" never changes. "Still needed" can turn into garbage as
		// the window moves, so it is trusted only briefly; keeping a record
		// a little longer is always safe.
		return s.shared.gcStale
	}
	horizon := s.db.OldestReadable()
	stale := true
	err := s.db.History(metaKey(key), func(v lsm.Version) bool {
		// A version can only show the fields while it is not expired, and
		// readers start at the window's beginning at the earliest.
		from := v.At
		if from.Before(horizon) {
			from = horizon
		}
		visible := !v.Deleted && (v.Item.ExpireAt == 0 || v.Item.ExpireAt > from.UnixMilli())
		if visible {
			if m, err := decodeMeta(v.Item); err == nil && m.typ.isContainer() && m.version == gen {
				stale = false
				return false
			}
		}
		// Keep going through the window; the first version at or before
		// its start is the last one a reader can see.
		return v.At.After(horizon)
	})
	if err != nil {
		return false // when in doubt, keep
	}
	s.shared.gcKey, s.shared.gcVer, s.shared.gcStale, s.shared.gcChecked = bytes.Clone(key), gen, stale, now
	return stale
}

// Rollback makes the whole keyspace equal to what it was at time at, and
// returns the number of keys it changed. See lsm.DB.RollbackTo; like it, the
// rollback is recorded in history and can be undone.
func (s *Store) Rollback(at time.Time) (int, error) {
	if !s.at.IsZero() {
		return 0, ErrReadOnly
	}
	n := 0
	err := s.db.RollbackTo(at, func(k []byte) {
		if len(k) > 0 && k[0] == metaPrefix {
			n++
		}
	})
	return n, err
}

// WatchVersion returns a number that changes whenever key is written, for
// WATCH. Every write to a key of any type rewrites its metadata record, so
// the record's latest version is enough.
func (s *Store) WatchVersion(key []byte) (uint64, error) {
	return s.db.LatestVersion(metaKey(key))
}

// Change is one past version of a key, as reported by History.
type Change struct {
	At    time.Time
	Type  Type   // TypeNone for a deletion
	Value []byte // strings
	Size  int64  // hashes and sets: number of fields or members
}

// History calls fn with the key's past versions, newest first, until fn
// returns false. Only versions inside the History window are guaranteed.
func (s *Store) History(key []byte, fn func(c Change) bool) error {
	var derr error
	err := s.db.History(metaKey(key), func(v lsm.Version) bool {
		c := Change{At: v.At}
		if !v.Deleted {
			m, err := decodeMeta(v.Item)
			if err != nil {
				derr = err
				return false
			}
			c.Type, c.Value, c.Size = m.typ, m.value, m.size
		}
		return fn(c)
	})
	if err == nil {
		err = derr
	}
	return err
}

// ---- generic key operations ----

// Type returns the type of key, or TypeNone if it does not exist.
func (s *Store) Type(key []byte) (Type, error) {
	m, err := loadMeta(s, key)
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
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
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
	m, err := loadMeta(s, key)
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
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
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
	return s.db.ScanAt(metaKey(start), s.at, func(k []byte, item lsm.Item) bool {
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
		chunk := keys[:n]
		b := new(lsm.Batch)
		for _, k := range chunk {
			b.Delete(metaKey(k))
		}
		if err := s.write(chunk, b); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

// ---- strings ----

// Get returns the string stored at key, or nil if the key does not exist.
func (s *Store) Get(key []byte) (*lsm.Item, error) {
	m, err := loadMeta(s, key)
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
	return s.txn([][]byte{key}, func(tx *lsm.Txn) error {
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
	keys := make([][]byte, 0, len(pairs)/2)
	b := new(lsm.Batch)
	for i := 0; i+1 < len(pairs); i += 2 {
		keys = append(keys, pairs[i])
		b.Set(metaKey(pairs[i]), encodeString(pairs[i+1]), 0)
	}
	return s.write(keys, b)
}

// SetEx sets a string with an expiry (unix ms, 0 for none), overwriting any
// type. It is a blind write, but still takes the key's lock so it cannot land
// between another operation's read and write of the same key.
func (s *Store) SetEx(key, value []byte, expireAt int64) error {
	return s.write([][]byte{key}, new(lsm.Batch).Set(metaKey(key), encodeString(value), expireAt))
}

// encodeString builds a string's metadata record in one allocation.
func encodeString(value []byte) []byte {
	b := make([]byte, 1+len(value))
	b[0] = byte(TypeString)
	copy(b[1:], value)
	return b
}
