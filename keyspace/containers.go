package keyspace

import (
	"bytes"
	"errors"
	"math"
	"strconv"

	"github.com/1001harshit/emberkv/lsm"
)

// ErrNotInteger and ErrOverflow match the Redis error text for HINCRBY.
var (
	ErrNotInteger = errors.New("ERR hash value is not an integer")
	ErrOverflow   = errors.New("ERR increment or decrement would overflow")
)

// container is a hash or set being modified inside a transaction.
type container struct {
	tx  *lsm.Txn
	key []byte
	m   meta
	new bool
}

// openContainer loads the hash or set at key for modification. If the key
// does not exist and create is true, a new empty container with a fresh
// version is returned; if create is false, nil is returned.
func (s *Store) openContainer(tx *lsm.Txn, key []byte, typ Type, create bool) (*container, error) {
	m, err := loadMeta(txnGetter{tx}, key)
	if err != nil {
		return nil, err
	}
	if m == nil {
		if !create {
			return nil, nil
		}
		return &container{tx: tx, key: key, m: meta{typ: typ, version: s.newVersion()}, new: true}, nil
	}
	if m.typ != typ {
		return nil, ErrWrongType
	}
	return &container{tx: tx, key: key, m: *m}, nil
}

func (c *container) get(field []byte) ([]byte, bool, error) {
	item, err := c.tx.Get(memberKey(c.key, c.m.version, field))
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, false, nil
	}
	return item.Value, err == nil, err
}

// set writes field and reports whether it was newly added.
func (c *container) set(field, value []byte) (bool, error) {
	_, exists, err := c.get(field)
	if err != nil {
		return false, err
	}
	c.tx.Set(memberKey(c.key, c.m.version, field), value, 0)
	if !exists {
		c.m.size++
	}
	return !exists, nil
}

// del removes field and reports whether it existed.
func (c *container) del(field []byte) (bool, error) {
	_, exists, err := c.get(field)
	if err != nil || !exists {
		return false, err
	}
	c.tx.Delete(memberKey(c.key, c.m.version, field))
	c.m.size--
	return true, nil
}

// save writes the updated metadata. An emptied container is deleted, as in
// Redis, and a new container that stayed empty writes nothing.
func (c *container) save() {
	switch {
	case c.m.size > 0:
		c.tx.Set(metaKey(c.key), c.m.encode(), c.m.expireAt)
	case !c.new:
		c.tx.Delete(metaKey(c.key))
	}
}

// readContainer returns the live metadata for a hash or set, or nil.
func (s *Store) readContainer(key []byte, typ Type) (*meta, error) {
	m, err := loadMeta(s, key)
	if err != nil || m == nil {
		return nil, err
	}
	if m.typ != typ {
		return nil, ErrWrongType
	}
	return m, nil
}

// members calls fn for each field and value of the container at m.
func (s *Store) members(key []byte, m *meta, fn func(field, value []byte)) error {
	prefix := memberPrefixOf(key, m.version)
	return s.db.ScanAt(prefix, s.at, func(k []byte, item lsm.Item) bool {
		if !bytes.HasPrefix(k, prefix) {
			return false
		}
		fn(k[len(prefix):], item.Value)
		return true
	})
}

func (s *Store) readMember(key []byte, typ Type, field []byte) ([]byte, bool, error) {
	m, err := s.readContainer(key, typ)
	if err != nil || m == nil {
		return nil, false, err
	}
	item, err := s.GetItem(memberKey(key, m.version, field))
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, false, nil
	}
	return item.Value, err == nil, err
}

func (s *Store) size(key []byte, typ Type) (int64, error) {
	m, err := s.readContainer(key, typ)
	if err != nil || m == nil {
		return 0, err
	}
	return m.size, nil
}

// ---- hashes ----

// HSet sets field/value pairs and returns how many fields were added.
// With nx, existing fields are left alone (HSETNX).
func (s *Store) HSet(key []byte, pairs [][]byte, nx bool) (int64, error) {
	var added int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeHash, true)
		if err != nil {
			return err
		}
		for i := 0; i+1 < len(pairs); i += 2 {
			if nx {
				if _, exists, err := c.get(pairs[i]); err != nil || exists {
					if err != nil {
						return err
					}
					continue
				}
			}
			isNew, err := c.set(pairs[i], pairs[i+1])
			if err != nil {
				return err
			}
			if isNew {
				added++
			}
		}
		c.save()
		return nil
	})
	return added, err
}

// HGet returns the value of field, or nil if the hash or field is missing.
func (s *Store) HGet(key, field []byte) ([]byte, bool, error) {
	return s.readMember(key, TypeHash, field)
}

// HDel removes fields and returns how many existed.
func (s *Store) HDel(key []byte, fields [][]byte) (int64, error) {
	return s.removeMembers(key, TypeHash, fields)
}

// HLen returns the number of fields.
func (s *Store) HLen(key []byte) (int64, error) { return s.size(key, TypeHash) }

// HGetAll calls fn for every field and value in field order.
func (s *Store) HGetAll(key []byte, fn func(field, value []byte)) error {
	m, err := s.readContainer(key, TypeHash)
	if err != nil || m == nil {
		return err
	}
	return s.members(key, m, fn)
}

// HIncrBy adds delta to the integer in field, creating it if needed.
func (s *Store) HIncrBy(key, field []byte, delta int64) (int64, error) {
	var result int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeHash, true)
		if err != nil {
			return err
		}
		v, exists, err := c.get(field)
		if err != nil {
			return err
		}
		var n int64
		if exists {
			if n, err = strconv.ParseInt(string(v), 10, 64); err != nil {
				return ErrNotInteger
			}
		}
		if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
			return ErrOverflow
		}
		result = n + delta
		if _, err := c.set(field, strconv.AppendInt(nil, result, 10)); err != nil {
			return err
		}
		c.save()
		return nil
	})
	return result, err
}

// ---- sets ----

// SAdd adds members and returns how many were new.
func (s *Store) SAdd(key []byte, members [][]byte) (int64, error) {
	var added int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeSet, true)
		if err != nil {
			return err
		}
		for _, mem := range members {
			isNew, err := c.set(mem, nil)
			if err != nil {
				return err
			}
			if isNew {
				added++
			}
		}
		c.save()
		return nil
	})
	return added, err
}

// SRem removes members and returns how many existed.
func (s *Store) SRem(key []byte, members [][]byte) (int64, error) {
	return s.removeMembers(key, TypeSet, members)
}

// SIsMember reports whether member is in the set.
func (s *Store) SIsMember(key, member []byte) (bool, error) {
	_, ok, err := s.readMember(key, TypeSet, member)
	return ok, err
}

// SCard returns the number of members.
func (s *Store) SCard(key []byte) (int64, error) { return s.size(key, TypeSet) }

// SMembers returns all members in sorted order.
func (s *Store) SMembers(key []byte) ([][]byte, error) {
	m, err := s.readContainer(key, TypeSet)
	if err != nil || m == nil {
		return nil, err
	}
	var out [][]byte
	err = s.members(key, m, func(member, _ []byte) { out = append(out, member) })
	return out, err
}

func (s *Store) removeMembers(key []byte, typ Type, fields [][]byte) (int64, error) {
	var removed int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, typ, false)
		if err != nil || c == nil {
			return err
		}
		for _, f := range fields {
			ok, err := c.del(f)
			if err != nil {
				return err
			}
			if ok {
				removed++
			}
		}
		c.save()
		return nil
	})
	return removed, err
}
