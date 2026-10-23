package keyspace

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/1001harshit/emberkv/lsm"
)

// ErrNaN matches Redis's error for an increment that produces NaN.
var ErrNaN = errors.New("ERR resulting score is not a number (NaN)")

// ScoredMember is one member of a sorted set.
type ScoredMember struct {
	Member []byte
	Score  float64
}

// ScoreBound is one end of a score range; Exclusive is Redis's "(" prefix.
type ScoreBound struct {
	Value     float64
	Exclusive bool
}

// ZAddOptions are ZADD's flags.
type ZAddOptions struct {
	NX, XX, GT, LT, CH, Incr bool
}

// encodeScore maps a float64 to 8 bytes whose byte order is numeric order:
// flip the sign bit of non-negative numbers and every bit of negative ones.
func encodeScore(f float64) []byte {
	if f == 0 {
		f = 0 // -0 and +0 are one score
	}
	b := math.Float64bits(f)
	if b>>63 == 0 {
		b ^= 1 << 63
	} else {
		b = ^b
	}
	return binary.BigEndian.AppendUint64(nil, b)
}

func decodeScore(p []byte) float64 {
	b := binary.BigEndian.Uint64(p)
	if b>>63 == 1 {
		b ^= 1 << 63
	} else {
		b = ^b
	}
	return math.Float64frombits(b)
}

func scorePrefixOf(key []byte, version uint64) []byte {
	return familyPrefix(scorePrefix, key, version)
}

func (c *container) scoreKey(score float64, member []byte) []byte {
	return append(append(scorePrefixOf(c.key, c.m.version), encodeScore(score)...), member...)
}

// memberScore returns a member's score as the transaction sees it.
func (c *container) memberScore(member []byte) (float64, bool, error) {
	v, ok, err := c.get(member)
	if err != nil || !ok {
		return 0, false, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(v)), true, nil
}

func (c *container) setScore(member []byte, old float64, had bool, score float64) {
	if had {
		c.tx.Delete(c.scoreKey(old, member))
	} else {
		c.m.size++
	}
	c.tx.Set(memberKey(c.key, c.m.version, member), binary.BigEndian.AppendUint64(nil, math.Float64bits(score)), 0)
	c.tx.Set(c.scoreKey(score, member), nil, 0)
}

func (c *container) removeMember(member []byte, score float64) {
	c.tx.Delete(memberKey(c.key, c.m.version, member))
	c.tx.Delete(c.scoreKey(score, member))
	c.m.size--
}

// ZAdd adds or updates members. It returns the number added (plus the
// number changed, with CH) and, with Incr, the member's new score (nil if
// the flags prevented the update).
func (s *Store) ZAdd(key []byte, members []ScoredMember, o ZAddOptions) (int64, *float64, error) {
	var count int64
	var incrResult *float64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeZSet, !o.XX)
		if err != nil || c == nil {
			return err
		}
		for _, sm := range members {
			old, had, err := c.memberScore(sm.Member)
			if err != nil {
				return err
			}
			if (o.NX && had) || (o.XX && !had) {
				continue
			}
			score := sm.Score
			if o.Incr && had {
				score = old + sm.Score
			}
			if math.IsNaN(score) {
				return ErrNaN
			}
			if had && ((o.GT && score <= old) || (o.LT && score >= old)) {
				continue
			}
			if o.Incr {
				incrResult = &score
			}
			switch {
			case !had:
				count++
			case score != old:
				if o.CH {
					count++
				}
			default:
				continue
			}
			c.setScore(sm.Member, old, had, score)
		}
		c.save()
		return nil
	})
	return count, incrResult, err
}

// ZRem removes members and returns how many existed.
func (s *Store) ZRem(key []byte, members [][]byte) (int64, error) {
	var n int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeZSet, false)
		if err != nil || c == nil {
			return err
		}
		for _, m := range members {
			score, ok, err := c.memberScore(m)
			if err != nil {
				return err
			}
			if ok {
				c.removeMember(m, score)
				n++
			}
		}
		c.save()
		return nil
	})
	return n, err
}

// ZScore returns a member's score.
func (s *Store) ZScore(key, member []byte) (float64, bool, error) {
	v, ok, err := s.readMember(key, TypeZSet, member)
	if err != nil || !ok {
		return 0, false, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(v)), true, nil
}

// ZCard returns the number of members.
func (s *Store) ZCard(key []byte) (int64, error) { return s.size(key, TypeZSet) }

// byScore calls fn for members in ascending (score, member) order, starting
// at score from (nil for the lowest), until fn returns false.
func (s *Store) byScore(key []byte, m *meta, from *float64, fn func(sm ScoredMember) bool) error {
	prefix := scorePrefixOf(key, m.version)
	start := prefix
	if from != nil {
		start = append(bytes.Clone(prefix), encodeScore(*from)...)
	}
	return s.db.ScanAt(start, s.at, func(k []byte, _ lsm.Item) bool {
		if !bytes.HasPrefix(k, prefix) || len(k) < len(prefix)+8 {
			return false
		}
		return fn(ScoredMember{Member: k[len(prefix)+8:], Score: decodeScore(k[len(prefix) : len(prefix)+8])})
	})
}

func (b ScoreBound) belowOrAt(f float64) bool { // b is a lower bound
	return f > b.Value || (!b.Exclusive && f == b.Value)
}

func (b ScoreBound) aboveOrAt(f float64) bool { // b is an upper bound
	return f < b.Value || (!b.Exclusive && f == b.Value)
}

// ZRangeByScore returns members with min <= score <= max (honouring
// exclusive bounds), ascending, or descending with rev, skipping offset and
// returning at most count (count < 0: all).
func (s *Store) ZRangeByScore(key []byte, min, max ScoreBound, rev bool, offset, count int64) ([]ScoredMember, error) {
	m, err := s.readContainer(key, TypeZSet)
	if err != nil || m == nil {
		return nil, err
	}
	var all []ScoredMember
	lo := min.Value
	err = s.byScore(key, m, &lo, func(sm ScoredMember) bool {
		if !max.aboveOrAt(sm.Score) {
			return false
		}
		if min.belowOrAt(sm.Score) {
			all = append(all, sm)
		}
		return rev || count < 0 || int64(len(all)) < offset+count
	})
	if err != nil {
		return nil, err
	}
	if rev {
		reverse(all)
	}
	return window(all, offset, count), nil
}

// ZCount returns the number of members with a score in [min, max].
func (s *Store) ZCount(key []byte, min, max ScoreBound) (int64, error) {
	r, err := s.ZRangeByScore(key, min, max, false, 0, -1)
	return int64(len(r)), err
}

// ZRange returns members by rank, start..stop inclusive (negative counts
// from the end), ascending or, with rev, descending.
func (s *Store) ZRange(key []byte, start, stop int64, rev bool) ([]ScoredMember, error) {
	m, err := s.readContainer(key, TypeZSet)
	if err != nil || m == nil {
		return nil, err
	}
	n := m.size
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start, stop = max(start, 0), min(stop, n-1)
	if start > stop {
		return nil, nil
	}
	if rev { // rank r from the top is rank n-1-r from the bottom
		start, stop = n-1-stop, n-1-start
	}
	var out []ScoredMember
	var i int64
	err = s.byScore(key, m, nil, func(sm ScoredMember) bool {
		if i >= start {
			out = append(out, sm)
		}
		i++
		return i <= stop
	})
	if rev {
		reverse(out)
	}
	return out, err
}

// ZRank returns a member's 0-based rank, ascending or, with rev, descending.
func (s *Store) ZRank(key, member []byte, rev bool) (int64, bool, error) {
	m, err := s.readContainer(key, TypeZSet)
	if err != nil || m == nil {
		return 0, false, err
	}
	score, ok, err := s.ZScore(key, member)
	if err != nil || !ok {
		return 0, false, err
	}
	var rank int64
	err = s.byScore(key, m, nil, func(sm ScoredMember) bool {
		if sm.Score == score && bytes.Equal(sm.Member, member) {
			return false
		}
		rank++
		return true
	})
	if rev {
		rank = m.size - 1 - rank
	}
	return rank, true, err
}

// ZPop removes and returns up to count members with the lowest scores, or
// the highest with max.
func (s *Store) ZPop(key []byte, max bool, count int64) ([]ScoredMember, error) {
	var out []ScoredMember
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeZSet, false)
		if err != nil || c == nil {
			return err
		}
		if err := s.byScore(key, &c.m, nil, func(sm ScoredMember) bool {
			out = append(out, ScoredMember{Member: bytes.Clone(sm.Member), Score: sm.Score})
			return max || int64(len(out)) < count
		}); err != nil {
			return err
		}
		if max {
			reverse(out)
		}
		out = window(out, 0, count)
		for _, sm := range out {
			c.removeMember(sm.Member, sm.Score)
		}
		c.save()
		return nil
	})
	return out, err
}

// ZRemRangeByScore removes members with a score in [min, max].
func (s *Store) ZRemRangeByScore(key []byte, min, max ScoreBound) (int64, error) {
	return s.zremRange(key, func() ([]ScoredMember, error) { return s.ZRangeByScore(key, min, max, false, 0, -1) })
}

// ZRemRangeByRank removes members ranked start..stop (ascending).
func (s *Store) ZRemRangeByRank(key []byte, start, stop int64) (int64, error) {
	return s.zremRange(key, func() ([]ScoredMember, error) { return s.ZRange(key, start, stop, false) })
}

func (s *Store) zremRange(key []byte, pick func() ([]ScoredMember, error)) (int64, error) {
	var n int64
	err := s.txn([][]byte{key}, func(tx *lsm.Txn) error {
		c, err := s.openContainer(tx, key, TypeZSet, false)
		if err != nil || c == nil {
			return err
		}
		victims, err := pick() // reads committed state: no writes staged yet
		if err != nil {
			return err
		}
		for _, sm := range victims {
			c.removeMember(sm.Member, sm.Score)
		}
		n = int64(len(victims))
		c.save()
		return nil
	})
	return n, err
}

func reverse(s []ScoredMember) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// window applies LIMIT offset count (count < 0: no limit).
func window(s []ScoredMember, offset, count int64) []ScoredMember {
	if offset >= int64(len(s)) {
		return nil
	}
	s = s[offset:]
	if count >= 0 && count < int64(len(s)) {
		s = s[:count]
	}
	return s
}
