package keyspace

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

func strs(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, x := range bs {
		out[i] = string(x)
	}
	return out
}

// modelRange applies Redis's start/stop rules to a slice length.
func modelRange(n, start, stop int) (int, int, bool) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start, stop = max(start, 0), min(stop, n-1)
	return start, stop, start <= stop && start < n
}

// TestListsAgainstModel runs random list operations against the store and a
// plain slice, comparing every result.
func TestListsAgainstModel(t *testing.T) {
	s := open(t, lsm.Options{})
	rnd := rand.New(rand.NewSource(11))
	var model []string
	val := func() string { return fmt.Sprintf("v%d", rnd.Intn(8)) } // few values, so LREM/LINSERT hit
	k := b("list")

	for i := 0; i < 3000; i++ {
		switch op := rnd.Intn(11); op {
		case 0, 1:
			v := val()
			n, err := s.Push(k, bs(v), true, false)
			model = append([]string{v}, model...)
			if err != nil || n != int64(len(model)) {
				t.Fatalf("op %d LPUSH = %d, %v; want %d", i, n, err, len(model))
			}
		case 2, 3:
			v := val()
			s.Push(k, bs(v), false, false)
			model = append(model, v)
		case 4:
			cnt := int64(rnd.Intn(3) + 1)
			got, _, err := s.Pop(k, true, cnt)
			want := model[:min(int(cnt), len(model))]
			if err != nil || fmt.Sprint(strs(got)) != fmt.Sprint(want) {
				t.Fatalf("op %d LPOP %d = %v; want %v", i, cnt, strs(got), want)
			}
			model = model[len(want):]
		case 5:
			cnt := int64(rnd.Intn(3) + 1)
			got, _, _ := s.Pop(k, false, cnt)
			n := min(int(cnt), len(model))
			var want []string
			for j := len(model) - 1; j >= len(model)-n; j-- {
				want = append(want, model[j])
			}
			if fmt.Sprint(strs(got)) != fmt.Sprint(want) {
				t.Fatalf("op %d RPOP %d = %v; want %v", i, cnt, strs(got), want)
			}
			model = model[:len(model)-n]
		case 6:
			idx := int64(rnd.Intn(10) - 5)
			v := val()
			err := s.LSet(k, idx, b(v))
			lo, _, ok := modelRange(len(model), int(idx), int(idx))
			if (err == nil) != (ok && len(model) > 0) {
				t.Fatalf("op %d LSET %d err=%v, model len %d", i, idx, err, len(model))
			}
			if err == nil {
				model[lo] = v
			}
		case 7:
			cnt := int64(rnd.Intn(5) - 2)
			v := val()
			n, _ := s.LRem(k, cnt, b(v))
			var kept []string
			removed := 0
			limit := int(cnt)
			if limit < 0 {
				limit = -limit
			}
			drop := make([]bool, len(model))
			for j := 0; j < len(model); j++ {
				x := j
				if cnt < 0 {
					x = len(model) - 1 - j
				}
				if model[x] == v && (limit == 0 || removed < limit) {
					drop[x], removed = true, removed+1
				}
			}
			for j, x := range model {
				if !drop[j] {
					kept = append(kept, x)
				}
			}
			if n != int64(removed) {
				t.Fatalf("op %d LREM = %d, want %d", i, n, removed)
			}
			model = kept
		case 8:
			pivot, v, before := val(), val(), rnd.Intn(2) == 0
			n, _ := s.LInsert(k, before, b(pivot), b(v))
			at := -1
			for j, x := range model {
				if x == pivot {
					at = j
					break
				}
			}
			switch {
			case len(model) == 0:
				if n != 0 {
					t.Fatalf("op %d LINSERT on missing key = %d", i, n)
				}
			case at < 0:
				if n != -1 {
					t.Fatalf("op %d LINSERT missing pivot = %d", i, n)
				}
			default:
				if !before {
					at++
				}
				model = append(model[:at], append([]string{v}, model[at:]...)...)
				if n != int64(len(model)) {
					t.Fatalf("op %d LINSERT = %d, want %d", i, n, len(model))
				}
			}
		case 9:
			start, stop := int64(rnd.Intn(8)-3), int64(rnd.Intn(12)-3)
			s.LTrim(k, start, stop)
			lo, hi, ok := modelRange(len(model), int(start), int(stop))
			if ok {
				model = append([]string(nil), model[lo:hi+1]...)
			} else {
				model = nil
			}
		case 10:
			if len(model) < 20 {
				s.Push(k, bs("a", "b", "c", "d"), false, false)
				model = append(model, "a", "b", "c", "d")
			}
		}

		// Check the whole list and a random range and index.
		got, err := s.LRange(k, 0, -1)
		if err != nil || fmt.Sprint(strs(got)) != fmt.Sprint(model) {
			t.Fatalf("op %d: LRANGE = %v, %v\nmodel = %v", i, strs(got), err, model)
		}
		if n, _ := s.LLen(k); n != int64(len(model)) {
			t.Fatalf("op %d: LLEN = %d, want %d", i, n, len(model))
		}
		start, stop := rnd.Intn(10)-5, rnd.Intn(10)-5
		part, _ := s.LRange(k, int64(start), int64(stop))
		lo, hi, ok := modelRange(len(model), start, stop)
		var want []string
		if ok {
			want = model[lo : hi+1]
		}
		if fmt.Sprint(strs(part)) != fmt.Sprint(want) {
			t.Fatalf("op %d: LRANGE %d %d = %v, want %v", i, start, stop, strs(part), want)
		}
		if typ, _ := s.Type(k); (typ == TypeList) != (len(model) > 0) {
			t.Fatalf("op %d: TYPE = %v with %d elements", i, typ, len(model))
		}
	}
}

func TestLMove(t *testing.T) {
	s := open(t, lsm.Options{})
	s.Push(b("a"), bs("1", "2", "3"), false, false)
	if v, ok, _ := s.LMove(b("a"), b("b"), true, false); !ok || string(v) != "1" {
		t.Fatalf("LMOVE = %q", v)
	}
	if v, _, _ := s.LMove(b("a"), b("a"), false, true); string(v) != "3" { // rotate
		t.Fatalf("rotate = %q", v)
	}
	if got, _ := s.LRange(b("a"), 0, -1); fmt.Sprint(strs(got)) != "[3 2]" {
		t.Fatalf("a = %v", strs(got))
	}
	if got, _ := s.LRange(b("b"), 0, -1); fmt.Sprint(strs(got)) != "[1]" {
		t.Fatalf("b = %v", strs(got))
	}
	s.SetEx(b("str"), b("x"), 0)
	if _, _, err := s.LMove(b("a"), b("str"), true, true); err != ErrWrongType {
		t.Fatalf("LMOVE into a string = %v", err)
	}
	if got, _ := s.LRange(b("a"), 0, -1); len(got) != 2 {
		t.Fatal("a failed LMOVE changed the source")
	}
}

func TestScoreEncodingOrder(t *testing.T) {
	vals := []float64{math.Inf(-1), -1e300, -2.5, -1, -math.SmallestNonzeroFloat64, 0, math.SmallestNonzeroFloat64, 0.1, 1, 2.5, 1e300, math.Inf(1)}
	for i := 1; i < len(vals); i++ {
		if bytes.Compare(encodeScore(vals[i-1]), encodeScore(vals[i])) >= 0 {
			t.Fatalf("encode(%v) !< encode(%v)", vals[i-1], vals[i])
		}
	}
	for _, v := range vals {
		if got := decodeScore(encodeScore(v)); got != v {
			t.Fatalf("round trip %v -> %v", v, got)
		}
	}
	if !bytes.Equal(encodeScore(math.Copysign(0, -1)), encodeScore(0)) {
		t.Fatal("-0 and +0 encode differently")
	}
}

// TestZSetsAgainstModel runs random sorted-set operations against the
// store and a map, comparing order, ranks, score ranges and counts.
func TestZSetsAgainstModel(t *testing.T) {
	s := open(t, lsm.Options{})
	rnd := rand.New(rand.NewSource(12))
	model := map[string]float64{}
	k := b("z")
	sorted := func() []ScoredMember {
		var out []ScoredMember
		for m, sc := range model {
			out = append(out, ScoredMember{b(m), sc})
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Score != out[j].Score {
				return out[i].Score < out[j].Score
			}
			return bytes.Compare(out[i].Member, out[j].Member) < 0
		})
		return out
	}
	show := func(s []ScoredMember) string {
		var parts []string
		for _, x := range s {
			parts = append(parts, fmt.Sprintf("%s=%g", x.Member, x.Score))
		}
		return fmt.Sprint(parts)
	}
	score := func() float64 { return float64(rnd.Intn(21)-10) / 2 } // ties are common

	for i := 0; i < 3000; i++ {
		m := fmt.Sprintf("m%02d", rnd.Intn(40))
		switch rnd.Intn(6) {
		case 0, 1:
			sc := score()
			n, _, err := s.ZAdd(k, []ScoredMember{{b(m), sc}}, ZAddOptions{})
			_, had := model[m]
			if err != nil || (n == 1) == had {
				t.Fatalf("op %d ZADD %s = %d, %v (had %v)", i, m, n, err, had)
			}
			model[m] = sc
		case 2:
			inc := score()
			_, res, _ := s.ZAdd(k, []ScoredMember{{b(m), inc}}, ZAddOptions{Incr: true})
			model[m] += inc
			if res == nil || *res != model[m] {
				t.Fatalf("op %d ZINCRBY %s", i, m)
			}
		case 3:
			n, _ := s.ZRem(k, bs(m))
			_, had := model[m]
			if (n == 1) != had {
				t.Fatalf("op %d ZREM %s = %d", i, m, n)
			}
			delete(model, m)
		case 4:
			sc := score()
			o := ZAddOptions{GT: rnd.Intn(2) == 0}
			o.LT = !o.GT
			s.ZAdd(k, []ScoredMember{{b(m), sc}}, o)
			if old, had := model[m]; !had || (o.GT && sc > old) || (o.LT && sc < old) {
				model[m] = sc
			}
		case 5:
			if cnt := int64(rnd.Intn(3)); len(model) > 30 {
				maxSide := rnd.Intn(2) == 0
				got, _ := s.ZPop(k, maxSide, cnt)
				all := sorted()
				var want []ScoredMember
				for j := 0; j < int(cnt) && j < len(all); j++ {
					if maxSide {
						want = append(want, all[len(all)-1-j])
					} else {
						want = append(want, all[j])
					}
				}
				if show(got) != show(want) {
					t.Fatalf("op %d ZPOP max=%v %d = %s, want %s", i, maxSide, cnt, show(got), show(want))
				}
				for _, x := range want {
					delete(model, string(x.Member))
				}
			}
		}

		all := sorted()
		got, _ := s.ZRange(k, 0, -1, false)
		if show(got) != show(all) {
			t.Fatalf("op %d: ZRANGE = %s\nwant %s", i, show(got), show(all))
		}
		if n, _ := s.ZCard(k); n != int64(len(all)) {
			t.Fatalf("op %d: ZCARD = %d", i, n)
		}
		if len(all) > 0 {
			j := rnd.Intn(len(all))
			r, ok, _ := s.ZRank(k, all[j].Member, false)
			rr, _, _ := s.ZRank(k, all[j].Member, true)
			if !ok || r != int64(j) || rr != int64(len(all)-1-j) {
				t.Fatalf("op %d: ZRANK %s = %d/%d, want %d", i, all[j].Member, r, rr, j)
			}
			rev, _ := s.ZRange(k, 0, 2, true)
			var wantRev []ScoredMember
			for x := len(all) - 1; x >= 0 && len(wantRev) < 3; x-- {
				wantRev = append(wantRev, all[x])
			}
			if show(rev) != show(wantRev) {
				t.Fatalf("op %d: ZREVRANGE 0 2 = %s, want %s", i, show(rev), show(wantRev))
			}
		}
		lo, hi := score(), score()
		if lo > hi {
			lo, hi = hi, lo
		}
		minB, maxB := ScoreBound{lo, rnd.Intn(2) == 0}, ScoreBound{hi, rnd.Intn(2) == 0}
		var want []ScoredMember
		for _, x := range all {
			if minB.belowOrAt(x.Score) && maxB.aboveOrAt(x.Score) {
				want = append(want, x)
			}
		}
		got, _ = s.ZRangeByScore(k, minB, maxB, false, 0, -1)
		if show(got) != show(want) {
			t.Fatalf("op %d: ZRANGEBYSCORE %v %v = %s, want %s", i, minB, maxB, show(got), show(want))
		}
		lim, _ := s.ZRangeByScore(k, minB, maxB, true, 1, 2)
		reverse(want)
		if show(lim) != show(window(want, 1, 2)) {
			t.Fatalf("op %d: ZREVRANGEBYSCORE LIMIT 1 2 = %s", i, show(lim))
		}
	}
}

func TestListsAndZSetsTimeTravelAndGC(t *testing.T) {
	c := newClock()
	s := open(t, lsm.Options{Now: c.Now, History: 10 * time.Minute})
	s.Push(b("q"), bs("a", "b", "c"), false, false)
	s.ZAdd(b("lb"), []ScoredMember{{b("ann"), 10}, {b("bob"), 20}}, ZAddOptions{})
	before := c.Tick(time.Second)
	c.Tick(time.Second)
	s.Pop(b("q"), true, 2)
	s.ZAdd(b("lb"), []ScoredMember{{b("ann"), 30}}, ZAddOptions{})
	s.Delete(b("lb"))

	if got, _ := s.AsOf(before).LRange(b("q"), 0, -1); fmt.Sprint(strs(got)) != "[a b c]" {
		t.Fatalf("past LRANGE = %v", strs(got))
	}
	past, _ := s.AsOf(before).ZRange(b("lb"), 0, -1, true)
	if len(past) != 2 || string(past[0].Member) != "bob" {
		t.Fatalf("past ZREVRANGE = %v", past)
	}

	c.Tick(time.Hour) // everything above is now outside the window
	s.DB().Compact()
	var records uint64
	for _, ti := range s.DB().Stats().Tables {
		records += ti.Keys
	}
	// Only the list's metadata and its last element survive; the deleted
	// sorted set's member and score records are all garbage-collected.
	if records != 2 {
		t.Fatalf("%d records on disk after compaction, want 2", records)
	}
}
