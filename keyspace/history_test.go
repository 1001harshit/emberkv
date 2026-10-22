package keyspace

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

type clock struct{ ns atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.ns.Store(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *clock) Now() time.Time { return time.Unix(0, c.ns.Load()) }
func (c *clock) Tick(d time.Duration) time.Time {
	c.ns.Add(int64(d))
	return c.Now()
}

func fields(t *testing.T, s *Store, key string) string {
	t.Helper()
	var out []string
	if err := s.HGetAll(b(key), func(f, v []byte) { out = append(out, string(f)+"="+string(v)) }); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return fmt.Sprint(out)
}

func TestAsOfHashThroughItsLife(t *testing.T) {
	c := newClock()
	s := open(t, lsm.Options{Now: c.Now, History: time.Hour})

	// Each read time sits strictly between two writes.
	s.HSet(b("h"), bs("f1", "v1"), false)
	t1 := c.Tick(time.Second)
	c.Tick(time.Second)
	s.HSet(b("h"), bs("f2", "v2"), false)
	t2 := c.Tick(time.Second)
	c.Tick(time.Second)
	s.Delete(b("h"))
	t3 := c.Tick(time.Second)
	c.Tick(time.Second)
	s.HSet(b("h"), bs("g", "new"), false) // a new generation of the same key
	t4 := c.Tick(time.Second)

	if err := s.DB().Compact(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		at   time.Time
		want string
	}{
		{t1, "[f1=v1]"}, {t2, "[f1=v1 f2=v2]"}, {t3, "[]"}, {t4, "[g=new]"}, {time.Time{}, "[g=new]"},
	} {
		if got := fields(t, s.AsOf(tc.at), "h"); got != tc.want {
			t.Errorf("HGETALL at %v = %s, want %s", tc.at.Format("15:04:05"), got, tc.want)
		}
	}
	if typ, _ := s.AsOf(t3).Type(b("h")); typ != TypeNone {
		t.Errorf("TYPE at t3 = %v, want none", typ)
	}
	if n, _ := s.AsOf(t2).HLen(b("h")); n != 2 {
		t.Errorf("HLEN at t2 = %d", n)
	}
	if _, err := s.AsOf(t1).HSet(b("h"), bs("x", "y"), false); !errors.Is(err, ErrReadOnly) {
		t.Errorf("write to the past = %v, want ErrReadOnly", err)
	}
	if err := s.AsOf(t1).SetEx(b("k"), b("v"), 0); !errors.Is(err, ErrReadOnly) {
		t.Errorf("blind write to the past = %v, want ErrReadOnly", err)
	}
}

// TestFieldsKeptWhileVisible checks that compaction keeps a deleted hash's
// fields while the history window can still see them, and drops them after.
func TestFieldsKeptWhileVisible(t *testing.T) {
	c := newClock()
	s := open(t, lsm.Options{Now: c.Now, History: 10 * time.Minute})
	var pairs [][]byte
	for i := 0; i < 300; i++ {
		pairs = append(pairs, b(fmt.Sprintf("f%03d", i)), b("v"))
	}
	s.HSet(b("big"), pairs, false)
	before := c.Tick(time.Second)
	c.Tick(time.Second)
	s.Delete(b("big"))
	c.Tick(time.Minute)

	records := func() (n uint64) {
		s.DB().Compact()
		for _, ti := range s.DB().Stats().Tables {
			n += ti.Keys
		}
		return n
	}
	if n := records(); n < 300 {
		t.Fatalf("only %d records after compaction inside the window; fields were dropped too early", n)
	}
	if n, _ := s.AsOf(before).HLen(b("big")); n != 300 {
		t.Fatalf("HLEN before the delete = %d", n)
	}
	if got := fields(t, s.AsOf(before), "big"); len(got) < 100 {
		t.Fatalf("HGETALL before the delete = %s", got)
	}

	c.Tick(20 * time.Minute) // the delete is now older than the window
	if n := records(); n != 0 {
		t.Fatalf("%d records left after the window passed, want 0", n)
	}
}

func TestStoreHistory(t *testing.T) {
	c := newClock()
	s := open(t, lsm.Options{Now: c.Now, History: time.Hour})
	start := c.Now()
	s.SetEx(b("k"), b("one"), 0)
	c.Tick(time.Second)
	s.SetEx(b("k"), b("two"), 0)
	c.Tick(time.Second)
	s.Delete(b("k"))
	c.Tick(time.Second)
	s.SAdd(b("k"), bs("a", "b"))
	c.Tick(time.Second)

	var got []string
	s.History(b("k"), func(ch Change) bool {
		desc := ch.Type.String()
		switch ch.Type {
		case TypeString:
			desc += ":" + string(ch.Value)
		case TypeSet, TypeHash:
			desc += fmt.Sprintf(":%d", ch.Size)
		}
		got = append(got, fmt.Sprintf("+%v %s", ch.At.Sub(start), desc))
		return true
	})
	if want := "[+3s set:2 +2s none +1s string:two +0s string:one]"; fmt.Sprint(got) != want {
		t.Fatalf("History = %v\n want %s", got, want)
	}
}
