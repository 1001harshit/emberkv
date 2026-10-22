package keyspace

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/wal"
	"github.com/1001harshit/emberkv/lsm"
)

func open(t *testing.T, opts lsm.Options) *Store {
	t.Helper()
	if opts.MemtableSize == 0 {
		opts.MemtableSize = 8 << 10
	}
	opts.SyncMode = wal.SyncNever
	s, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func b(s string) []byte { return []byte(s) }

func bs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestHashBasics(t *testing.T) {
	s := open(t, lsm.Options{})
	if n, err := s.HSet(b("h"), bs("a", "1", "b", "2", "a", "3"), false); err != nil || n != 2 {
		t.Fatalf("HSet = %d, %v; want 2 new fields", n, err)
	}
	if v, ok, _ := s.HGet(b("h"), b("a")); !ok || string(v) != "3" {
		t.Fatalf("HGet(a) = %q, %v", v, ok)
	}
	if n, _ := s.HLen(b("h")); n != 2 {
		t.Fatalf("HLen = %d", n)
	}
	if n, _ := s.HSet(b("h"), bs("a", "x", "c", "4"), true); n != 1 {
		t.Fatalf("HSETNX-style HSet added %d, want 1", n)
	}
	if v, _, _ := s.HGet(b("h"), b("a")); string(v) != "3" {
		t.Fatal("nx overwrote an existing field")
	}
	var got []string
	s.HGetAll(b("h"), func(f, v []byte) { got = append(got, string(f)+"="+string(v)) })
	if fmt.Sprint(got) != "[a=3 b=2 c=4]" {
		t.Fatalf("HGetAll = %v", got)
	}
	if n, _ := s.HIncrBy(b("h"), b("b"), 40); n != 42 {
		t.Fatalf("HIncrBy = %d", n)
	}
	if _, err := s.HIncrBy(b("h"), b("a"), 1); err != nil {
		t.Fatal(err)
	}
	s.HSet(b("h"), bs("txt", "abc"), false)
	if _, err := s.HIncrBy(b("h"), b("txt"), 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("HIncrBy on text = %v", err)
	}

	// Removing the last field removes the key, as in Redis.
	if n, _ := s.HDel(b("h"), bs("a", "b", "c", "txt", "nope")); n != 4 {
		t.Fatalf("HDel = %d, want 4", n)
	}
	if typ, _ := s.Type(b("h")); typ != TypeNone {
		t.Fatalf("emptied hash still exists as %v", typ)
	}
}

func TestSetBasics(t *testing.T) {
	s := open(t, lsm.Options{})
	if n, _ := s.SAdd(b("s"), bs("x", "y", "x", "z")); n != 3 {
		t.Fatalf("SAdd = %d", n)
	}
	if n, _ := s.SAdd(b("s"), bs("x", "w")); n != 1 {
		t.Fatalf("second SAdd = %d", n)
	}
	if ok, _ := s.SIsMember(b("s"), b("y")); !ok {
		t.Fatal("y not a member")
	}
	if n, _ := s.SRem(b("s"), bs("y", "nope")); n != 1 {
		t.Fatalf("SRem = %d", n)
	}
	m, _ := s.SMembers(b("s"))
	if fmt.Sprintf("%s", m) != "[w x z]" {
		t.Fatalf("SMembers = %s", m)
	}
	if n, _ := s.SCard(b("s")); n != 3 {
		t.Fatalf("SCard = %d", n)
	}
}

func TestWrongType(t *testing.T) {
	s := open(t, lsm.Options{})
	s.SetEx(b("str"), b("v"), 0)
	s.HSet(b("hash"), bs("f", "v"), false)
	s.SAdd(b("set"), bs("m"))

	checks := []struct {
		name string
		err  error
	}{
		{"HSet on string", func() error { _, err := s.HSet(b("str"), bs("f", "v"), false); return err }()},
		{"SAdd on hash", func() error { _, err := s.SAdd(b("hash"), bs("m")); return err }()},
		{"HGet on set", func() error { _, _, err := s.HGet(b("set"), b("f")); return err }()},
		{"Get on hash", func() error { _, err := s.Get(b("hash")); return err }()},
		{"Update on set", s.Update(b("set"), func(*lsm.Item) (*lsm.Item, bool, error) { return nil, false, nil })},
	}
	for _, c := range checks {
		if !errors.Is(c.err, ErrWrongType) {
			t.Errorf("%s: err = %v, want ErrWrongType", c.name, c.err)
		}
	}

	types := map[string]Type{"str": TypeString, "hash": TypeHash, "set": TypeSet, "none": TypeNone}
	for k, want := range types {
		if got, _ := s.Type(b(k)); got != want {
			t.Errorf("Type(%s) = %v, want %v", k, got, want)
		}
	}
}

// TestDeleteIsConstantTimeAndRecreateIsClean checks the version scheme: a
// deleted hash's fields must never leak into a new hash with the same name.
func TestDeleteIsConstantTimeAndRecreateIsClean(t *testing.T) {
	s := open(t, lsm.Options{})
	var pairs [][]byte
	for i := 0; i < 1000; i++ {
		pairs = append(pairs, b(fmt.Sprintf("f%04d", i)), b("v"))
	}
	s.HSet(b("big"), pairs, false)

	if existed, err := s.Delete(b("big")); err != nil || !existed {
		t.Fatalf("Delete = %v, %v", existed, err)
	}
	if n, _ := s.HLen(b("big")); n != 0 {
		t.Fatalf("HLen after delete = %d", n)
	}
	s.HSet(b("big"), bs("only", "1"), false)
	var fields []string
	s.HGetAll(b("big"), func(f, _ []byte) { fields = append(fields, string(f)) })
	if fmt.Sprint(fields) != "[only]" {
		t.Fatalf("recreated hash has fields %v", fields)
	}

	// Overwriting with a string also orphans the fields.
	s.SetUpdate(b("big"), func(*lsm.Item, bool) (*lsm.Item, bool, error) {
		return &lsm.Item{Value: b("now a string")}, true, nil
	})
	if it, err := s.Get(b("big")); err != nil || string(it.Value) != "now a string" {
		t.Fatalf("Get = %v, %v", it, err)
	}
}

// TestCompactionFilterReclaimsOrphans checks that fields of deleted,
// overwritten and expired containers are physically removed by compaction.
func TestCompactionFilterReclaimsOrphans(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Unix(1_700_000_000, 0).UnixNano())
	s := open(t, lsm.Options{Now: func() time.Time { return time.Unix(0, clock.Load()) }})

	add := func(key string, n int) {
		var pairs [][]byte
		for i := 0; i < n; i++ {
			pairs = append(pairs, b(fmt.Sprintf("f%d", i)), b("value"))
		}
		s.HSet(b(key), pairs, false)
	}
	add("deleted", 300)
	add("overwritten", 300)
	add("expiring", 300)
	add("kept", 50)
	s.SAdd(b("set"), bs("a", "b", "c"))

	s.Delete(b("deleted"))
	s.SetEx(b("overwritten"), b("plain"), 0)
	s.UpdateExpiry(b("expiring"), func(int64) (int64, bool) {
		return time.Unix(0, clock.Load()).Add(time.Second).UnixMilli(), true
	})
	clock.Add(int64(2 * time.Second))

	if err := s.DB().Compact(); err != nil {
		t.Fatal(err)
	}
	var records uint64
	for _, ti := range s.DB().Stats().Tables {
		records += ti.Keys
	}
	// kept: 1 metadata + 50 fields; set: 1 + 3; overwritten: 1 string.
	if want := uint64(51 + 4 + 1); records != want {
		t.Fatalf("%d records on disk after compaction, want %d", records, want)
	}
	if n, _ := s.HLen(b("kept")); n != 50 {
		t.Fatalf("HLen(kept) = %d", n)
	}
	if n, _ := s.Count(); n != 3 {
		t.Fatalf("Count = %d, want 3", n)
	}
}

func TestConcurrentHashUpdates(t *testing.T) {
	s := open(t, lsm.Options{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := s.HIncrBy(b("counters"), b("total"), 1); err != nil {
					t.Error(err)
				}
				s.HSet(b("counters"), bs(fmt.Sprintf("g%d", g), fmt.Sprint(i)), false)
				s.SAdd(b("seen"), bs(fmt.Sprintf("%d-%d", g, i)))
			}
		}(g)
	}
	wg.Wait()
	if v, _, _ := s.HGet(b("counters"), b("total")); string(v) != "800" {
		t.Fatalf("total = %s", v)
	}
	if n, _ := s.HLen(b("counters")); n != 9 {
		t.Fatalf("HLen = %d, want 9", n)
	}
	if n, _ := s.SCard(b("seen")); n != 800 {
		t.Fatalf("SCard = %d, want 800", n)
	}
	members, _ := s.SMembers(b("seen"))
	if len(members) != 800 {
		t.Fatalf("SMembers returned %d, want 800 (size counter and records disagree)", len(members))
	}
}

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{MemtableSize: 4 << 10, SyncMode: wal.SyncNever}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		s.HSet(b("h"), bs(fmt.Sprintf("f%d", i), "v"), false)
	}
	s.SAdd(b("s"), bs("a", "b"))
	s.Close()

	s, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if n, _ := s.HLen(b("h")); n != 200 {
		t.Fatalf("HLen after reopen = %d", n)
	}
	// A new container after reopen must get a version that does not collide.
	s.Delete(b("s"))
	s.SAdd(b("s"), bs("c"))
	if m, _ := s.SMembers(b("s")); fmt.Sprintf("%s", m) != "[c]" {
		t.Fatalf("SMembers = %s", m)
	}
}

// TestBlindWriteDoesNotInterleaveWithReadModifyWrite races SET against
// HINCRBY on the same key. Without the key lock, a SET could land between
// HINCRBY's read and write and be silently overwritten; with it, the
// operations serialise and the key's metadata always matches its fields.
func TestBlindWriteDoesNotInterleaveWithReadModifyWrite(t *testing.T) {
	s := open(t, lsm.Options{})
	for trial := 0; trial < 200; trial++ {
		key := b(fmt.Sprintf("k%d", trial))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := s.HIncrBy(key, b("n"), 1); err != nil && !errors.Is(err, ErrWrongType) {
					t.Error(err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				s.Delete(key)
			}
		}()
		wg.Wait()
		// The key's metadata and its fields must agree: if it is a hash, the
		// field it reports must exist under the live version.
		if typ, _ := s.Type(key); typ == TypeHash {
			n, _ := s.HLen(key)
			var fields int
			s.HGetAll(key, func([]byte, []byte) { fields++ })
			if int64(fields) != n {
				t.Fatalf("trial %d: HLEN %d but %d fields", trial, n, fields)
			}
		}
	}
}

// TestBlindBatchWritesRespectKeyLocks races MSET and SET against HINCRBY on
// the same keys: the batch path must still take the key locks, so a key's
// metadata and its fields always agree afterwards.
func TestBlindBatchWritesRespectKeyLocks(t *testing.T) {
	s := open(t, lsm.Options{})
	keys := bs("a", "b", "c", "d")
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.MSet(bs("a", "1", "b", "2", "c", "3"))
				s.SetEx(b("d"), b("x"), 0)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				for _, k := range keys {
					if _, err := s.HIncrBy(k, b(fmt.Sprintf("f%d", i%3)), 1); err != nil && !errors.Is(err, ErrWrongType) {
						t.Error(err)
					}
				}
			}
		}()
	}
	wg.Wait()
	for _, k := range keys {
		switch typ, _ := s.Type(k); typ {
		case TypeHash:
			n, _ := s.HLen(k)
			var fields int64
			s.HGetAll(k, func([]byte, []byte) { fields++ })
			if fields != n {
				t.Fatalf("%s: HLEN %d but %d fields", k, n, fields)
			}
		case TypeString:
			if _, err := s.Get(k); err != nil {
				t.Fatalf("%s: %v", k, err)
			}
		default:
			t.Fatalf("%s has type %v", k, typ)
		}
	}
}

func BenchmarkMSet(bm *testing.B) {
	s, err := Open(bm.TempDir(), lsm.Options{SyncMode: wal.SyncNever})
	if err != nil {
		bm.Fatal(err)
	}
	defer s.Close()
	val := bytes.Repeat([]byte("v"), 64)
	bm.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pairs := make([][]byte, 0, 20)
			for k := 0; k < 10; k++ {
				pairs = append(pairs, []byte(fmt.Sprintf("key:%d", (i*10+k)%100000)), val)
			}
			s.MSet(pairs)
			i++
		}
	})
}
