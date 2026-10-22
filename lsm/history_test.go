package lsm

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/wal"
)

// fakeClock is a settable clock for Options.Now.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func historyOpts(clock *fakeClock, history time.Duration) Options {
	return Options{MemtableSize: 4 << 10, CompactionTrigger: 2, SyncMode: wal.SyncNever, Now: clock.Now, History: history}
}

// TestTimeTravelAgainstModel performs random writes while a fake clock moves
// forward, keeping a copy of the expected data at many points in time. After
// flushes, compactions and reopens, reading at each of those times must give
// exactly the copy taken then.
func TestTimeTravelAgainstModel(t *testing.T) {
	clock := newFakeClock()
	dir := t.TempDir()
	opts := historyOpts(clock, 24*time.Hour)
	db := mustOpen(t, dir, opts)
	defer func() { db.Close() }()

	type snapshot struct {
		at   time.Time
		data map[string]string
	}
	model := map[string]string{}
	var snaps []snapshot
	rnd := rand.New(rand.NewSource(7))

	for i := 0; i < 6000; i++ {
		k := fmt.Sprintf("k%03d", rnd.Intn(300))
		if rnd.Intn(4) == 0 {
			db.Delete([]byte(k))
			delete(model, k)
		} else {
			v := fmt.Sprintf("v%d", i)
			db.Put([]byte(k), []byte(v))
			model[k] = v
		}
		clock.Advance(time.Duration(rnd.Intn(3)) * time.Millisecond)
		if i%400 == 399 {
			clock.Advance(time.Second)
			cp := make(map[string]string, len(model))
			for k, v := range model {
				cp[k] = v
			}
			snaps = append(snaps, snapshot{clock.Now(), cp})
			clock.Advance(time.Second)
		}
		if i == 3000 {
			db.Close()
			db = mustOpen(t, dir, opts)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	check := func(stage string) {
		for si, sn := range snaps {
			for k := 0; k < 300; k++ {
				key := fmt.Sprintf("k%03d", k)
				item, err := db.GetItemAt([]byte(key), sn.at)
				want, ok := sn.data[key]
				if ok != (err == nil) || string(item.Value) != want {
					t.Fatalf("%s: snapshot %d: GetItemAt(%s) = %q, %v; want %q, %v", stage, si, key, item.Value, err, want, ok)
				}
			}
			n := 0
			db.ScanAt(nil, sn.at, func(k []byte, it Item) bool {
				if sn.data[string(k)] != string(it.Value) {
					t.Errorf("%s: snapshot %d: ScanAt %s = %q, want %q", stage, si, k, it.Value, sn.data[string(k)])
				}
				n++
				return true
			})
			if n != len(sn.data) {
				t.Fatalf("%s: snapshot %d: ScanAt saw %d keys, want %d", stage, si, n, len(sn.data))
			}
		}
	}
	check("after compaction")
	db.Close()
	db = mustOpen(t, dir, opts)
	check("after reopen")
}

func TestHistoryRetention(t *testing.T) {
	clock := newFakeClock()
	db := mustOpen(t, t.TempDir(), historyOpts(clock, 10*time.Minute))
	defer db.Close()

	start := clock.Now()
	for i := 1; i <= 5; i++ {
		db.Put([]byte("k"), []byte(fmt.Sprint(i)))
		clock.Advance(5 * time.Minute)
	}
	// Writes were at +0, +5, +10, +15, +20 min; now is +25 min, so the
	// window starts at +15. A read at +15 sees the write made at +15, which
	// is therefore the oldest version still needed; +0, +5 and +10 go.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	var got []string
	db.History([]byte("k"), func(v Version) bool {
		got = append(got, fmt.Sprintf("%s@+%v", v.Item.Value, v.At.Sub(start)))
		return true
	})
	if fmt.Sprint(got) != "[5@+20m0s 4@+15m0s]" {
		t.Fatalf("History after compaction = %v", got)
	}
	if it, _ := db.GetItemAt([]byte("k"), start.Add(16*time.Minute)); string(it.Value) != "4" {
		t.Fatalf("read at +16m = %q, want 4", it.Value)
	}
}

func TestHistoryZeroKeepsOnlyCurrent(t *testing.T) {
	clock := newFakeClock()
	db := mustOpen(t, t.TempDir(), historyOpts(clock, 0))
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.Put([]byte("counter"), []byte(fmt.Sprint(i)))
		clock.Advance(time.Millisecond)
	}
	db.Flush()
	n := 0
	db.History([]byte("counter"), func(Version) bool { n++; return true })
	if n != 1 {
		t.Fatalf("%d versions kept with History=0, want 1", n)
	}
	mustGet(t, db, "counter", "99")
}

func TestVersionsStayOrderedWhenClockGoesBack(t *testing.T) {
	clock := newFakeClock()
	dir := t.TempDir()
	opts := historyOpts(clock, time.Hour)
	db := mustOpen(t, dir, opts)
	db.Put([]byte("k"), []byte("first"))
	db.Close()

	clock.Advance(-time.Hour) // the system clock jumped backwards
	db = mustOpen(t, dir, opts)
	defer db.Close()
	db.Put([]byte("k"), []byte("second"))
	mustGet(t, db, "k", "second")
	var order []string
	db.History([]byte("k"), func(v Version) bool { order = append(order, string(v.Item.Value)); return true })
	if fmt.Sprint(order) != "[second first]" {
		t.Fatalf("History = %v", order)
	}
}

func TestDeletedKeyVisibleInPast(t *testing.T) {
	clock := newFakeClock()
	db := mustOpen(t, t.TempDir(), historyOpts(clock, time.Hour))
	defer db.Close()
	db.Put([]byte("k"), []byte("v"))
	clock.Advance(time.Second)
	before := clock.Now()
	clock.Advance(time.Second)
	db.Delete([]byte("k"))
	db.Compact()
	mustMissing(t, db, "k")
	if it, err := db.GetItemAt([]byte("k"), before); err != nil || string(it.Value) != "v" {
		t.Fatalf("GetItemAt(before delete) = %q, %v", it.Value, err)
	}
	if _, err := db.GetItemAt([]byte("k"), before.Add(-time.Hour)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("read before the key existed = %v", err)
	}
}

func TestRefusesOlderFormat(t *testing.T) {
	dir := t.TempDir()
	// A v0.1.0 manifest had no "format" field.
	os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte(`{"next_file": 3, "tables": []}`), 0o644)
	_, err := Open(dir, Options{})
	if err == nil || !strings.Contains(err.Error(), "format 1") {
		t.Fatalf("Open of a format-1 directory = %v, want a format error", err)
	}
}

func TestRollbackTo(t *testing.T) {
	clock := newFakeClock()
	dir := t.TempDir()
	db := mustOpen(t, dir, historyOpts(clock, 24*time.Hour))
	defer db.Close()

	rnd := rand.New(rand.NewSource(3))
	model := map[string]string{}
	write := func(n int) {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("k%04d", rnd.Intn(2500)) // more keys than one rollback chunk
			if rnd.Intn(3) == 0 {
				db.Delete([]byte(k))
				delete(model, k)
			} else {
				v := fmt.Sprintf("v%d", rnd.Int())
				db.Put([]byte(k), []byte(v))
				model[k] = v
			}
			clock.Advance(time.Millisecond)
		}
	}
	write(4000)
	clock.Advance(time.Second)
	target := clock.Now()
	snapshot := make(map[string]string, len(model))
	for k, v := range model {
		snapshot[k] = v
	}
	clock.Advance(time.Second)
	write(4000)
	db.PutTTL([]byte("ttl"), []byte("x"), time.Hour)
	clock.Advance(time.Second)
	beforeRollback := clock.Now()
	current := make(map[string]string, len(model)+1)
	for k, v := range model {
		current[k] = v
	}
	current["ttl"] = "x"
	clock.Advance(time.Second)

	changed := 0
	if err := db.RollbackTo(target, func([]byte) { changed++ }); err != nil {
		t.Fatal(err)
	}
	if changed == 0 {
		t.Fatal("rollback changed nothing")
	}
	checkMatchesModel(t, db, snapshot)

	// The rollback is itself history: reading just before it shows the
	// state it replaced, so it can be undone.
	for k, want := range current {
		if it, err := db.GetItemAt([]byte(k), beforeRollback); err != nil || string(it.Value) != want {
			t.Fatalf("before rollback %s = %q, %v; want %q", k, it.Value, err, want)
		}
	}
	clock.Advance(time.Second)
	if err := db.RollbackTo(beforeRollback, nil); err != nil {
		t.Fatal(err)
	}
	checkMatchesModel(t, db, current)
}
