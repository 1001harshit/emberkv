package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/wal"
)

// smallOpts forces frequent flushes and compactions so tests exercise every
// layer of the tree.
func smallOpts() Options {
	return Options{MemtableSize: 4 << 10, CompactionTrigger: 2, SyncMode: wal.SyncNever}
}

func mustOpen(t testing.TB, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) err = %v, want ErrNotFound", key, err)
	}
}

func TestPutGetDelete(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{})
	defer db.Close()

	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Put([]byte("a"), []byte("3"))
	db.Delete([]byte("b"))

	mustGet(t, db, "a", "3")
	mustMissing(t, db, "b")
	mustMissing(t, db, "never")
}

func TestEmptyKeyAndValue(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{})
	defer db.Close()
	db.Put([]byte{}, []byte{})
	db.Flush()
	mustGet(t, db, "", "")
}

// TestAgainstModel runs random operations against the DB and a plain map,
// reopening the DB along the way, and checks they always agree.
func TestAgainstModel(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, smallOpts())
	defer func() { db.Close() }()

	model := map[string]string{}
	var flushes, compactions uint64
	rnd := rand.New(rand.NewSource(42))
	key := func() string { return fmt.Sprintf("key-%04d", rnd.Intn(800)) }

	for i := 0; i < 30000; i++ {
		switch r := rnd.Intn(100); {
		case r < 60:
			k, v := key(), fmt.Sprintf("v%d-%s", i, bytes.Repeat([]byte("x"), rnd.Intn(40)))
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			model[k] = v
		case r < 85:
			k := key()
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(model, k)
		case r < 95:
			b := new(Batch)
			for j := 0; j < 5; j++ {
				k := key()
				if rnd.Intn(2) == 0 {
					b.Set([]byte(k), []byte("batch"), 0)
					model[k] = "batch"
				} else {
					b.Delete([]byte(k))
					delete(model, k)
				}
			}
			if err := db.Write(b); err != nil {
				t.Fatal(err)
			}
		default:
			k := key()
			got, err := db.Get([]byte(k))
			want, ok := model[k]
			if ok != (err == nil) || string(got) != want {
				t.Fatalf("op %d: Get(%q) = %q, %v; model has %q, %v", i, k, got, err, want, ok)
			}
		}
		if i%7500 == 7499 {
			st := db.Stats()
			flushes += st.Flushes
			compactions += st.Compactions
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = mustOpen(t, dir, smallOpts())
		}
	}

	checkMatchesModel(t, db, model)
	if flushes == 0 || compactions == 0 {
		t.Fatalf("expected flushes and compactions, got %d and %d", flushes, compactions)
	}
	t.Logf("flushes=%d compactions=%d tables=%d", flushes, compactions, len(db.Stats().Tables))

	// A full compaction leaves one table holding exactly the live keys.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	if len(st.Tables) != 1 || st.Tables[0].Keys != uint64(len(model)) {
		t.Fatalf("after Compact: %+v, want 1 table with %d keys", st.Tables, len(model))
	}
	checkMatchesModel(t, db, model)
}

func checkMatchesModel(t *testing.T, db *DB, model map[string]string) {
	t.Helper()
	for k, v := range model {
		mustGet(t, db, k, v)
	}
	var keys []string
	err := db.Scan(nil, func(k []byte, item Item) bool {
		if model[string(k)] != string(item.Value) {
			t.Errorf("Scan: %q = %q, model has %q", k, item.Value, model[string(k)])
		}
		keys = append(keys, string(k))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != len(model) {
		t.Fatalf("Scan returned %d keys, model has %d", len(keys), len(model))
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("Scan keys are not sorted")
	}
}

// TestCrashRecovery copies the data directory while the DB is still open,
// which is exactly what the disk looks like after a power cut, and checks
// that the copy opens with every acknowledged write.
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := smallOpts()
	opts.SyncMode = wal.SyncAlways
	db := mustOpen(t, dir, opts)
	defer db.Close()

	model := map[string]string{}
	for i := 0; i < 3000; i++ {
		k, v := fmt.Sprintf("k%d", i%700), fmt.Sprintf("v%d", i)
		db.Put([]byte(k), []byte(v))
		model[k] = v
	}
	for i := 0; i < 700; i += 3 {
		k := fmt.Sprintf("k%d", i)
		db.Delete([]byte(k))
		delete(model, k)
	}

	crashed := filepath.Join(t.TempDir(), "crashed")
	copyDir(t, dir, crashed)
	db2 := mustOpen(t, crashed, opts)
	defer db2.Close()
	checkMatchesModel(t, db2, model)
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	os.MkdirAll(dst, 0o755)
	ents, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		in, err := os.Open(filepath.Join(src, e.Name()))
		if err != nil {
			continue // removed by a concurrent compaction
		}
		out, _ := os.Create(filepath.Join(dst, e.Name()))
		io.Copy(out, in)
		in.Close()
		out.Close()
	}
}

func TestTTL(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	opts := smallOpts()
	opts.Now = clock
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()

	db.PutTTL([]byte("short"), []byte("1"), time.Second)
	db.PutTTL([]byte("long"), []byte("2"), time.Hour)
	db.Put([]byte("forever"), []byte("3"))

	item, err := db.GetItem([]byte("short"))
	if err != nil || item.ExpireAt != now.Add(time.Second).UnixMilli() {
		t.Fatalf("GetItem(short) = %+v, %v", item, err)
	}
	db.Flush() // expiry must survive the trip to disk

	advance(2 * time.Second)
	mustMissing(t, db, "short")
	mustGet(t, db, "long", "2")
	mustGet(t, db, "forever", "3")
	if n, _ := db.Count(); n != 2 {
		t.Fatalf("Count = %d, want 2", n)
	}
}

func TestCompactReclaimsDeletedAndExpired(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Unix(1_700_000_000, 0).UnixNano())
	opts := smallOpts()
	opts.Now = func() time.Time { return time.Unix(0, clock.Load()) }
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()

	val := bytes.Repeat([]byte("v"), 100)
	for round := 0; round < 3; round++ {
		for i := 0; i < 500; i++ {
			db.Put([]byte(fmt.Sprintf("k%03d", i)), val)
		}
	}
	for i := 0; i < 400; i++ {
		db.Delete([]byte(fmt.Sprintf("k%03d", i)))
	}
	db.PutTTL([]byte("k450"), val, time.Millisecond) // expired by the time we compact
	clock.Add(int64(time.Second))

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	if len(st.Tables) != 1 || st.Tables[0].Keys != 99 {
		t.Fatalf("after Compact: %+v, want one table with 99 keys", st.Tables)
	}
	if n, _ := db.Count(); n != 99 {
		t.Fatalf("Count = %d, want 99", n)
	}
}

func TestBackgroundCompactionBoundsTableCount(t *testing.T) {
	opts := smallOpts()
	opts.CompactionTrigger = 4
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()

	val := bytes.Repeat([]byte("v"), 64)
	for i := 0; i < 20000; i++ {
		db.Put([]byte(fmt.Sprintf("k%06d", i)), val)
	}
	db.Flush()
	deadline := time.Now().Add(10 * time.Second)
	for len(db.Stats().Tables) >= opts.CompactionTrigger {
		if time.Now().After(deadline) {
			t.Fatalf("table count stuck at %d", len(db.Stats().Tables))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n, _ := db.Count(); n != 20000 {
		t.Fatalf("Count = %d", n)
	}
}

func TestPickCompaction(t *testing.T) {
	cases := []struct {
		sizes      []int64
		start, end int
	}{
		{[]int64{10, 10, 10}, -1, -1},           // below trigger
		{[]int64{10, 10, 10, 10}, 0, 4},         // newer data is 3x the oldest: full merge
		{[]int64{10, 10, 30, 100}, 0, 2},        // equal-sized newest pair
		{[]int64{10, 25, 60, 100}, 0, 2},        // no similar run: merge newest to get under trigger
		{[]int64{5, 40, 41, 100, 400}, 1, 3},    // similar-sized run in the middle
		{[]int64{1, 2, 4, 8, 16, 32, 64}, 0, 5}, // too many tables
	}
	for _, c := range cases {
		s, e := pickCompaction(c.sizes, 4)
		if s != c.start || e != c.end {
			t.Errorf("pickCompaction(%v) = [%d,%d), want [%d,%d)", c.sizes, s, e, c.start, c.end)
		}
	}
}

func TestUpdateIsAtomic(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				err := db.Update([]byte("counter"), func(cur *Item) (*Item, bool, error) {
					n := 0
					if cur != nil {
						fmt.Sscan(string(cur.Value), &n)
					}
					return &Item{Value: []byte(fmt.Sprint(n + 1))}, true, nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	mustGet(t, db, "counter", "2000")
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				k := []byte(fmt.Sprintf("w%d-%d", w, i%300))
				if err := db.Put(k, k); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		go func() {
			for !stop.Load() {
				k := []byte(fmt.Sprintf("w%d-%d", rand.Intn(4), rand.Intn(300)))
				if v, err := db.Get(k); err == nil && !bytes.Equal(v, k) {
					t.Errorf("Get(%s) = %s", k, v)
				}
				db.Scan(k, func([]byte, Item) bool { return false })
			}
		}()
	}
	wg.Wait()
	stop.Store(true)
	if n, _ := db.Count(); n != 1200 {
		t.Fatalf("Count = %d, want 1200", n)
	}
}

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, Options{})
	defer db.Close()
	if db2, err := Open(dir, Options{}); err == nil {
		db2.Close()
		t.Fatal("second Open of the same directory succeeded")
	}
}

func TestClosedDB(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{})
	db.Close()
	if err := db.Put([]byte("a"), []byte("b")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close = %v", err)
	}
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v", err)
	}
}

func BenchmarkPut(b *testing.B) {
	db := mustOpen(b, b.TempDir(), Options{SyncMode: wal.SyncNever})
	defer db.Close()
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("key-%010d", i)), val)
	}
}

func BenchmarkGet(b *testing.B) {
	db := mustOpen(b, b.TempDir(), Options{SyncMode: wal.SyncNever})
	defer db.Close()
	const n = 200000
	val := bytes.Repeat([]byte("v"), 100)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key-%010d", i)), val)
	}
	db.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key-%010d", i%n)))
	}
}

func TestTxnReadsOwnWritesAndIsAtomic(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()

	err := db.Txn(func(tx *Txn) error {
		tx.Set([]byte("a"), []byte("1"), 0)
		if it, err := tx.Get([]byte("a")); err != nil || string(it.Value) != "1" {
			t.Errorf("tx.Get(a) = %q, %v", it.Value, err)
		}
		tx.Delete([]byte("a"))
		if _, err := tx.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
			t.Errorf("tx.Get after Delete = %v", err)
		}
		tx.Set([]byte("b"), []byte("2"), 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustMissing(t, db, "a")
	mustGet(t, db, "b", "2")

	// An error aborts every staged write.
	boom := errors.New("boom")
	if err := db.Txn(func(tx *Txn) error {
		tx.Set([]byte("c"), []byte("3"), 0)
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Txn error = %v", err)
	}
	mustMissing(t, db, "c")

	// Concurrent transfers between two keys keep the total constant.
	db.Put([]byte("x"), []byte("1000"))
	db.Put([]byte("y"), []byte("0"))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				db.Txn(func(tx *Txn) error {
					var x, y int
					a, _ := tx.Get([]byte("x"))
					b, _ := tx.Get([]byte("y"))
					fmt.Sscan(string(a.Value), &x)
					fmt.Sscan(string(b.Value), &y)
					tx.Set([]byte("x"), []byte(fmt.Sprint(x-1)), 0)
					tx.Set([]byte("y"), []byte(fmt.Sprint(y+1)), 0)
					return nil
				})
			}
		}()
	}
	wg.Wait()
	mustGet(t, db, "x", "200")
	mustGet(t, db, "y", "800")
}

func TestCompactionFilter(t *testing.T) {
	opts := smallOpts()
	opts.CompactionFilter = func(key []byte, _ Item) bool { return bytes.HasPrefix(key, []byte("tmp:")) }
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()
	for i := 0; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("tmp:%d", i)), []byte("x"))
		db.Put([]byte(fmt.Sprintf("keep:%d", i)), []byte("x"))
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	if len(st.Tables) != 1 || st.Tables[0].Keys != 200 {
		t.Fatalf("after Compact: %+v, want one table with 200 keys", st.Tables)
	}
	mustMissing(t, db, "tmp:7")
	mustGet(t, db, "keep:7", "x")
}
