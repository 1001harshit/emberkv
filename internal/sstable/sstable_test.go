package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/1001harshit/emberkv/internal/base"
	"github.com/1001harshit/emberkv/internal/cache"
)

func key(i int) []byte { return []byte(fmt.Sprintf("key-%06d", i)) }

func buildTable(t *testing.T, n int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := Create(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		e := base.Entry{Key: key(i * 2), Value: []byte(fmt.Sprintf("value-%d", i)), Kind: base.KindSet}
		if i%10 == 0 {
			e = base.Entry{Key: key(i * 2), Kind: base.KindDelete}
		}
		if err := w.Add(&e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGet(t *testing.T) {
	const n = 20000 // spans many blocks
	r, err := Open(buildTable(t, n))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Count() != n {
		t.Fatalf("Count = %d", r.Count())
	}
	for i := 0; i < n; i++ {
		e, ok, err := r.Get(key(i*2), base.MaxVersion)
		if err != nil || !ok {
			t.Fatalf("Get(%s) = %v, %v", key(i*2), ok, err)
		}
		if i%10 == 0 {
			if e.Kind != base.KindDelete {
				t.Fatalf("expected tombstone at %d", i)
			}
		} else if string(e.Value) != fmt.Sprintf("value-%d", i) {
			t.Fatalf("Get(%s) = %q", key(i*2), e.Value)
		}
		// Odd keys fall between stored keys and must be absent.
		if _, ok, _ := r.Get(key(i*2+1), base.MaxVersion); ok {
			t.Fatalf("found absent key %s", key(i*2+1))
		}
	}
	if _, ok, _ := r.Get([]byte("a"), base.MaxVersion); ok {
		t.Fatal("found key before first block")
	}
}

func TestIterator(t *testing.T) {
	const n = 5000
	r, err := Open(buildTable(t, n))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	count := 0
	for it := r.Iterator(nil); it.Next(); count++ {
		if want := key(count * 2); string(it.Entry().Key) != string(want) {
			t.Fatalf("entry %d = %s, want %s", count, it.Entry().Key, want)
		}
	}
	if count != n {
		t.Fatalf("iterated %d entries, want %d", count, n)
	}

	// Seeking to an absent key starts at the next one.
	it := r.Iterator(key(2001))
	if !it.Next() || string(it.Entry().Key) != string(key(2002)) {
		t.Fatalf("seek landed on %s", it.Entry().Key)
	}
}

func TestOutOfOrderRejected(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "x.sst"), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	w.Add(&base.Entry{Key: []byte("b"), Kind: base.KindSet})
	if err := w.Add(&base.Entry{Key: []byte("a"), Kind: base.KindSet}); err == nil {
		t.Fatal("out-of-order key accepted")
	}
}

func TestCorruptBlockDetected(t *testing.T) {
	path := buildTable(t, 1000)
	data, _ := os.ReadFile(path)
	data[100] ^= 0xff // inside the first data block
	os.WriteFile(path, data, 0o644)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err) // index and footer are intact
	}
	defer r.Close()
	it := r.Iterator(nil)
	for it.Next() {
	}
	if !errors.Is(it.Err(), base.ErrCorrupt) {
		t.Fatalf("iterator error = %v, want ErrCorrupt", it.Err())
	}
}

func TestBadMagicRejected(t *testing.T) {
	path := buildTable(t, 10)
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	os.WriteFile(path, data, 0o644)
	if _, err := Open(path); err == nil {
		t.Fatal("opened a table with a bad footer")
	}
}

func TestBlockCache(t *testing.T) {
	r, err := Open(buildTable(t, 5000))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := cache.New(1 << 20)
	r.SetCache(7, c)

	for round := 0; round < 2; round++ {
		for i := 0; i < 5000; i += 50 {
			if _, ok, err := r.Get(key(i*2), base.MaxVersion); err != nil || !ok {
				t.Fatalf("Get(%s) = %v, %v", key(i*2), ok, err)
			}
		}
	}
	hits, misses, _ := c.Stats()
	if misses == 0 || hits < misses {
		t.Fatalf("second pass should hit the cache: %d hits, %d misses", hits, misses)
	}

	// Scans read through the cache but do not fill it.
	c2 := cache.New(1 << 20)
	r.SetCache(8, c2)
	for it := r.Iterator(nil); it.Next(); {
	}
	if _, _, size := c2.Stats(); size != 0 {
		t.Fatalf("a full scan cached %d bytes", size)
	}
}

// TestVersions writes many versions per key, enough that one key's versions
// span several blocks, and reads at different points in time.
func TestVersions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000002.sst")
	w, err := Create(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	const keys, versions = 50, 100
	big := make([]byte, 200)
	for k := 0; k < keys; k++ {
		for v := versions; v >= 1; v-- { // newest first, as tables store them
			e := base.Entry{Key: key(k), Value: append([]byte(fmt.Sprintf("%d:%d:", k, v)), big...),
				Kind: base.KindSet, Version: uint64(v * 10)}
			if err := w.Add(&e); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if len(r.index) < keys*2 {
		t.Fatalf("only %d blocks; versions should span blocks", len(r.index))
	}
	for k := 0; k < keys; k++ {
		for _, asOf := range []uint64{5, 10, 15, 500, 555, 1000, base.MaxVersion} {
			e, ok, err := r.Get(key(k), asOf)
			if err != nil {
				t.Fatal(err)
			}
			want := int(min(asOf, 1000) / 10)
			if want == 0 {
				if ok {
					t.Fatalf("Get(%d, %d) found version %d before the first write", k, asOf, e.Version)
				}
				continue
			}
			if !ok || e.Version != uint64(want*10) || !strings.HasPrefix(string(e.Value), fmt.Sprintf("%d:%d:", k, want)) {
				t.Fatalf("Get(%d, %d) = version %d, %v; want version %d", k, asOf, e.Version, ok, want*10)
			}
		}
	}
	// A key whose versions are all newer than asOf is not visible, even
	// though older keys exist in the table.
	if _, ok, _ := r.Get(key(keys), base.MaxVersion); ok {
		t.Fatal("found a key that was never written")
	}
}

func TestVersionOrderEnforced(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "x.sst"), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	w.Add(&base.Entry{Key: []byte("a"), Kind: base.KindSet, Version: 5})
	if err := w.Add(&base.Entry{Key: []byte("a"), Kind: base.KindSet, Version: 9}); err == nil {
		t.Fatal("older-first versions accepted")
	}
}
