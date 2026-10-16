package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/1001harshit/emberkv/internal/base"
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
		e, ok, err := r.Get(key(i * 2))
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
		if _, ok, _ := r.Get(key(i*2 + 1)); ok {
			t.Fatalf("found absent key %s", key(i*2+1))
		}
	}
	if _, ok, _ := r.Get([]byte("a")); ok {
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
