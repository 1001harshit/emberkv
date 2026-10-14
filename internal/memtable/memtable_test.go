package memtable

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/1001harshit/emberkv/internal/base"
)

func set(k, v string) base.Entry {
	return base.Entry{Key: []byte(k), Value: []byte(v), Kind: base.KindSet}
}

func TestPutGetOverwrite(t *testing.T) {
	m := New()
	m.Put(set("a", "1"))
	m.Put(set("b", "2"))
	m.Put(set("a", "3"))
	m.Put(base.Entry{Key: []byte("c"), Kind: base.KindDelete})

	if e, ok := m.Get([]byte("a")); !ok || string(e.Value) != "3" {
		t.Fatalf("Get(a) = %q, %v", e.Value, ok)
	}
	if e, ok := m.Get([]byte("c")); !ok || e.Kind != base.KindDelete {
		t.Fatalf("tombstone not stored")
	}
	if _, ok := m.Get([]byte("z")); ok {
		t.Fatal("Get(z) found a missing key")
	}
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
}

func TestPutCopiesBuffers(t *testing.T) {
	m := New()
	k, v := []byte("key"), []byte("val")
	m.Put(base.Entry{Key: k, Value: v, Kind: base.KindSet})
	k[0], v[0] = 'X', 'X'
	if e, ok := m.Get([]byte("key")); !ok || string(e.Value) != "val" {
		t.Fatal("memtable aliased caller's buffers")
	}
}

func TestIteratorIsSortedAndSeeks(t *testing.T) {
	m := New()
	rnd := rand.New(rand.NewSource(1))
	want := map[string]bool{}
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("k%05d", rnd.Intn(5000))
		want[k] = true
		m.Put(set(k, "v"))
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var got []string
	for it := m.Iterator(nil); it.Next(); {
		got = append(got, string(it.Entry().Key))
	}
	if fmt.Sprint(got) != fmt.Sprint(keys) {
		t.Fatal("full iteration is not the sorted key set")
	}

	start := keys[len(keys)/2]
	it := m.Iterator([]byte(start))
	if !it.Next() || string(it.Entry().Key) != start {
		t.Fatalf("seek to %s landed on %s", start, it.Entry().Key)
	}
}
