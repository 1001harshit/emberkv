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

	if e, ok := m.Get([]byte("a"), base.MaxVersion); !ok || string(e.Value) != "3" {
		t.Fatalf("Get(a) = %q, %v", e.Value, ok)
	}
	if e, ok := m.Get([]byte("c"), base.MaxVersion); !ok || e.Kind != base.KindDelete {
		t.Fatalf("tombstone not stored")
	}
	if _, ok := m.Get([]byte("z"), base.MaxVersion); ok {
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
	if e, ok := m.Get([]byte("key"), base.MaxVersion); !ok || string(e.Value) != "val" {
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

func TestVersions(t *testing.T) {
	m := New()
	for v := uint64(1); v <= 5; v++ {
		m.Put(base.Entry{Key: []byte("k"), Value: []byte{byte('0' + v)}, Kind: base.KindSet, Version: v * 10})
	}
	m.Put(base.Entry{Key: []byte("k"), Kind: base.KindDelete, Version: 60})
	m.Put(base.Entry{Key: []byte("j"), Value: []byte("other"), Kind: base.KindSet, Version: 35})

	cases := []struct {
		asOf uint64
		want string // "" = not found, "-" = tombstone
	}{
		{5, ""}, {10, "1"}, {19, "1"}, {20, "2"}, {35, "3"}, {50, "5"}, {59, "5"}, {60, "-"}, {base.MaxVersion, "-"},
	}
	for _, c := range cases {
		e, ok := m.Get([]byte("k"), c.asOf)
		got := ""
		if ok {
			got = string(e.Value)
			if e.Kind == base.KindDelete {
				got = "-"
			}
		}
		if got != c.want {
			t.Errorf("Get(k, %d) = %q, want %q", c.asOf, got, c.want)
		}
	}
	if m.Len() != 7 {
		t.Fatalf("Len = %d, want 7 versions", m.Len())
	}
	// Iteration yields every version, newest first within a key.
	var order []uint64
	for it := m.Iterator([]byte("k")); it.Next(); {
		order = append(order, it.Entry().Version)
	}
	if fmt.Sprint(order) != "[60 50 40 30 20 10]" {
		t.Fatalf("versions of k in order %v", order)
	}
}

func TestLatestOnly(t *testing.T) {
	m := NewLatestOnly()
	for v := uint64(1); v <= 100; v++ {
		m.Put(base.Entry{Key: []byte("hot"), Value: []byte(fmt.Sprint(v)), Kind: base.KindSet, Version: v})
	}
	m.Put(base.Entry{Key: []byte("hot"), Value: []byte("stale"), Kind: base.KindSet, Version: 50}) // older: ignored
	m.Put(base.Entry{Key: []byte("cold"), Value: []byte("x"), Kind: base.KindSet, Version: 7})
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (one entry per key)", m.Len())
	}
	if e, ok := m.Get([]byte("hot"), base.MaxVersion); !ok || string(e.Value) != "100" {
		t.Fatalf("Get(hot) = %q", e.Value)
	}
	m.Put(base.Entry{Key: []byte("hot"), Kind: base.KindDelete, Version: 101})
	if e, _ := m.Get([]byte("hot"), base.MaxVersion); e.Kind != base.KindDelete {
		t.Fatal("tombstone did not replace the value")
	}
}
