package bloom

import (
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	var hashes []uint64
	for i := 0; i < 10000; i++ {
		hashes = append(hashes, Hash([]byte(fmt.Sprintf("key-%d", i))))
	}
	f, err := Unmarshal(Build(hashes, 10).Marshal())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		if !f.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	var hashes []uint64
	for i := 0; i < 10000; i++ {
		hashes = append(hashes, Hash([]byte(fmt.Sprintf("key-%d", i))))
	}
	f := Build(hashes, 10)
	fp := 0
	const probes = 100000
	for i := 0; i < probes; i++ {
		if f.MayContain([]byte(fmt.Sprintf("other-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / probes
	t.Logf("false-positive rate at 10 bits/key: %.3f%%", rate*100)
	if rate > 0.02 {
		t.Fatalf("false-positive rate %.3f too high", rate)
	}
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {1}, {0, 0}, {0, 99}} {
		if _, err := Unmarshal(b); err == nil {
			t.Errorf("Unmarshal(%v) succeeded", b)
		}
	}
}
