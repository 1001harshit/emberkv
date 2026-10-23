package keyspace

import (
	"fmt"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

// drain pushes n elements onto a list and a sorted set, then pops them all,
// the pattern redis-benchmark uses, and returns how long it took.
func drain(t *testing.T, n int) time.Duration {
	s := open(t, lsm.Options{History: time.Hour, MemtableSize: 1 << 20})
	start := time.Now()
	for i := 0; i < n; i++ {
		s.Push(b("q"), bs(fmt.Sprint(i)), true, false)
		s.ZAdd(b("z"), []ScoredMember{{b(fmt.Sprint(i)), float64(i)}}, ZAddOptions{})
	}
	for i := 0; i < n; i++ {
		v, _, err := s.Pop(b("q"), false, 1)
		if err != nil || len(v) != 1 || string(v[0]) != fmt.Sprint(i) {
			t.Fatalf("RPOP %d = %q, %v", i, v, err)
		}
		m, err := s.ZPop(b("z"), false, 1)
		if err != nil || len(m) != 1 || m[0].Score != float64(i) {
			t.Fatalf("ZPOPMIN %d = %v, %v", i, m, err)
		}
	}
	return time.Since(start)
}

// TestQueuePatternsStayLinear checks that popping does not step over the
// entries deleted by earlier pops: if it did, draining 4x as many elements
// would take about 16x as long rather than 4x.
func TestQueuePatternsStayLinear(t *testing.T) {
	small, large := drain(t, 3000), drain(t, 12000)
	if ratio := float64(large) / float64(small); ratio > 9 {
		t.Fatalf("draining 4x the elements took %.1fx as long (%v vs %v): popping is rescanning", ratio, large, small)
	}
}
