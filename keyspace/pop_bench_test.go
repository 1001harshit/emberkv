package keyspace

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/wal"
	"github.com/1001harshit/emberkv/lsm"
)

// BenchmarkPop pops from one list that many clients share, as
// redis-benchmark's LPOP and RPOP tests do: 50 clients, with the list
// filled by the same number of pushes first.
func BenchmarkPop(b *testing.B) {
	for _, h := range []time.Duration{time.Hour, 0} {
		b.Run("history="+h.String(), func(b *testing.B) {
			s, err := Open(b.TempDir(), lsm.Options{History: h, SyncMode: wal.SyncNever})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			v := make([]byte, 64)
			run := func(op func()) {
				var left atomic.Int64
				left.Store(int64(b.N))
				var wg sync.WaitGroup
				for c := 0; c < 50; c++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for left.Add(-1) >= 0 {
							op()
						}
					}()
				}
				wg.Wait()
			}
			run(func() { s.Push([]byte("mylist"), [][]byte{v}, true, false) })
			b.ResetTimer()
			run(func() { s.Pop([]byte("mylist"), true, 1) })
		})
	}
}
