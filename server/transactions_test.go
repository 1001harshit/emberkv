package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/lsm"
)

func TestMultiExec(t *testing.T) {
	c := newClient(t)
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "SET", "a", "1")
	c.expect("QUEUED", "INCR", "a")
	c.expect("QUEUED", "LPUSH", "a", "x") // fails at run time: wrong type
	c.expect("QUEUED", "GET", "a")
	got := c.do("EXEC").([]any)
	if len(got) != 4 || got[0] != "OK" || got[1] != int64(2) || got[3] != "2" {
		t.Fatalf("EXEC = %#v", got)
	}
	if e, ok := got[2].(resp.Error); !ok || e[:9] != "WRONGTYPE" {
		t.Fatalf("EXEC[2] = %#v, want a WRONGTYPE error", got[2])
	}

	c.expectErr("ERR EXEC without MULTI", "EXEC")
	c.expectErr("ERR DISCARD without MULTI", "DISCARD")

	c.expect("OK", "MULTI")
	c.expectErr("ERR MULTI calls can not be nested", "MULTI")
	c.expect("QUEUED", "SET", "b", "1")
	c.expectErr("ERR unknown command", "NOSUCH")
	c.expectErr("ERR wrong number of arguments", "GET")
	c.expect(resp.Error("EXECABORT Transaction discarded because of previous errors."), "EXEC")
	c.expect(int64(0), "EXISTS", "b")

	c.expect("OK", "MULTI")
	c.expect("QUEUED", "SET", "b", "1")
	c.expect("OK", "DISCARD")
	c.expect(int64(0), "EXISTS", "b")

	c.expect("OK", "MULTI")
	c.expect(list(), "EXEC") // an empty transaction
}

func TestWatch(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	c, other := dial(t, addr), dial(t, addr)
	c.do("SET", "balance", "100")

	// Untouched watched key: the transaction runs.
	c.expect("OK", "WATCH", "balance")
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "DECRBY", "balance", "30")
	c.expect(list(int64(70)), "EXEC")

	// Another client writes the watched key: EXEC is cancelled.
	c.expect("OK", "WATCH", "balance")
	other.do("SET", "balance", "500")
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "DECRBY", "balance", "30")
	c.expect(nil, "EXEC")
	c.expect("500", "GET", "balance")

	// Watches on other types, and UNWATCH.
	c.do("RPUSH", "q", "a")
	c.expect("OK", "WATCH", "q")
	other.do("LSET", "q", "0", "b")
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "RPOP", "q")
	c.expect(nil, "EXEC")
	c.expect("OK", "WATCH", "q")
	other.do("RPUSH", "q", "c")
	c.expect("OK", "UNWATCH")
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "LLEN", "q")
	c.expect(list(int64(2)), "EXEC")

	c.expect("OK", "MULTI")
	c.expectErr("ERR WATCH inside MULTI is not allowed", "WATCH", "q")
	c.expect("OK", "DISCARD")
}

// TestExecIsIsolated moves "money" between two keys inside transactions
// while readers check, outside any transaction, that the total never
// changes: no reader may see a transaction half done.
func TestExecIsIsolated(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	setup := dial(t, addr)
	setup.do("MSET", "x", "1000", "y", "0")

	var stop atomic.Bool
	var torn atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, addr)
			for !stop.Load() {
				v := c.do("MGET", "x", "y").([]any)
				var a, b int
				fmt.Sscan(v[0].(string), &a)
				fmt.Sscan(v[1].(string), &b)
				if a+b != 1000 {
					torn.Add(1)
				}
			}
		}()
	}
	var movers sync.WaitGroup
	for m := 0; m < 4; m++ {
		movers.Add(1)
		go func() {
			defer movers.Done()
			c := dial(t, addr)
			for i := 0; i < 200; i++ {
				c.do("MULTI")
				c.do("DECR", "x")
				c.do("INCR", "y")
				c.do("EXEC")
			}
		}()
	}
	movers.Wait()
	stop.Store(true)
	wg.Wait()
	if torn.Load() != 0 {
		t.Fatalf("readers saw a half-applied transaction %d times", torn.Load())
	}
	setup.expect(list("200", "800"), "MGET", "x", "y")
}
