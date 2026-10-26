package server

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

// waitBlocked waits until INFO reports n blocked clients.
func (c *client) waitBlocked(n int) {
	c.t.Helper()
	want := "blocked_clients:" + strconv.Itoa(n)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(c.do("INFO", "clients").(string), want+"\r\n") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("never reached %s", want)
}

// expectNext checks the next reply on a connection that sent a command
// without waiting for it.
func (c *client) expectNext(want any) {
	c.t.Helper()
	if got := c.next(); !reflect.DeepEqual(got, want) {
		c.t.Fatalf("reply = %#v, want %#v", got, want)
	}
}

func TestBlockingPopsWithoutBlocking(t *testing.T) {
	c := newClient(t)
	c.do("RPUSH", "q", "a", "b")
	c.expect(list("q", "a"), "BLPOP", "missing", "q", "0")
	c.expect(list("q", "b"), "BRPOP", "q", "0")

	start := time.Now()
	c.expect(nil, "BLPOP", "q", "0.1")
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("BLPOP with a 0.1s timeout returned after %v", d)
	}
	c.expect(nil, "BLMOVE", "q", "d", "LEFT", "RIGHT", "0.01")

	c.expectErr("ERR timeout is negative", "BLPOP", "q", "-1")
	c.expectErr("ERR timeout is not a float or out of range", "BLPOP", "q", "soon")
	c.expectErr("ERR timeout is not a float or out of range", "BLPOP", "q", "inf")
	c.expectErr("ERR syntax error", "BLMOVE", "q", "d", "UP", "LEFT", "0")
	c.do("SET", "s", "x")
	c.expectErr("WRONGTYPE", "BLPOP", "s", "0")
}

func TestBlockedClientIsServedByPush(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	waiting, pusher := dial(t, addr), dial(t, addr)

	waiting.send("BLPOP", "q1", "q2", "0")
	pusher.waitBlocked(1)
	pusher.expect(int64(1), "RPUSH", "q2", "x")
	waiting.expectNext(list("q2", "x"))
	pusher.expect(int64(0), "LLEN", "q2") // the element went to the waiter
	pusher.waitBlocked(0)
	waiting.expect("PONG", "PING") // the connection works normally again
}

func TestBlockedClientsAreServedInOrder(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	a, b, c, pusher := dial(t, addr), dial(t, addr), dial(t, addr), dial(t, addr)
	for i, cl := range []*client{a, b, c} {
		cl.send("BRPOP", "q", "0")
		pusher.waitBlocked(i + 1)
	}
	pusher.expect(int64(2), "LPUSH", "q", "1", "2")
	a.expectNext(list("q", "1"))
	b.expectNext(list("q", "2"))
	pusher.waitBlocked(1)
	pusher.do("LPUSH", "q", "3")
	c.expectNext(list("q", "3"))
}

func TestBLMoveChainsToWaitersOnDestination(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	onDst, mover, pusher := dial(t, addr), dial(t, addr), dial(t, addr)
	onDst.send("BLPOP", "dst", "0")
	pusher.waitBlocked(1)
	mover.send("BLMOVE", "src", "dst", "RIGHT", "LEFT", "0")
	pusher.waitBlocked(2)

	pusher.do("LPUSH", "src", "v")
	mover.expectNext("v")
	onDst.expectNext(list("dst", "v"))
	pusher.expect(int64(0), "EXISTS", "src", "dst")

	pusher.do("RPUSH", "src", "a", "b")
	pusher.expect("b", "BRPOPLPUSH", "src", "dst", "0")
	pusher.expect(list("b"), "LRANGE", "dst", "0", "-1")
}

func TestBlockingInsideTransactions(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	waiting, tx := dial(t, addr), dial(t, addr)

	// Inside EXEC a blocking pop never blocks.
	tx.do("MULTI")
	tx.do("BLPOP", "empty", "0")
	tx.expect(list(nil), "EXEC")

	// A waiter is served after the transaction, so the transaction sees
	// its own push.
	waiting.send("BLPOP", "q", "0")
	tx.waitBlocked(1)
	tx.do("MULTI")
	tx.do("RPUSH", "q", "mine", "theirs")
	tx.do("LPOP", "q")
	tx.expect(list(int64(2), "mine"), "EXEC")
	waiting.expectNext(list("q", "theirs"))
}

func TestDisconnectedWaiterTakesNothing(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	waiting, pusher := dial(t, addr), dial(t, addr)
	waiting.send("BLPOP", "q", "0")
	pusher.waitBlocked(1)
	waiting.conn.Close()
	pusher.waitBlocked(0)
	pusher.do("RPUSH", "q", "x")
	pusher.expect(int64(1), "LLEN", "q")
}

func TestCommandsPipelinedBehindABlockingOne(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	waiting, pusher := dial(t, addr), dial(t, addr)
	waiting.w.WriteCommand("BLPOP", "q", "0")
	waiting.w.WriteCommand("PING")
	waiting.w.Flush()
	pusher.waitBlocked(1)
	pusher.do("RPUSH", "q", "x")
	waiting.expectNext(list("q", "x"))
	waiting.expectNext("PONG")

	// A client still blocked when the server closes must not hold it up;
	// startServer's cleanup closes it.
	waiting.send("BLPOP", "q", "0")
	pusher.waitBlocked(1)
}

func TestBlockingAfterRollback(t *testing.T) {
	_, addr := startServer(t, lsm.Options{History: time.Hour})
	waiting, other := dial(t, addr), dial(t, addr)
	other.do("RPUSH", "q", "x")
	time.Sleep(20 * time.Millisecond)
	before := time.Now().UnixMilli()
	time.Sleep(20 * time.Millisecond)
	other.do("DEL", "q")

	waiting.send("BLPOP", "q", "0")
	other.waitBlocked(1)
	other.do("ROLLBACK", strconv.FormatInt(before, 10))
	waiting.expectNext(list("q", "x"))
}
