package server

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

func TestAsOfAndHistory(t *testing.T) {
	var ns atomic.Int64
	ns.Store(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, ns.Load()) }
	tick := func(d time.Duration) time.Time { ns.Add(int64(d)); return now() }
	ms := func(t time.Time) string { return fmt.Sprint(t.UnixMilli()) }

	_, addr := startServer(t, lsm.Options{Now: now, History: time.Hour})
	c := dial(t, addr)

	c.do("SET", "greeting", "hello", "EX", "100")
	c.do("HSET", "user:1", "name", "ada")
	t1 := tick(time.Second)
	tick(time.Second)
	c.do("SET", "greeting", "bonjour")
	c.do("HSET", "user:1", "lang", "go")
	c.do("SADD", "tags", "x")
	t2 := tick(time.Second)
	tick(time.Second)
	c.do("FLUSHDB") // the accident
	tick(time.Second)

	c.expect(nil, "GET", "greeting")
	c.expect("hello", "ASOF", ms(t1), "GET", "greeting")
	c.expect("bonjour", "ASOF", ms(t2), "GET", "greeting")
	c.expect("hello", "ASOF", t1.UTC().Format(time.RFC3339Nano), "GET", "greeting")
	c.expect("bonjour", "ASOF", "-3s", "GET", "greeting")
	c.expect(list("name", "ada"), "ASOF", ms(t1), "HGETALL", "user:1")
	c.expect(list("lang", "go", "name", "ada"), "ASOF", ms(t2), "HGETALL", "user:1")
	c.expect(int64(3), "ASOF", ms(t2), "DBSIZE")
	c.expect(list("greeting", "tags", "user:1"), "ASOF", ms(t2), "KEYS", "*")
	c.expect(int64(0), "ASOF", ms(t1), "EXISTS", "tags")
	c.expect("set", "ASOF", ms(t2), "TYPE", "tags")
	// TTL is judged at the time being read: 100s from the write, 1s later.
	c.expect(int64(99), "ASOF", ms(t1), "TTL", "greeting")

	c.expectErr("ERR ASOF runs read commands only", "ASOF", ms(t1), "SET", "greeting", "x")
	c.expectErr("ERR ASOF runs read commands only", "ASOF", ms(t1), "FLUSHDB")
	c.expectErr("ERR time is older than the history window", "ASOF", "-2h", "GET", "greeting")
	c.expectErr("ERR invalid time", "ASOF", "yesterday", "GET", "greeting")
	c.expectErr("ERR wrong number of arguments for 'get'", "ASOF", ms(t1), "GET")

	h := c.do("HISTORY", "greeting").([]any)
	if len(h) != 3 {
		t.Fatalf("HISTORY greeting = %v", h)
	}
	types := []string{h[0].([]any)[1].(string), h[1].([]any)[1].(string), h[2].([]any)[1].(string)}
	if fmt.Sprint(types) != "[none string string]" || h[1].([]any)[2] != "bonjour" || h[2].([]any)[2] != "hello" {
		t.Fatalf("HISTORY greeting = %v", h)
	}
	if h[2].([]any)[0].(int64) >= h[1].([]any)[0].(int64) {
		t.Fatalf("HISTORY not newest first: %v", h)
	}
	c.expect(list(list(h[0].([]any)[0], "none", nil)), "HISTORY", "greeting", "COUNT", "1")

	info := c.do("INFO", "storage").(string)
	if !strings.Contains(info, "history_seconds:3600") || !strings.Contains(info, "oldest_readable_ms:") {
		t.Fatalf("INFO storage missing history fields:\n%s", info)
	}
}

func TestRollbackUndoesFlush(t *testing.T) {
	var ns atomic.Int64
	ns.Store(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, ns.Load()) }
	tick := func(d time.Duration) time.Time { ns.Add(int64(d)); return now() }

	_, addr := startServer(t, lsm.Options{Now: now, History: time.Hour})
	c := dial(t, addr)
	c.do("SET", "greeting", "hello", "EX", "500")
	c.do("HSET", "user:1", "name", "ada", "lang", "go")
	c.do("SADD", "tags", "a", "b")
	c.do("INCR", "visits")
	good := tick(time.Second)
	tick(time.Second)
	c.do("FLUSHDB")
	c.do("SET", "new-after-flush", "x")
	tick(time.Second)

	c.expect(int64(5), "ROLLBACK", fmt.Sprint(good.UnixMilli()))
	c.expect("hello", "GET", "greeting")
	c.expect(int64(497), "TTL", "greeting") // the expiry came back too
	c.expect(list("lang", "go", "name", "ada"), "HGETALL", "user:1")
	c.expect(list("a", "b"), "SMEMBERS", "tags")
	c.expect("1", "GET", "visits")
	c.expect(int64(0), "EXISTS", "new-after-flush")
	c.expect(int64(4), "DBSIZE")

	// Undo the rollback.
	tick(time.Second)
	c.expect(int64(5), "ROLLBACK", "-1500ms")
	c.expect(list("new-after-flush"), "KEYS", "*")
	c.expectErr("ERR time is older than the history window", "ROLLBACK", "-3h")
}
