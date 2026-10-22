package server

import (
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/keyspace"
	"github.com/1001harshit/emberkv/lsm"
)

type client struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer
}

func startServer(t *testing.T, opts lsm.Options) (*Server, string) {
	t.Helper()
	st, err := keyspace.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(st, nil)
	go s.Serve(ln)
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	return s, ln.Addr().String()
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn, r: resp.NewReader(conn), w: resp.NewWriter(conn)}
}

func newClient(t *testing.T) *client {
	_, addr := startServer(t, lsm.Options{})
	return dial(t, addr)
}

// do sends one command and returns the reply with bulk strings converted to
// Go strings, which makes expectations easy to write.
func (c *client) do(args ...string) any {
	c.t.Helper()
	c.w.WriteCommand(args...)
	if err := c.w.Flush(); err != nil {
		c.t.Fatal(err)
	}
	reply, err := c.r.ReadReply()
	if err != nil {
		c.t.Fatalf("%v: %v", args, err)
	}
	return normalize(reply)
}

func normalize(v any) any {
	switch v := v.(type) {
	case []byte:
		return string(v)
	case []any:
		for i := range v {
			v[i] = normalize(v[i])
		}
	}
	return v
}

func (c *client) expect(want any, args ...string) {
	c.t.Helper()
	if got := c.do(args...); !reflect.DeepEqual(got, want) {
		c.t.Fatalf("%s = %#v, want %#v", strings.Join(args, " "), got, want)
	}
}

func (c *client) expectErr(prefix string, args ...string) {
	c.t.Helper()
	got := c.do(args...)
	e, ok := got.(resp.Error)
	if !ok || !strings.HasPrefix(string(e), prefix) {
		c.t.Fatalf("%s = %#v, want error starting %q", strings.Join(args, " "), got, prefix)
	}
}

func list(items ...any) []any {
	if items == nil {
		return []any{}
	}
	return items
}

func TestBasicCommands(t *testing.T) {
	c := newClient(t)
	c.expect("PONG", "PING")
	c.expect("hi", "PING", "hi")
	c.expect("hello", "ECHO", "hello")
	c.expect(nil, "GET", "missing")
	c.expect("OK", "SET", "name", "ember")
	c.expect("ember", "GET", "name")
	c.expect(int64(5), "STRLEN", "name")
	c.expect(int64(9), "APPEND", "name", "kv!!")
	c.expect("emberkv!!", "GET", "name")
	c.expect("string", "TYPE", "name")
	c.expect("none", "TYPE", "nope")
	c.expect(int64(2), "EXISTS", "name", "name", "nope")
	c.expect(int64(1), "DEL", "name", "nope")
	c.expect(int64(0), "EXISTS", "name")
	c.expect("OK", "SELECT", "0")
	c.expectErr("ERR DB index", "SELECT", "1")
}

func TestSetOptions(t *testing.T) {
	c := newClient(t)
	c.expect("OK", "SET", "k", "v1", "NX")
	c.expect(nil, "SET", "k", "v2", "NX")
	c.expect("v1", "GET", "k")
	c.expect(nil, "SET", "other", "v", "XX")
	c.expect("OK", "SET", "k", "v3", "XX")
	c.expect("v3", "SET", "k", "v4", "GET")
	c.expect(nil, "SET", "fresh", "x", "GET")
	c.expect(int64(1), "SETNX", "n", "1")
	c.expect(int64(0), "SETNX", "n", "2")
	c.expect("1", "GETSET", "n", "3")
	c.expect("3", "GETDEL", "n")
	c.expect(nil, "GET", "n")

	c.expectErr("ERR syntax error", "SET", "k", "v", "NX", "XX")
	c.expectErr("ERR syntax error", "SET", "k", "v", "BOGUS")
	c.expectErr("ERR invalid expire time", "SET", "k", "v", "EX", "0")
	c.expectErr("ERR value is not an integer", "SET", "k", "v", "EX", "soon")
	c.expectErr("ERR wrong number of arguments", "SET", "k")
	c.expect(resp.Error("ERR unknown command 'FROBNICATE', with args beginning with: "), "FROBNICATE")
	c.expect(resp.Error("ERR unknown command 'frob', with args beginning with: 'a' 'b c' "), "frob", "a", "b c")
}

func TestCounters(t *testing.T) {
	c := newClient(t)
	c.expect(int64(1), "INCR", "c")
	c.expect(int64(11), "INCRBY", "c", "10")
	c.expect(int64(10), "DECR", "c")
	c.expect(int64(-5), "DECRBY", "c", "15")
	c.expect("-5", "GET", "c")
	c.do("SET", "s", "abc")
	c.expectErr("ERR value is not an integer", "INCR", "s")
	c.do("SET", "big", "9223372036854775807")
	c.expectErr("ERR increment or decrement would overflow", "INCR", "big")
}

func TestExpiry(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	_, addr := startServer(t, lsm.Options{Now: func() time.Time { mu.Lock(); defer mu.Unlock(); return now }})
	c := dial(t, addr)

	c.expect("OK", "SET", "a", "1", "EX", "10")
	c.expect(int64(10), "TTL", "a")
	c.expect(int64(10000), "PTTL", "a")
	c.do("SET", "b", "2")
	c.expect(int64(-1), "TTL", "b")
	c.expect(int64(-2), "TTL", "nope")

	c.expect(int64(1), "EXPIRE", "b", "5")
	c.expect(int64(0), "EXPIRE", "nope", "5")
	c.expect(int64(1), "PERSIST", "b")
	c.expect(int64(-1), "TTL", "b")
	c.expect(int64(0), "PERSIST", "b")

	c.expect("OK", "SET", "a", "11", "KEEPTTL")
	c.expect(int64(10), "TTL", "a")
	c.expect(int64(12), "INCR", "a") // INCR keeps the TTL too
	c.expect(int64(10), "TTL", "a")

	c.expect("OK", "SETEX", "s", "3", "x")
	c.expect("OK", "PSETEX", "p", "1500", "y")
	advance(2 * time.Second)
	c.expect(nil, "GET", "p")
	c.expect("x", "GET", "s")
	c.expect(int64(1), "TTL", "s")
	advance(20 * time.Second)
	c.expect(nil, "GET", "a")
	c.expect(nil, "GET", "s")

	c.do("SET", "gone", "1")
	c.expect(int64(1), "EXPIRE", "gone", "-1") // a past deadline deletes the key
	c.expect(int64(0), "EXISTS", "gone")
}

func TestMultiKeyAndKeyspace(t *testing.T) {
	c := newClient(t)
	c.expect("OK", "MSET", "user:1", "ann", "user:2", "bob", "post:1", "hi")
	c.expect(list("ann", nil, "hi"), "MGET", "user:1", "user:9", "post:1")
	c.expectErr("ERR wrong number of arguments", "MSET", "a", "1", "b")
	c.expect(int64(3), "DBSIZE")

	keys := c.do("KEYS", "user:*").([]any)
	if !reflect.DeepEqual(keys, list("user:1", "user:2")) {
		t.Fatalf("KEYS user:* = %v", keys)
	}
	c.expect(list("post:1", "user:1", "user:2"), "KEYS", "*")

	c.expect("OK", "FLUSHDB")
	c.expect(int64(0), "DBSIZE")
	c.expect(list(), "KEYS", "*")
}

func TestScanVisitsEveryKeyOnce(t *testing.T) {
	c := newClient(t)
	want := map[string]bool{}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key:%04d", i)
		c.do("SET", k, "v")
		want[k] = true
	}
	c.do("SET", "other", "v")

	var got []string
	cursor := "0"
	for pages := 0; ; pages++ {
		reply := c.do("SCAN", cursor, "MATCH", "key:*", "COUNT", "37").([]any)
		cursor = reply[0].(string)
		for _, k := range reply[1].([]any) {
			got = append(got, k.(string))
		}
		if cursor == "0" {
			break
		}
		if pages > 100 {
			t.Fatal("SCAN did not terminate")
		}
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("SCAN returned %d keys, want %d", len(got), len(want))
	}
	for i := 1; i < len(got); i++ {
		if got[i] == got[i-1] {
			t.Fatalf("SCAN returned %s twice", got[i])
		}
	}
	c.expectErr("ERR invalid cursor", "SCAN", "999999")
}

func TestPipelining(t *testing.T) {
	c := newClient(t)
	const n = 1000
	for i := 0; i < n; i++ {
		c.w.WriteCommand("INCR", "pipelined")
	}
	c.w.Flush()
	for i := 1; i <= n; i++ {
		reply, err := c.r.ReadReply()
		if err != nil {
			t.Fatal(err)
		}
		if reply != int64(i) {
			t.Fatalf("reply %d = %v", i, reply)
		}
	}
}

func TestInlineCommands(t *testing.T) {
	c := newClient(t)
	fmt.Fprintf(c.conn, "SET greeting hello\r\nGET greeting\r\n\r\nPING\r\n")
	for _, want := range []any{"OK", []byte("hello"), "PONG"} {
		got, err := c.r.ReadReply()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	c := newClient(t)
	fmt.Fprintf(c.conn, "*1\r\n+notabulk\r\n")
	got, err := c.r.ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := got.(resp.Error); !ok || !strings.Contains(string(e), "Protocol error") && !strings.Contains(string(e), "protocol error") {
		t.Fatalf("got %#v, want a protocol error", got)
	}
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadReply(); err == nil {
		t.Fatal("connection still open after protocol error")
	}
}

func TestQuit(t *testing.T) {
	c := newClient(t)
	c.expect("OK", "QUIT")
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadReply(); err == nil {
		t.Fatal("connection still open after QUIT")
	}
}

func TestConcurrentClients(t *testing.T) {
	_, addr := startServer(t, lsm.Options{MemtableSize: 16 << 10})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := dial(t, addr)
			for j := 0; j < 200; j++ {
				k := fmt.Sprintf("c%d:%d", i, j)
				c.expect("OK", "SET", k, k)
				c.expect(k, "GET", k)
				c.do("INCR", "shared")
			}
		}(i)
	}
	wg.Wait()
	c := dial(t, addr)
	c.expect("4000", "GET", "shared")
	c.expect(int64(4001), "DBSIZE")
}

func TestInfoAndCompact(t *testing.T) {
	c := newClient(t)
	c.do("SET", "a", "1")
	c.expect("OK", "COMPACT")
	info := c.do("INFO").(string)
	for _, want := range []string{"# Server", "emberkv_version:", "# Storage", "sstables:1", "db0:keys=1"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO missing %q:\n%s", want, info)
		}
	}
	section := c.do("INFO", "clients").(string)
	if strings.Contains(section, "# Server") || !strings.Contains(section, "connected_clients:1") {
		t.Errorf("INFO clients = %q", section)
	}
	c.expect(list(), "COMMAND", "DOCS")
	c.expect("OK", "CLIENT", "SETNAME", "test")
}

func TestHashCommands(t *testing.T) {
	c := newClient(t)
	c.expect(int64(2), "HSET", "user:1", "name", "ada", "lang", "go")
	c.expect(int64(0), "HSET", "user:1", "name", "Ada")
	c.expect("Ada", "HGET", "user:1", "name")
	c.expect(nil, "HGET", "user:1", "nope")
	c.expect(nil, "HGET", "missing", "name")
	c.expect(list("Ada", nil, "go"), "HMGET", "user:1", "name", "x", "lang")
	c.expect(list("lang", "go", "name", "Ada"), "HGETALL", "user:1")
	c.expect(list("lang", "name"), "HKEYS", "user:1")
	c.expect(list("go", "Ada"), "HVALS", "user:1")
	c.expect(int64(2), "HLEN", "user:1")
	c.expect(int64(1), "HEXISTS", "user:1", "lang")
	c.expect(int64(0), "HEXISTS", "user:1", "zzz")
	c.expect(int64(3), "HSTRLEN", "user:1", "name")
	c.expect(int64(0), "HSETNX", "user:1", "name", "x")
	c.expect(int64(1), "HSETNX", "user:1", "age", "36")
	c.expect(int64(37), "HINCRBY", "user:1", "age", "1")
	c.expect(int64(5), "HINCRBY", "user:1", "visits", "5")
	c.expectErr("ERR hash value is not an integer", "HINCRBY", "user:1", "name", "1")
	c.expect("OK", "HMSET", "user:1", "a", "1", "b", "2")
	c.expectErr("ERR wrong number of arguments", "HSET", "user:1", "a")
	c.expect(int64(2), "HDEL", "user:1", "a", "b", "nope")
	c.expect("hash", "TYPE", "user:1")
	c.expect(list(), "HGETALL", "missing")
	c.expect(int64(0), "HLEN", "missing")
}

func TestSetCommands(t *testing.T) {
	c := newClient(t)
	c.expect(int64(3), "SADD", "s1", "a", "b", "c")
	c.expect(int64(1), "SADD", "s1", "a", "d")
	c.expect(list("a", "b", "c", "d"), "SMEMBERS", "s1")
	c.expect(int64(1), "SISMEMBER", "s1", "a")
	c.expect(int64(0), "SISMEMBER", "s1", "z")
	c.expect(list(int64(1), int64(0)), "SMISMEMBER", "s1", "b", "z")
	c.expect(int64(4), "SCARD", "s1")
	c.do("SADD", "s2", "c", "d", "e")
	c.expect(list("c", "d"), "SINTER", "s1", "s2")
	c.expect(list("a", "b", "c", "d", "e"), "SUNION", "s1", "s2")
	c.expect(list("a", "b"), "SDIFF", "s1", "s2")
	c.expect(list(), "SINTER", "s1", "missing")
	c.expect(int64(2), "SREM", "s1", "a", "b", "zz")
	c.expect("set", "TYPE", "s1")
	c.expect(int64(2), "SREM", "s1", "c", "d")
	c.expect(int64(0), "EXISTS", "s1") // emptied sets disappear
}

func TestTypesAcrossGenericCommands(t *testing.T) {
	c := newClient(t)
	c.do("SET", "str", "v")
	c.do("HSET", "hash", "f", "v")
	c.do("SADD", "set", "m")

	for _, cmd := range [][]string{
		{"GET", "hash"}, {"INCR", "set"}, {"APPEND", "hash", "x"}, {"HGET", "str", "f"},
		{"SADD", "hash", "m"}, {"HSET", "set", "f", "v"}, {"SMEMBERS", "str"}, {"SET", "hash", "v", "GET"},
	} {
		c.expectErr("WRONGTYPE", cmd...)
	}
	c.expect(list("v", nil, nil), "MGET", "str", "hash", "set")

	c.expect(int64(3), "EXISTS", "str", "hash", "set")
	c.expect(int64(3), "DBSIZE")
	c.expect(list("hash", "set", "str"), "KEYS", "*")
	reply := c.do("SCAN", "0", "TYPE", "hash").([]any)
	if !reflect.DeepEqual(reply[1], list("hash")) {
		t.Fatalf("SCAN TYPE hash = %v", reply[1])
	}

	// EXPIRE, TTL and PERSIST work on every type.
	c.expect(int64(1), "EXPIRE", "hash", "100")
	c.expect(int64(100), "TTL", "hash")
	c.expect(int64(1), "PERSIST", "hash")
	c.expect(int64(-1), "TTL", "hash")
	c.expect("v", "HGET", "hash", "f")

	// SET overwrites any type; DEL removes any type.
	c.expect("OK", "SET", "set", "now-a-string")
	c.expect("string", "TYPE", "set")
	c.expect(int64(1), "DEL", "hash")
	c.expect("none", "TYPE", "hash")
	c.expect(int64(0), "HLEN", "hash")

	c.expect("OK", "FLUSHDB")
	c.expect(int64(0), "DBSIZE")
}
