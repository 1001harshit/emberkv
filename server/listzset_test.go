package server

import (
	"testing"
	"time"

	"github.com/1001harshit/emberkv/lsm"
)

func TestListCommands(t *testing.T) {
	c := newClient(t)
	c.expect(int64(3), "RPUSH", "q", "a", "b", "c")
	c.expect(int64(5), "LPUSH", "q", "y", "z") // z y a b c
	c.expect(list("z", "y", "a", "b", "c"), "LRANGE", "q", "0", "-1")
	c.expect(list("a", "b"), "LRANGE", "q", "2", "3")
	c.expect(list(), "LRANGE", "q", "10", "20")
	c.expect(list(), "LRANGE", "missing", "0", "-1")
	c.expect(int64(5), "LLEN", "q")
	c.expect("c", "LINDEX", "q", "-1")
	c.expect(nil, "LINDEX", "q", "99")
	c.expect("z", "LPOP", "q")
	c.expect("c", "RPOP", "q")
	c.expect(list("y", "a"), "LPOP", "q", "2")
	c.expect(list("b"), "RPOP", "q", "5")
	c.expect(int64(0), "EXISTS", "q") // emptied lists disappear
	c.expect(nil, "LPOP", "q")
	c.expect(nil, "LPOP", "q", "2") // null array for a missing key with a count
	c.expectErr("ERR value is out of range, must be positive", "LPOP", "q", "-1")

	c.do("RPUSH", "l", "1", "2", "3", "2", "1")
	c.expect(int64(0), "LPUSHX", "nope", "x")
	c.expect(int64(6), "RPUSHX", "l", "4")
	c.expect("OK", "LSET", "l", "0", "one")
	c.expectErr("ERR index out of range", "LSET", "l", "99", "x")
	c.expectErr("ERR no such key", "LSET", "nope", "0", "x")
	c.expect(int64(2), "LREM", "l", "0", "2")
	c.expect(list("one", "3", "1", "4"), "LRANGE", "l", "0", "-1")
	c.expect(int64(5), "LINSERT", "l", "BEFORE", "3", "2.5")
	c.expect(int64(-1), "LINSERT", "l", "AFTER", "nope", "x")
	c.expect(int64(0), "LINSERT", "nope", "AFTER", "x", "y")
	c.expect("OK", "LTRIM", "l", "1", "-2")
	c.expect(list("2.5", "3", "1"), "LRANGE", "l", "0", "-1")
	c.expect("1", "RPOPLPUSH", "l", "other")
	c.expect("2.5", "LMOVE", "l", "other", "LEFT", "RIGHT")
	c.expect(list("1", "2.5"), "LRANGE", "other", "0", "-1")
	c.expect("list", "TYPE", "other")
	c.do("SET", "s", "x")
	c.expectErr("WRONGTYPE", "LPUSH", "s", "a")
	c.expectErr("WRONGTYPE", "LMOVE", "l", "s", "LEFT", "LEFT")
}

func TestSortedSetCommands(t *testing.T) {
	c := newClient(t)
	c.expect(int64(3), "ZADD", "lb", "10", "ann", "20", "bob", "15", "cy")
	c.expect(int64(0), "ZADD", "lb", "12", "ann")
	c.expect(int64(1), "ZADD", "lb", "CH", "13", "ann")
	c.expect(int64(0), "ZADD", "lb", "NX", "99", "ann")
	c.expect(int64(0), "ZADD", "lb", "XX", "5", "nobody")
	c.expect(int64(0), "ZADD", "lb", "GT", "1", "ann") // lower: not applied
	c.expect("13", "ZSCORE", "lb", "ann")
	c.expect("16", "ZINCRBY", "lb", "3", "ann")
	c.expect("17.5", "ZADD", "lb", "INCR", "1.5", "ann")
	c.expect(nil, "ZADD", "lb", "NX", "INCR", "1", "ann")
	c.expect(list("cy", "ann", "bob"), "ZRANGE", "lb", "0", "-1")
	c.expect(list("bob", "20", "ann", "17.5"), "ZREVRANGE", "lb", "0", "1", "WITHSCORES")
	c.expect(list("cy", "ann"), "ZRANGEBYSCORE", "lb", "-inf", "(20")
	c.expect(list("ann"), "ZRANGEBYSCORE", "lb", "15", "+inf", "LIMIT", "1", "1")
	c.expect(list("bob", "ann"), "ZREVRANGEBYSCORE", "lb", "+inf", "(15")
	c.expect(list("bob", "ann"), "ZRANGE", "lb", "+inf", "(15", "BYSCORE", "REV")
	c.expect(int64(2), "ZCOUNT", "lb", "15", "17.5")
	c.expect(int64(1), "ZRANK", "lb", "ann")
	c.expect(int64(0), "ZREVRANK", "lb", "bob")
	c.expect(nil, "ZRANK", "lb", "nobody")
	c.expect(list("17.5", nil), "ZMSCORE", "lb", "ann", "nobody")
	c.expect(int64(3), "ZCARD", "lb")
	c.expect("zset", "TYPE", "lb")

	c.expectErr("ERR XX and NX options at the same time are not compatible", "ZADD", "lb", "NX", "XX", "1", "a")
	c.expectErr("ERR GT, LT, and/or NX options at the same time are not compatible", "ZADD", "lb", "GT", "LT", "1", "a")
	c.expectErr("ERR value is not a valid float", "ZADD", "lb", "abc", "a")
	c.expectErr("ERR syntax error", "ZADD", "lb", "NX", "1")
	c.expectErr("ERR wrong number of arguments", "ZADD", "lb", "1")
	c.expectErr("ERR INCR option supports a single increment-element pair", "ZADD", "lb", "INCR", "1", "a", "2", "b")
	c.expectErr("ERR min or max is not a float", "ZCOUNT", "lb", "x", "1")
	c.expectErr("ERR syntax error, LIMIT is only supported", "ZRANGE", "lb", "0", "1", "LIMIT", "0", "1")

	c.do("ZADD", "p", "1", "a", "2", "b", "3", "c", "4", "d")
	c.expect(list("a", "1"), "ZPOPMIN", "p")
	c.expect(list("d", "4", "c", "3"), "ZPOPMAX", "p", "2")
	c.expect(int64(1), "ZREM", "p", "b", "nope")
	c.expect(int64(0), "EXISTS", "p")
	c.do("ZADD", "r", "1", "a", "2", "b", "3", "c", "4", "d")
	c.expect(int64(2), "ZREMRANGEBYSCORE", "r", "(1", "3")
	c.expect(int64(1), "ZREMRANGEBYRANK", "r", "-1", "-1")
	c.expect(list("a", "1"), "ZRANGE", "r", "0", "-1", "WITHSCORES")
	c.do("ZADD", "inf", "+inf", "top", "-inf", "bottom", "0.1", "tenth")
	c.expect(list("bottom", "-inf", "tenth", "0.10000000000000001", "top", "inf"), "ZRANGE", "inf", "0", "-1", "WITHSCORES")
}

func TestListsAndZSetsAsOf(t *testing.T) {
	_, addr := startServer(t, lsm.Options{History: time.Hour})
	c := dial(t, addr)
	c.do("RPUSH", "q", "a", "b")
	c.do("ZADD", "z", "1", "x")
	time.Sleep(20 * time.Millisecond)
	c.do("LPOP", "q")
	c.do("ZADD", "z", "5", "x")
	c.expect(list("a", "b"), "ASOF", "-15ms", "LRANGE", "q", "0", "-1")
	c.expect("1", "ASOF", "-15ms", "ZSCORE", "z", "x")
	c.expect(list("b"), "LRANGE", "q", "0", "-1")
}
