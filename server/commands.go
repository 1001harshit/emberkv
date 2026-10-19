package server

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/lsm"
)

type ctx struct {
	s    *Server
	w    *resp.Writer
	args [][]byte
	quit bool
}

type command struct {
	fn func(*ctx)
	// arity follows Redis: N means exactly N arguments including the command
	// name, -N means at least N.
	arity int
}

var commands map[string]command

func init() {
	commands = map[string]command{
		"ping":      {cmdPing, -1},
		"echo":      {cmdEcho, 2},
		"quit":      {cmdQuit, -1},
		"select":    {cmdSelect, 2},
		"get":       {cmdGet, 2},
		"set":       {cmdSet, -3},
		"setnx":     {cmdSetNX, 3},
		"setex":     {cmdSetEx(time.Second), 4},
		"psetex":    {cmdSetEx(time.Millisecond), 4},
		"getset":    {cmdGetSet, 3},
		"getdel":    {cmdGetDel, 2},
		"del":       {cmdDel, -2},
		"unlink":    {cmdDel, -2},
		"exists":    {cmdExists, -2},
		"incr":      {cmdIncr(1), 2},
		"decr":      {cmdIncr(-1), 2},
		"incrby":    {cmdIncrBy(1), 3},
		"decrby":    {cmdIncrBy(-1), 3},
		"append":    {cmdAppend, 3},
		"strlen":    {cmdStrlen, 2},
		"mget":      {cmdMGet, -2},
		"mset":      {cmdMSet, -3},
		"expire":    {cmdExpire(time.Second, false), 3},
		"pexpire":   {cmdExpire(time.Millisecond, false), 3},
		"expireat":  {cmdExpire(time.Second, true), 3},
		"pexpireat": {cmdExpire(time.Millisecond, true), 3},
		"ttl":       {cmdTTL(time.Second), 2},
		"pttl":      {cmdTTL(time.Millisecond), 2},
		"persist":   {cmdPersist, 2},
		"type":      {cmdType, 2},
		"keys":      {cmdKeys, 2},
		"scan":      {cmdScan, -2},
		"dbsize":    {cmdDBSize, 1},
		"flushdb":   {cmdFlushDB, -1},
		"flushall":  {cmdFlushDB, -1},
		"compact":   {cmdCompact, 1},
		"info":      {cmdInfo, -1},
		"time":      {cmdTime, 1},
		"command":   {cmdCommand, -1},
		"client":    {cmdClient, -2},
		"config":    {cmdConfig, -2},
	}
}

// replyErr is an error whose text is sent to the client verbatim.
type replyErr string

func (e replyErr) Error() string { return string(e) }

const (
	errSyntax     = replyErr("ERR syntax error")
	errNotInteger = replyErr("ERR value is not an integer or out of range")
	errOverflow   = replyErr("ERR increment or decrement would overflow")
)

func (c *ctx) db() *lsm.DB  { return c.s.db }
func (c *ctx) nowMs() int64 { return c.s.db.Now().UnixMilli() }

func (c *ctx) fail(err error) {
	var re replyErr
	if errors.As(err, &re) {
		c.w.WriteError(string(re))
		return
	}
	c.w.WriteError("ERR " + err.Error())
}

func (c *ctx) ok() { c.w.WriteSimple("OK") }

func (c *ctx) writeItem(item *lsm.Item) {
	if item == nil {
		c.w.WriteNull()
	} else {
		c.w.WriteBulk(item.Value)
	}
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, errNotInteger
	}
	return n, nil
}

// get returns the item for key, or nil if it does not exist.
func (c *ctx) get(key []byte) (*lsm.Item, error) {
	item, err := c.db().GetItem(key)
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// ---- connection ----

func cmdPing(c *ctx) {
	switch len(c.args) {
	case 1:
		c.w.WriteSimple("PONG")
	case 2:
		c.w.WriteBulk(c.args[1])
	default:
		c.w.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

func cmdEcho(c *ctx) { c.w.WriteBulk(c.args[1]) }

func cmdQuit(c *ctx) {
	c.ok()
	c.quit = true
}

func cmdSelect(c *ctx) {
	if string(c.args[1]) != "0" {
		c.w.WriteError("ERR DB index is out of range")
		return
	}
	c.ok()
}

// ---- strings ----

func cmdGet(c *ctx) {
	item, err := c.get(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.writeItem(item)
}

// expireAt converts a SET/EXPIRE argument to an absolute unix-ms deadline.
func expireAt(n int64, unit time.Duration, absolute bool, now int64) (int64, bool) {
	ms := n
	if unit == time.Second {
		if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
			return 0, false
		}
		ms = n * 1000
	}
	if absolute {
		return ms, true
	}
	if ms > 0 && now > math.MaxInt64-ms {
		return 0, false
	}
	return now + ms, true
}

func cmdSet(c *ctx) {
	key, val := c.args[1], c.args[2]
	var nx, xx, get, keepTTL, hasExpire bool
	var exp int64
	now := c.nowMs()
	for i := 3; i < len(c.args); i++ {
		switch opt := strings.ToUpper(string(c.args[i])); opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpire || i+1 >= len(c.args) {
				c.fail(errSyntax)
				return
			}
			n, err := parseInt(c.args[i+1])
			if err != nil {
				c.fail(err)
				return
			}
			unit := time.Second
			if opt[0] == 'P' {
				unit = time.Millisecond
			}
			var ok bool
			exp, ok = expireAt(n, unit, strings.HasSuffix(opt, "AT"), now)
			if n <= 0 || !ok {
				c.w.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			hasExpire = true
			i++
		default:
			c.fail(errSyntax)
			return
		}
	}
	if (nx && xx) || (keepTTL && hasExpire) {
		c.fail(errSyntax)
		return
	}

	var old *lsm.Item
	wrote := false
	err := c.db().Update(key, func(cur *lsm.Item) (*lsm.Item, bool, error) {
		old = cur
		if (nx && cur != nil) || (xx && cur == nil) {
			return nil, false, nil
		}
		next := &lsm.Item{Value: val, ExpireAt: exp}
		if keepTTL && cur != nil {
			next.ExpireAt = cur.ExpireAt
		}
		wrote = true
		return next, true, nil
	})
	switch {
	case err != nil:
		c.fail(err)
	case get:
		c.writeItem(old)
	case wrote:
		c.ok()
	default:
		c.w.WriteNull()
	}
}

func cmdSetNX(c *ctx) {
	var wrote int64
	err := c.db().Update(c.args[1], func(cur *lsm.Item) (*lsm.Item, bool, error) {
		if cur != nil {
			return nil, false, nil
		}
		wrote = 1
		return &lsm.Item{Value: c.args[2]}, true, nil
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(wrote)
}

func cmdSetEx(unit time.Duration) func(*ctx) {
	return func(c *ctx) {
		n, err := parseInt(c.args[2])
		if err != nil {
			c.fail(err)
			return
		}
		exp, ok := expireAt(n, unit, false, c.nowMs())
		if n <= 0 || !ok {
			c.w.WriteError("ERR invalid expire time in '" + strings.ToLower(string(c.args[0])) + "' command")
			return
		}
		if err := c.db().Write(new(lsm.Batch).Set(c.args[1], c.args[3], exp)); err != nil {
			c.fail(err)
			return
		}
		c.ok()
	}
}

func cmdGetSet(c *ctx) {
	var old *lsm.Item
	err := c.db().Update(c.args[1], func(cur *lsm.Item) (*lsm.Item, bool, error) {
		old = cur
		return &lsm.Item{Value: c.args[2]}, true, nil
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.writeItem(old)
}

func cmdGetDel(c *ctx) {
	var old *lsm.Item
	err := c.db().Update(c.args[1], func(cur *lsm.Item) (*lsm.Item, bool, error) {
		old = cur
		return nil, cur != nil, nil
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.writeItem(old)
}

func cmdDel(c *ctx) {
	var n int64
	for _, key := range c.args[1:] {
		err := c.db().Update(key, func(cur *lsm.Item) (*lsm.Item, bool, error) {
			if cur == nil {
				return nil, false, nil
			}
			n++
			return nil, true, nil
		})
		if err != nil {
			c.fail(err)
			return
		}
	}
	c.w.WriteInt(n)
}

func cmdExists(c *ctx) {
	var n int64
	for _, key := range c.args[1:] {
		item, err := c.get(key)
		if err != nil {
			c.fail(err)
			return
		}
		if item != nil {
			n++
		}
	}
	c.w.WriteInt(n)
}

func (c *ctx) incrBy(key []byte, delta int64) {
	var result int64
	err := c.db().Update(key, func(cur *lsm.Item) (*lsm.Item, bool, error) {
		var n, exp int64
		if cur != nil {
			v, err := parseInt(cur.Value)
			if err != nil {
				return nil, false, err
			}
			n, exp = v, cur.ExpireAt
		}
		if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
			return nil, false, errOverflow
		}
		result = n + delta
		return &lsm.Item{Value: strconv.AppendInt(nil, result, 10), ExpireAt: exp}, true, nil
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(result)
}

func cmdIncr(sign int64) func(*ctx) {
	return func(c *ctx) { c.incrBy(c.args[1], sign) }
}

func cmdIncrBy(sign int64) func(*ctx) {
	return func(c *ctx) {
		n, err := parseInt(c.args[2])
		if err != nil {
			c.fail(err)
			return
		}
		if sign < 0 {
			if n == math.MinInt64 {
				c.fail(errOverflow)
				return
			}
			n = -n
		}
		c.incrBy(c.args[1], n)
	}
}

func cmdAppend(c *ctx) {
	var length int
	err := c.db().Update(c.args[1], func(cur *lsm.Item) (*lsm.Item, bool, error) {
		next := &lsm.Item{}
		if cur != nil {
			next.Value = append(append([]byte{}, cur.Value...), c.args[2]...)
			next.ExpireAt = cur.ExpireAt
		} else {
			next.Value = c.args[2]
		}
		length = len(next.Value)
		return next, true, nil
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(int64(length))
}

func cmdStrlen(c *ctx) {
	item, err := c.get(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	if item == nil {
		c.w.WriteInt(0)
		return
	}
	c.w.WriteInt(int64(len(item.Value)))
}

func cmdMGet(c *ctx) {
	items := make([]*lsm.Item, 0, len(c.args)-1)
	for _, key := range c.args[1:] {
		item, err := c.get(key)
		if err != nil {
			c.fail(err)
			return
		}
		items = append(items, item)
	}
	c.w.WriteArray(len(items))
	for _, item := range items {
		c.writeItem(item)
	}
}

func cmdMSet(c *ctx) {
	if len(c.args)%2 != 1 {
		c.w.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	b := new(lsm.Batch)
	for i := 1; i < len(c.args); i += 2 {
		b.Set(c.args[i], c.args[i+1], 0)
	}
	if err := c.db().Write(b); err != nil {
		c.fail(err)
		return
	}
	c.ok()
}

// ---- expiry ----

func cmdExpire(unit time.Duration, absolute bool) func(*ctx) {
	return func(c *ctx) {
		n, err := parseInt(c.args[2])
		if err != nil {
			c.fail(err)
			return
		}
		now := c.nowMs()
		exp, ok := expireAt(n, unit, absolute, now)
		if !ok {
			c.w.WriteError("ERR invalid expire time in '" + strings.ToLower(string(c.args[0])) + "' command")
			return
		}
		var res int64
		err = c.db().Update(c.args[1], func(cur *lsm.Item) (*lsm.Item, bool, error) {
			if cur == nil {
				return nil, false, nil
			}
			res = 1
			if exp <= now {
				return nil, true, nil // a deadline in the past deletes the key
			}
			return &lsm.Item{Value: cur.Value, ExpireAt: exp}, true, nil
		})
		if err != nil {
			c.fail(err)
			return
		}
		c.w.WriteInt(res)
	}
}

func cmdTTL(unit time.Duration) func(*ctx) {
	return func(c *ctx) {
		item, err := c.get(c.args[1])
		switch {
		case err != nil:
			c.fail(err)
		case item == nil:
			c.w.WriteInt(-2)
		case item.ExpireAt == 0:
			c.w.WriteInt(-1)
		default:
			ms := max(item.ExpireAt-c.nowMs(), 0)
			if unit == time.Second {
				c.w.WriteInt((ms + 500) / 1000)
			} else {
				c.w.WriteInt(ms)
			}
		}
	}
}

func cmdPersist(c *ctx) {
	var res int64
	err := c.db().Update(c.args[1], func(cur *lsm.Item) (*lsm.Item, bool, error) {
		if cur == nil || cur.ExpireAt == 0 {
			return nil, false, nil
		}
		res = 1
		return &lsm.Item{Value: cur.Value}, true, nil
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(res)
}

// ---- keyspace ----

func cmdType(c *ctx) {
	item, err := c.get(c.args[1])
	switch {
	case err != nil:
		c.fail(err)
	case item == nil:
		c.w.WriteSimple("none")
	default:
		c.w.WriteSimple("string")
	}
}

func cmdKeys(c *ctx) {
	pattern := c.args[1]
	var keys [][]byte
	err := c.db().Scan(nil, func(key []byte, _ lsm.Item) bool {
		if globMatch(pattern, key) {
			keys = append(keys, key)
		}
		return true
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteArray(len(keys))
	for _, k := range keys {
		c.w.WriteBulk(k)
	}
}

func cmdScan(c *ctx) {
	cursor, err := strconv.ParseUint(string(c.args[1]), 10, 64)
	if err != nil {
		c.w.WriteError("ERR invalid cursor")
		return
	}
	var start []byte
	if cursor != 0 {
		k, ok := c.s.cursors.get(cursor)
		if !ok {
			c.w.WriteError("ERR invalid cursor")
			return
		}
		start = k
	}

	count := 10
	var match []byte
	onlyStrings := true
	for i := 2; i < len(c.args); i += 2 {
		if i+1 >= len(c.args) {
			c.fail(errSyntax)
			return
		}
		switch strings.ToUpper(string(c.args[i])) {
		case "MATCH":
			match = c.args[i+1]
		case "COUNT":
			n, err := parseInt(c.args[i+1])
			if err != nil {
				c.fail(err)
				return
			}
			if n < 1 {
				c.fail(errSyntax)
				return
			}
			count = int(min(n, 1<<20))
		case "TYPE":
			onlyStrings = strings.EqualFold(string(c.args[i+1]), "string")
		default:
			c.fail(errSyntax)
			return
		}
	}

	// COUNT bounds how many keys are examined, as in Redis; MATCH filters
	// afterwards, so a page may come back short or even empty.
	var next []byte
	var page [][]byte
	examined := 0
	err = c.db().Scan(start, func(key []byte, _ lsm.Item) bool {
		if examined == count {
			next = key
			return false
		}
		examined++
		if onlyStrings && (match == nil || globMatch(match, key)) {
			page = append(page, key)
		}
		return true
	})
	if err != nil {
		c.fail(err)
		return
	}
	nextCursor := "0"
	if next != nil {
		nextCursor = strconv.FormatUint(c.s.cursors.put(next), 10)
	}
	c.w.WriteArray(2)
	c.w.WriteBulkString(nextCursor)
	c.w.WriteArray(len(page))
	for _, k := range page {
		c.w.WriteBulk(k)
	}
}

func cmdDBSize(c *ctx) {
	n, err := c.db().Count()
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(int64(n))
}

func cmdFlushDB(c *ctx) {
	if len(c.args) > 2 {
		c.fail(errSyntax)
		return
	}
	var keys [][]byte
	if err := c.db().Scan(nil, func(key []byte, _ lsm.Item) bool {
		keys = append(keys, key)
		return true
	}); err != nil {
		c.fail(err)
		return
	}
	for len(keys) > 0 {
		n := min(len(keys), 1000)
		b := new(lsm.Batch)
		for _, k := range keys[:n] {
			b.Delete(k)
		}
		if err := c.db().Write(b); err != nil {
			c.fail(err)
			return
		}
		keys = keys[n:]
	}
	c.ok()
}

// ---- server ----

// cmdCompact is an emberkv extension: flush and fully compact the store,
// reclaiming space held by overwritten, deleted and expired keys.
func cmdCompact(c *ctx) {
	if err := c.db().Compact(); err != nil {
		c.fail(err)
		return
	}
	c.ok()
}

func cmdTime(c *ctx) {
	now := time.Now()
	c.w.WriteArray(2)
	c.w.WriteBulkString(strconv.FormatInt(now.Unix(), 10))
	c.w.WriteBulkString(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))
}

func cmdCommand(c *ctx) {
	if len(c.args) > 1 && strings.EqualFold(string(c.args[1]), "count") {
		c.w.WriteInt(int64(len(commands)))
		return
	}
	// redis-cli asks for COMMAND DOCS on startup; an empty reply is valid.
	c.w.WriteArray(0)
}

func cmdClient(c *ctx) {
	switch strings.ToUpper(string(c.args[1])) {
	case "GETNAME":
		c.w.WriteNull()
	case "ID":
		c.w.WriteInt(1)
	default: // SETNAME, SETINFO, ...
		c.ok()
	}
}

func cmdConfig(c *ctx) {
	if strings.EqualFold(string(c.args[1]), "get") {
		c.w.WriteArray(0)
		return
	}
	c.w.WriteError("ERR CONFIG " + strings.ToUpper(string(c.args[1])) + " is not supported")
}

func cmdInfo(c *ctx) {
	section := "all"
	if len(c.args) > 1 {
		section = strings.ToLower(string(c.args[1]))
	}
	want := func(name string) bool {
		return section == "all" || section == "default" || section == "everything" || section == name
	}

	var b strings.Builder
	line := func(format string, args ...any) {
		fmt.Fprintf(&b, format+"\r\n", args...)
	}
	if want("server") {
		line("# Server")
		line("redis_version:7.0.0")
		line("emberkv_version:%s", Version)
		line("go_version:%s", runtime.Version())
		line("os:%s %s", runtime.GOOS, runtime.GOARCH)
		line("process_id:%d", os.Getpid())
		line("uptime_in_seconds:%d", int(time.Since(c.s.started).Seconds()))
		line("")
	}
	if want("clients") {
		line("# Clients")
		line("connected_clients:%d", c.s.clientCount())
		line("")
	}
	if want("stats") {
		line("# Stats")
		line("total_connections_received:%d", c.s.totalConns.Load())
		line("total_commands_processed:%d", c.s.totalCmds.Load())
		line("")
	}
	if want("storage") {
		st := c.db().Stats()
		var bytes int64
		sizes := make([]string, len(st.Tables))
		for i, t := range st.Tables {
			bytes += t.Bytes
			sizes[i] = strconv.FormatInt(t.Bytes, 10)
		}
		line("# Storage")
		line("memtable_bytes:%d", st.MemtableBytes)
		line("memtable_keys:%d", st.MemtableKeys)
		line("immutable_memtables:%d", st.Immutable)
		line("sstables:%d", len(st.Tables))
		line("sstable_bytes:%d", bytes)
		line("sstable_sizes:%s", strings.Join(sizes, ","))
		line("flushes:%d", st.Flushes)
		line("compactions:%d", st.Compactions)
		line("")
	}
	if want("keyspace") {
		n, err := c.db().Count()
		if err != nil {
			c.fail(err)
			return
		}
		line("# Keyspace")
		if n > 0 {
			line("db0:keys=%d,expires=0,avg_ttl=0", n)
		}
	}
	c.w.WriteBulkString(b.String())
}
