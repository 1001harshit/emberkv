package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/1001harshit/emberkv/keyspace"
)

// readCommands may run inside ASOF: they only read.
var readCommands = map[string]bool{
	"get": true, "mget": true, "strlen": true, "exists": true, "type": true, "ttl": true, "pttl": true,
	"keys": true, "scan": true, "dbsize": true,
	"hget": true, "hmget": true, "hgetall": true, "hkeys": true, "hvals": true, "hlen": true,
	"hexists": true, "hstrlen": true,
	"smembers": true, "sismember": true, "smismember": true, "scard": true,
	"sinter": true, "sunion": true, "sdiff": true,
}

// parseTime reads a point in time as one of:
//
//	-10m, -1h30m      relative to now (a negative Go duration)
//	1729640000000     unix milliseconds (or seconds, if below 1e11)
//	2026-10-23T01:00:00Z   RFC 3339
func parseTime(arg string, now time.Time) (time.Time, error) {
	if strings.HasPrefix(arg, "-") {
		d, err := time.ParseDuration(arg)
		if err != nil {
			return time.Time{}, err
		}
		return now.Add(d), nil
	}
	if n, err := strconv.ParseInt(arg, 10, 64); err == nil {
		if n < 1e11 {
			return time.Unix(n, 0), nil
		}
		return time.UnixMilli(n), nil
	}
	return time.Parse(time.RFC3339Nano, arg)
}

func (c *ctx) parseTimeArg(arg []byte) (time.Time, bool) {
	now := c.db().Now()
	t, err := parseTime(string(arg), now)
	if err != nil {
		c.w.WriteError("ERR invalid time '" + string(arg) + "': use -10m, unix milliseconds or RFC 3339")
		return time.Time{}, false
	}
	if oldest := c.db().OldestReadable(); t.Before(oldest) {
		c.w.WriteError("ERR time is older than the history window, which starts at " +
			oldest.UTC().Format(time.RFC3339) + " (see the -history flag)")
		return time.Time{}, false
	}
	if t.After(now) {
		t = now
	}
	return t, true
}

// cmdAsOf is an emberkv extension: ASOF <time> <read command> [args...]
// runs a read-only command against the data as it was at <time>.
func cmdAsOf(c *ctx) {
	at, ok := c.parseTimeArg(c.args[1])
	if !ok {
		return
	}
	name := strings.ToLower(string(c.args[2]))
	cmd, known := commands[name]
	if !known || !readCommands[name] {
		c.w.WriteError("ERR ASOF runs read commands only, and '" + name + "' is not one")
		return
	}
	args := c.args[2:]
	if !cmd.arityOK(len(args)) {
		c.w.WriteError("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	cmd.fn(&ctx{s: c.s, st: c.st.AsOf(at), w: c.w, args: args, at: at})
}

// cmdHistory is an emberkv extension: HISTORY <key> [COUNT n] lists the key's
// past versions, newest first, as [unix_ms, type, value] triples. The value
// is the string for strings, the number of fields or members for hashes and
// sets, and nil when the key was deleted.
func cmdHistory(c *ctx) {
	count := 10
	switch {
	case len(c.args) == 4 && strings.EqualFold(string(c.args[2]), "COUNT"):
		n, err := parseInt(c.args[3])
		if err != nil || n < 1 {
			c.fail(errSyntax)
			return
		}
		count = int(min(n, 10000))
	case len(c.args) != 2:
		c.fail(errSyntax)
		return
	}
	var changes []keyspace.Change
	err := c.store().History(c.args[1], func(ch keyspace.Change) bool {
		changes = append(changes, ch)
		return len(changes) < count
	})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteArray(len(changes))
	for _, ch := range changes {
		c.w.WriteArray(3)
		c.w.WriteInt(ch.At.UnixMilli())
		c.w.WriteBulkString(ch.Type.String())
		switch ch.Type {
		case keyspace.TypeNone:
			c.w.WriteNull()
		case keyspace.TypeString:
			c.w.WriteBulk(ch.Value)
		default:
			c.w.WriteInt(ch.Size)
		}
	}
}
