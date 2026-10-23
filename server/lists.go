package server

import (
	"strings"
)

var listCommands = map[string]command{
	"lpush":     {cmdPush(true, false), -3},
	"rpush":     {cmdPush(false, false), -3},
	"lpushx":    {cmdPush(true, true), -3},
	"rpushx":    {cmdPush(false, true), -3},
	"lpop":      {cmdPop(true), -2},
	"rpop":      {cmdPop(false), -2},
	"llen":      {cmdLLen, 2},
	"lrange":    {cmdLRange, 4},
	"lindex":    {cmdLIndex, 3},
	"lset":      {cmdLSet, 4},
	"ltrim":     {cmdLTrim, 4},
	"lrem":      {cmdLRem, 4},
	"linsert":   {cmdLInsert, 5},
	"lmove":     {cmdLMove, 5},
	"rpoplpush": {cmdRPopLPush, 3},
}

var errNotPositive = replyErr("ERR value is out of range, must be positive")

func cmdPush(left, onlyIfExists bool) func(*ctx) {
	return func(c *ctx) {
		n, err := c.store().Push(c.args[1], c.args[2:], left, onlyIfExists)
		if err != nil {
			c.fail(err)
			return
		}
		c.w.WriteInt(n)
	}
}

// cmdPop is LPOP/RPOP key [count]: a single element (or nil) without count,
// an array (or a null array for a missing key) with it.
func cmdPop(left bool) func(*ctx) {
	return func(c *ctx) {
		count, withCount := int64(1), len(c.args) == 3
		switch {
		case len(c.args) > 3:
			c.fail(errSyntax)
			return
		case withCount:
			n, err := parseInt(c.args[2])
			if err != nil || n < 0 {
				c.fail(errNotPositive)
				return
			}
			count = n
		}
		vals, exists, err := c.store().Pop(c.args[1], left, count)
		switch {
		case err != nil:
			c.fail(err)
		case !withCount && len(vals) == 0:
			c.w.WriteNull()
		case !withCount:
			c.w.WriteBulk(vals[0])
		case !exists:
			c.w.WriteNullArray()
		default:
			c.writeBulks(vals)
		}
	}
}

func cmdLLen(c *ctx) {
	n, err := c.store().LLen(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func (c *ctx) ints(args ...[]byte) ([]int64, bool) {
	out := make([]int64, len(args))
	for i, a := range args {
		n, err := parseInt(a)
		if err != nil {
			c.fail(err)
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

func cmdLRange(c *ctx) {
	n, ok := c.ints(c.args[2], c.args[3])
	if !ok {
		return
	}
	vals, err := c.store().LRange(c.args[1], n[0], n[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.writeBulks(vals)
}

func cmdLIndex(c *ctx) {
	n, ok := c.ints(c.args[2])
	if !ok {
		return
	}
	v, found, err := c.store().LIndex(c.args[1], n[0])
	switch {
	case err != nil:
		c.fail(err)
	case !found:
		c.w.WriteNull()
	default:
		c.w.WriteBulk(v)
	}
}

func cmdLSet(c *ctx) {
	n, ok := c.ints(c.args[2])
	if !ok {
		return
	}
	if err := c.store().LSet(c.args[1], n[0], c.args[3]); err != nil {
		c.fail(err)
		return
	}
	c.ok()
}

func cmdLTrim(c *ctx) {
	n, ok := c.ints(c.args[2], c.args[3])
	if !ok {
		return
	}
	if err := c.store().LTrim(c.args[1], n[0], n[1]); err != nil {
		c.fail(err)
		return
	}
	c.ok()
}

func cmdLRem(c *ctx) {
	n, ok := c.ints(c.args[2])
	if !ok {
		return
	}
	removed, err := c.store().LRem(c.args[1], n[0], c.args[3])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(removed)
}

func cmdLInsert(c *ctx) {
	var before bool
	switch strings.ToUpper(string(c.args[2])) {
	case "BEFORE":
		before = true
	case "AFTER":
	default:
		c.fail(errSyntax)
		return
	}
	n, err := c.store().LInsert(c.args[1], before, c.args[3], c.args[4])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func side(arg []byte) (left, ok bool) {
	switch strings.ToUpper(string(arg)) {
	case "LEFT":
		return true, true
	case "RIGHT":
		return false, true
	}
	return false, false
}

func (c *ctx) move(src, dst []byte, fromLeft, toLeft bool) {
	v, ok, err := c.store().LMove(src, dst, fromLeft, toLeft)
	switch {
	case err != nil:
		c.fail(err)
	case !ok:
		c.w.WriteNull()
	default:
		c.w.WriteBulk(v)
	}
}

func cmdLMove(c *ctx) {
	from, ok1 := side(c.args[3])
	to, ok2 := side(c.args[4])
	if !ok1 || !ok2 {
		c.fail(errSyntax)
		return
	}
	c.move(c.args[1], c.args[2], from, to)
}

func cmdRPopLPush(c *ctx) { c.move(c.args[1], c.args[2], false, true) }
