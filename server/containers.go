package server

import (
	"bytes"
	"sort"
)

// containerCommands are merged into commands by its initializer.
var containerCommands = map[string]command{
	"hset":       {cmdHSet(false, false), -4},
	"hmset":      {cmdHSet(false, true), -4},
	"hsetnx":     {cmdHSet(true, false), 4},
	"hget":       {cmdHGet, 3},
	"hmget":      {cmdHMGet, -3},
	"hgetall":    {cmdHGetAll(true, true), 2},
	"hkeys":      {cmdHGetAll(true, false), 2},
	"hvals":      {cmdHGetAll(false, true), 2},
	"hdel":       {cmdHDel, -3},
	"hlen":       {cmdHLen, 2},
	"hexists":    {cmdHExists, 3},
	"hstrlen":    {cmdHStrlen, 3},
	"hincrby":    {cmdHIncrBy, 4},
	"sadd":       {cmdSAdd, -3},
	"srem":       {cmdSRem, -3},
	"smembers":   {cmdSMembers, 2},
	"sismember":  {cmdSIsMember, 3},
	"smismember": {cmdSMIsMember, -3},
	"scard":      {cmdSCard, 2},
	"sinter":     {cmdSetOp(setInter), -2},
	"sunion":     {cmdSetOp(setUnion), -2},
	"sdiff":      {cmdSetOp(setDiff), -2},
}

func (c *ctx) writeBulks(items [][]byte) {
	c.w.WriteArray(len(items))
	for _, b := range items {
		c.w.WriteBulk(b)
	}
}

// ---- hashes ----

func cmdHSet(nx, legacyOK bool) func(*ctx) {
	return func(c *ctx) {
		pairs := c.args[2:]
		if len(pairs)%2 != 0 {
			c.w.WriteError("ERR wrong number of arguments for '" + string(bytes.ToLower(c.args[0])) + "' command")
			return
		}
		added, err := c.st().HSet(c.args[1], pairs, nx)
		switch {
		case err != nil:
			c.fail(err)
		case legacyOK: // HMSET replies OK
			c.ok()
		default:
			c.w.WriteInt(added)
		}
	}
}

func cmdHGet(c *ctx) {
	v, ok, err := c.st().HGet(c.args[1], c.args[2])
	switch {
	case err != nil:
		c.fail(err)
	case !ok:
		c.w.WriteNull()
	default:
		c.w.WriteBulk(v)
	}
}

func cmdHMGet(c *ctx) {
	vals := make([][]byte, 0, len(c.args)-2)
	found := make([]bool, 0, len(c.args)-2)
	for _, f := range c.args[2:] {
		v, ok, err := c.st().HGet(c.args[1], f)
		if err != nil {
			c.fail(err)
			return
		}
		vals, found = append(vals, v), append(found, ok)
	}
	c.w.WriteArray(len(vals))
	for i, v := range vals {
		if found[i] {
			c.w.WriteBulk(v)
		} else {
			c.w.WriteNull()
		}
	}
}

func cmdHGetAll(fields, values bool) func(*ctx) {
	return func(c *ctx) {
		var out [][]byte
		err := c.st().HGetAll(c.args[1], func(f, v []byte) {
			if fields {
				out = append(out, f)
			}
			if values {
				out = append(out, v)
			}
		})
		if err != nil {
			c.fail(err)
			return
		}
		c.writeBulks(out)
	}
}

func cmdHDel(c *ctx) {
	n, err := c.st().HDel(c.args[1], c.args[2:])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdHLen(c *ctx) {
	n, err := c.st().HLen(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdHExists(c *ctx) {
	_, ok, err := c.st().HGet(c.args[1], c.args[2])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(boolInt(ok))
}

func cmdHStrlen(c *ctx) {
	v, _, err := c.st().HGet(c.args[1], c.args[2])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(int64(len(v)))
}

func cmdHIncrBy(c *ctx) {
	delta, err := parseInt(c.args[3])
	if err != nil {
		c.fail(err)
		return
	}
	n, err := c.st().HIncrBy(c.args[1], c.args[2], delta)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

// ---- sets ----

func cmdSAdd(c *ctx) {
	n, err := c.st().SAdd(c.args[1], c.args[2:])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdSRem(c *ctx) {
	n, err := c.st().SRem(c.args[1], c.args[2:])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdSMembers(c *ctx) {
	members, err := c.st().SMembers(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.writeBulks(members)
}

func cmdSIsMember(c *ctx) {
	ok, err := c.st().SIsMember(c.args[1], c.args[2])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(boolInt(ok))
}

func cmdSMIsMember(c *ctx) {
	res := make([]int64, 0, len(c.args)-2)
	for _, m := range c.args[2:] {
		ok, err := c.st().SIsMember(c.args[1], m)
		if err != nil {
			c.fail(err)
			return
		}
		res = append(res, boolInt(ok))
	}
	c.w.WriteArray(len(res))
	for _, r := range res {
		c.w.WriteInt(r)
	}
}

func cmdSCard(c *ctx) {
	n, err := c.st().SCard(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

type setOp int

const (
	setInter setOp = iota
	setUnion
	setDiff
)

// cmdSetOp implements SINTER, SUNION and SDIFF by loading each set; results
// are returned in sorted order.
func cmdSetOp(op setOp) func(*ctx) {
	return func(c *ctx) {
		var result map[string]bool
		for i, key := range c.args[1:] {
			members, err := c.st().SMembers(key)
			if err != nil {
				c.fail(err)
				return
			}
			set := make(map[string]bool, len(members))
			for _, m := range members {
				set[string(m)] = true
			}
			switch {
			case i == 0:
				result = set
			case op == setInter:
				for m := range result {
					if !set[m] {
						delete(result, m)
					}
				}
			case op == setUnion:
				for m := range set {
					result[m] = true
				}
			case op == setDiff:
				for m := range set {
					delete(result, m)
				}
			}
		}
		out := make([]string, 0, len(result))
		for m := range result {
			out = append(out, m)
		}
		sort.Strings(out)
		c.w.WriteArray(len(out))
		for _, m := range out {
			c.w.WriteBulkString(m)
		}
	}
}
