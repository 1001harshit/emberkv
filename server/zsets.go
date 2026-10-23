package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/1001harshit/emberkv/keyspace"
)

var zsetCommands = map[string]command{
	"zadd":             {cmdZAdd, -4},
	"zincrby":          {cmdZIncrBy, 4},
	"zrem":             {cmdZRem, -3},
	"zscore":           {cmdZScore, 3},
	"zmscore":          {cmdZMScore, -3},
	"zcard":            {cmdZCard, 2},
	"zcount":           {cmdZCount, 4},
	"zrank":            {cmdZRank(false), 3},
	"zrevrank":         {cmdZRank(true), 3},
	"zrange":           {cmdZRange, -4},
	"zrevrange":        {cmdZRevRange, -4},
	"zrangebyscore":    {cmdZRangeByScore(false), -4},
	"zrevrangebyscore": {cmdZRangeByScore(true), -4},
	"zpopmin":          {cmdZPop(false), -2},
	"zpopmax":          {cmdZPop(true), -2},
	"zremrangebyscore": {cmdZRemRangeByScore, 4},
	"zremrangebyrank":  {cmdZRemRangeByRank, 4},
}

var (
	errNotFloat    = replyErr("ERR value is not a valid float")
	errBoundsFloat = replyErr("ERR min or max is not a float")
)

func parseScore(b []byte) (float64, error) {
	switch strings.ToLower(string(b)) {
	case "inf", "+inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(f) {
		return 0, errNotFloat
	}
	return f, nil
}

// formatScore matches Redis 7.0's replies for doubles: "%.17g", with "inf"
// and "-inf".
func formatScore(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', 17, 64)
}

func parseBound(b []byte) (keyspace.ScoreBound, error) {
	bound := keyspace.ScoreBound{}
	if len(b) > 0 && b[0] == '(' {
		bound.Exclusive, b = true, b[1:]
	}
	f, err := parseScore(b)
	if err != nil {
		return bound, errBoundsFloat
	}
	bound.Value = f
	return bound, nil
}

func (c *ctx) writeScored(ms []keyspace.ScoredMember, withScores bool) {
	n := len(ms)
	if withScores {
		n *= 2
	}
	c.w.WriteArray(n)
	for _, m := range ms {
		c.w.WriteBulk(m.Member)
		if withScores {
			c.w.WriteBulkString(formatScore(m.Score))
		}
	}
}

func cmdZAdd(c *ctx) {
	var o keyspace.ZAddOptions
	i := 2
flags:
	for ; i < len(c.args); i++ {
		switch strings.ToUpper(string(c.args[i])) {
		case "NX":
			o.NX = true
		case "XX":
			o.XX = true
		case "GT":
			o.GT = true
		case "LT":
			o.LT = true
		case "CH":
			o.CH = true
		case "INCR":
			o.Incr = true
		default:
			break flags
		}
	}
	pairs := c.args[i:]
	switch {
	case o.NX && o.XX:
		c.w.WriteError("ERR XX and NX options at the same time are not compatible")
		return
	case (o.GT && o.LT) || (o.NX && (o.GT || o.LT)):
		c.w.WriteError("ERR GT, LT, and/or NX options at the same time are not compatible")
		return
	case len(pairs) == 0 || len(pairs)%2 != 0:
		c.fail(errSyntax)
		return
	case o.Incr && len(pairs) != 2:
		c.w.WriteError("ERR INCR option supports a single increment-element pair")
		return
	}
	members := make([]keyspace.ScoredMember, 0, len(pairs)/2)
	for j := 0; j < len(pairs); j += 2 {
		f, err := parseScore(pairs[j])
		if err != nil {
			c.fail(err)
			return
		}
		members = append(members, keyspace.ScoredMember{Score: f, Member: pairs[j+1]})
	}
	n, incr, err := c.store().ZAdd(c.args[1], members, o)
	switch {
	case err != nil:
		c.fail(err)
	case o.Incr && incr == nil:
		c.w.WriteNull()
	case o.Incr:
		c.w.WriteBulkString(formatScore(*incr))
	default:
		c.w.WriteInt(n)
	}
}

func cmdZIncrBy(c *ctx) {
	f, err := parseScore(c.args[2])
	if err != nil {
		c.fail(err)
		return
	}
	_, res, err := c.store().ZAdd(c.args[1], []keyspace.ScoredMember{{Member: c.args[3], Score: f}}, keyspace.ZAddOptions{Incr: true})
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteBulkString(formatScore(*res))
}

func cmdZRem(c *ctx) {
	n, err := c.store().ZRem(c.args[1], c.args[2:])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdZScore(c *ctx) {
	f, ok, err := c.store().ZScore(c.args[1], c.args[2])
	switch {
	case err != nil:
		c.fail(err)
	case !ok:
		c.w.WriteNull()
	default:
		c.w.WriteBulkString(formatScore(f))
	}
}

func cmdZMScore(c *ctx) {
	scores := make([]*float64, 0, len(c.args)-2)
	for _, m := range c.args[2:] {
		f, ok, err := c.store().ZScore(c.args[1], m)
		if err != nil {
			c.fail(err)
			return
		}
		if ok {
			scores = append(scores, &f)
		} else {
			scores = append(scores, nil)
		}
	}
	c.w.WriteArray(len(scores))
	for _, f := range scores {
		if f == nil {
			c.w.WriteNull()
		} else {
			c.w.WriteBulkString(formatScore(*f))
		}
	}
}

func cmdZCard(c *ctx) {
	n, err := c.store().ZCard(c.args[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdZCount(c *ctx) {
	lo, err1 := parseBound(c.args[2])
	hi, err2 := parseBound(c.args[3])
	if err1 != nil || err2 != nil {
		c.fail(errBoundsFloat)
		return
	}
	n, err := c.store().ZCount(c.args[1], lo, hi)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdZRank(rev bool) func(*ctx) {
	return func(c *ctx) {
		r, ok, err := c.store().ZRank(c.args[1], c.args[2], rev)
		switch {
		case err != nil:
			c.fail(err)
		case !ok:
			c.w.WriteNull()
		default:
			c.w.WriteInt(r)
		}
	}
}

// rangeOpts parses ZRANGE's trailing options: BYSCORE, BYLEX, REV,
// LIMIT offset count and WITHSCORES.
type rangeOpts struct {
	byScore, rev, withScores, limit bool
	offset, count                   int64
}

func (c *ctx) parseRangeOpts(args [][]byte, allowed string) (rangeOpts, bool) {
	o := rangeOpts{count: -1}
	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		if !strings.Contains(" "+allowed+" ", " "+opt+" ") {
			c.fail(errSyntax)
			return o, false
		}
		switch opt {
		case "BYSCORE":
			o.byScore = true
		case "BYLEX":
			c.w.WriteError("ERR BYLEX is not supported by emberkv yet")
			return o, false
		case "REV":
			o.rev = true
		case "WITHSCORES":
			o.withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				c.fail(errSyntax)
				return o, false
			}
			n, ok := c.ints(args[i+1], args[i+2])
			if !ok {
				return o, false
			}
			o.limit, o.offset, o.count = true, n[0], n[1]
			i += 2
		}
	}
	return o, true
}

func (c *ctx) zrange(key, start, stop []byte, o rangeOpts) {
	if o.limit && !o.byScore {
		c.w.WriteError("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
		return
	}
	var ms []keyspace.ScoredMember
	var err error
	if o.byScore {
		if o.rev { // ZRANGE ... BYSCORE REV takes max first
			start, stop = stop, start
		}
		lo, err1 := parseBound(start)
		hi, err2 := parseBound(stop)
		if err1 != nil || err2 != nil {
			c.fail(errBoundsFloat)
			return
		}
		if o.offset < 0 {
			c.w.WriteArray(0)
			return
		}
		ms, err = c.store().ZRangeByScore(key, lo, hi, o.rev, o.offset, o.count)
	} else {
		n, ok := c.ints(start, stop)
		if !ok {
			return
		}
		ms, err = c.store().ZRange(key, n[0], n[1], o.rev)
	}
	if err != nil {
		c.fail(err)
		return
	}
	c.writeScored(ms, o.withScores)
}

func cmdZRange(c *ctx) {
	if o, ok := c.parseRangeOpts(c.args[4:], "BYSCORE BYLEX REV LIMIT WITHSCORES"); ok {
		c.zrange(c.args[1], c.args[2], c.args[3], o)
	}
}

func cmdZRevRange(c *ctx) {
	if o, ok := c.parseRangeOpts(c.args[4:], "WITHSCORES"); ok {
		o.rev = true
		c.zrange(c.args[1], c.args[2], c.args[3], o)
	}
}

// cmdZRangeByScore is ZRANGEBYSCORE key min max and ZREVRANGEBYSCORE key
// max min, with WITHSCORES and LIMIT.
func cmdZRangeByScore(rev bool) func(*ctx) {
	return func(c *ctx) {
		o, ok := c.parseRangeOpts(c.args[4:], "WITHSCORES LIMIT")
		if !ok {
			return
		}
		o.byScore, o.rev = true, rev
		c.zrange(c.args[1], c.args[2], c.args[3], o)
	}
}

func cmdZPop(max bool) func(*ctx) {
	return func(c *ctx) {
		count := int64(1)
		switch {
		case len(c.args) > 3:
			c.fail(errSyntax)
			return
		case len(c.args) == 3:
			n, err := parseInt(c.args[2])
			if err != nil || n < 0 {
				c.fail(errNotPositive)
				return
			}
			count = n
		}
		ms, err := c.store().ZPop(c.args[1], max, count)
		if err != nil {
			c.fail(err)
			return
		}
		c.writeScored(ms, true)
	}
}

func cmdZRemRangeByScore(c *ctx) {
	lo, err1 := parseBound(c.args[2])
	hi, err2 := parseBound(c.args[3])
	if err1 != nil || err2 != nil {
		c.fail(errBoundsFloat)
		return
	}
	n, err := c.store().ZRemRangeByScore(c.args[1], lo, hi)
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(n)
}

func cmdZRemRangeByRank(c *ctx) {
	n, ok := c.ints(c.args[2], c.args[3])
	if !ok {
		return
	}
	removed, err := c.store().ZRemRangeByRank(c.args[1], n[0], n[1])
	if err != nil {
		c.fail(err)
		return
	}
	c.w.WriteInt(removed)
}
